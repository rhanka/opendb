use crate::ast::{
    JoinClause, JoinKind, JoinedOrderBy, JoinedPredicate, OrderBy, OrderDirection, Predicate,
    SelectColumns, SelectExpr, SelectExprItem, SelectFunction, Statement,
};
use opendb_common::{OpenDbError, OpenDbResult};
use opendb_storage::commit_stream::{
    AlterTableOp, ColumnDefinition, ColumnType, ConstraintKind, DefaultExpr, IndexDescriptor,
    NamedConstraint, ReferentialAction, Value,
};

pub fn parse(sql: &str) -> OpenDbResult<Statement> {
    let trimmed = sql.trim();
    let normalized = if let Some(without_terminator) = trimmed.strip_suffix(';') {
        if without_terminator.trim_end().ends_with(';') {
            return Err(OpenDbError::Sql(
                "repeated semicolon terminators".to_owned(),
            ));
        }
        without_terminator.trim()
    } else {
        trimmed
    };
    let upper = normalized.to_ascii_uppercase();
    if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION" {
        return Ok(Statement::Begin);
    }
    if upper == "COMMIT" || upper == "COMMIT TRANSACTION" || upper == "END" {
        return Ok(Statement::Commit);
    }
    if upper == "ROLLBACK" || upper == "ROLLBACK TRANSACTION" || upper == "ABORT" {
        return Ok(Statement::Rollback);
    }
    if upper.starts_with("CREATE TABLE ") {
        parse_create_table(normalized)
    } else if upper.starts_with("CREATE UNIQUE INDEX ") || upper.starts_with("CREATE INDEX ") {
        parse_create_index(normalized)
    } else if upper.starts_with("INSERT INTO ") {
        parse_insert(normalized)
    } else if upper.starts_with("SELECT * FROM ") {
        parse_select_all(normalized)
    } else if upper.starts_with("SELECT ") {
        parse_select_with_projection(normalized)
    } else if upper.starts_with("ALTER TABLE ") {
        parse_alter_table(normalized)
    } else if upper.starts_with("DELETE FROM ") {
        parse_delete(normalized)
    } else if upper.starts_with("UPDATE ") {
        parse_update(normalized)
    } else if upper.starts_with("DO ") || upper.starts_with("DO$") {
        parse_do_block(normalized)
    } else {
        Err(OpenDbError::Sql(format!("unsupported SQL: {normalized}")))
    }
}

/// Sprint 13: `UPDATE <table> SET <col1> = <lit1> [, ...] WHERE <pk> = <literal>`.
fn parse_update(sql: &str) -> OpenDbResult<Statement> {
    let rest = strip_keyword_prefix(sql, "UPDATE ")
        .ok_or_else(|| OpenDbError::Sql("invalid UPDATE".to_owned()))?
        .trim();
    let upper_rest = rest.to_ascii_uppercase();
    let set_pos = upper_rest
        .find(" SET ")
        .ok_or_else(|| OpenDbError::Sql("UPDATE requires SET".to_owned()))?;
    let table = rest[..set_pos].trim();
    if table.is_empty() {
        return Err(OpenDbError::Sql("UPDATE requires table".to_owned()));
    }
    let after_set = &rest[set_pos + " SET ".len()..];
    let upper_after_set = after_set.to_ascii_uppercase();
    let where_pos = upper_after_set
        .find(" WHERE ")
        .ok_or_else(|| OpenDbError::Sql("UPDATE requires WHERE".to_owned()))?;
    let assignments_text = after_set[..where_pos].trim();
    let predicate_text = after_set[where_pos + " WHERE ".len()..].trim();

    let assignments = split_top_level_commas(assignments_text)?
        .into_iter()
        .map(parse_assignment)
        .collect::<OpenDbResult<Vec<(String, Value)>>>()?;
    if assignments.is_empty() {
        return Err(OpenDbError::Sql(
            "UPDATE requires at least one SET assignment".to_owned(),
        ));
    }
    let predicate = parse_predicate(predicate_text)?;
    let key = match predicate.value {
        Value::Int64(v) => v.to_string(),
        Value::Text(v) => v,
        Value::Bool(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Timestamp(v) => v.to_string(),
        Value::Json(v) => v.to_string(),
        Value::Null => {
            return Err(OpenDbError::Sql(
                "UPDATE WHERE primary key cannot be NULL".to_owned(),
            ));
        }
    };
    Ok(Statement::UpdateRow {
        table: unquote_identifier(table),
        key,
        assignments,
    })
}

fn parse_assignment(raw: &str) -> OpenDbResult<(String, Value)> {
    let equals_positions = equality_positions_outside_quotes(raw)?;
    let Some(equals_pos) = equals_positions.first().copied() else {
        return Err(OpenDbError::Sql(format!("invalid SET assignment: {raw}")));
    };
    if equals_positions.len() != 1 {
        return Err(OpenDbError::Sql(format!(
            "SET assignment has more than one `=`: {raw}"
        )));
    }
    let column = raw[..equals_pos].trim();
    let value_text = raw[equals_pos + 1..].trim();
    if column.is_empty() || value_text.is_empty() {
        return Err(OpenDbError::Sql(format!(
            "SET assignment requires column and literal: {raw}"
        )));
    }
    Ok((unqualified_column_name(column), parse_value(value_text)?))
}

fn parse_delete(sql: &str) -> OpenDbResult<Statement> {
    let rest = strip_keyword_prefix(sql, "DELETE FROM ")
        .ok_or_else(|| OpenDbError::Sql("invalid DELETE".to_owned()))?
        .trim();
    let upper_rest = rest.to_ascii_uppercase();
    let where_pos = upper_rest
        .find(" WHERE ")
        .ok_or_else(|| OpenDbError::Sql("DELETE requires WHERE primary-key equality".to_owned()))?;
    let table = rest[..where_pos].trim().to_owned();
    let predicate_text = rest[where_pos + " WHERE ".len()..].trim();
    let predicate = parse_predicate(predicate_text)?;
    let key = match predicate.value {
        Value::Int64(v) => v.to_string(),
        Value::Text(v) => v,
        Value::Bool(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Timestamp(v) => v.to_string(),
        Value::Json(v) => v.to_string(),
        Value::Null => {
            return Err(OpenDbError::Sql(
                "DELETE WHERE primary key cannot be NULL".to_owned(),
            ));
        }
    };
    Ok(Statement::DeleteRow {
        table: unquote_identifier(&table),
        key,
    })
}

fn parse_alter_table(sql: &str) -> OpenDbResult<Statement> {
    let rest = strip_keyword_prefix(sql, "ALTER TABLE ")
        .ok_or_else(|| OpenDbError::Sql("invalid ALTER TABLE".to_owned()))?
        .trim();
    let (table_name, remainder) = split_first_word(rest)?;
    let upper_remainder = remainder.to_ascii_uppercase();
    if let Some(after) = strip_keyword(remainder, &upper_remainder, "ADD COLUMN ") {
        let column = parse_column_definition(after.trim())?;
        return Ok(Statement::AlterTable {
            table: unquote_identifier(table_name),
            op: AlterTableOp::AddColumn(column),
        });
    }
    if let Some(after) = strip_keyword(remainder, &upper_remainder, "DROP COLUMN ") {
        let column = strip_optional_terminators(after.trim());
        return Ok(Statement::AlterTable {
            table: unquote_identifier(table_name),
            op: AlterTableOp::DropColumn {
                column: unquote_identifier(column),
            },
        });
    }
    if let Some(after) = strip_keyword(remainder, &upper_remainder, "RENAME COLUMN ") {
        let parts: Vec<&str> = after.split_whitespace().collect();
        if parts.len() != 3 || !parts[1].eq_ignore_ascii_case("TO") {
            return Err(OpenDbError::Sql(format!(
                "invalid RENAME COLUMN clause: {after}"
            )));
        }
        return Ok(Statement::AlterTable {
            table: unquote_identifier(table_name),
            op: AlterTableOp::RenameColumn {
                from: unquote_identifier(parts[0]),
                to: unquote_identifier(parts[2]),
            },
        });
    }
    if let Some(after) = strip_keyword(remainder, &upper_remainder, "ADD CONSTRAINT ") {
        let constraint = parse_add_constraint(after.trim())?;
        return Ok(Statement::AlterTable {
            table: unquote_identifier(table_name),
            op: AlterTableOp::AddConstraint(constraint),
        });
    }
    Err(OpenDbError::Sql(format!(
        "unsupported ALTER TABLE clause: {remainder}"
    )))
}

fn split_first_word(input: &str) -> OpenDbResult<(&str, &str)> {
    let trimmed = input.trim_start();
    let mut end = 0;
    let mut in_quote = false;
    for (index, ch) in trimmed.char_indices() {
        match ch {
            '"' => in_quote = !in_quote,
            c if c.is_whitespace() && !in_quote => {
                end = index;
                break;
            }
            _ => {}
        }
    }
    if end == 0 {
        return Err(OpenDbError::Sql(format!("expected identifier in {input}")));
    }
    Ok((trimmed[..end].trim(), trimmed[end..].trim_start()))
}

fn strip_keyword<'a>(original: &'a str, upper: &str, keyword: &str) -> Option<&'a str> {
    if let Some(stripped_upper) = upper.strip_prefix(keyword) {
        let consumed = upper.len() - stripped_upper.len();
        Some(&original[consumed..])
    } else {
        None
    }
}

fn parse_add_constraint(input: &str) -> OpenDbResult<NamedConstraint> {
    let (name, rest) = split_first_word(input)?;
    let upper_rest = rest.to_ascii_uppercase();
    if let Some(after) = strip_keyword(rest, &upper_rest, "FOREIGN KEY") {
        let after = after.trim_start();
        let (columns_text, after_cols) = extract_parenthesized(after)
            .ok_or_else(|| OpenDbError::Sql("FK columns".to_owned()))?;
        let after_cols_trimmed = after_cols.trim_start();
        let upper_after_cols = after_cols_trimmed.to_ascii_uppercase();
        let after_refs = strip_keyword(after_cols_trimmed, &upper_after_cols, "REFERENCES")
            .ok_or_else(|| OpenDbError::Sql("missing REFERENCES".to_owned()))?
            .trim_start();
        let (ref_table, after_ref_table) = split_first_word(after_refs)?;
        let (ref_cols_text, tail) = extract_parenthesized(after_ref_table)
            .ok_or_else(|| OpenDbError::Sql("REFERENCES columns".to_owned()))?;
        let (on_delete, on_update) = parse_referential_actions(tail)?;
        Ok(NamedConstraint {
            name: unquote_identifier(name),
            kind: ConstraintKind::ForeignKey {
                columns: parse_identifier_list(columns_text),
                references_table: unquote_identifier(ref_table),
                references_columns: parse_identifier_list(ref_cols_text),
                on_delete,
                on_update,
            },
        })
    } else if let Some(after) = strip_keyword(rest, &upper_rest, "UNIQUE") {
        let after = after.trim_start();
        let (columns_text, _tail) = extract_parenthesized(after)
            .ok_or_else(|| OpenDbError::Sql("UNIQUE columns".to_owned()))?;
        Ok(NamedConstraint {
            name: unquote_identifier(name),
            kind: ConstraintKind::Unique {
                columns: parse_identifier_list(columns_text),
            },
        })
    } else {
        Err(OpenDbError::Sql(format!(
            "unsupported constraint kind in {rest}"
        )))
    }
}

fn extract_parenthesized(input: &str) -> Option<(&str, &str)> {
    let open = input.find('(')?;
    let mut depth = 0;
    for (index, ch) in input[open..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    let close = open + index;
                    return Some((&input[open + 1..close], &input[close + 1..]));
                }
            }
            _ => {}
        }
    }
    None
}

fn parse_identifier_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|part| unquote_identifier(part.trim()))
        .filter(|s| !s.is_empty())
        .collect()
}

fn unquote_identifier(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches(';').trim();
    // Drizzle (and any pgwire client respecting SQL standards) wraps
    // identifiers in double quotes — strip them.
    if let Some(stripped) = trimmed.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        stripped.to_owned()
    } else {
        trimmed.to_owned()
    }
}

/// Strips schema/table qualifiers and quotes from an identifier. Sprint
/// 12 handles only the single-table read path, so a qualified reference
/// like `"folders_smoke"."id"` collapses to `id`.
fn unqualified_column_name(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches(';').trim();
    let last = trimmed.rsplit('.').next().unwrap_or(trimmed);
    unquote_identifier(last)
}

fn parse_referential_actions(tail: &str) -> OpenDbResult<(ReferentialAction, ReferentialAction)> {
    let upper = tail.to_ascii_uppercase();
    let mut on_delete = ReferentialAction::NoAction;
    let mut on_update = ReferentialAction::NoAction;
    let mut cursor = upper.as_str();
    let mut original = tail;
    loop {
        let trimmed_upper = cursor.trim_start();
        let advanced = cursor.len() - trimmed_upper.len();
        original = &original[advanced..];
        cursor = trimmed_upper;
        if let Some(rest_upper) = cursor.strip_prefix("ON DELETE ") {
            let (action, after) = take_referential_action(rest_upper)?;
            on_delete = action;
            let consumed = cursor.len() - after.len();
            cursor = after;
            original = &original[consumed..];
        } else if let Some(rest_upper) = cursor.strip_prefix("ON UPDATE ") {
            let (action, after) = take_referential_action(rest_upper)?;
            on_update = action;
            let consumed = cursor.len() - after.len();
            cursor = after;
            original = &original[consumed..];
        } else {
            break;
        }
    }
    Ok((on_delete, on_update))
}

fn take_referential_action(input: &str) -> OpenDbResult<(ReferentialAction, &str)> {
    let trimmed = input.trim_start();
    let upper = trimmed;
    if let Some(rest) = upper.strip_prefix("NO ACTION") {
        Ok((ReferentialAction::NoAction, rest))
    } else if let Some(rest) = upper.strip_prefix("CASCADE") {
        Ok((ReferentialAction::Cascade, rest))
    } else if let Some(rest) = upper.strip_prefix("SET NULL") {
        Ok((ReferentialAction::SetNull, rest))
    } else if let Some(rest) = upper.strip_prefix("SET DEFAULT") {
        Ok((ReferentialAction::SetDefault, rest))
    } else if let Some(rest) = upper.strip_prefix("RESTRICT") {
        Ok((ReferentialAction::Restrict, rest))
    } else {
        Err(OpenDbError::Sql(format!(
            "unsupported referential action: {input}"
        )))
    }
}

fn parse_create_index(sql: &str) -> OpenDbResult<Statement> {
    let upper = sql.to_ascii_uppercase();
    let (unique, rest_after_unique) =
        if let Some(rest) = strip_keyword(sql, &upper, "CREATE UNIQUE INDEX ") {
            (true, rest)
        } else {
            let rest = strip_keyword(sql, &upper, "CREATE INDEX ")
                .ok_or_else(|| OpenDbError::Sql("invalid CREATE INDEX".to_owned()))?;
            (false, rest)
        };
    let rest_upper = rest_after_unique.to_ascii_uppercase();
    let (if_not_exists, remainder) =
        if let Some(rest) = strip_keyword(rest_after_unique, &rest_upper, "IF NOT EXISTS ") {
            (true, rest)
        } else {
            (false, rest_after_unique)
        };
    let (name, after_name) = split_first_word(remainder)?;
    let upper_after_name = after_name.to_ascii_uppercase();
    let after_on = strip_keyword(after_name, &upper_after_name, "ON ")
        .ok_or_else(|| OpenDbError::Sql("CREATE INDEX requires ON".to_owned()))?;
    let (table, after_table) = split_first_word(after_on)?;
    let after_table_upper = after_table.to_ascii_uppercase();
    let columns_input =
        if let Some(after_using) = strip_keyword(after_table, &after_table_upper, "USING ") {
            // skip the index method name (btree, hash, ...) and resume at its tail
            let (_method, after_method) = split_first_word(after_using)?;
            after_method
        } else {
            after_table
        };
    let (columns_text, _) = extract_parenthesized(columns_input)
        .ok_or_else(|| OpenDbError::Sql("CREATE INDEX columns".to_owned()))?;
    let columns = parse_identifier_list(columns_text);
    Ok(Statement::CreateIndex {
        table: unquote_identifier(table),
        index: IndexDescriptor {
            name: unquote_identifier(name),
            columns,
            unique,
            if_not_exists,
        },
    })
}

fn parse_do_block(sql: &str) -> OpenDbResult<Statement> {
    let trimmed = sql.trim();
    let after_do = trimmed[2..].trim_start();
    let body = after_do
        .strip_prefix("$$")
        .ok_or_else(|| OpenDbError::Sql("DO block must use $$ delimiter".to_owned()))?;
    let inner_text = body
        .strip_suffix(";")
        .unwrap_or(body)
        .trim()
        .strip_suffix("$$")
        .ok_or_else(|| OpenDbError::Sql("DO block must close with $$".to_owned()))?;
    let inner_text = inner_text.trim();
    let upper_inner = inner_text.to_ascii_uppercase();
    let body_after_begin = strip_keyword(inner_text, &upper_inner, "BEGIN")
        .unwrap_or(inner_text)
        .trim();
    let end_index = body_after_begin
        .to_ascii_uppercase()
        .rfind("END")
        .ok_or_else(|| OpenDbError::Sql("DO block missing END".to_owned()))?;
    let body_text = &body_after_begin[..end_index];
    let mut swallow_duplicate = false;
    let inner_statements_text =
        if let Some(exception_pos) = body_text.to_ascii_uppercase().find("EXCEPTION") {
            let main_body = &body_text[..exception_pos];
            let exception_body = &body_text[exception_pos..];
            if exception_body
                .to_ascii_uppercase()
                .contains("DUPLICATE_OBJECT")
            {
                swallow_duplicate = true;
            }
            main_body
        } else {
            body_text
        };
    let inner = split_statements(inner_statements_text)
        .into_iter()
        .map(|stmt| parse(&stmt))
        .collect::<OpenDbResult<Vec<_>>>()?;
    Ok(Statement::DoBlock {
        inner,
        swallow_duplicate,
    })
}

fn split_statements(raw: &str) -> Vec<String> {
    raw.split(';')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect()
}

fn strip_optional_terminators(input: &str) -> &str {
    input.trim_end_matches(';').trim()
}

fn parse_create_table(sql: &str) -> OpenDbResult<Statement> {
    let rest = strip_keyword_prefix(sql, "CREATE TABLE ")
        .ok_or_else(|| OpenDbError::Sql("invalid CREATE TABLE".to_owned()))?;
    let open = rest
        .find('(')
        .ok_or_else(|| OpenDbError::Sql("missing column list".to_owned()))?;
    let close = rest
        .rfind(')')
        .ok_or_else(|| OpenDbError::Sql("missing closing paren".to_owned()))?;
    if open >= close {
        return Err(OpenDbError::Sql("malformed column list".to_owned()));
    }
    if !rest[close + 1..].trim().is_empty() {
        return Err(OpenDbError::Sql(
            "trailing input after CREATE TABLE".to_owned(),
        ));
    }
    let table = rest[..open].trim().to_owned();
    let columns = rest[open + 1..close]
        .split(',')
        .map(parse_column_definition)
        .collect::<OpenDbResult<Vec<_>>>()?;
    if table.is_empty() || columns.is_empty() {
        return Err(OpenDbError::Sql(
            "CREATE TABLE requires table and columns".to_owned(),
        ));
    }
    Ok(Statement::CreateTable { table, columns })
}

fn parse_column_definition(raw: &str) -> OpenDbResult<ColumnDefinition> {
    let trimmed = raw.trim();
    let tokens = split_definition_tokens(trimmed)?;
    if tokens.len() < 2 {
        return Err(OpenDbError::Sql(format!(
            "invalid column definition: {trimmed}"
        )));
    }
    let name = &tokens[0];
    if name.is_empty() {
        return Err(OpenDbError::Sql("column name is required".to_owned()));
    }

    let (data_type, type_token_count) = parse_column_type_tokens(&tokens[1..])?;
    let mut index = 1 + type_token_count;
    let mut primary_key = false;
    let mut not_null = false;
    let mut default: Option<DefaultExpr> = None;

    while index < tokens.len() {
        let token = tokens[index].to_ascii_uppercase();
        match token.as_str() {
            "PRIMARY" => {
                if index + 1 >= tokens.len() || !tokens[index + 1].eq_ignore_ascii_case("KEY") {
                    return Err(OpenDbError::Sql(format!(
                        "unsupported column constraint on {name}"
                    )));
                }
                primary_key = true;
                index += 2;
            }
            "NOT" => {
                if index + 1 >= tokens.len() || !tokens[index + 1].eq_ignore_ascii_case("NULL") {
                    return Err(OpenDbError::Sql(format!(
                        "unsupported column constraint on {name}"
                    )));
                }
                not_null = true;
                index += 2;
            }
            "DEFAULT" => {
                if index + 1 >= tokens.len() {
                    return Err(OpenDbError::Sql(format!(
                        "DEFAULT requires an expression on {name}"
                    )));
                }
                let candidate = &tokens[index + 1];
                if candidate.eq_ignore_ascii_case("NOW()") {
                    default = Some(DefaultExpr::Now);
                    index += 2;
                } else {
                    default = Some(DefaultExpr::Const(parse_value(candidate)?));
                    index += 2;
                }
            }
            _ => {
                return Err(OpenDbError::Sql(format!(
                    "unsupported column constraint on {name}"
                )));
            }
        }
    }

    let definition = if primary_key {
        let mut pk = ColumnDefinition::primary_key(name, data_type);
        pk.default = default;
        pk
    } else {
        ColumnDefinition {
            name: name.clone(),
            data_type,
            primary_key: false,
            nullable: !not_null,
            default,
        }
    };
    Ok(definition)
}

fn split_definition_tokens(raw: &str) -> OpenDbResult<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    for ch in raw.chars() {
        match ch {
            '\'' => {
                current.push(ch);
                in_quote = !in_quote;
            }
            c if c.is_whitespace() && !in_quote => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }
    if in_quote {
        return Err(OpenDbError::Sql("unterminated quoted literal".to_owned()));
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    Ok(tokens)
}

fn parse_column_type_tokens(tokens: &[String]) -> OpenDbResult<(ColumnType, usize)> {
    if tokens.is_empty() {
        return Err(OpenDbError::Sql("column type is required".to_owned()));
    }
    let first = tokens[0].to_ascii_uppercase();
    match first.as_str() {
        "INT" | "INTEGER" | "INT64" | "BIGINT" => Ok((ColumnType::Int64, 1)),
        "TEXT" => Ok((ColumnType::Text, 1)),
        "BOOL" | "BOOLEAN" => Ok((ColumnType::Bool, 1)),
        "FLOAT8" | "FLOAT64" => Ok((ColumnType::Float64, 1)),
        "DOUBLE" => {
            if tokens.len() >= 2 && tokens[1].eq_ignore_ascii_case("PRECISION") {
                Ok((ColumnType::Float64, 2))
            } else {
                Err(OpenDbError::Sql(format!(
                    "unsupported column type: {}",
                    tokens[0]
                )))
            }
        }
        "TIMESTAMP" => Ok((ColumnType::Timestamp, 1)),
        "JSON" | "JSONB" => Ok((ColumnType::Json, 1)),
        _ => Err(OpenDbError::Sql(format!(
            "unsupported column type: {}",
            tokens[0]
        ))),
    }
}

fn parse_insert(sql: &str) -> OpenDbResult<Statement> {
    let values_marker = " VALUES ";
    let upper = sql.to_ascii_uppercase();
    let values_pos = upper
        .find(values_marker)
        .ok_or_else(|| OpenDbError::Sql("INSERT requires VALUES".to_owned()))?;
    let header = strip_keyword_prefix(&sql[..values_pos], "INSERT INTO ")
        .ok_or_else(|| OpenDbError::Sql("invalid INSERT".to_owned()))?
        .trim();
    if header.is_empty() {
        return Err(OpenDbError::Sql("INSERT requires table".to_owned()));
    }
    let (table, columns) = if let Some(open) = header.find('(') {
        let close = header
            .rfind(')')
            .ok_or_else(|| OpenDbError::Sql("missing INSERT column list close paren".to_owned()))?;
        if open >= close {
            return Err(OpenDbError::Sql("malformed INSERT column list".to_owned()));
        }
        if !header[close + 1..].trim().is_empty() {
            return Err(OpenDbError::Sql(
                "trailing input between INSERT column list and VALUES".to_owned(),
            ));
        }
        let table = unquote_identifier(header[..open].trim());
        let columns = split_values(&header[open + 1..close])?
            .into_iter()
            .map(|c| {
                let trimmed = c.trim();
                if trimmed.is_empty() || trimmed.split_whitespace().count() != 1 {
                    Err(OpenDbError::Sql(format!(
                        "invalid INSERT column name: {trimmed}"
                    )))
                } else {
                    Ok(unqualified_column_name(trimmed))
                }
            })
            .collect::<OpenDbResult<Vec<String>>>()?;
        if columns.is_empty() {
            return Err(OpenDbError::Sql(
                "INSERT column list must not be empty".to_owned(),
            ));
        }
        (table, Some(columns))
    } else {
        (unquote_identifier(header), None)
    };
    if table.is_empty() {
        return Err(OpenDbError::Sql("INSERT requires table".to_owned()));
    }
    let values_part = sql[values_pos + values_marker.len()..].trim();
    let open = values_part
        .find('(')
        .ok_or_else(|| OpenDbError::Sql("missing values open paren".to_owned()))?;
    let close = values_part
        .rfind(')')
        .ok_or_else(|| OpenDbError::Sql("missing values close paren".to_owned()))?;
    if open >= close {
        return Err(OpenDbError::Sql("malformed values list".to_owned()));
    }
    if !values_part[..open].trim().is_empty() || !values_part[close + 1..].trim().is_empty() {
        return Err(OpenDbError::Sql("trailing input after INSERT".to_owned()));
    }
    let values = split_values(&values_part[open + 1..close])?
        .into_iter()
        .map(parse_value)
        .collect::<OpenDbResult<Vec<_>>>()?;
    Ok(Statement::Insert {
        table,
        columns,
        values,
    })
}

fn split_values(raw: &str) -> OpenDbResult<Vec<&str>> {
    let mut values = Vec::new();
    let mut start = 0;
    let mut in_quote = false;

    for (index, ch) in raw.char_indices() {
        match ch {
            '\'' => in_quote = !in_quote,
            ',' if !in_quote => {
                values.push(raw[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }

    if in_quote {
        return Err(OpenDbError::Sql("unterminated quoted literal".to_owned()));
    }

    values.push(raw[start..].trim());
    Ok(values)
}

fn parse_value(value: &str) -> OpenDbResult<Value> {
    let trimmed = strip_cast_suffix(value.trim());
    if trimmed.eq_ignore_ascii_case("NULL") {
        return Ok(Value::Null);
    }
    if trimmed.eq_ignore_ascii_case("TRUE") {
        return Ok(Value::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("FALSE") {
        return Ok(Value::Bool(false));
    }
    if let Some(text) = trimmed
        .strip_prefix('\'')
        .and_then(|v| v.strip_suffix('\''))
    {
        return Ok(Value::Text(text.to_owned()));
    }
    if let Ok(int_value) = trimmed.parse::<i64>() {
        return Ok(Value::Int64(int_value));
    }
    if let Ok(float_value) = trimmed.parse::<f64>() {
        return Ok(Value::Float64(float_value));
    }
    Err(OpenDbError::Sql(format!("unsupported literal: {trimmed}")))
}

/// Strips a trailing PostgreSQL cast suffix (`::jsonb`, `::json`) from a
/// literal. Other casts pass through unchanged so unsupported forms surface
/// as parse errors and demand explicit support.
fn strip_cast_suffix(value: &str) -> &str {
    for suffix in [
        "::jsonb", "::JSONB", "::Jsonb", "::json", "::JSON", "::Json",
    ] {
        if let Some(stripped) = value.strip_suffix(suffix) {
            return stripped.trim_end();
        }
    }
    value
}

fn parse_select_all(sql: &str) -> OpenDbResult<Statement> {
    let rest = strip_keyword_prefix(sql, "SELECT * FROM ")
        .ok_or_else(|| OpenDbError::Sql("invalid SELECT".to_owned()))?
        .trim();
    // Sprint 10.5: detect JOIN clauses and route to a separate parser.
    let upper_rest_for_join = rest.to_ascii_uppercase();
    if upper_rest_for_join.contains(" INNER JOIN ")
        || upper_rest_for_join.contains(" LEFT JOIN ")
        || upper_rest_for_join.contains(" JOIN ")
    {
        return parse_select_with_join(rest);
    }

    // Sprint 10: split off trailing OFFSET / LIMIT / ORDER BY clauses
    // before the predicate parsing so the existing logic is reused.
    let (rest, offset) = take_trailing_keyword_value(rest, " OFFSET ")?;
    let (rest, limit) = take_trailing_keyword_value(&rest, " LIMIT ")?;
    let (rest, order_by_text) = take_trailing_keyword(&rest, " ORDER BY ");

    let upper_rest = rest.to_ascii_uppercase();
    let (table, predicate) = if let Some(where_pos) = upper_rest.find(" WHERE ") {
        let table = rest[..where_pos].trim();
        let predicate = parse_predicate(rest[where_pos + " WHERE ".len()..].trim())?;
        (table, Some(predicate))
    } else {
        (rest.trim(), None)
    };
    if table.is_empty() {
        return Err(OpenDbError::Sql("SELECT requires table".to_owned()));
    }
    if table.split_whitespace().count() != 1 {
        return Err(OpenDbError::Sql(
            "SELECT only supports a table name after FROM".to_owned(),
        ));
    }
    let order_by = match order_by_text.as_deref() {
        Some(text) => Some(parse_order_by(text)?),
        None => None,
    };
    Ok(Statement::SelectAll {
        table: unquote_identifier(table),
        predicate,
        order_by,
        limit,
        offset,
        columns: SelectColumns::Star,
    })
}

/// Sprint 12.1: parser for `SELECT col1, col2 FROM t [...]` and
/// `SELECT <expr> [AS alias] [, ...]` (no FROM, driver-level probes).
fn parse_select_with_projection(sql: &str) -> OpenDbResult<Statement> {
    let rest = strip_keyword_prefix(sql, "SELECT ")
        .ok_or_else(|| OpenDbError::Sql("invalid SELECT".to_owned()))?
        .trim();
    let upper_rest = rest.to_ascii_uppercase();
    // Detect optional `FROM <table>` boundary while respecting quoted strings.
    let from_pos = find_keyword_outside_quotes(rest, " FROM ");
    if let Some(from_pos) = from_pos {
        let columns_text = rest[..from_pos].trim();
        let after_from = rest[from_pos + " FROM ".len()..].trim();
        let columns = split_top_level_commas(columns_text)?
            .into_iter()
            .map(|token| {
                let trimmed = token.trim();
                if trimmed.is_empty() || trimmed.split_whitespace().count() != 1 {
                    Err(OpenDbError::Sql(format!(
                        "invalid SELECT column: {trimmed}"
                    )))
                } else {
                    Ok(unqualified_column_name(trimmed))
                }
            })
            .collect::<OpenDbResult<Vec<String>>>()?;
        if columns.is_empty() {
            return Err(OpenDbError::Sql(
                "SELECT projection must not be empty".to_owned(),
            ));
        }
        // Reuse the regular SelectAll plumbing: parse FROM/WHERE/ORDER BY/LIMIT/OFFSET
        // with the existing helper. We rebuild a normalised string `SELECT * FROM <rest>` so
        // the existing parser handles the WHERE/ORDER BY/LIMIT/OFFSET grammar.
        let synthetic = format!("SELECT * FROM {after_from}");
        let parsed = parse_select_all(&synthetic)?;
        if let Statement::SelectAll {
            table,
            predicate,
            order_by,
            limit,
            offset,
            ..
        } = parsed
        {
            return Ok(Statement::SelectAll {
                table,
                predicate,
                order_by,
                limit,
                offset,
                columns: SelectColumns::Explicit(columns),
            });
        }
        return Err(OpenDbError::Sql(
            "internal: SELECT projection inner parse mismatch".to_owned(),
        ));
    }

    // No FROM → SELECT <expr> [AS alias] [, ...]
    let items = split_top_level_commas(rest)?
        .into_iter()
        .map(parse_select_expr_item)
        .collect::<OpenDbResult<Vec<SelectExprItem>>>()?;
    if items.is_empty() {
        return Err(OpenDbError::Sql(
            "SELECT requires at least one expression".to_owned(),
        ));
    }
    let _ = upper_rest; // suppress dead-code warning when items branch taken
    Ok(Statement::SelectExpr { items })
}

fn parse_select_expr_item(raw: &str) -> OpenDbResult<SelectExprItem> {
    let trimmed = raw.trim();
    // Optional `<expr> AS <alias>` or `<expr> <alias>` (Sprint 12.1 only
    // supports the AS form for clarity).
    let upper = trimmed.to_ascii_uppercase();
    let (expr_text, alias) = if let Some(as_pos) = upper.find(" AS ") {
        let expr_text = trimmed[..as_pos].trim();
        let alias = trimmed[as_pos + 4..].trim().to_owned();
        if alias.is_empty() {
            return Err(OpenDbError::Sql("alias after AS is required".to_owned()));
        }
        (expr_text, Some(alias))
    } else {
        (trimmed, None)
    };
    let expr = parse_select_expr(expr_text)?;
    Ok(SelectExprItem { expr, alias })
}

fn parse_select_expr(raw: &str) -> OpenDbResult<SelectExpr> {
    let upper = raw.to_ascii_uppercase();
    if upper == "VERSION()" {
        return Ok(SelectExpr::Function(SelectFunction::Version));
    }
    if upper == "NOW()" {
        return Ok(SelectExpr::Function(SelectFunction::Now));
    }
    if upper == "CURRENT_TIMESTAMP" || upper == "CURRENT_TIMESTAMP()" {
        return Ok(SelectExpr::Function(SelectFunction::CurrentTimestamp));
    }
    // Fallback: any literal accepted by parse_value (int / float / 'text' /
    // TRUE / FALSE / NULL).
    let value = parse_value(raw)?;
    Ok(SelectExpr::Literal(value))
}

/// Split a comma-separated list at the top level (respecting quoted text
/// and balanced parentheses).
fn split_top_level_commas(raw: &str) -> OpenDbResult<Vec<&str>> {
    let mut tokens = Vec::new();
    let mut start = 0;
    let mut in_quote = false;
    let mut paren_depth: i32 = 0;
    for (index, ch) in raw.char_indices() {
        match ch {
            '\'' => in_quote = !in_quote,
            '(' if !in_quote => paren_depth += 1,
            ')' if !in_quote => paren_depth -= 1,
            ',' if !in_quote && paren_depth == 0 => {
                tokens.push(raw[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    if in_quote {
        return Err(OpenDbError::Sql("unterminated quoted literal".to_owned()));
    }
    tokens.push(raw[start..].trim());
    Ok(tokens)
}

fn find_keyword_outside_quotes(raw: &str, keyword: &str) -> Option<usize> {
    let upper = raw.to_ascii_uppercase();
    let lowercase_bytes = raw.as_bytes();
    let mut in_quote = false;
    let mut index = 0;
    while index + keyword.len() <= upper.len() {
        let ch = lowercase_bytes[index];
        if ch == b'\'' {
            in_quote = !in_quote;
        }
        if !in_quote && upper[index..].starts_with(keyword) {
            return Some(index);
        }
        index += 1;
    }
    None
}

fn parse_select_with_join(rest: &str) -> OpenDbResult<Statement> {
    // Strip trailing OFFSET/LIMIT/ORDER BY first (same approach as the
    // simple-select parser).
    let (rest, offset) = take_trailing_keyword_value(rest, " OFFSET ")?;
    let (rest, limit) = take_trailing_keyword_value(&rest, " LIMIT ")?;
    let (rest, order_by_text) = take_trailing_keyword(&rest, " ORDER BY ");
    let upper_rest = rest.to_ascii_uppercase();
    let (rest, where_text) = if let Some(pos) = upper_rest.find(" WHERE ") {
        let head = rest[..pos].trim_end().to_owned();
        let tail = rest[pos + " WHERE ".len()..].trim().to_owned();
        (head, Some(tail))
    } else {
        (rest, None)
    };

    let (join_kind, join_keyword) = if rest.to_ascii_uppercase().contains(" INNER JOIN ") {
        (JoinKind::Inner, " INNER JOIN ")
    } else if rest.to_ascii_uppercase().contains(" LEFT JOIN ") {
        (JoinKind::Left, " LEFT JOIN ")
    } else if rest.to_ascii_uppercase().contains(" JOIN ") {
        (JoinKind::Inner, " JOIN ")
    } else {
        return Err(OpenDbError::Sql("expected JOIN clause".to_owned()));
    };

    let upper_rest = rest.to_ascii_uppercase();
    let join_pos = upper_rest
        .find(join_keyword)
        .ok_or_else(|| OpenDbError::Sql("join keyword".to_owned()))?;
    let left_table = rest[..join_pos].trim().to_owned();
    let right_clause = rest[join_pos + join_keyword.len()..].trim();
    let upper_right = right_clause.to_ascii_uppercase();
    let on_pos = upper_right
        .find(" ON ")
        .ok_or_else(|| OpenDbError::Sql("join requires ON".to_owned()))?;
    let right_table = right_clause[..on_pos].trim().to_owned();
    let on_expr = right_clause[on_pos + " ON ".len()..].trim();

    let (left_qualified, right_qualified) = parse_join_equality(on_expr)?;
    if left_qualified.qualifier.as_deref() != Some(left_table.as_str())
        && right_qualified.qualifier.as_deref() != Some(left_table.as_str())
    {
        return Err(OpenDbError::Sql(format!(
            "JOIN ON clause must reference the left table {left_table}"
        )));
    }
    // Normalize so left_column comes from left_table.
    let (left_column, right_column) =
        if left_qualified.qualifier.as_deref() == Some(left_table.as_str()) {
            (left_qualified.column, right_qualified.column)
        } else {
            (right_qualified.column, left_qualified.column)
        };

    let join = JoinClause {
        kind: join_kind,
        right: right_table,
        left_column,
        right_column,
    };

    let where_clause = match where_text {
        Some(text) => Some(parse_joined_predicate(&text)?),
        None => None,
    };
    let order_by = match order_by_text {
        Some(text) => Some(parse_joined_order_by(&text)?),
        None => None,
    };

    Ok(Statement::Select {
        left: left_table,
        join,
        where_clause,
        order_by,
        limit,
        offset,
    })
}

#[derive(Debug)]
struct QualifiedColumn {
    qualifier: Option<String>,
    column: String,
}

fn parse_join_equality(expr: &str) -> OpenDbResult<(QualifiedColumn, QualifiedColumn)> {
    let parts: Vec<&str> = expr.split('=').collect();
    if parts.len() != 2 {
        return Err(OpenDbError::Sql(format!("invalid ON expression: {expr}")));
    }
    Ok((
        parse_qualified_column(parts[0])?,
        parse_qualified_column(parts[1])?,
    ))
}

fn parse_qualified_column(raw: &str) -> OpenDbResult<QualifiedColumn> {
    let trimmed = raw.trim();
    if let Some((qualifier, column)) = trimmed.split_once('.') {
        Ok(QualifiedColumn {
            qualifier: Some(qualifier.trim().to_owned()),
            column: column.trim().to_owned(),
        })
    } else {
        Ok(QualifiedColumn {
            qualifier: None,
            column: trimmed.to_owned(),
        })
    }
}

fn parse_joined_predicate(raw: &str) -> OpenDbResult<JoinedPredicate> {
    let equals_positions = equality_positions_outside_quotes(raw)?;
    let Some(equals_pos) = equals_positions.first().copied() else {
        return Err(OpenDbError::Sql(
            "joined SELECT WHERE only supports equality predicates".to_owned(),
        ));
    };
    if equals_positions.len() != 1 {
        return Err(OpenDbError::Sql(
            "joined SELECT WHERE only supports one equality predicate".to_owned(),
        ));
    }
    let column = raw[..equals_pos].trim();
    let value = raw[equals_pos + 1..].trim();
    if column.is_empty() || value.is_empty() {
        return Err(OpenDbError::Sql(
            "joined SELECT WHERE requires column and literal".to_owned(),
        ));
    }
    let qualified = parse_qualified_column(column)?;
    Ok(JoinedPredicate {
        qualifier: qualified.qualifier,
        column: qualified.column,
        value: parse_value(value)?,
    })
}

fn parse_joined_order_by(raw: &str) -> OpenDbResult<JoinedOrderBy> {
    let trimmed = raw.trim();
    let mut parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.is_empty() {
        return Err(OpenDbError::Sql("empty ORDER BY clause".to_owned()));
    }
    let direction = if parts.len() >= 2 {
        let last = parts.last().expect("non-empty");
        if last.eq_ignore_ascii_case("ASC") {
            parts.pop();
            OrderDirection::Asc
        } else if last.eq_ignore_ascii_case("DESC") {
            parts.pop();
            OrderDirection::Desc
        } else {
            OrderDirection::Asc
        }
    } else {
        OrderDirection::Asc
    };
    if parts.len() != 1 {
        return Err(OpenDbError::Sql(format!(
            "invalid joined ORDER BY clause: {trimmed}"
        )));
    }
    let qualified = parse_qualified_column(parts[0])?;
    Ok(JoinedOrderBy {
        qualifier: qualified.qualifier,
        column: qualified.column,
        direction,
    })
}

fn take_trailing_keyword(input: &str, keyword: &str) -> (String, Option<String>) {
    let upper = input.to_ascii_uppercase();
    if let Some(pos) = upper.rfind(keyword) {
        let head = input[..pos].trim_end().to_owned();
        let tail = input[pos + keyword.len()..].trim().to_owned();
        return (head, Some(tail));
    }
    (input.to_owned(), None)
}

fn take_trailing_keyword_value(input: &str, keyword: &str) -> OpenDbResult<(String, Option<u64>)> {
    let (rest, value_text) = take_trailing_keyword(input, keyword);
    match value_text {
        Some(text) => {
            let value = text.parse::<u64>().map_err(|_| {
                OpenDbError::Sql(format!("invalid value for {}: {text}", keyword.trim()))
            })?;
            Ok((rest, Some(value)))
        }
        None => Ok((rest, None)),
    }
}

fn parse_order_by(text: &str) -> OpenDbResult<OrderBy> {
    let trimmed = text.trim();
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    let (column, direction) = match parts.as_slice() {
        [column] => (unqualified_column_name(column), OrderDirection::Asc),
        [column, direction] if direction.eq_ignore_ascii_case("ASC") => {
            (unqualified_column_name(column), OrderDirection::Asc)
        }
        [column, direction] if direction.eq_ignore_ascii_case("DESC") => {
            (unqualified_column_name(column), OrderDirection::Desc)
        }
        _ => {
            return Err(OpenDbError::Sql(format!(
                "invalid ORDER BY clause: {trimmed}"
            )));
        }
    };
    Ok(OrderBy { column, direction })
}

fn parse_predicate(raw: &str) -> OpenDbResult<Predicate> {
    let equals_positions = equality_positions_outside_quotes(raw)?;
    let Some(equals_pos) = equals_positions.first().copied() else {
        return Err(OpenDbError::Sql(
            "SELECT WHERE only supports equality predicates".to_owned(),
        ));
    };
    if equals_positions.len() != 1 {
        return Err(OpenDbError::Sql(
            "SELECT WHERE only supports one equality predicate".to_owned(),
        ));
    }
    let column = raw[..equals_pos].trim();
    let value = raw[equals_pos + 1..].trim();
    if column.is_empty() || value.is_empty() {
        return Err(OpenDbError::Sql(
            "SELECT WHERE requires column and literal".to_owned(),
        ));
    }
    if column.split_whitespace().count() != 1 {
        return Err(OpenDbError::Sql(
            "SELECT WHERE only supports a single column".to_owned(),
        ));
    }
    Ok(Predicate {
        column: unqualified_column_name(column),
        value: parse_value(value)?,
    })
}

fn equality_positions_outside_quotes(raw: &str) -> OpenDbResult<Vec<usize>> {
    let mut positions = Vec::new();
    let mut in_quote = false;
    for (index, ch) in raw.char_indices() {
        match ch {
            '\'' => in_quote = !in_quote,
            '=' if !in_quote => positions.push(index),
            _ => {}
        }
    }
    if in_quote {
        return Err(OpenDbError::Sql("unterminated quoted literal".to_owned()));
    }
    Ok(positions)
}

fn strip_keyword_prefix<'a>(sql: &'a str, prefix: &str) -> Option<&'a str> {
    sql.get(..prefix.len())
        .filter(|head| head.eq_ignore_ascii_case(prefix))
        .and_then(|_| sql.get(prefix.len()..))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_insert_and_select_subset() {
        assert_eq!(
            parse("CREATE TABLE accounts (id INT, name TEXT);").expect("create"),
            Statement::CreateTable {
                table: "accounts".to_owned(),
                columns: vec![
                    ColumnDefinition::new("id", ColumnType::Int64),
                    ColumnDefinition::new("name", ColumnType::Text),
                ],
            }
        );
        assert_eq!(
            parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("insert"),
            Statement::Insert {
                table: "accounts".to_owned(),
                columns: None,
                values: vec![Value::Int64(1), Value::Text("Ada".to_owned())],
            }
        );
        assert_eq!(
            parse("SELECT * FROM accounts").expect("select"),
            Statement::select_all_legacy("accounts".to_owned(), None)
        );
    }

    #[test]
    fn parses_mixed_case_keywords() {
        assert_eq!(
            parse("cReAtE tAbLe accounts (id INT, name TEXT)").expect("create"),
            Statement::CreateTable {
                table: "accounts".to_owned(),
                columns: vec![
                    ColumnDefinition::new("id", ColumnType::Int64),
                    ColumnDefinition::new("name", ColumnType::Text),
                ],
            }
        );
        assert_eq!(
            parse("iNsErT iNtO accounts vAlUeS (1, 'Ada')").expect("insert"),
            Statement::Insert {
                table: "accounts".to_owned(),
                columns: None,
                values: vec![Value::Int64(1), Value::Text("Ada".to_owned())],
            }
        );
        assert_eq!(
            parse("sElEcT * fRoM accounts").expect("select"),
            Statement::select_all_legacy("accounts".to_owned(), None)
        );
    }

    #[test]
    fn parses_quoted_text_with_comma_as_single_value() {
        assert_eq!(
            parse("INSERT INTO accounts VALUES (1, 'Ada, Lovelace')").expect("insert"),
            Statement::Insert {
                table: "accounts".to_owned(),
                columns: None,
                values: vec![Value::Int64(1), Value::Text("Ada, Lovelace".to_owned())],
            }
        );
    }

    #[test]
    fn rejects_unterminated_quoted_value() {
        assert!(matches!(
            parse("INSERT INTO accounts VALUES (1, 'Ada, Lovelace)"),
            Err(OpenDbError::Sql(_))
        ));
    }

    #[test]
    fn rejects_trailing_garbage_after_create_and_insert() {
        assert!(matches!(
            parse("CREATE TABLE accounts (id INT) trailing"),
            Err(OpenDbError::Sql(_))
        ));
        assert!(matches!(
            parse("INSERT INTO accounts VALUES (1) trailing"),
            Err(OpenDbError::Sql(_))
        ));
    }

    #[test]
    fn rejects_repeated_semicolon_terminators() {
        assert!(matches!(
            parse("CREATE TABLE accounts (id INT);;"),
            Err(OpenDbError::Sql(_))
        ));
    }

    #[test]
    fn rejects_parentheses_in_reverse_order() {
        assert!(matches!(
            parse("CREATE TABLE accounts ) ("),
            Err(OpenDbError::Sql(_))
        ));
        assert!(matches!(
            parse("INSERT INTO accounts VALUES ) ("),
            Err(OpenDbError::Sql(_))
        ));
    }

    #[test]
    fn rejects_malformed_select_where_at_parse_time() {
        assert!(matches!(
            parse("SELECT * FROM accounts WHERE id > 1"),
            Err(OpenDbError::Sql(_))
        ));
        assert!(matches!(
            parse("SELECT * FROM accounts WHERE id = "),
            Err(OpenDbError::Sql(_))
        ));
    }

    #[test]
    fn parses_primary_key_column_metadata() {
        assert_eq!(
            parse("CREATE TABLE accounts (id BIGINT PRIMARY KEY, name TEXT)").expect("create"),
            Statement::CreateTable {
                table: "accounts".to_owned(),
                columns: vec![
                    ColumnDefinition::primary_key("id", ColumnType::Int64),
                    ColumnDefinition::new("name", ColumnType::Text),
                ],
            }
        );
    }

    #[test]
    fn rejects_unknown_column_type() {
        assert!(matches!(
            parse("CREATE TABLE accounts (id UUID PRIMARY KEY)"),
            Err(OpenDbError::Sql(_))
        ));
    }

    #[test]
    fn parses_primary_key_equality_predicate() {
        assert_eq!(
            parse("SELECT * FROM accounts WHERE id = 1").expect("select where"),
            Statement::select_all_legacy(
                "accounts".to_owned(),
                Some(Predicate {
                    column: "id".to_owned(),
                    value: Value::Int64(1),
                }),
            )
        );
        assert_eq!(
            parse("select * from accounts where name = 'Ada'").expect("select where text"),
            Statement::select_all_legacy(
                "accounts".to_owned(),
                Some(Predicate {
                    column: "name".to_owned(),
                    value: Value::Text("Ada".to_owned()),
                }),
            )
        );
    }

    #[test]
    fn parses_extended_column_types_with_not_null_and_default() {
        let stmt = parse(
            "CREATE TABLE typed (id INT PRIMARY KEY, completed BOOL DEFAULT false, ratio FLOAT8, label TEXT NOT NULL DEFAULT 'completed', created_at TIMESTAMP NOT NULL DEFAULT NOW())",
        )
        .expect("create typed");
        let Statement::CreateTable { table, columns } = stmt else {
            panic!("expected CreateTable");
        };
        assert_eq!(table, "typed");
        assert_eq!(columns[0].name, "id");
        assert!(columns[0].primary_key);
        assert!(!columns[0].nullable);
        assert!(columns[0].default.is_none());

        assert_eq!(columns[1].name, "completed");
        assert!(matches!(columns[1].data_type, ColumnType::Bool));
        assert!(columns[1].nullable);
        assert_eq!(
            columns[1].default,
            Some(DefaultExpr::Const(Value::Bool(false)))
        );

        assert_eq!(columns[2].name, "ratio");
        assert!(matches!(columns[2].data_type, ColumnType::Float64));

        assert_eq!(columns[3].name, "label");
        assert!(!columns[3].nullable);
        assert_eq!(
            columns[3].default,
            Some(DefaultExpr::Const(Value::Text("completed".to_owned())))
        );

        assert_eq!(columns[4].name, "created_at");
        assert!(matches!(columns[4].data_type, ColumnType::Timestamp));
        assert!(!columns[4].nullable);
        assert_eq!(columns[4].default, Some(DefaultExpr::Now));
    }

    #[test]
    fn parses_double_precision_alias() {
        let stmt =
            parse("CREATE TABLE t (id INT PRIMARY KEY, ratio DOUBLE PRECISION)").expect("create");
        let Statement::CreateTable { columns, .. } = stmt else {
            panic!("expected CreateTable");
        };
        assert!(matches!(columns[1].data_type, ColumnType::Float64));
    }

    #[test]
    fn parses_insert_with_named_columns() {
        let stmt = parse("INSERT INTO accounts (id, name) VALUES (1, 'Ada')").expect("insert");
        assert_eq!(
            stmt,
            Statement::Insert {
                table: "accounts".to_owned(),
                columns: Some(vec!["id".to_owned(), "name".to_owned()]),
                values: vec![Value::Int64(1), Value::Text("Ada".to_owned())],
            }
        );
    }

    #[test]
    fn rejects_insert_with_empty_named_column_list() {
        assert!(matches!(
            parse("INSERT INTO accounts () VALUES (1)"),
            Err(OpenDbError::Sql(_))
        ));
    }

    #[test]
    fn parses_null_and_boolean_literals() {
        let stmt = parse("INSERT INTO t VALUES (NULL, TRUE, FALSE)").expect("insert");
        let Statement::Insert { values, .. } = stmt else {
            panic!("expected Insert");
        };
        assert_eq!(
            values,
            vec![Value::Null, Value::Bool(true), Value::Bool(false)]
        );
    }

    #[test]
    fn parses_select_literal_without_from() {
        let stmt = parse("SELECT 1").expect("select 1");
        let Statement::SelectExpr { items } = stmt else {
            panic!("expected SelectExpr");
        };
        assert_eq!(items.len(), 1);
        assert!(matches!(
            items[0].expr,
            SelectExpr::Literal(Value::Int64(1))
        ));
    }

    #[test]
    fn parses_select_version_function() {
        let stmt = parse("SELECT version()").expect("select version");
        let Statement::SelectExpr { items } = stmt else {
            panic!("expected SelectExpr");
        };
        assert!(matches!(
            items[0].expr,
            SelectExpr::Function(SelectFunction::Version)
        ));
    }

    #[test]
    fn parses_select_with_alias() {
        let stmt = parse("SELECT 1 AS one").expect("select 1 as one");
        let Statement::SelectExpr { items } = stmt else {
            panic!("expected SelectExpr");
        };
        assert_eq!(items[0].alias.as_deref(), Some("one"));
    }

    #[test]
    fn parses_explicit_column_projection() {
        let stmt = parse("SELECT id, name FROM accounts").expect("select projection");
        let Statement::SelectAll { columns, table, .. } = stmt else {
            panic!("expected SelectAll");
        };
        assert_eq!(table, "accounts");
        assert!(
            matches!(columns, SelectColumns::Explicit(ref cols) if cols == &vec!["id".to_owned(), "name".to_owned()])
        );
    }

    #[test]
    fn parses_begin_commit_rollback_in_all_synonyms() {
        for sql in ["BEGIN", "begin transaction", "START TRANSACTION"] {
            assert!(matches!(parse(sql).expect("begin"), Statement::Begin));
        }
        for sql in ["COMMIT", "commit transaction", "END"] {
            assert!(matches!(parse(sql).expect("commit"), Statement::Commit));
        }
        for sql in ["ROLLBACK", "rollback transaction", "ABORT"] {
            assert!(matches!(parse(sql).expect("rollback"), Statement::Rollback));
        }
    }

    #[test]
    fn parses_select_with_order_by_limit_offset() {
        let stmt = parse("SELECT * FROM accounts ORDER BY name DESC LIMIT 10 OFFSET 20")
            .expect("select with order by limit offset");
        let Statement::SelectAll {
            order_by,
            limit,
            offset,
            ..
        } = stmt
        else {
            panic!("expected SelectAll");
        };
        let order_by = order_by.expect("order_by");
        assert_eq!(order_by.column, "name");
        assert!(matches!(order_by.direction, OrderDirection::Desc));
        assert_eq!(limit, Some(10));
        assert_eq!(offset, Some(20));
    }

    #[test]
    fn parses_select_with_where_then_order_by() {
        let stmt = parse("SELECT * FROM accounts WHERE id = 1 ORDER BY id ASC LIMIT 5")
            .expect("select where order by");
        let Statement::SelectAll {
            predicate,
            order_by,
            limit,
            ..
        } = stmt
        else {
            panic!("expected SelectAll");
        };
        assert!(predicate.is_some());
        let order_by = order_by.expect("order_by");
        assert_eq!(order_by.column, "id");
        assert!(matches!(order_by.direction, OrderDirection::Asc));
        assert_eq!(limit, Some(5));
    }

    #[test]
    fn parses_update_set_where_pk() {
        let stmt = parse("UPDATE accounts SET name = 'Bob', status = 'active' WHERE id = 1")
            .expect("update");
        let Statement::UpdateRow {
            table,
            key,
            assignments,
        } = stmt
        else {
            panic!("expected UpdateRow");
        };
        assert_eq!(table, "accounts");
        assert_eq!(key, "1");
        assert_eq!(assignments.len(), 2);
        assert_eq!(assignments[0].0, "name");
        assert_eq!(assignments[1].0, "status");
    }

    #[test]
    fn rejects_update_without_where() {
        assert!(matches!(
            parse("UPDATE accounts SET name = 'Bob'"),
            Err(OpenDbError::Sql(_))
        ));
    }

    #[test]
    fn parses_delete_from_where_primary_key_equality() {
        let stmt = parse("DELETE FROM accounts WHERE id = 1").expect("delete");
        let Statement::DeleteRow { table, key } = stmt else {
            panic!("expected DeleteRow");
        };
        assert_eq!(table, "accounts");
        assert_eq!(key, "1");
    }

    #[test]
    fn rejects_delete_without_where_predicate() {
        assert!(matches!(
            parse("DELETE FROM accounts"),
            Err(OpenDbError::Sql(_))
        ));
    }

    #[test]
    fn parses_alter_table_add_column_with_default() {
        let stmt = parse("ALTER TABLE accounts ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
            .expect("alter add");
        let Statement::AlterTable { table, op } = stmt else {
            panic!("expected AlterTable");
        };
        assert_eq!(table, "accounts");
        match op {
            AlterTableOp::AddColumn(column) => {
                assert_eq!(column.name, "status");
                assert!(!column.nullable);
                assert_eq!(
                    column.default,
                    Some(DefaultExpr::Const(Value::Text("active".to_owned())))
                );
            }
            other => panic!("expected AddColumn, got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_table_drop_and_rename_column() {
        let drop_stmt = parse("ALTER TABLE accounts DROP COLUMN legacy").expect("alter drop");
        let Statement::AlterTable { op, .. } = drop_stmt else {
            panic!("expected AlterTable");
        };
        assert!(matches!(op, AlterTableOp::DropColumn { column } if column == "legacy"));

        let rename_stmt =
            parse("ALTER TABLE accounts RENAME COLUMN old TO renamed").expect("alter rename");
        let Statement::AlterTable { op, .. } = rename_stmt else {
            panic!("expected AlterTable");
        };
        match op {
            AlterTableOp::RenameColumn { from, to } => {
                assert_eq!(from, "old");
                assert_eq!(to, "renamed");
            }
            _ => panic!("expected RenameColumn"),
        }
    }

    #[test]
    fn parses_alter_table_add_foreign_key_constraint() {
        let stmt = parse(
            "ALTER TABLE users ADD CONSTRAINT users_org_fk FOREIGN KEY (org_id) REFERENCES orgs (id) ON DELETE CASCADE",
        )
        .expect("alter fk");
        let Statement::AlterTable { op, .. } = stmt else {
            panic!("expected AlterTable");
        };
        let AlterTableOp::AddConstraint(constraint) = op else {
            panic!("expected AddConstraint");
        };
        assert_eq!(constraint.name, "users_org_fk");
        let ConstraintKind::ForeignKey {
            columns,
            references_table,
            references_columns,
            on_delete,
            on_update,
        } = constraint.kind
        else {
            panic!("expected ForeignKey");
        };
        assert_eq!(columns, vec!["org_id".to_owned()]);
        assert_eq!(references_table, "orgs");
        assert_eq!(references_columns, vec!["id".to_owned()]);
        assert!(matches!(on_delete, ReferentialAction::Cascade));
        assert!(matches!(on_update, ReferentialAction::NoAction));
    }

    #[test]
    fn parses_create_index_if_not_exists_with_btree() {
        let stmt =
            parse("CREATE INDEX IF NOT EXISTS accounts_name_idx ON accounts USING btree (name)")
                .expect("create index");
        let Statement::CreateIndex { table, index } = stmt else {
            panic!("expected CreateIndex");
        };
        assert_eq!(table, "accounts");
        assert_eq!(index.name, "accounts_name_idx");
        assert_eq!(index.columns, vec!["name".to_owned()]);
        assert!(index.if_not_exists);
        assert!(!index.unique);
    }

    #[test]
    fn parses_do_block_with_exception_duplicate_object() {
        let stmt = parse(
            "DO $$ BEGIN ALTER TABLE accounts ADD COLUMN legacy TEXT; EXCEPTION WHEN duplicate_object THEN null; END $$",
        )
        .expect("do block");
        let Statement::DoBlock {
            inner,
            swallow_duplicate,
        } = stmt
        else {
            panic!("expected DoBlock");
        };
        assert!(swallow_duplicate);
        assert_eq!(inner.len(), 1);
        assert!(matches!(inner[0], Statement::AlterTable { .. }));
    }

    #[test]
    fn parses_jsonb_type_and_default_cast() {
        let stmt = parse("CREATE TABLE t (id INT PRIMARY KEY, data JSONB DEFAULT '{}'::jsonb)")
            .expect("create");
        let Statement::CreateTable { columns, .. } = stmt else {
            panic!("expected CreateTable");
        };
        assert!(matches!(columns[1].data_type, ColumnType::Json));
        assert_eq!(
            columns[1].default,
            Some(DefaultExpr::Const(Value::Text("{}".to_owned())))
        );
    }

    #[test]
    fn parses_jsonb_alias_as_json() {
        let stmt =
            parse("CREATE TABLE t (id INT PRIMARY KEY, data JSON)").expect("create json column");
        let Statement::CreateTable { columns, .. } = stmt else {
            panic!("expected CreateTable");
        };
        assert!(matches!(columns[1].data_type, ColumnType::Json));
    }

    #[test]
    fn parses_jsonb_literal_in_named_insert() {
        let stmt =
            parse("INSERT INTO t (id, data) VALUES (1, '{\"k\":\"v\"}'::jsonb)").expect("insert");
        let Statement::Insert {
            values, columns, ..
        } = stmt
        else {
            panic!("expected Insert");
        };
        assert_eq!(columns, Some(vec!["id".to_owned(), "data".to_owned()]));
        assert_eq!(
            values,
            vec![Value::Int64(1), Value::Text("{\"k\":\"v\"}".to_owned()),]
        );
    }

    #[test]
    fn parses_float_literal() {
        let stmt = parse("INSERT INTO t VALUES (1, 3.5)").expect("insert");
        let Statement::Insert { values, .. } = stmt else {
            panic!("expected Insert");
        };
        assert_eq!(values, vec![Value::Int64(1), Value::Float64(3.5)]);
    }

    #[test]
    fn parses_equality_predicate_with_equals_inside_quoted_literal() {
        assert_eq!(
            parse("SELECT * FROM sessions WHERE token = 'a=b'").expect("select where text pk"),
            Statement::select_all_legacy(
                "sessions".to_owned(),
                Some(Predicate {
                    column: "token".to_owned(),
                    value: Value::Text("a=b".to_owned()),
                }),
            )
        );
    }
}
