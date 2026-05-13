use crate::ast::{Predicate, Statement};
use opendb_common::{OpenDbError, OpenDbResult};
use opendb_storage::commit_stream::{ColumnDefinition, ColumnType, DefaultExpr, Value};

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
    if upper.starts_with("CREATE TABLE ") {
        parse_create_table(normalized)
    } else if upper.starts_with("INSERT INTO ") {
        parse_insert(normalized)
    } else if upper.starts_with("SELECT * FROM ") {
        parse_select_all(normalized)
    } else {
        Err(OpenDbError::Sql(format!("unsupported SQL: {normalized}")))
    }
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
        let table = header[..open].trim().to_owned();
        let columns = split_values(&header[open + 1..close])?
            .into_iter()
            .map(|c| {
                let trimmed = c.trim();
                if trimmed.is_empty() || trimmed.split_whitespace().count() != 1 {
                    Err(OpenDbError::Sql(format!(
                        "invalid INSERT column name: {trimmed}"
                    )))
                } else {
                    Ok(trimmed.to_owned())
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
        (header.to_owned(), None)
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
        "::jsonb",
        "::JSONB",
        "::Jsonb",
        "::json",
        "::JSON",
        "::Json",
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
    let upper_rest = rest.to_ascii_uppercase();
    let (table, predicate) = if let Some(where_pos) = upper_rest.find(" WHERE ") {
        let table = rest[..where_pos].trim();
        let predicate = parse_predicate(rest[where_pos + " WHERE ".len()..].trim())?;
        (table, Some(predicate))
    } else {
        (rest, None)
    };
    if table.is_empty() {
        return Err(OpenDbError::Sql("SELECT requires table".to_owned()));
    }
    if table.split_whitespace().count() != 1 {
        return Err(OpenDbError::Sql(
            "SELECT only supports a table name after FROM".to_owned(),
        ));
    }
    Ok(Statement::SelectAll {
        table: table.to_owned(),
        predicate,
    })
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
        column: column.to_owned(),
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
            Statement::SelectAll {
                table: "accounts".to_owned(),
                predicate: None,
            }
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
            Statement::SelectAll {
                table: "accounts".to_owned(),
                predicate: None,
            }
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
            Statement::SelectAll {
                table: "accounts".to_owned(),
                predicate: Some(Predicate {
                    column: "id".to_owned(),
                    value: Value::Int64(1),
                }),
            }
        );
        assert_eq!(
            parse("select * from accounts where name = 'Ada'").expect("select where text"),
            Statement::SelectAll {
                table: "accounts".to_owned(),
                predicate: Some(Predicate {
                    column: "name".to_owned(),
                    value: Value::Text("Ada".to_owned()),
                }),
            }
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
        let stmt = parse("INSERT INTO t (id, data) VALUES (1, '{\"k\":\"v\"}'::jsonb)")
            .expect("insert");
        let Statement::Insert {
            values, columns, ..
        } = stmt
        else {
            panic!("expected Insert");
        };
        assert_eq!(columns, Some(vec!["id".to_owned(), "data".to_owned()]));
        assert_eq!(
            values,
            vec![
                Value::Int64(1),
                Value::Text("{\"k\":\"v\"}".to_owned()),
            ]
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
            Statement::SelectAll {
                table: "sessions".to_owned(),
                predicate: Some(Predicate {
                    column: "token".to_owned(),
                    value: Value::Text("a=b".to_owned()),
                }),
            }
        );
    }
}
