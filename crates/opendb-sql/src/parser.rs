use crate::ast::{
    AggregateArg, AggregateExpr, AggregateFunction, AggregateOrColumn, AggregateProjection,
    AggregateSelectItem, HavingPredicate, JoinClause, JoinKind, JoinedOrderBy, JoinedPredicate,
    OrderBy, OrderDirection, Predicate, ReturningClause, SelectColumns, SelectExpr, SelectExprItem,
    SelectFunction, Statement, WhereOp,
};
use opendb_common::{OpenDbError, OpenDbResult};
use opendb_storage::commit_stream::{
    AlterTableOp, ColumnDefinition, ColumnType, ConstraintKind, DefaultExpr, IndexDescriptor,
    NamedConstraint, ReferentialAction, Value,
};

pub fn parse(sql: &str) -> OpenDbResult<Statement> {
    // Sprint 18.A.1.1: strip SQL comments before any keyword sniffing.
    // Drizzle migrations routinely lead with `-- migration description` lines
    // or interleave `/* ... */` notes; the rest of the parser would otherwise
    // see the comment as the statement and reject it as `unsupported SQL`.
    let stripped = strip_sql_comments(sql);
    let trimmed = stripped.trim();
    // Sprint 18.A.1.1: a comment-only or whitespace-only input (after strip)
    // is a no-op in Postgres. Drizzle migrations + the migrate-poc splitter
    // can produce these chunks (trailing `--` notes after the last `;`).
    if trimmed.is_empty() {
        return Ok(Statement::DoBlock {
            inner: Vec::new(),
            swallow_duplicate: true,
        });
    }
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
    // Sprint 19.C: LISTEN / UNLISTEN / NOTIFY are pgwire-level
    // pub/sub primitives. opendb does not implement async notifications, but
    // sentropic's `purgeAllLocksAtStartup` and chat lock SSE flow issue them
    // unconditionally. Accept them as no-ops so the bootstrap doesn't crash.
    if upper.starts_with("LISTEN ")
        || upper.starts_with("UNLISTEN ")
        || upper.starts_with("NOTIFY ")
    {
        return Ok(Statement::DoBlock {
            inner: Vec::new(),
            swallow_duplicate: true,
        });
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
    } else if upper.starts_with("DROP TABLE ") || upper == "DROP TABLE" {
        parse_drop_table(normalized)
    } else if upper.starts_with("TRUNCATE ") || upper == "TRUNCATE" {
        parse_truncate_table(normalized)
    } else if upper == "VACUUM" || upper.starts_with("VACUUM ") || upper.starts_with("ANALYZE") {
        // Phase A 2026-05-22: pgbench -i calls `vacuum analyze pgbench_*`
        // and PG clients commonly issue ANALYZE. opendb does not maintain
        // planner stats yet — accept as a no-op so the bench bootstrap
        // doesn't crash. Wrap in an empty DoBlock so the rest of the
        // pipeline gets a valid Statement.
        Ok(Statement::DoBlock {
            inner: Vec::new(),
            swallow_duplicate: true,
        })
    } else {
        Err(OpenDbError::Sql(format!("unsupported SQL: {normalized}")))
    }
}

/// Phase A 2026-05-22: `DROP TABLE [IF EXISTS] name[, name, ...]`.
///
/// Multi-table comma list expands to a DoBlock of N DropTable statements,
/// same shape parser.rs uses for multi-row INSERT VALUES. The `IF EXISTS`
/// modifier is preserved on each inner statement so the executor can elide
/// missing tables individually.
fn parse_drop_table(input: &str) -> OpenDbResult<Statement> {
    let upper = input.to_ascii_uppercase();
    let prefix_len = if let Some(idx) = upper.find("DROP TABLE") {
        idx + "DROP TABLE".len()
    } else {
        return Err(OpenDbError::Sql(format!(
            "DROP TABLE expected at start of statement: {input}"
        )));
    };
    let rest = input[prefix_len..].trim_start();
    let upper_rest = rest.to_ascii_uppercase();
    let (if_exists, body) = if let Some(stripped) = upper_rest.strip_prefix("IF EXISTS ") {
        let consumed = upper_rest.len() - stripped.len();
        (true, rest[consumed..].trim_start())
    } else if upper_rest == "IF EXISTS" {
        return Err(OpenDbError::Sql(format!(
            "DROP TABLE IF EXISTS missing table name: {input}"
        )));
    } else {
        (false, rest)
    };
    let body = body
        .trim_end_matches(';')
        .trim_end_matches(|c: char| c.is_whitespace());
    // Trim trailing CASCADE / RESTRICT — PG-specific, no FK enforcement
    // story for opendb yet, so we accept both modifiers as a no-op.
    let body = strip_trailing_drop_modifier(body, "CASCADE");
    let body = strip_trailing_drop_modifier(body, "RESTRICT");
    if body.is_empty() {
        return Err(OpenDbError::Sql(format!(
            "DROP TABLE missing table name(s): {input}"
        )));
    }
    let names: Vec<&str> = body
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if names.is_empty() {
        return Err(OpenDbError::Sql(format!(
            "DROP TABLE missing table name(s): {input}"
        )));
    }
    let inner: Vec<Statement> = names
        .into_iter()
        .map(|name| Statement::DropTable {
            table: unquote_drop_identifier(name).to_owned(),
            if_exists,
        })
        .collect();
    if inner.len() == 1 {
        Ok(inner.into_iter().next().unwrap())
    } else {
        Ok(Statement::DoBlock {
            inner,
            swallow_duplicate: false,
        })
    }
}

/// Phase A 2026-05-22: `TRUNCATE [TABLE] [ONLY] name[, name, ...]
/// [RESTART IDENTITY | CONTINUE IDENTITY] [CASCADE | RESTRICT]`.
/// Multi-table comma list expands to a DoBlock of N TruncateTable
/// statements. The `RESTART IDENTITY`/`CONTINUE IDENTITY` and
/// `CASCADE`/`RESTRICT` modifiers are accepted as no-ops; opendb has no
/// sequences and no FK action semantics yet.
fn parse_truncate_table(input: &str) -> OpenDbResult<Statement> {
    let upper = input.to_ascii_uppercase();
    let prefix_len = "TRUNCATE".len();
    let after_truncate = input[prefix_len..].trim_start();
    let upper_after = upper[prefix_len..].trim_start().to_ascii_uppercase();
    let body = if let Some(stripped) = upper_after.strip_prefix("TABLE ") {
        let consumed = upper_after.len() - stripped.len();
        after_truncate[consumed..].trim_start()
    } else {
        after_truncate
    };
    // Optional ONLY keyword (PG flag for inheritance — ignored).
    let body_upper = body.to_ascii_uppercase();
    let body = if let Some(stripped) = body_upper.strip_prefix("ONLY ") {
        let consumed = body_upper.len() - stripped.len();
        body[consumed..].trim_start()
    } else {
        body
    };
    let body = body
        .trim_end_matches(';')
        .trim_end_matches(|c: char| c.is_whitespace());
    let body = strip_trailing_drop_modifier(body, "CASCADE");
    let body = strip_trailing_drop_modifier(body, "RESTRICT");
    let body = strip_trailing_drop_modifier(body, "CONTINUE IDENTITY");
    let body = strip_trailing_drop_modifier(body, "RESTART IDENTITY");
    if body.is_empty() {
        return Err(OpenDbError::Sql(format!(
            "TRUNCATE missing table name(s): {input}"
        )));
    }
    let names: Vec<&str> = body
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if names.is_empty() {
        return Err(OpenDbError::Sql(format!(
            "TRUNCATE missing table name(s): {input}"
        )));
    }
    let inner: Vec<Statement> = names
        .into_iter()
        .map(|name| Statement::TruncateTable {
            table: unquote_drop_identifier(name).to_owned(),
        })
        .collect();
    if inner.len() == 1 {
        Ok(inner.into_iter().next().unwrap())
    } else {
        Ok(Statement::DoBlock {
            inner,
            swallow_duplicate: false,
        })
    }
}

fn strip_trailing_drop_modifier<'a>(input: &'a str, keyword: &str) -> &'a str {
    let upper = input.to_ascii_uppercase();
    if upper.ends_with(keyword) {
        let cut = input.len() - keyword.len();
        input[..cut].trim_end()
    } else {
        input
    }
}

fn unquote_drop_identifier(name: &str) -> &str {
    let trimmed = name.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    }
}

/// Sprint 18.B: split a `"quoted"` identifier from the rest of the input.
/// Returns `(inner, remainder)` on success, `None` if the input does not
/// start with a quoted identifier. The remainder is left untrimmed so the
/// caller can decide whether to follow up on `.<next>` (schema qualifier).
fn strip_quoted_segment(input: &str) -> Option<(&str, &str)> {
    if !input.starts_with('"') {
        return None;
    }
    let bytes = input.as_bytes();
    let mut i = 1usize;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            return Some((&input[1..i], &input[i + 1..]));
        }
        i += 1;
    }
    None
}

/// Sprint 18.A.1.1: strip SQL line comments (`-- ... \n`) and block comments
/// (`/* ... */`) outside of single-quoted string literals. Quoted strings are
/// preserved verbatim so a literal like `'foo -- bar'` keeps its content.
/// Operates byte-by-byte (safe for ASCII syntax; UTF-8 inside literals or
/// identifiers is passed through unchanged).
fn strip_sql_comments(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(bytes.len());
    let mut i = 0usize;
    let mut in_quote = false;
    while i < bytes.len() {
        let c = bytes[i];
        if in_quote {
            out.push(c as char);
            if c == b'\'' {
                // Postgres doubled-quote escape: `''` stays inside the literal.
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_quote = false;
            }
            i += 1;
            continue;
        }
        if c == b'\'' {
            in_quote = true;
            out.push('\'');
            i += 1;
            continue;
        }
        // Line comment: `-- ... \n` (or end-of-input).
        if c == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            // Replace the comment span with a single space so adjacent tokens
            // don't accidentally fuse (`SELECT 1--c\nFROM t` → `SELECT 1 FROM t`).
            out.push(' ');
            continue;
        }
        // Block comment: `/* ... */`. Postgres allows nesting; we follow.
        if c == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            let mut depth = 1usize;
            while i < bytes.len() && depth > 0 {
                if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            out.push(' ');
            continue;
        }
        // Sprint 18.B: collapse newlines/tabs to spaces OUTSIDE quotes so the
        // keyword sniffers (`upper.find(" VALUES ")`, etc.) work on multi-line
        // statements emitted by Drizzle migrations. Avoid double-spaces by
        // checking the previous output character.
        if c == b'\n' || c == b'\r' || c == b'\t' {
            if !out.ends_with(' ') {
                out.push(' ');
            }
            i += 1;
            continue;
        }
        out.push(c as char);
        i += 1;
    }
    out
}

/// Sprint 13: `UPDATE <table> SET <col1> = <lit1> [, ...] WHERE <pk> = <literal>`.
fn parse_update(sql: &str) -> OpenDbResult<Statement> {
    let rest = strip_keyword_prefix(sql, "UPDATE ")
        .ok_or_else(|| OpenDbError::Sql("invalid UPDATE".to_owned()))?
        .trim();
    // Sprint 16.B: peel off optional `RETURNING ...` first so the WHERE
    // parser doesn't try to interpret it as a literal.
    let (rest, returning) = split_off_returning(rest)?;
    let rest = rest.trim();
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
    let predicates = parse_predicate_conjunction(predicate_text)?;
    // Sprint 14.D: the executor is responsible for the PK-equality fast
    // path (it knows the schema). The parser unconditionally produces
    // `UpdateWhere`.
    Ok(Statement::UpdateWhere {
        table: unquote_identifier(table),
        predicate: predicates,
        assignments,
        returning,
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
    // Sprint 16.B: peel off optional `RETURNING ...` before any other parsing.
    let (rest, returning) = split_off_returning(rest)?;
    let rest = rest.trim();
    let upper_rest = rest.to_ascii_uppercase();
    // Sprint 16.B: support `DELETE FROM t RETURNING ...` (no WHERE) — Drizzle
    // emits this for `db.delete(t).returning()` to wipe + return all rows.
    let (table_text, predicate_text) = if let Some(where_pos) = upper_rest.find(" WHERE ") {
        (
            rest[..where_pos].trim(),
            Some(rest[where_pos + " WHERE ".len()..].trim()),
        )
    } else {
        (rest, None)
    };
    if table_text.is_empty() {
        return Err(OpenDbError::Sql("DELETE requires a table".to_owned()));
    }
    let predicates = match predicate_text {
        Some(text) => parse_predicate_conjunction(text)?,
        None => Vec::new(),
    };
    Ok(Statement::DeleteWhere {
        table: unquote_identifier(table_text),
        predicate: predicates,
        returning,
    })
}

fn parse_alter_table(sql: &str) -> OpenDbResult<Statement> {
    let rest = strip_keyword_prefix(sql, "ALTER TABLE ")
        .ok_or_else(|| OpenDbError::Sql("invalid ALTER TABLE".to_owned()))?
        .trim();
    let (table_name, remainder) = split_first_word(rest)?;
    let upper_remainder = remainder.to_ascii_uppercase();
    if let Some(after) = strip_keyword(remainder, &upper_remainder, "ADD COLUMN ") {
        // Sprint 18.A.1.2: `ADD COLUMN IF NOT EXISTS`. Wrap in DoBlock so a
        // duplicate-column error becomes a no-op.
        let (after, swallow) = strip_if_not_exists(after);
        let column = parse_column_definition(after.trim())?;
        let inner = Statement::AlterTable {
            table: unquote_identifier(table_name),
            op: AlterTableOp::AddColumn(column),
        };
        if swallow {
            return Ok(Statement::DoBlock {
                inner: vec![inner],
                swallow_duplicate: true,
            });
        }
        return Ok(inner);
    }
    if let Some(after) = strip_keyword(remainder, &upper_remainder, "DROP COLUMN ") {
        // Sprint 18.A.1.2: `DROP COLUMN IF EXISTS` swallows missing-column.
        let (after, swallow) = strip_if_exists(after);
        let column = strip_optional_terminators(after.trim());
        let inner = Statement::AlterTable {
            table: unquote_identifier(table_name),
            op: AlterTableOp::DropColumn {
                column: unquote_identifier(column),
            },
        };
        if swallow {
            return Ok(Statement::DoBlock {
                inner: vec![inner],
                swallow_duplicate: true,
            });
        }
        return Ok(inner);
    }
    if let Some(after) = strip_keyword(remainder, &upper_remainder, "DROP CONSTRAINT ") {
        // Sprint 18.A.1.2: opendb does not maintain a per-table constraint
        // registry beyond what's encoded in `ColumnDefinition`/`NamedConstraint`,
        // so DROP CONSTRAINT (with or without IF EXISTS) is a no-op for now.
        // Always swallow — Drizzle uses this to clean up legacy constraints
        // before re-adding them in the same migration.
        let (_after, _swallow) = strip_if_exists(after);
        return Ok(Statement::DoBlock {
            inner: Vec::new(),
            swallow_duplicate: true,
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
    // Sprint 18.A.1.4: ALTER COLUMN ... {SET NOT NULL | DROP NOT NULL |
    // DROP DEFAULT | SET DEFAULT <expr>}. opendb does not currently re-validate
    // existing rows against the new constraint, so for now we accept these
    // as no-ops at the storage layer — the migration succeeds, and any
    // INSERT after the migration will use the new column metadata once
    // Sprint 18 wires the alteration through (out of scope for the
    // migration-replay gate). The wrapper DoBlock with empty mutations keeps
    // execute() happy.
    if let Some(after) = strip_keyword(remainder, &upper_remainder, "ALTER COLUMN ") {
        let _ = after; // consumed; we only verify it parses lazily
        return Ok(Statement::DoBlock {
            inner: Vec::new(),
            swallow_duplicate: true,
        });
    }
    // Sprint 18.A.1.6: ALTER TABLE ... RENAME TO <new-table>. Emits a
    // RenameTable mutation that re-keys the projection. Drizzle uses this in
    // migration 0024 to rename `use_cases → initiatives`.
    if let Some(after) = strip_keyword(remainder, &upper_remainder, "RENAME TO ") {
        let new_name = unquote_identifier(strip_optional_terminators(after.trim()));
        if new_name.is_empty() {
            return Err(OpenDbError::Sql(
                "RENAME TO requires a target table name".to_owned(),
            ));
        }
        return Ok(Statement::AlterTable {
            table: unquote_identifier(table_name),
            op: AlterTableOp::RenameTable { to: new_name },
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
        // Sprint 18.A.1.5: Drizzle emits `REFERENCES "table"("col")` with
        // no whitespace between the table identifier and the column list, so
        // `split_first_word` would consume the whole `"table"("col")` blob.
        // Detect the boundary at the first `(` (outside the optional quoted
        // identifier) instead.
        let (ref_table, after_ref_table) = split_table_then_paren(after_refs)?;
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

/// Sprint 18.A.1.5: split `"table"("col1","col2") <rest>` into
/// (`"table"`, `("col1","col2") <rest>`). Walks until the first `(` outside
/// quoted-identifier context. Falls back to whitespace-split if no paren is
/// present (e.g., legacy `REFERENCES table (col)` with whitespace).
fn split_table_then_paren(input: &str) -> OpenDbResult<(&str, &str)> {
    let trimmed = input.trim_start();
    let bytes = trimmed.as_bytes();
    let mut in_quote = false;
    for (i, b) in bytes.iter().enumerate() {
        match *b {
            b'"' => in_quote = !in_quote,
            b'(' if !in_quote => {
                let table = trimmed[..i].trim();
                let after = &trimmed[i..];
                if table.is_empty() {
                    return Err(OpenDbError::Sql(
                        "REFERENCES requires a table name before (".to_owned(),
                    ));
                }
                return Ok((table, after));
            }
            c if (c as char).is_whitespace() && !in_quote => {
                let table = trimmed[..i].trim();
                let after = trimmed[i..].trim_start();
                if table.is_empty() {
                    continue;
                }
                return Ok((table, after));
            }
            _ => {}
        }
    }
    Err(OpenDbError::Sql(format!(
        "REFERENCES expects table name then (...): {trimmed}"
    )))
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
    // Sprint 18.B: handle Drizzle's qualified `"schema"."identifier"` form by
    // peeling the schema prefix (opendb does not model schemas — every table
    // lives in the implicit default).
    if trimmed.starts_with('"') {
        if let Some((maybe_schema, rest)) = strip_quoted_segment(trimmed) {
            if rest.is_empty() {
                return maybe_schema.to_owned();
            }
            if let Some(after_dot) = rest.strip_prefix('.') {
                let after_dot = after_dot.trim();
                if let Some(stripped) = after_dot
                    .strip_prefix('"')
                    .and_then(|s| s.strip_suffix('"'))
                {
                    return stripped.to_owned();
                }
                return after_dot.to_owned();
            }
        }
    }
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

/// Sprint 15.F: like `unqualified_column_name` but preserves the qualifier
/// when present, returning `qualifier.column` (both segments unquoted).
/// Used by aggregate / GROUP BY / HAVING parsing because joined SELECTs need
/// to distinguish e.g. `folders.id` from `initiatives.id`.
fn qualified_column_name(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches(';').trim();
    if let Some((qual, col)) = trimmed.rsplit_once('.') {
        format!("{}.{}", unquote_identifier(qual), unquote_identifier(col))
    } else {
        unquote_identifier(trimmed)
    }
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
    // Sprint 18.B: PL/pgSQL `IF EXISTS (...) THEN <stmts> END IF;` is used by
    // Drizzle migrations to make rename operations idempotent. opendb does
    // not implement PL/pgSQL conditionals; we unwrap the body and force
    // `swallow_duplicate` so missing-target / already-exists errors are
    // tolerated. Same semantics for `IF NOT EXISTS(...) THEN ...`.
    let (unwrapped_text, forced_swallow) = unwrap_if_then(inner_statements_text);
    if forced_swallow {
        swallow_duplicate = true;
    }
    let inner = split_statements(unwrapped_text)
        .into_iter()
        .map(|stmt| parse(&stmt))
        .collect::<OpenDbResult<Vec<_>>>()?;
    Ok(Statement::DoBlock {
        inner,
        swallow_duplicate,
    })
}

/// Sprint 18.B: extract the body of a PL/pgSQL `IF ... THEN <body> END IF;`
/// block. Drizzle uses this for idempotent rename / drop-constraint sequences
/// inside DO $$ ... $$. Returns `(body, true)` when an IF was unwrapped
/// (caller forces swallow_duplicate), or `(original, false)` if no IF was
/// detected. Nested IFs are not supported — only the outermost one is
/// unwrapped, which matches the Drizzle pattern.
fn unwrap_if_then(text: &str) -> (&str, bool) {
    let upper = text.to_ascii_uppercase();
    let trimmed_start = upper.trim_start();
    if !trimmed_start.starts_with("IF ") && !trimmed_start.starts_with("IF\n") {
        return (text, false);
    }
    // Find `THEN` outside parens / quotes — the `IF` condition may contain
    // nested SELECT subqueries with their own parens.
    let bytes = upper.as_bytes();
    let leading = text.len() - trimmed_start.len();
    let mut i = leading + 3; // skip "IF "
    let mut depth: i32 = 0;
    let mut in_quote = false;
    let needle_then = b" THEN";
    let mut then_pos: Option<usize> = None;
    while i + needle_then.len() <= bytes.len() {
        let b = bytes[i];
        match b {
            b'\'' => in_quote = !in_quote,
            b'(' if !in_quote => depth += 1,
            b')' if !in_quote => depth -= 1,
            _ => {}
        }
        if !in_quote && depth == 0 && &bytes[i..i + needle_then.len()] == needle_then {
            then_pos = Some(i + needle_then.len());
            break;
        }
        i += 1;
    }
    let Some(then_start) = then_pos else {
        return (text, false);
    };
    // Find the matching `END IF` (case-insensitive) outside quotes.
    let needle_end_if = b"END IF";
    let mut j = then_start;
    let mut in_quote2 = false;
    while j + needle_end_if.len() <= bytes.len() {
        let b = bytes[j];
        if b == b'\'' {
            in_quote2 = !in_quote2;
        }
        if !in_quote2 && &bytes[j..j + needle_end_if.len()] == needle_end_if {
            let body = text[then_start..j].trim();
            return (body, true);
        }
        j += 1;
    }
    (text, false)
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
    // Sprint 18.A.1.2: optional `IF NOT EXISTS` clause — Drizzle emits this
    // unconditionally. We wrap the resulting CreateTable in a DoBlock so the
    // existing `swallow_duplicate` plumbing turns "table already exists" into
    // a no-op (idempotent migrations).
    let (rest, swallow_duplicate) = strip_if_not_exists(rest);
    let open = rest
        .find('(')
        .ok_or_else(|| OpenDbError::Sql("missing column list".to_owned()))?;
    // Phase A 2026-05-22: scan balanced parens to find the matching close
    // for the column list. `rfind(')')` mis-binds for statements with
    // post-table modifiers like `WITH (fillfactor=100)` (pgbench-style) or
    // `INHERITS (parent)`.
    let close = find_matching_close_paren(rest, open)?;
    if open >= close {
        return Err(OpenDbError::Sql("malformed column list".to_owned()));
    }
    let trailing = rest[close + 1..].trim();
    if !trailing.is_empty() && !is_create_table_trailing_modifier(trailing) {
        return Err(OpenDbError::Sql(format!(
            "trailing input after CREATE TABLE: {trailing}"
        )));
    }
    let table = unquote_identifier(rest[..open].trim());
    // Sprint 18.A.1.3: column list may contain table-level CONSTRAINT clauses
    // (UNIQUE / CHECK / FOREIGN KEY / composite PRIMARY KEY) whose body
    // contains commas. We must split at top-level commas only.
    let entries = split_top_level_commas(&rest[open + 1..close])?;
    let mut columns: Vec<ColumnDefinition> = Vec::with_capacity(entries.len());
    for entry in entries {
        if let Some(column) = parse_table_member(entry)? {
            columns.push(column);
        }
    }
    if table.is_empty() || columns.is_empty() {
        return Err(OpenDbError::Sql(
            "CREATE TABLE requires table and columns".to_owned(),
        ));
    }
    // Phase A 2026-05-22: PG accepts CREATE TABLE without an explicit
    // PRIMARY KEY (heap tables with internal ctid). opendb requires
    // exactly one PK column, so when the parser sees no PK declared we
    // inject a synthetic `__opendb_rowid BIGINT NOT NULL PRIMARY KEY
    // DEFAULT <auto>` at the end of the column list. The executor
    // materializes the default from a monotonic atomic counter on
    // INSERT. SELECT * stays unaffected because the column name is
    // unambiguously internal; clients that don't reference it never see
    // it.
    let has_explicit_pk = columns.iter().any(|c| c.primary_key);
    if !has_explicit_pk {
        let synthetic = ColumnDefinition::primary_key("__opendb_rowid", ColumnType::Int64)
            .with_default(DefaultExpr::AutoRowId);
        columns.push(synthetic);
    }
    let inner = Statement::CreateTable { table, columns };
    if swallow_duplicate {
        Ok(Statement::DoBlock {
            inner: vec![inner],
            swallow_duplicate: true,
        })
    } else {
        Ok(inner)
    }
}

/// Phase A 2026-05-22: walk balanced parens starting at the byte index of
/// an opening `(` and return the byte index of its matching `)`. Used so
/// `CREATE TABLE t (col1 int(4), col2 text) WITH (fillfactor=100)` matches
/// the column list close, not the trailing `WITH (...)` close.
fn find_matching_close_paren(input: &str, open_index: usize) -> OpenDbResult<usize> {
    let bytes = input.as_bytes();
    if bytes.get(open_index) != Some(&b'(') {
        return Err(OpenDbError::Sql("expected '(' at position".to_owned()));
    }
    let mut depth: usize = 0;
    let mut i = open_index;
    let mut in_single_quote = false;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            in_single_quote = !in_single_quote;
        } else if !in_single_quote {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                depth -= 1;
                if depth == 0 {
                    return Ok(i);
                }
            }
        }
        i += 1;
    }
    Err(OpenDbError::Sql(
        "missing closing paren for column list".to_owned(),
    ))
}

/// Phase A 2026-05-22: accept post-column-list modifiers that opendb does
/// not yet enforce but are common in PG-flavored DDL — `WITH (...)`,
/// `WITHOUT OIDS`, `INHERITS (...)`, `TABLESPACE ...`. Treated as no-ops
/// so pgbench / pg_dump-style CREATE TABLE statements land.
fn is_create_table_trailing_modifier(trailing: &str) -> bool {
    let upper = trailing.to_ascii_uppercase();
    upper.starts_with("WITH ")
        || upper.starts_with("WITH(")
        || upper == "WITHOUT OIDS"
        || upper.starts_with("WITHOUT OIDS")
        || upper.starts_with("INHERITS ")
        || upper.starts_with("INHERITS(")
        || upper.starts_with("TABLESPACE ")
}

/// Sprint 18.A.1.2: peel an optional `IF NOT EXISTS` (case-insensitive) off
/// the front of an SQL fragment. Returns the remainder + whether it was
/// present so the caller can wrap the resulting Statement in a DoBlock with
/// `swallow_duplicate` semantics.
fn strip_if_not_exists(rest: &str) -> (&str, bool) {
    let trimmed = rest.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("IF NOT EXISTS ") {
        (trimmed["IF NOT EXISTS ".len()..].trim_start(), true)
    } else {
        (rest, false)
    }
}

/// Sprint 18.A.1.2: peel an optional `IF EXISTS` (case-insensitive) off the
/// front of an SQL fragment. Used by `DROP CONSTRAINT IF EXISTS`. Returns
/// the remainder + whether it was present so the caller can swallow
/// "object not found" errors.
fn strip_if_exists(rest: &str) -> (&str, bool) {
    let trimmed = rest.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("IF EXISTS ") {
        (trimmed["IF EXISTS ".len()..].trim_start(), true)
    } else {
        (rest, false)
    }
}

/// Sprint 18.A.1.3: dispatch a single comma-separated entry from a CREATE
/// TABLE column list. Returns `None` for table-level CONSTRAINT clauses
/// (`CONSTRAINT "..." UNIQUE/CHECK/FOREIGN KEY/PRIMARY KEY (...)`) and bare
/// constraints (`UNIQUE (...)`, `CHECK (...)`, `FOREIGN KEY (...)`,
/// `PRIMARY KEY (...)`) — opendb does not yet enforce these at the storage
/// layer, so they're silently dropped to let Drizzle migrations land. The
/// FK constraints are typically re-added later via
/// `ALTER TABLE ADD CONSTRAINT` (which IS supported), so referential
/// integrity is preserved end-to-end on a full migration replay.
fn parse_table_member(raw: &str) -> OpenDbResult<Option<ColumnDefinition>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let upper = trimmed.to_ascii_uppercase();
    let is_table_constraint = upper.starts_with("CONSTRAINT ")
        || upper.starts_with("CONSTRAINT\"")
        || upper.starts_with("UNIQUE ")
        || upper.starts_with("UNIQUE(")
        || upper.starts_with("CHECK ")
        || upper.starts_with("CHECK(")
        || upper.starts_with("FOREIGN KEY ")
        || upper.starts_with("FOREIGN KEY(")
        || upper.starts_with("PRIMARY KEY ")
        || upper.starts_with("PRIMARY KEY(");
    if is_table_constraint {
        return Ok(None);
    }
    Ok(Some(parse_column_definition(trimmed)?))
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
                // Sprint 18.A.1.4: accept `NOW()` and `CURRENT_TIMESTAMP`
                // (Drizzle uses both interchangeably across migrations).
                if candidate.eq_ignore_ascii_case("NOW()")
                    || candidate.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
                {
                    default = Some(DefaultExpr::Now);
                    index += 2;
                } else {
                    default = Some(DefaultExpr::Const(parse_value(candidate)?));
                    index += 2;
                }
            }
            // Sprint 18.A.1.3: column-level UNIQUE — accepted but not yet
            // enforced at storage. Drizzle emits this for auth tables.
            "UNIQUE" => {
                index += 1;
            }
            // Sprint 18.A.1.3: column-level CHECK / REFERENCES — accepted as
            // no-op. Both are typically followed by either a parenthesized
            // expression (CHECK(expr)) or a table-name then a paren list
            // (REFERENCES "t"("c")). Drizzle joins the table name and
            // column list without whitespace ("t"("c")), so we consume any
            // token that contains `(` as the FK target spec.
            "CHECK" | "REFERENCES" => {
                index += 1;
                // Skip the FK target token (with or without embedded paren list).
                if index < tokens.len() {
                    index += 1;
                    // If the previous token didn't include a `(`, and the
                    // next one starts with `(`, consume it too.
                    if index < tokens.len() && tokens[index].starts_with('(') {
                        index += 1;
                    }
                }
                // Some FK references include `ON DELETE CASCADE` / `ON UPDATE
                // CASCADE` / `ON DELETE SET NULL` / `ON DELETE NO ACTION`
                // suffixes — consume them.
                while index + 1 < tokens.len() && tokens[index].eq_ignore_ascii_case("ON") {
                    index += 2; // ON {DELETE|UPDATE}
                    if index < tokens.len() {
                        // CASCADE / SET NULL / NO ACTION / RESTRICT
                        if tokens[index].eq_ignore_ascii_case("SET")
                            || tokens[index].eq_ignore_ascii_case("NO")
                        {
                            index += 2;
                        } else {
                            index += 1;
                        }
                    }
                }
            }
            _ => {
                return Err(OpenDbError::Sql(format!(
                    "unsupported column constraint on {name}"
                )));
            }
        }
    }

    // Sprint 18.A.1.3: Drizzle quotes column names (`"expires_at"`); strip
    // before storing so subsequent CREATE INDEX / FK lookups using bare
    // identifiers can resolve them.
    let bare_name = unquote_identifier(name);
    let definition = if primary_key {
        let mut pk = ColumnDefinition::primary_key(&bare_name, data_type);
        pk.default = default;
        pk
    } else {
        ColumnDefinition {
            name: bare_name,
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
    // Phase A 2026-05-22: strip a trailing `(N)` or `(N, M)` from the head
    // token so `char(22)`, `varchar(255)`, `numeric(10,2)`, `decimal(8,2)`
    // resolve to their base type. The length / precision parameters are
    // dropped — opendb stores TEXT untruncated and NUMERIC as Float64.
    // Keeping the dropped-parameters behavior matches what pgbench / Drizzle
    // expect at the protocol level (the column reads back as text / number)
    // even though we don't enforce the constraint.
    let head_token = tokens[0].as_str();
    let head_root_end = head_token.find('(').unwrap_or(head_token.len());
    let head_root = head_token[..head_root_end].to_ascii_uppercase();
    match head_root.as_str() {
        "INT" | "INTEGER" | "INT64" | "BIGINT" | "INT2" | "INT4" | "INT8" | "SMALLINT"
        | "SERIAL" | "BIGSERIAL" => Ok((ColumnType::Int64, 1)),
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER" | "BPCHAR" | "STRING" => {
            Ok((ColumnType::Text, 1))
        }
        "BOOL" | "BOOLEAN" => Ok((ColumnType::Bool, 1)),
        "FLOAT8" | "FLOAT64" | "FLOAT4" | "REAL" | "NUMERIC" | "DECIMAL" => {
            Ok((ColumnType::Float64, 1))
        }
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
        "CHARACTER" if tokens.len() >= 2 && tokens[1].eq_ignore_ascii_case("VARYING") => {
            Ok((ColumnType::Text, 2))
        }
        "TIMESTAMP" | "TIMESTAMPTZ" | "DATE" | "TIME" => Ok((ColumnType::Timestamp, 1)),
        "JSON" | "JSONB" => Ok((ColumnType::Json, 1)),
        _ => Err(OpenDbError::Sql(format!(
            "unsupported column type: {}",
            tokens[0]
        ))),
    }
}

fn parse_insert(sql: &str) -> OpenDbResult<Statement> {
    // Phase A 2026-05-22: accept ` VALUES ` (canonical), ` VALUES(` (no
    // space before paren, pgbench style), and ` VALUES\n` (newline-after,
    // pg_dump style). We find ` VALUES` and verify the next char is
    // whitespace or `(`. After the keyword, advance past any whitespace
    // before parsing the tuple list.
    let upper = sql.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let mut values_pos: Option<usize> = None;
    let mut search_from = 0usize;
    while let Some(rel) = upper[search_from..].find(" VALUES") {
        let pos = search_from + rel;
        let after = pos + " VALUES".len();
        let next = bytes.get(after).copied();
        if next.is_none()
            || next == Some(b'(')
            || matches!(next, Some(c) if c.is_ascii_whitespace())
        {
            values_pos = Some(pos);
            break;
        }
        search_from = pos + 1;
    }
    let values_pos =
        values_pos.ok_or_else(|| OpenDbError::Sql("INSERT requires VALUES".to_owned()))?;
    let values_marker_len = " VALUES".len();
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
    let values_part = sql[values_pos + values_marker_len..].trim_start();
    let values_part = values_part.trim();
    // Sprint 16.A: peel an optional `RETURNING ...` suffix off the end first
    // so the rest of the parser keeps its strict "trailing input is an error"
    // contract.
    let (values_part, returning) = split_off_returning(values_part)?;
    // Sprint 18.B: also peel an optional `ON CONFLICT ... DO NOTHING` /
    // `ON CONFLICT ... DO UPDATE SET ...` suffix. opendb does not yet
    // implement upsert semantics; we ignore the clause and let the
    // underlying INSERT either succeed (no duplicate) or fail at the PK
    // uniqueness check. Drizzle migrations use `DO NOTHING` to seed default
    // rows idempotently — wrap the parsed INSERT in a DoBlock with
    // swallow_duplicate so a re-run is a no-op.
    let (values_part, swallow_conflict) = split_off_on_conflict(values_part);
    let values_part = values_part.trim();
    // Sprint 18.B: support multi-row INSERT `VALUES (a,b),(c,d),...`.
    // Drizzle uses this any time `db.insert(t).values([...])` is called with
    // an array. Extract each top-level `(...)` tuple as a separate row.
    let row_tuples = extract_value_tuples(values_part)?;
    if row_tuples.is_empty() {
        return Err(OpenDbError::Sql("missing values open paren".to_owned()));
    }
    // Single-row path keeps the legacy behaviour. Multi-row path emits a
    // DoBlock of single-row Inserts so the downstream pipeline (which
    // assumes one statement / one mutation per `Insert`) works unchanged.
    if row_tuples.len() > 1 {
        let mut inner: Vec<Statement> = Vec::with_capacity(row_tuples.len());
        for tuple in &row_tuples {
            let raw_values = split_values(tuple)?;
            let (filtered_values, filtered_columns) =
                filter_default_columns(&raw_values, columns.as_ref())?;
            inner.push(Statement::Insert {
                table: table.clone(),
                columns: filtered_columns,
                values: filtered_values,
                returning: returning.clone(),
            });
        }
        return Ok(Statement::DoBlock {
            inner,
            swallow_duplicate: swallow_conflict,
        });
    }
    let raw_values = split_values(row_tuples[0])?;
    let (values, filtered_columns) = filter_default_columns(&raw_values, columns.as_ref())?;
    let columns = filtered_columns.or(columns);
    let inner = Statement::Insert {
        table,
        columns,
        values,
        returning,
    };
    if swallow_conflict {
        Ok(Statement::DoBlock {
            inner: vec![inner],
            swallow_duplicate: true,
        })
    } else {
        Ok(inner)
    }
}

/// Sprint 18.B: Drizzle emits an explicit `DEFAULT` keyword for omitted
/// columns inside a VALUES tuple. Strip those slots from both the value
/// vector and the named-column list so `materialize_insert_values` re-fills
/// the gap from the column's DEFAULT clause. Used by both single-row and
/// multi-row insert paths.
fn filter_default_columns(
    raw_values: &[&str],
    columns: Option<&Vec<String>>,
) -> OpenDbResult<(Vec<Value>, Option<Vec<String>>)> {
    let mut filtered_values: Vec<Value> = Vec::with_capacity(raw_values.len());
    let mut filtered_columns: Option<Vec<String>> = columns.map(|c| Vec::with_capacity(c.len()));
    for (idx, raw) in raw_values.iter().enumerate() {
        let trimmed = raw.trim();
        if trimmed.eq_ignore_ascii_case("DEFAULT") {
            if columns.is_none() {
                return Err(OpenDbError::Sql(
                    "DEFAULT in unnamed VALUES tuple is not supported".to_owned(),
                ));
            }
            continue;
        }
        filtered_values.push(parse_value(raw)?);
        if let (Some(src), Some(dst)) = (columns, filtered_columns.as_mut()) {
            if let Some(name) = src.get(idx) {
                dst.push(name.clone());
            }
        }
    }
    Ok((filtered_values, filtered_columns))
}

/// Sprint 18.B: parse the VALUES tail into individual row tuples. Accepts
/// `(a,b)`, `(a,b),(c,d)`, optional whitespace between tuples, and returns
/// each tuple's interior content (without the surrounding parens). The
/// caller is responsible for further parsing each tuple via `split_values`.
fn extract_value_tuples(input: &str) -> OpenDbResult<Vec<&str>> {
    let trimmed = input.trim();
    let bytes = trimmed.as_bytes();
    let mut tuples: Vec<&str> = Vec::new();
    let mut i = 0usize;
    let mut in_quote = false;
    while i < bytes.len() {
        while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b',') {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        if bytes[i] != b'(' {
            return Err(OpenDbError::Sql(format!(
                "trailing input after INSERT VALUES tuples: {}",
                &trimmed[i..]
            )));
        }
        let start = i + 1;
        i += 1;
        let mut depth = 1i32;
        while i < bytes.len() && depth > 0 {
            match bytes[i] {
                b'\'' => in_quote = !in_quote,
                b'(' if !in_quote => depth += 1,
                b')' if !in_quote => depth -= 1,
                _ => {}
            }
            i += 1;
        }
        if depth != 0 {
            return Err(OpenDbError::Sql(
                "unterminated VALUES tuple (unbalanced parens)".to_owned(),
            ));
        }
        let end = i - 1;
        tuples.push(&trimmed[start..end]);
    }
    if in_quote {
        return Err(OpenDbError::Sql(
            "unterminated quoted literal in VALUES".to_owned(),
        ));
    }
    Ok(tuples)
}

/// Sprint 18.B: split an SQL fragment on a trailing `ON CONFLICT ...` clause
/// (case-insensitive, outside quotes/parens). Treats both `DO NOTHING` and
/// `DO UPDATE SET ...` forms as a single boolean flag — we don't actually
/// upsert, we just swallow duplicate-key errors at execute time.
fn split_off_on_conflict(input: &str) -> (&str, bool) {
    let upper = input.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let mut depth: i32 = 0;
    let mut in_quote = false;
    let needle = b" ON CONFLICT";
    let mut found: Option<usize> = None;
    for i in 0..bytes.len() {
        match bytes[i] {
            b'\'' => in_quote = !in_quote,
            b'(' if !in_quote => depth += 1,
            b')' if !in_quote => depth -= 1,
            _ => {}
        }
        if !in_quote
            && depth == 0
            && i + needle.len() <= bytes.len()
            && &bytes[i..i + needle.len()] == needle
        {
            found = Some(i);
            break;
        }
    }
    match found {
        Some(pos) => (&input[..pos], true),
        None => (input, false),
    }
}

/// Sprint 16.A: split an SQL fragment on the trailing `RETURNING ...` clause
/// (case-insensitive, outside quotes/parens). Returns `(rest, None)` if no
/// `RETURNING` is present. The returned `rest` is left un-trimmed because
/// callers reuse their own trim/strip semantics.
fn split_off_returning(input: &str) -> OpenDbResult<(&str, Option<ReturningClause>)> {
    let upper = input.to_ascii_uppercase();
    // Look for ` RETURNING ` outside quotes/parens; scan right-to-left so we
    // pick the actual top-level clause if a literal contains the substring.
    let bytes = upper.as_bytes();
    let mut depth: i32 = 0;
    let mut in_quote = false;
    let mut found: Option<usize> = None;
    let needle = b" RETURNING ";
    for i in 0..bytes.len() {
        match bytes[i] {
            b'\'' => in_quote = !in_quote,
            b'(' if !in_quote => depth += 1,
            b')' if !in_quote => depth -= 1,
            _ => {}
        }
        if !in_quote
            && depth == 0
            && i + needle.len() <= bytes.len()
            && &bytes[i..i + needle.len()] == needle
        {
            found = Some(i);
        }
    }
    let Some(pos) = found else {
        return Ok((input, None));
    };
    let head = &input[..pos];
    let tail = input[pos + needle.len()..].trim();
    if tail.is_empty() {
        return Err(OpenDbError::Sql(
            "RETURNING requires at least one column or *".to_owned(),
        ));
    }
    let returning = if tail == "*" {
        ReturningClause::Star
    } else {
        let columns = split_top_level_commas(tail)?
            .into_iter()
            .map(|t| {
                let trimmed = t.trim();
                if trimmed.is_empty() {
                    Err(OpenDbError::Sql(
                        "RETURNING column list contains empty entry".to_owned(),
                    ))
                } else {
                    Ok(qualified_column_name(trimmed))
                }
            })
            .collect::<OpenDbResult<Vec<String>>>()?;
        ReturningClause::Columns(columns)
    };
    Ok((head, Some(returning)))
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
    // Sprint 18.B: accept `now()` and `current_timestamp` as wall-clock
    // sentinels inside VALUES tuples. The executor's coerce_value will pass
    // these through to TIMESTAMP columns; here we resolve them to a fresh
    // microsecond reading so each INSERT row gets a real timestamp.
    if trimmed.eq_ignore_ascii_case("NOW()")
        || trimmed.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
        || trimmed.eq_ignore_ascii_case("CURRENT_TIMESTAMP()")
    {
        let micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        return Ok(Value::Timestamp(micros));
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
    // Sprint 15.C: HAVING sits between GROUP BY and ORDER BY in SQL grammar
    // ("...GROUP BY <cols> HAVING <preds> [ORDER BY ...]"). We already stripped
    // ORDER BY / LIMIT / OFFSET so HAVING is now trailing.
    let (rest, having_text) = take_trailing_keyword(&rest, " HAVING ");
    // Sprint 15: extract optional GROUP BY clause sitting between WHERE and HAVING.
    let (rest, group_by_text) = take_trailing_keyword(&rest, " GROUP BY ");

    let upper_rest = rest.to_ascii_uppercase();
    let (table, predicates) = if let Some(where_pos) = upper_rest.find(" WHERE ") {
        let table = rest[..where_pos].trim();
        let predicate_text = rest[where_pos + " WHERE ".len()..].trim();
        let predicates = parse_predicate_conjunction(predicate_text)?;
        (table, predicates)
    } else {
        (rest.trim(), Vec::new())
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
    let group_by = match group_by_text.as_deref() {
        Some(text) => parse_group_by(text)?,
        None => Vec::new(),
    };
    let having = match having_text.as_deref() {
        Some(text) => parse_having(text)?,
        None => Vec::new(),
    };
    Ok(Statement::SelectAll {
        table: unquote_identifier(table),
        predicate: predicates,
        order_by,
        limit,
        offset,
        columns: SelectColumns::Star,
        group_by,
        having,
    })
}

/// Sprint 15.C: parse a HAVING clause as a conjunction of agg-or-column
/// comparison predicates. Mirrors `parse_predicate_conjunction` but the LHS is
/// allowed to be `count(*)` / `sum(c)` / etc.
fn parse_having(text: &str) -> OpenDbResult<Vec<HavingPredicate>> {
    let parts = split_top_level_and(text)?;
    parts
        .into_iter()
        .map(parse_having_predicate)
        .collect::<OpenDbResult<Vec<_>>>()
}

fn parse_having_predicate(raw: &str) -> OpenDbResult<HavingPredicate> {
    let trimmed = raw.trim();
    // `find_first_comparison_op` walks left-to-right; but it doesn't skip over
    // parens, so for `count(*) > 5` the first `>` is fine. To be safe against
    // future regression on `sum(x)`-style operators in args, we look for the
    // first comparator that sits at top-level paren depth.
    let (op, op_pos, op_len) = find_first_comparison_op_outside_parens(trimmed)?;
    let lhs = trimmed[..op_pos].trim();
    let rhs = trimmed[op_pos + op_len..].trim();
    let expr = parse_aggregate_or_column(lhs)?;
    let value = parse_value(rhs)?;
    Ok(HavingPredicate { expr, op, value })
}

/// Sprint 15.C: like `find_first_comparison_op` but only matches operators
/// that sit outside any `(...)` group. Used by HAVING because the LHS may be
/// `count(*)` / `sum(col)`.
fn find_first_comparison_op_outside_parens(
    raw: &str,
) -> OpenDbResult<(crate::ast::WhereOp, usize, usize)> {
    let bytes = raw.as_bytes();
    let mut in_quote = false;
    let mut depth: i32 = 0;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' {
            in_quote = !in_quote;
            i += 1;
            continue;
        }
        if !in_quote {
            if c == b'(' {
                depth += 1;
                i += 1;
                continue;
            }
            if c == b')' {
                depth -= 1;
                i += 1;
                continue;
            }
            if depth == 0 {
                if i + 2 <= bytes.len() {
                    let two = &bytes[i..i + 2];
                    if two == b"!=" {
                        return Ok((WhereOp::NotEq, i, 2));
                    }
                    if two == b"<=" {
                        return Ok((WhereOp::Lte, i, 2));
                    }
                    if two == b">=" {
                        return Ok((WhereOp::Gte, i, 2));
                    }
                    if two == b"<>" {
                        return Ok((WhereOp::NotEq, i, 2));
                    }
                }
                if c == b'=' {
                    return Ok((WhereOp::Eq, i, 1));
                }
                if c == b'<' {
                    return Ok((WhereOp::Lt, i, 1));
                }
                if c == b'>' {
                    return Ok((WhereOp::Gt, i, 1));
                }
            }
        }
        i += 1;
    }
    Err(OpenDbError::Sql(format!(
        "HAVING predicate missing comparison: {raw}"
    )))
}

/// Sprint 15: parse `GROUP BY <col1>[, <col2> ...]`. Identifiers may be quoted
/// or qualified (`t.col`) — we strip the qualifier the same way bare projection
/// columns do.
fn parse_group_by(text: &str) -> OpenDbResult<Vec<String>> {
    let tokens = split_top_level_commas(text)?;
    let mut columns = Vec::with_capacity(tokens.len());
    for token in tokens {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            return Err(OpenDbError::Sql("empty GROUP BY column".to_owned()));
        }
        // Sprint 15.F: keep the qualifier so joined SELECT can distinguish
        // `folders.id` from `initiatives.id`. The simple-table aggregator
        // strips qualifiers via `column_basename` at lookup time.
        columns.push(qualified_column_name(trimmed));
    }
    if columns.is_empty() {
        return Err(OpenDbError::Sql(
            "GROUP BY requires at least one column".to_owned(),
        ));
    }
    Ok(columns)
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
        let tokens = split_top_level_commas(columns_text)?;
        // Sprint 15: if any token references an aggregate function, parse the
        // whole projection as an aggregated query. Otherwise fall through to
        // the legacy column-list path.
        let aggregated = tokens.iter().any(|token| token_is_aggregate(token));
        let synthetic = format!("SELECT * FROM {after_from}");
        let parsed = parse_select_all(&synthetic)?;
        // Build the desired SelectColumns once; reuse for both joined and
        // simple variants below.
        let new_columns = if aggregated {
            let items = tokens
                .iter()
                .map(|token| parse_aggregate_select_item(token))
                .collect::<OpenDbResult<Vec<AggregateSelectItem>>>()?;
            SelectColumns::Aggregated(AggregateProjection { items })
        } else {
            // Sprint 19.A: keep the qualifier (e.g., `organizations.name`)
            // so multi-JOIN projections that surface two columns with the
            // same bare suffix (`folders.name` vs `organizations.name`) can
            // be resolved unambiguously by the executor.
            let columns = tokens
                .into_iter()
                .map(|token| {
                    let trimmed = token.trim();
                    if trimmed.is_empty() || trimmed.split_whitespace().count() != 1 {
                        Err(OpenDbError::Sql(format!(
                            "invalid SELECT column: {trimmed}"
                        )))
                    } else {
                        Ok(qualified_column_name(trimmed))
                    }
                })
                .collect::<OpenDbResult<Vec<String>>>()?;
            if columns.is_empty() {
                return Err(OpenDbError::Sql(
                    "SELECT projection must not be empty".to_owned(),
                ));
            }
            SelectColumns::Explicit(columns)
        };
        match parsed {
            Statement::SelectAll {
                table,
                predicate,
                order_by,
                limit,
                offset,
                group_by,
                having,
                ..
            } => {
                return Ok(Statement::SelectAll {
                    table,
                    predicate,
                    order_by,
                    limit,
                    offset,
                    columns: new_columns,
                    group_by,
                    having,
                });
            }
            // Sprint 15.F: joined SELECT with explicit/aggregated projection.
            Statement::Select {
                left,
                joins,
                where_clause,
                order_by,
                limit,
                offset,
                group_by,
                having,
                ..
            } => {
                return Ok(Statement::Select {
                    left,
                    joins,
                    where_clause,
                    order_by,
                    limit,
                    offset,
                    columns: new_columns,
                    group_by,
                    having,
                });
            }
            _ => {
                return Err(OpenDbError::Sql(
                    "internal: SELECT projection inner parse mismatch".to_owned(),
                ));
            }
        }
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

/// Sprint 15: cheap detection of an aggregate token. Strips an optional `AS
/// <alias>` suffix and checks whether the remaining expression starts with a
/// recognised aggregate function name followed by `(`.
fn token_is_aggregate(token: &str) -> bool {
    let trimmed = token.trim();
    let upper = trimmed.to_ascii_uppercase();
    // Strip trailing ` AS <alias>` (or bare alias, but Sprint 15 requires AS).
    let head = match upper.find(" AS ") {
        Some(pos) => upper[..pos].trim(),
        None => upper.as_str(),
    };
    for name in ["COUNT(", "SUM(", "MAX(", "MIN(", "AVG("] {
        if head.starts_with(name) {
            return true;
        }
    }
    false
}

/// Sprint 15: parse a single item from an aggregated projection. The item is
/// either an aggregate expression (`COUNT(*)`, `SUM(amount)`, ...) or a bare
/// column reference that participates in `GROUP BY`.
fn parse_aggregate_select_item(raw: &str) -> OpenDbResult<AggregateSelectItem> {
    let trimmed = raw.trim();
    let upper = trimmed.to_ascii_uppercase();
    let (expr_text, alias) = if let Some(as_pos) = upper.find(" AS ") {
        let expr_text = trimmed[..as_pos].trim();
        let alias = trimmed[as_pos + 4..].trim().to_owned();
        if alias.is_empty() {
            return Err(OpenDbError::Sql("alias after AS is required".to_owned()));
        }
        (expr_text, Some(unquote_identifier(&alias)))
    } else {
        (trimmed, None)
    };
    let expr = parse_aggregate_or_column(expr_text)?;
    Ok(AggregateSelectItem { expr, alias })
}

fn parse_aggregate_or_column(raw: &str) -> OpenDbResult<AggregateOrColumn> {
    let trimmed = raw.trim();
    let upper = trimmed.to_ascii_uppercase();
    for (name, func) in [
        ("COUNT(", AggregateFunction::Count),
        ("SUM(", AggregateFunction::Sum),
        ("MAX(", AggregateFunction::Max),
        ("MIN(", AggregateFunction::Min),
        ("AVG(", AggregateFunction::Avg),
    ] {
        if upper.starts_with(name) && trimmed.ends_with(')') {
            let inner = trimmed[name.len()..trimmed.len() - 1].trim();
            let arg = if inner == "*" {
                if !matches!(func, AggregateFunction::Count) {
                    return Err(OpenDbError::Sql(format!(
                        "{name}*) is only valid for COUNT"
                    )));
                }
                AggregateArg::Star
            } else if inner.is_empty() {
                return Err(OpenDbError::Sql(format!("{name}) is missing an argument")));
            } else {
                AggregateArg::Column(qualified_column_name(inner))
            };
            return Ok(AggregateOrColumn::Aggregate(AggregateExpr { func, arg }));
        }
    }
    // Bare column reference (must be in GROUP BY at execution time).
    if trimmed.is_empty() || trimmed.split_whitespace().count() != 1 {
        return Err(OpenDbError::Sql(format!("invalid SELECT item: {trimmed}")));
    }
    Ok(AggregateOrColumn::Column(qualified_column_name(trimmed)))
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
    // Sprint 15.F: HAVING + GROUP BY also sit between WHERE and ORDER BY in a
    // joined SELECT (same grammar as the simple SELECT path).
    let (rest, having_text) = take_trailing_keyword(&rest, " HAVING ");
    let (rest, group_by_text) = take_trailing_keyword(&rest, " GROUP BY ");
    let upper_rest = rest.to_ascii_uppercase();
    let (rest, where_text) = if let Some(pos) = upper_rest.find(" WHERE ") {
        let head = rest[..pos].trim_end().to_owned();
        let tail = rest[pos + " WHERE ".len()..].trim().to_owned();
        (head, Some(tail))
    } else {
        (rest, None)
    };

    // Sprint 18.C.1: walk all JOIN clauses left-to-right, building a chain.
    // Recognised keywords (in priority order): ` INNER JOIN `, ` LEFT JOIN `,
    // bare ` JOIN ` (treated as INNER per SQL spec).
    let join_positions = find_join_keyword_positions(&rest);
    if join_positions.is_empty() {
        return Err(OpenDbError::Sql("expected JOIN clause".to_owned()));
    }
    let left_table = unquote_identifier(rest[..join_positions[0].pos].trim());
    let known_tables: Vec<String> = std::iter::once(left_table.clone())
        .chain(join_positions.iter().filter_map(|hit| {
            // We'll fill these in once we extract right_table per join below.
            let _ = hit;
            None
        }))
        .collect();
    let _ = known_tables;
    // Compute the end of each join-segment as the start of the next join
    // keyword (or end-of-input for the last segment).
    let mut joins: Vec<JoinClause> = Vec::with_capacity(join_positions.len());
    let mut tables_seen: Vec<String> = vec![left_table.clone()];
    for (i, hit) in join_positions.iter().enumerate() {
        let seg_start = hit.pos + hit.keyword.len();
        let seg_end = join_positions
            .get(i + 1)
            .map(|h| h.pos)
            .unwrap_or(rest.len());
        let segment = rest[seg_start..seg_end].trim();
        let upper_segment = segment.to_ascii_uppercase();
        let on_pos = upper_segment
            .find(" ON ")
            .ok_or_else(|| OpenDbError::Sql("join requires ON".to_owned()))?;
        let right_table = unquote_identifier(segment[..on_pos].trim());
        let on_expr = segment[on_pos + " ON ".len()..].trim();
        let on_unwrapped = strip_optional_outer_parens(on_expr);
        let on_parts = split_top_level_and(on_unwrapped)?;
        let mut equi_join: Option<(QualifiedColumn, QualifiedColumn)> = None;
        let mut extra: Vec<JoinedPredicate> = Vec::new();
        for part in on_parts {
            let part = part.trim();
            if let Ok((lhs, rhs)) = parse_join_equality(part) {
                let both_qualified = lhs.qualifier.is_some() && rhs.qualifier.is_some();
                if both_qualified && equi_join.is_none() {
                    equi_join = Some((lhs, rhs));
                    continue;
                }
            }
            let pred = parse_joined_predicate(part)?;
            extra.push(pred);
        }
        let (qual_a, qual_b) = equi_join.ok_or_else(|| {
            OpenDbError::Sql(format!("JOIN ON requires an equi-join clause: {on_expr}"))
        })?;
        // For a chained join, the "left side" of this equi-join may be any
        // previously-seen table (T1.col = Tnew.col), and the "right side"
        // must be the newly-introduced right_table. Normalize so the
        // JoinClause stores left_column from a previously-seen table and
        // right_column from `right_table`.
        let a_is_prev = qual_a
            .qualifier
            .as_deref()
            .map(|q| tables_seen.iter().any(|t| t == q))
            .unwrap_or(false);
        let b_is_prev = qual_b
            .qualifier
            .as_deref()
            .map(|q| tables_seen.iter().any(|t| t == q))
            .unwrap_or(false);
        let (left_column, right_column) = match (a_is_prev, b_is_prev) {
            (true, _) if qual_b.qualifier.as_deref() == Some(right_table.as_str()) => {
                (qual_a.column, qual_b.column)
            }
            (_, true) if qual_a.qualifier.as_deref() == Some(right_table.as_str()) => {
                (qual_b.column, qual_a.column)
            }
            _ => {
                return Err(OpenDbError::Sql(format!(
                    "JOIN ON clause must reference a previously-seen table and {right_table}"
                )));
            }
        };
        joins.push(JoinClause {
            kind: hit.kind,
            right: right_table.clone(),
            left_column,
            right_column,
            extra,
        });
        tables_seen.push(right_table);
    }

    let where_clause = match where_text {
        Some(text) => {
            // Sprint 18.C.1: Drizzle wraps multi-clause WHERE in parens,
            // e.g. `WHERE ("a" = 1 AND "b" = 2)`. Strip the outer parens
            // before splitting so the conjunction is visible at top level.
            let unwrapped = strip_optional_outer_parens(text.trim()).to_owned();
            split_top_level_and(&unwrapped)?
                .into_iter()
                .map(parse_joined_predicate)
                .collect::<OpenDbResult<Vec<_>>>()?
        }
        None => Vec::new(),
    };
    let order_by = match order_by_text {
        Some(text) => Some(parse_joined_order_by(&text)?),
        None => None,
    };

    let group_by = match group_by_text.as_deref() {
        Some(text) => parse_group_by(text)?,
        None => Vec::new(),
    };
    let having = match having_text.as_deref() {
        Some(text) => parse_having(text)?,
        None => Vec::new(),
    };
    Ok(Statement::Select {
        left: left_table,
        joins,
        where_clause,
        order_by,
        limit,
        offset,
        columns: SelectColumns::Star,
        group_by,
        having,
    })
}

/// Sprint 18.C.1: find every JOIN-keyword position in a clause. Returns hits
/// sorted by `pos` ascending so callers can slice in order. `INNER JOIN` and
/// `LEFT JOIN` are matched before bare `JOIN` so we don't double-count
/// `INNER JOIN` as a bare JOIN.
struct JoinKeywordHit {
    pos: usize,
    kind: JoinKind,
    keyword: &'static str,
}
fn find_join_keyword_positions(rest: &str) -> Vec<JoinKeywordHit> {
    let upper = rest.to_ascii_uppercase();
    let mut hits: Vec<JoinKeywordHit> = Vec::new();
    let bytes = upper.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        // ` INNER JOIN ` (12 bytes)
        if i + 12 <= bytes.len() && &bytes[i..i + 12] == b" INNER JOIN " {
            hits.push(JoinKeywordHit {
                pos: i,
                kind: JoinKind::Inner,
                keyword: " INNER JOIN ",
            });
            i += 12;
            continue;
        }
        // ` LEFT JOIN ` (11 bytes)
        if i + 11 <= bytes.len() && &bytes[i..i + 11] == b" LEFT JOIN " {
            hits.push(JoinKeywordHit {
                pos: i,
                kind: JoinKind::Left,
                keyword: " LEFT JOIN ",
            });
            i += 11;
            continue;
        }
        // ` JOIN ` (6 bytes) — only when not preceded by INNER/LEFT keyword.
        if i + 6 <= bytes.len() && &bytes[i..i + 6] == b" JOIN " {
            hits.push(JoinKeywordHit {
                pos: i,
                kind: JoinKind::Inner,
                keyword: " JOIN ",
            });
            i += 6;
            continue;
        }
        i += 1;
    }
    hits
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
            qualifier: Some(unquote_identifier(qualifier.trim())),
            column: unquote_identifier(column.trim()),
        })
    } else {
        Ok(QualifiedColumn {
            qualifier: None,
            column: unquote_identifier(trimmed),
        })
    }
}

/// Sprint 15.F: if `expr` is wrapped in a single matching pair of parens that
/// span the whole expression, strip them. Drizzle wraps `ON` conjunctions
/// like `ON (a = b AND c = d)`. No-op for `a = b`.
fn strip_optional_outer_parens(expr: &str) -> &str {
    let trimmed = expr.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return trimmed;
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    // Verify the outer parens actually balance across the entire span: walk
    // and ensure depth never drops to 0 before the end.
    let mut depth: i32 = 1;
    let mut in_quote = false;
    let bytes = inner.as_bytes();
    for (i, ch) in bytes.iter().enumerate() {
        match *ch {
            b'\'' => in_quote = !in_quote,
            b'(' if !in_quote => depth += 1,
            b')' if !in_quote => {
                depth -= 1;
                if depth == 0 && i + 1 != bytes.len() {
                    return trimmed; // outer parens don't span the whole expr
                }
            }
            _ => {}
        }
    }
    inner.trim()
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
    // Sprint 14: support `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=` as operators.
    // Sprint 14.C also handles `col IN (v1, v2, ...)`, `col IS NULL`, and
    // `col IS NOT NULL` as alternative predicate shapes.
    let trimmed = raw.trim();
    let upper = trimmed.to_ascii_uppercase();
    if let Some(in_pos) = find_keyword_outside_quotes(trimmed, " IN ") {
        let column_text = trimmed[..in_pos].trim();
        let rest = trimmed[in_pos + " IN ".len()..].trim();
        let open = rest
            .find('(')
            .ok_or_else(|| OpenDbError::Sql("IN expects `(`".to_owned()))?;
        let close = rest
            .rfind(')')
            .ok_or_else(|| OpenDbError::Sql("IN expects `)`".to_owned()))?;
        if open >= close {
            return Err(OpenDbError::Sql("malformed IN list".to_owned()));
        }
        let values_text = &rest[open + 1..close];
        let values = split_top_level_commas(values_text)?
            .into_iter()
            .map(parse_value)
            .collect::<OpenDbResult<Vec<Value>>>()?;
        if values.is_empty() {
            return Err(OpenDbError::Sql("IN list must not be empty".to_owned()));
        }
        return Ok(Predicate {
            column: unqualified_column_name(column_text),
            value: Value::Null,
            op: crate::ast::WhereOp::In(values),
        });
    }
    if let Some(is_pos) = find_keyword_outside_quotes(trimmed, " IS ") {
        let column_text = trimmed[..is_pos].trim();
        let rest_upper = upper[is_pos + " IS ".len()..].trim();
        if rest_upper == "NULL" {
            return Ok(Predicate {
                column: unqualified_column_name(column_text),
                value: Value::Null,
                op: crate::ast::WhereOp::IsNull,
            });
        }
        if rest_upper == "NOT NULL" {
            return Ok(Predicate {
                column: unqualified_column_name(column_text),
                value: Value::Null,
                op: crate::ast::WhereOp::IsNotNull,
            });
        }
        return Err(OpenDbError::Sql(format!(
            "unsupported IS form in WHERE predicate: {raw}"
        )));
    }
    parse_predicate_with_op(raw)
}

/// Sprint 14.B: parse a conjunctive WHERE clause (`a = 1 AND b > 2 AND ...`).
fn parse_predicate_conjunction(raw: &str) -> OpenDbResult<Vec<Predicate>> {
    // Sprint 19.A: Drizzle wraps conjunctions in parens (`(a=1 AND b=2)`);
    // strip them before splitting on AND so the parts are recognised.
    let unwrapped = strip_optional_outer_parens(raw.trim());
    split_top_level_and(unwrapped)?
        .into_iter()
        .map(|part| parse_predicate(part.trim()))
        .collect()
}

fn split_top_level_and(raw: &str) -> OpenDbResult<Vec<&str>> {
    let mut parts = Vec::new();
    let upper = raw.to_ascii_uppercase();
    let bytes = raw.as_bytes();
    let mut start = 0usize;
    let mut in_quote = false;
    let mut paren_depth: i32 = 0;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' {
            in_quote = !in_quote;
            i += 1;
            continue;
        }
        if !in_quote {
            if c == b'(' {
                paren_depth += 1;
            } else if c == b')' {
                paren_depth -= 1;
            }
            if paren_depth == 0
                && i + 5 <= bytes.len()
                && upper.as_bytes().get(i..i + 5) == Some(b" AND ".as_slice())
            {
                parts.push(raw[start..i].trim());
                start = i + 5;
                i += 5;
                continue;
            }
        }
        i += 1;
    }
    if in_quote {
        return Err(OpenDbError::Sql("unterminated quoted literal".to_owned()));
    }
    parts.push(raw[start..].trim());
    Ok(parts.into_iter().filter(|p| !p.is_empty()).collect())
}

/// Sprint 14: parse a single predicate with an explicit comparison
/// operator (`=`, `!=`, `<`, `<=`, `>`, `>=`). Used by composite WHERE
/// parsing — single-equality predicates still flow through
/// `parse_predicate` for backwards compatibility.
fn parse_predicate_with_op(raw: &str) -> OpenDbResult<Predicate> {
    let (op, op_pos, op_len) = find_first_comparison_op(raw)?;
    let column = raw[..op_pos].trim();
    let value_text = raw[op_pos + op_len..].trim();
    if column.is_empty() || value_text.is_empty() {
        return Err(OpenDbError::Sql(format!(
            "WHERE predicate requires column and literal: {raw}"
        )));
    }
    Ok(Predicate {
        column: unqualified_column_name(column),
        value: parse_value(value_text)?,
        op,
    })
}

fn find_first_comparison_op(raw: &str) -> OpenDbResult<(crate::ast::WhereOp, usize, usize)> {
    use crate::ast::WhereOp;
    let bytes = raw.as_bytes();
    let mut in_quote = false;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' {
            in_quote = !in_quote;
            i += 1;
            continue;
        }
        if !in_quote {
            // Order matters: longer operators first.
            if i + 2 <= bytes.len() {
                let two = &bytes[i..i + 2];
                if two == b"!=" {
                    return Ok((WhereOp::NotEq, i, 2));
                }
                if two == b"<=" {
                    return Ok((WhereOp::Lte, i, 2));
                }
                if two == b">=" {
                    return Ok((WhereOp::Gte, i, 2));
                }
                if two == b"<>" {
                    return Ok((WhereOp::NotEq, i, 2));
                }
            }
            if c == b'=' {
                return Ok((WhereOp::Eq, i, 1));
            }
            if c == b'<' {
                return Ok((WhereOp::Lt, i, 1));
            }
            if c == b'>' {
                return Ok((WhereOp::Gt, i, 1));
            }
        }
        i += 1;
    }
    Err(OpenDbError::Sql(format!(
        "WHERE predicate has no comparison operator: {raw}"
    )))
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
    fn parses_drop_table_single() {
        assert_eq!(
            parse("DROP TABLE foo").expect("drop"),
            Statement::DropTable {
                table: "foo".to_owned(),
                if_exists: false,
            }
        );
        assert_eq!(
            parse("DROP TABLE IF EXISTS foo;").expect("drop if exists"),
            Statement::DropTable {
                table: "foo".to_owned(),
                if_exists: true,
            }
        );
    }

    #[test]
    fn parses_drop_table_multi_explodes_to_do_block() {
        let parsed = parse("DROP TABLE IF EXISTS a, b, c").expect("drop multi");
        match parsed {
            Statement::DoBlock {
                inner,
                swallow_duplicate,
            } => {
                assert!(!swallow_duplicate);
                assert_eq!(inner.len(), 3);
                assert_eq!(
                    inner[0],
                    Statement::DropTable {
                        table: "a".to_owned(),
                        if_exists: true,
                    }
                );
                assert_eq!(
                    inner[2],
                    Statement::DropTable {
                        table: "c".to_owned(),
                        if_exists: true,
                    }
                );
            }
            other => panic!("expected DoBlock, got {other:?}"),
        }
    }

    #[test]
    fn parses_drop_table_trailing_cascade_restrict_as_noop() {
        for sql in [
            "DROP TABLE IF EXISTS pgbench_accounts CASCADE",
            "DROP TABLE pgbench_accounts RESTRICT;",
        ] {
            let parsed = parse(sql).unwrap_or_else(|e| panic!("parse {sql}: {e}"));
            match parsed {
                Statement::DropTable { table, .. } => {
                    assert_eq!(table, "pgbench_accounts");
                }
                other => panic!("expected DropTable for {sql}, got {other:?}"),
            }
        }
    }

    #[test]
    fn parses_vacuum_and_analyze_as_no_op_do_block() {
        for sql in ["VACUUM", "VACUUM ANALYZE pgbench_accounts", "ANALYZE"] {
            let parsed = parse(sql).unwrap_or_else(|e| panic!("parse {sql}: {e}"));
            assert!(
                matches!(
                    &parsed,
                    Statement::DoBlock { inner, .. } if inner.is_empty()
                ),
                "{sql} should parse to empty DoBlock, got {parsed:?}"
            );
        }
    }

    #[test]
    fn parses_create_insert_and_select_subset() {
        // Phase A 2026-05-22: CREATE TABLE without explicit PK auto-injects
        // `__opendb_rowid BIGINT PRIMARY KEY DEFAULT auto` at the end.
        assert_eq!(
            parse("CREATE TABLE accounts (id INT, name TEXT);").expect("create"),
            Statement::CreateTable {
                table: "accounts".to_owned(),
                columns: vec![
                    ColumnDefinition::new("id", ColumnType::Int64),
                    ColumnDefinition::new("name", ColumnType::Text),
                    ColumnDefinition::primary_key("__opendb_rowid", ColumnType::Int64)
                        .with_default(DefaultExpr::AutoRowId),
                ],
            }
        );
        assert_eq!(
            parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("insert"),
            Statement::Insert {
                table: "accounts".to_owned(),
                columns: None,
                values: vec![Value::Int64(1), Value::Text("Ada".to_owned())],
                returning: None,
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
                    ColumnDefinition::primary_key("__opendb_rowid", ColumnType::Int64)
                        .with_default(DefaultExpr::AutoRowId),
                ],
            }
        );
        assert_eq!(
            parse("iNsErT iNtO accounts vAlUeS (1, 'Ada')").expect("insert"),
            Statement::Insert {
                table: "accounts".to_owned(),
                columns: None,
                values: vec![Value::Int64(1), Value::Text("Ada".to_owned())],
                returning: None,
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
                returning: None,
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
        // Sprint 14: `>` is now a real comparison operator; the legacy assertion
        // was that `>` was unsupported. The new contract: the parser parses it,
        // executor enforces semantics.
        assert!(matches!(
            parse("SELECT * FROM accounts WHERE id > 1"),
            Ok(Statement::SelectAll { .. })
        ));
        assert!(matches!(
            parse("SELECT * FROM accounts WHERE id = "),
            Err(OpenDbError::Sql(_))
        ));
    }

    #[test]
    fn parses_comparison_operators_in_where() {
        for (sql, op) in [
            ("SELECT * FROM t WHERE c = 1", crate::ast::WhereOp::Eq),
            ("SELECT * FROM t WHERE c != 1", crate::ast::WhereOp::NotEq),
            ("SELECT * FROM t WHERE c <> 1", crate::ast::WhereOp::NotEq),
            ("SELECT * FROM t WHERE c < 1", crate::ast::WhereOp::Lt),
            ("SELECT * FROM t WHERE c <= 1", crate::ast::WhereOp::Lte),
            ("SELECT * FROM t WHERE c > 1", crate::ast::WhereOp::Gt),
            ("SELECT * FROM t WHERE c >= 1", crate::ast::WhereOp::Gte),
        ] {
            let parsed = parse(sql).expect(sql);
            let Statement::SelectAll { predicate, .. } = parsed else {
                panic!("{sql} should produce a SelectAll");
            };
            let p = predicate
                .first()
                .unwrap_or_else(|| panic!("{sql} should produce a predicate"));
            assert_eq!(p.op, op, "{sql}");
        }
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
                Some(Predicate::eq("id".to_owned(), Value::Int64(1))),
            )
        );
        assert_eq!(
            parse("select * from accounts where name = 'Ada'").expect("select where text"),
            Statement::select_all_legacy(
                "accounts".to_owned(),
                Some(Predicate::eq(
                    "name".to_owned(),
                    Value::Text("Ada".to_owned()),
                )),
            )
        );
    }

    #[test]
    #[test]
    fn debug_comments_table() {
        let sql = r#"CREATE TABLE IF NOT EXISTS "comments" (
  "id" text PRIMARY KEY NOT NULL,
  "workspace_id" text NOT NULL REFERENCES "workspaces"("id") ON DELETE CASCADE,
  "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp DEFAULT CURRENT_TIMESTAMP
)"#;
        match parse(sql) {
            Ok(_) => {}
            Err(e) => panic!("parse failed: {e}"),
        }
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
                returning: None,
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
        assert!(!predicate.is_empty());
        let order_by = order_by.expect("order_by");
        assert_eq!(order_by.column, "id");
        assert!(matches!(order_by.direction, OrderDirection::Asc));
        assert_eq!(limit, Some(5));
    }

    #[test]
    fn parses_update_set_where_pk() {
        let stmt = parse("UPDATE accounts SET name = 'Bob', status = 'active' WHERE id = 1")
            .expect("update");
        let Statement::UpdateWhere {
            table,
            predicate,
            assignments,
            ..
        } = stmt
        else {
            panic!("expected UpdateWhere");
        };
        assert_eq!(table, "accounts");
        assert_eq!(predicate.len(), 1);
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
        // Sprint 14.D: parser unconditionally emits `DeleteWhere`; the
        // executor handles the PK fast path.
        let Statement::DeleteWhere {
            table, predicate, ..
        } = stmt
        else {
            panic!("expected DeleteWhere");
        };
        assert_eq!(table, "accounts");
        assert_eq!(predicate.len(), 1);
        assert_eq!(predicate[0].column, "id");
        assert_eq!(predicate[0].value, Value::Int64(1));
    }

    #[test]
    fn parses_delete_without_where_clears_table() {
        // Sprint 16.B: `DELETE FROM t` (no WHERE) is now legal — Drizzle
        // emits this for `db.delete(t)` to truncate. The executor honors
        // the empty predicate set as "match all rows".
        let stmt = parse("DELETE FROM accounts").expect("delete all");
        let Statement::DeleteWhere {
            table, predicate, ..
        } = stmt
        else {
            panic!("expected DeleteWhere");
        };
        assert_eq!(table, "accounts");
        assert!(predicate.is_empty());
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
                Some(Predicate::eq(
                    "token".to_owned(),
                    Value::Text("a=b".to_owned()),
                )),
            )
        );
    }
}
