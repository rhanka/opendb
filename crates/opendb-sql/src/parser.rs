use crate::ast::Statement;
use opendb_common::{OpenDbError, OpenDbResult};
use opendb_storage::commit_stream::Value;

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
        .map(|part| {
            part.trim()
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_owned()
        })
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    if table.is_empty() || columns.is_empty() {
        return Err(OpenDbError::Sql(
            "CREATE TABLE requires table and columns".to_owned(),
        ));
    }
    Ok(Statement::CreateTable { table, columns })
}

fn parse_insert(sql: &str) -> OpenDbResult<Statement> {
    let values_marker = " VALUES ";
    let upper = sql.to_ascii_uppercase();
    let values_pos = upper
        .find(values_marker)
        .ok_or_else(|| OpenDbError::Sql("INSERT requires VALUES".to_owned()))?;
    let table = strip_keyword_prefix(&sql[..values_pos], "INSERT INTO ")
        .ok_or_else(|| OpenDbError::Sql("invalid INSERT".to_owned()))?
        .trim()
        .to_owned();
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
    Ok(Statement::Insert { table, values })
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
    if let Some(text) = value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')) {
        return Ok(Value::Text(text.to_owned()));
    }
    value
        .parse::<i64>()
        .map(Value::Int64)
        .map_err(|_| OpenDbError::Sql(format!("unsupported literal: {value}")))
}

fn parse_select_all(sql: &str) -> OpenDbResult<Statement> {
    let table = strip_keyword_prefix(sql, "SELECT * FROM ")
        .ok_or_else(|| OpenDbError::Sql("invalid SELECT".to_owned()))?
        .trim();
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
    })
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
                columns: vec!["id".to_owned(), "name".to_owned()],
            }
        );
        assert_eq!(
            parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("insert"),
            Statement::Insert {
                table: "accounts".to_owned(),
                values: vec![Value::Int64(1), Value::Text("Ada".to_owned())],
            }
        );
        assert_eq!(
            parse("SELECT * FROM accounts").expect("select"),
            Statement::SelectAll {
                table: "accounts".to_owned()
            }
        );
    }

    #[test]
    fn parses_mixed_case_keywords() {
        assert_eq!(
            parse("cReAtE tAbLe accounts (id INT, name TEXT)").expect("create"),
            Statement::CreateTable {
                table: "accounts".to_owned(),
                columns: vec!["id".to_owned(), "name".to_owned()],
            }
        );
        assert_eq!(
            parse("iNsErT iNtO accounts vAlUeS (1, 'Ada')").expect("insert"),
            Statement::Insert {
                table: "accounts".to_owned(),
                values: vec![Value::Int64(1), Value::Text("Ada".to_owned())],
            }
        );
        assert_eq!(
            parse("sElEcT * fRoM accounts").expect("select"),
            Statement::SelectAll {
                table: "accounts".to_owned()
            }
        );
    }

    #[test]
    fn parses_quoted_text_with_comma_as_single_value() {
        assert_eq!(
            parse("INSERT INTO accounts VALUES (1, 'Ada, Lovelace')").expect("insert"),
            Statement::Insert {
                table: "accounts".to_owned(),
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
    fn rejects_select_where_at_parse_time() {
        assert!(matches!(
            parse("SELECT * FROM accounts WHERE id = 1"),
            Err(OpenDbError::Sql(_))
        ));
    }
}
