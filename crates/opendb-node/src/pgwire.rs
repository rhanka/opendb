use crate::database::Database;
use anyhow::{Context, bail};
use opendb_common::OpenDbError;
use opendb_sql::{ast::QueryResult, parser::parse};
use opendb_storage::commit_stream::Value;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

#[derive(Clone, Debug, Default)]
struct ExtendedSession {
    statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, BoundPortal>,
}

#[derive(Clone, Debug)]
struct PreparedStatement {
    sql: String,
}

#[derive(Clone, Debug)]
struct BoundPortal {
    sql_substituted: String,
}

const SSL_REQUEST_CODE: i32 = 80877103;
const PROTOCOL_VERSION_3: i32 = 196608;
const MAX_FRAME_LEN: usize = 1024 * 1024;

pub async fn serve(addr: SocketAddr, database: Arc<Mutex<Database>>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind pgwire listener on {addr}"))?;
    tracing::info!(%addr, "pgwire listener ready");

    loop {
        let (stream, peer) = listener.accept().await.context("accept pgwire client")?;
        let database = Arc::clone(&database);
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, database).await {
                tracing::debug!(%peer, %error, "pgwire connection closed");
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    database: Arc<Mutex<Database>>,
) -> anyhow::Result<()> {
    stream
        .set_nodelay(true)
        .context("disable Nagle on pgwire client socket")?;
    loop {
        let startup = read_untagged_frame(&mut stream).await?;
        let code = read_i32(&startup)?;
        if code == SSL_REQUEST_CODE {
            stream.write_all(b"N").await?;
            continue;
        }
        if code != PROTOCOL_VERSION_3 {
            bail!("unsupported startup protocol version {code}");
        }
        break;
    }

    write_startup_ok(&mut stream).await?;

    let mut session = ExtendedSession::default();
    loop {
        let Some((tag, payload)) = read_tagged_frame(&mut stream).await? else {
            return Ok(());
        };

        match tag {
            b'Q' => {
                let sql = cstring_payload(&payload)?;
                execute_simple_query(&mut stream, &database, sql).await?;
            }
            b'P' => handle_parse(&mut stream, &mut session, &payload).await?,
            b'B' => handle_bind(&mut stream, &mut session, &payload).await?,
            b'D' => handle_describe(&mut stream, &session, &payload).await?,
            b'E' => handle_execute(&mut stream, &database, &session, &payload).await?,
            b'C' => handle_close(&mut stream, &mut session, &payload).await?,
            b'H' => {
                // Flush is a no-op at this layer; flush happens after every write.
            }
            b'S' => write_ready_for_query(&mut stream).await?,
            b'X' => return Ok(()),
            _ => {
                write_error_response(&mut stream, &format!("unsupported message tag {tag}"))
                    .await?;
                write_ready_for_query(&mut stream).await?;
            }
        }
    }
}

async fn handle_parse(
    stream: &mut TcpStream,
    session: &mut ExtendedSession,
    payload: &[u8],
) -> anyhow::Result<()> {
    let mut cursor = 0usize;
    let statement_name = read_cstring_from(payload, &mut cursor)?;
    let sql = read_cstring_from(payload, &mut cursor)?;
    // Skip the parameter-type OID array; Sprint 12 substitutes textually and
    // does not consult declared OIDs.
    if cursor + 2 > payload.len() {
        bail!("Parse payload truncated before param count");
    }
    let _param_count = read_u16_at(payload, &mut cursor)?;
    let _ = cursor;
    session.statements.insert(
        statement_name.to_owned(),
        PreparedStatement {
            sql: sql.to_owned(),
        },
    );
    write_message(stream, b'1', &[]).await
}

async fn handle_bind(
    stream: &mut TcpStream,
    session: &mut ExtendedSession,
    payload: &[u8],
) -> anyhow::Result<()> {
    let mut cursor = 0usize;
    let portal_name = read_cstring_from(payload, &mut cursor)?.to_owned();
    let statement_name = read_cstring_from(payload, &mut cursor)?.to_owned();
    let statement = session
        .statements
        .get(&statement_name)
        .ok_or_else(|| anyhow::anyhow!("unknown statement {statement_name}"))?
        .clone();

    // Parameter format codes.
    let format_count = read_u16_at(payload, &mut cursor)?;
    let mut formats: Vec<u16> = Vec::with_capacity(format_count as usize);
    for _ in 0..format_count {
        formats.push(read_u16_at(payload, &mut cursor)?);
    }
    // Parameter values.
    let value_count = read_u16_at(payload, &mut cursor)?;
    let mut values: Vec<Option<Vec<u8>>> = Vec::with_capacity(value_count as usize);
    for index in 0..value_count {
        let len = read_i32_at(payload, &mut cursor)?;
        if len < 0 {
            values.push(None);
        } else {
            let len = len as usize;
            if cursor + len > payload.len() {
                bail!("Bind payload truncated at param {index}");
            }
            values.push(Some(payload[cursor..cursor + len].to_vec()));
            cursor += len;
        }
    }
    // Skip result-format-codes; Sprint 12 always emits text.
    let result_format_count = read_u16_at(payload, &mut cursor)?;
    cursor += result_format_count as usize * 2;
    let _ = cursor;

    let sql_substituted = substitute_parameters(&statement.sql, &values, &formats)?;
    session
        .portals
        .insert(portal_name, BoundPortal { sql_substituted });
    write_message(stream, b'2', &[]).await
}

async fn handle_describe(
    stream: &mut TcpStream,
    _session: &ExtendedSession,
    payload: &[u8],
) -> anyhow::Result<()> {
    // For Sprint 12 we do not infer columns at describe time. Drizzle accepts
    // `NoData` ('n') here and re-discovers the RowDescription during Execute.
    if payload.is_empty() {
        bail!("Describe payload too short");
    }
    let _kind = payload[0];
    write_message(stream, b'n', &[]).await
}

async fn handle_execute(
    stream: &mut TcpStream,
    database: &Arc<Mutex<Database>>,
    session: &ExtendedSession,
    payload: &[u8],
) -> anyhow::Result<()> {
    let mut cursor = 0usize;
    let portal_name = read_cstring_from(payload, &mut cursor)?.to_owned();
    let _max_rows = read_i32_at(payload, &mut cursor)?;
    let portal = session
        .portals
        .get(&portal_name)
        .ok_or_else(|| anyhow::anyhow!("unknown portal {portal_name}"))?
        .clone();
    execute_extended_query(stream, database, &portal.sql_substituted).await
}

async fn handle_close(
    stream: &mut TcpStream,
    session: &mut ExtendedSession,
    payload: &[u8],
) -> anyhow::Result<()> {
    if payload.len() < 2 {
        bail!("Close payload too short");
    }
    let kind = payload[0];
    let mut cursor = 1usize;
    let name = read_cstring_from(payload, &mut cursor)?.to_owned();
    if kind == b'S' {
        session.statements.remove(&name);
    } else if kind == b'P' {
        session.portals.remove(&name);
    }
    write_message(stream, b'3', &[]).await
}

async fn execute_extended_query(
    stream: &mut TcpStream,
    database: &Arc<Mutex<Database>>,
    sql: &str,
) -> anyhow::Result<()> {
    let result = match parse(sql) {
        Ok(statement) => {
            let mut database = database.lock().await;
            database.execute(statement).await
        }
        Err(error) => Err(error),
    };

    match result {
        Ok(QueryResult::Command { tag }) => write_command_complete(stream, &tag).await?,
        Ok(QueryResult::Rows {
            columns,
            column_types,
            rows,
        }) => {
            let row_count = rows.len();
            let resolved_types = resolve_row_description_types(&columns, &column_types, &rows);
            write_row_description(stream, &columns, &resolved_types).await?;
            for row in rows {
                write_data_row(stream, &row).await?;
            }
            write_command_complete(stream, &format!("SELECT {row_count}")).await?;
        }
        Err(error) => write_open_db_error_response(stream, &error).await?,
    }
    Ok(())
}

/// Sprint 12: substitute `$1`/`$2`/... placeholders with the parameter
/// values as inline SQL literals. Both text-mode and binary-mode bytes
/// are decoded as UTF-8 strings and quoted (text mode is what Drizzle
/// emits by default).
fn substitute_parameters(
    sql: &str,
    values: &[Option<Vec<u8>>],
    formats: &[u16],
) -> anyhow::Result<String> {
    let mut output = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' {
            // Copy the entire quoted literal verbatim.
            output.push('\'');
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                output.push(c as char);
                i += 1;
                if c == b'\'' {
                    break;
                }
            }
            continue;
        }
        if ch == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            let number_str = std::str::from_utf8(&bytes[i + 1..j])?;
            let index: usize = number_str.parse().context("invalid $N parameter")?;
            let value = values
                .get(index - 1)
                .ok_or_else(|| anyhow::anyhow!("bind value {index} missing"))?;
            let format = formats.get(index - 1).copied().unwrap_or(0);
            output.push_str(&render_bind_value(value.as_deref(), format)?);
            i = j;
            continue;
        }
        output.push(ch as char);
        i += 1;
    }
    Ok(output)
}

fn render_bind_value(value: Option<&[u8]>, _format: u16) -> anyhow::Result<String> {
    match value {
        None => Ok("NULL".to_string()),
        Some(bytes) => {
            let text = std::str::from_utf8(bytes)
                .context("bind value is not valid UTF-8 (binary mode not supported)")?;
            // Sprint 12 minimum: always quote with single quotes and escape
            // single-quote inner. The parser distinguishes between integer /
            // float / boolean / NULL literals; if the original SQL gives the
            // column type expectation, the executor coerces appropriately.
            // We still pass numeric-looking strings unquoted so an `INSERT
            // INTO t (id) VALUES ($1)` against an INT column receives an
            // INT literal, not a TEXT literal.
            if looks_numeric_or_boolean(text) {
                Ok(text.to_owned())
            } else {
                Ok(format!("'{}'", text.replace('\'', "''")))
            }
        }
    }
}

fn looks_numeric_or_boolean(text: &str) -> bool {
    if text.eq_ignore_ascii_case("true")
        || text.eq_ignore_ascii_case("false")
        || text.eq_ignore_ascii_case("null")
    {
        return true;
    }
    let mut iter = text.chars().peekable();
    if let Some(&c) = iter.peek()
        && (c == '-' || c == '+')
    {
        iter.next();
    }
    let mut digits = false;
    let mut dot = false;
    for c in iter {
        if c.is_ascii_digit() {
            digits = true;
        } else if c == '.' && !dot {
            dot = true;
        } else {
            return false;
        }
    }
    digits
}

fn read_cstring_from<'a>(payload: &'a [u8], cursor: &mut usize) -> anyhow::Result<&'a str> {
    let start = *cursor;
    while *cursor < payload.len() && payload[*cursor] != 0 {
        *cursor += 1;
    }
    if *cursor >= payload.len() {
        bail!("cstring missing null terminator");
    }
    let slice = &payload[start..*cursor];
    *cursor += 1;
    std::str::from_utf8(slice).context("cstring is not utf8")
}

fn read_u16_at(payload: &[u8], cursor: &mut usize) -> anyhow::Result<u16> {
    if *cursor + 2 > payload.len() {
        bail!("frame truncated before u16");
    }
    let value = u16::from_be_bytes([payload[*cursor], payload[*cursor + 1]]);
    *cursor += 2;
    Ok(value)
}

fn read_i32_at(payload: &[u8], cursor: &mut usize) -> anyhow::Result<i32> {
    if *cursor + 4 > payload.len() {
        bail!("frame truncated before i32");
    }
    let value = i32::from_be_bytes([
        payload[*cursor],
        payload[*cursor + 1],
        payload[*cursor + 2],
        payload[*cursor + 3],
    ]);
    *cursor += 4;
    Ok(value)
}

async fn execute_simple_query(
    stream: &mut TcpStream,
    database: &Arc<Mutex<Database>>,
    sql: &str,
) -> anyhow::Result<()> {
    let result = match parse(sql) {
        Ok(statement) => {
            let mut database = database.lock().await;
            database.execute(statement).await
        }
        Err(error) => Err(error),
    };

    match result {
        Ok(QueryResult::Command { tag }) => write_command_complete(stream, &tag).await?,
        Ok(QueryResult::Rows {
            columns,
            column_types,
            rows,
        }) => {
            let row_count = rows.len();
            let resolved_types = resolve_row_description_types(&columns, &column_types, &rows);
            write_row_description(stream, &columns, &resolved_types).await?;
            for row in rows {
                write_data_row(stream, &row).await?;
            }
            write_command_complete(stream, &format!("SELECT {row_count}")).await?;
        }
        Err(error) => write_open_db_error_response(stream, &error).await?,
    }

    write_ready_for_query(stream).await
}

async fn read_untagged_frame(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let len = read_len(stream).await?;
    let payload_len = checked_payload_len(len)?;
    let mut payload = vec![0_u8; payload_len];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

async fn read_tagged_frame(stream: &mut TcpStream) -> anyhow::Result<Option<(u8, Vec<u8>)>> {
    let mut tag = [0_u8; 1];
    match stream.read_exact(&mut tag).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }

    let len = read_len(stream).await?;
    let payload_len = checked_payload_len(len)?;
    let mut payload = vec![0_u8; payload_len];
    stream.read_exact(&mut payload).await?;
    Ok(Some((tag[0], payload)))
}

async fn read_len(stream: &mut TcpStream) -> anyhow::Result<u32> {
    let mut len = [0_u8; 4];
    stream.read_exact(&mut len).await?;
    Ok(u32::from_be_bytes(len))
}

fn checked_payload_len(len: u32) -> anyhow::Result<usize> {
    let len = usize::try_from(len).context("frame length does not fit usize")?;
    if !(4..=MAX_FRAME_LEN).contains(&len) {
        bail!("invalid pgwire frame length {len}");
    }
    Ok(len - 4)
}

fn read_i32(payload: &[u8]) -> anyhow::Result<i32> {
    if payload.len() < 4 {
        bail!("startup frame too short");
    }
    Ok(i32::from_be_bytes(
        payload[0..4].try_into().expect("slice length checked"),
    ))
}

fn cstring_payload(payload: &[u8]) -> anyhow::Result<&str> {
    let Some(sql) = payload.strip_suffix(&[0]) else {
        bail!("simple query payload is missing terminator");
    };
    std::str::from_utf8(sql).context("simple query payload is not utf8")
}

async fn write_startup_ok(stream: &mut TcpStream) -> anyhow::Result<()> {
    write_authentication_ok(stream).await?;
    write_parameter_status(stream, "server_version", "16.0").await?;
    write_parameter_status(stream, "client_encoding", "UTF8").await?;
    write_parameter_status(stream, "DateStyle", "ISO").await?;
    write_ready_for_query(stream).await
}

async fn write_authentication_ok(stream: &mut TcpStream) -> anyhow::Result<()> {
    write_message(stream, b'R', &0_i32.to_be_bytes()).await
}

async fn write_parameter_status(
    stream: &mut TcpStream,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(key.as_bytes());
    payload.push(0);
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
    write_message(stream, b'S', &payload).await
}

async fn write_ready_for_query(stream: &mut TcpStream) -> anyhow::Result<()> {
    write_message(stream, b'Z', b"I").await
}

async fn write_command_complete(stream: &mut TcpStream, tag: &str) -> anyhow::Result<()> {
    let mut payload = Vec::from(tag.as_bytes());
    payload.push(0);
    write_message(stream, b'C', &payload).await
}

async fn write_row_description(
    stream: &mut TcpStream,
    columns: &[String],
    column_types: &[opendb_storage::commit_stream::ColumnType],
) -> anyhow::Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(
        &u16::try_from(columns.len())
            .context("too many columns")?
            .to_be_bytes(),
    );
    for (index, column) in columns.iter().enumerate() {
        payload.extend_from_slice(column.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&0_i32.to_be_bytes());
        payload.extend_from_slice(&0_i16.to_be_bytes());
        let oid = column_types
            .get(index)
            .map(oid_for_column_type)
            .unwrap_or(25);
        payload.extend_from_slice(&oid.to_be_bytes());
        payload.extend_from_slice(&(-1_i16).to_be_bytes());
        payload.extend_from_slice(&(-1_i32).to_be_bytes());
        payload.extend_from_slice(&0_i16.to_be_bytes());
    }
    write_message(stream, b'T', &payload).await
}

fn oid_for_column_type(column_type: &opendb_storage::commit_stream::ColumnType) -> i32 {
    use opendb_storage::commit_stream::ColumnType;
    match column_type {
        ColumnType::Int64 => 20,       // INT8
        ColumnType::Text => 25,        // TEXT
        ColumnType::Bool => 16,        // BOOL
        ColumnType::Float64 => 701,    // FLOAT8
        ColumnType::Timestamp => 1114, // TIMESTAMP
        ColumnType::Json => 3802,      // JSONB
    }
}

fn resolve_row_description_types(
    columns: &[String],
    column_types: &[opendb_storage::commit_stream::ColumnType],
    rows: &[Vec<Value>],
) -> Vec<opendb_storage::commit_stream::ColumnType> {
    use opendb_storage::commit_stream::ColumnType;
    if column_types.len() == columns.len() {
        return column_types.to_vec();
    }
    let mut resolved = vec![ColumnType::Text; columns.len()];
    if let Some(first_row) = rows.first() {
        for (index, value) in first_row.iter().enumerate().take(resolved.len()) {
            resolved[index] = match value {
                Value::Int64(_) => ColumnType::Int64,
                Value::Text(_) => ColumnType::Text,
                Value::Bool(_) => ColumnType::Bool,
                Value::Float64(_) => ColumnType::Float64,
                Value::Timestamp(_) => ColumnType::Timestamp,
                Value::Json(_) => ColumnType::Json,
                Value::Null => ColumnType::Text,
            };
        }
    }
    resolved
}

async fn write_data_row(stream: &mut TcpStream, row: &[Value]) -> anyhow::Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(
        &u16::try_from(row.len())
            .context("too many row values")?
            .to_be_bytes(),
    );
    for value in row {
        // Sprint 17.5: pgwire DataRow encodes SQL NULL as a field length of
        // -1 (i32 big-endian) with no payload bytes. Writing length 0 with
        // no payload is treated as an empty string by pg clients, which
        // surfaced as `gateConfig: ""` / `description: ""` on Drizzle.
        if matches!(value, Value::Null) {
            payload.extend_from_slice(&(-1_i32).to_be_bytes());
            continue;
        }
        let text = value_to_text(value);
        payload.extend_from_slice(
            &i32::try_from(text.len())
                .context("field is too large")?
                .to_be_bytes(),
        );
        payload.extend_from_slice(text.as_bytes());
    }
    write_message(stream, b'D', &payload).await
}

fn value_to_text(value: &Value) -> String {
    match value {
        Value::Int64(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Bool(true) => "t".to_string(),
        Value::Bool(false) => "f".to_string(),
        Value::Float64(value) => format!("{value}"),
        Value::Timestamp(value) => format_timestamp_micros(*value),
        Value::Json(value) => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
        Value::Null => String::new(),
    }
}

fn format_timestamp_micros(micros: i64) -> String {
    // Microseconds since 1970-01-01 (UTC, no timezone). For Sprint 6 we
    // accept a partial implementation that handles the common case
    // (non-negative). Negative values still render but with seconds
    // offset semantics — refined in a later sprint.
    let secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000) as u32;
    let datetime =
        chrono::DateTime::from_timestamp(secs, frac * 1_000).unwrap_or_else(chrono::Utc::now);
    datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

async fn write_error_response(stream: &mut TcpStream, message: &str) -> anyhow::Result<()> {
    write_error_response_with_sqlstate(stream, "XX000", message).await
}

async fn write_open_db_error_response(
    stream: &mut TcpStream,
    error: &OpenDbError,
) -> anyhow::Result<()> {
    write_error_response_with_sqlstate(stream, sqlstate(error), &error.to_string()).await
}

fn sqlstate(error: &OpenDbError) -> &'static str {
    match error {
        OpenDbError::NotLeader { .. } => "57P03",
        OpenDbError::Sql(_) => "42601",
        OpenDbError::NotFound(_) => "42P01",
        OpenDbError::InvalidInput(_) => "22023",
        OpenDbError::Storage(_) => "XX000",
    }
}

async fn write_error_response_with_sqlstate(
    stream: &mut TcpStream,
    sqlstate: &str,
    message: &str,
) -> anyhow::Result<()> {
    let mut payload = Vec::new();
    payload.push(b'S');
    payload.extend_from_slice(b"ERROR");
    payload.push(0);
    payload.push(b'C');
    payload.extend_from_slice(sqlstate.as_bytes());
    payload.push(0);
    payload.push(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.push(0);
    payload.push(0);
    write_message(stream, b'E', &payload).await
}

async fn write_message(stream: &mut TcpStream, tag: u8, payload: &[u8]) -> anyhow::Result<()> {
    let len = payload
        .len()
        .checked_add(4)
        .and_then(|len| u32::try_from(len).ok())
        .context("pgwire message too large")?;
    stream.write_all(&[tag]).await?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_malformed_frame_lengths() {
        assert!(checked_payload_len(3).is_err());
        assert!(checked_payload_len((MAX_FRAME_LEN + 1) as u32).is_err());
        assert_eq!(checked_payload_len(4).expect("empty payload"), 0);
    }

    #[test]
    fn parses_null_terminated_query_payload() {
        assert_eq!(
            cstring_payload(b"SELECT * FROM accounts\0").expect("query"),
            "SELECT * FROM accounts"
        );
        assert!(cstring_payload(b"SELECT * FROM accounts").is_err());
    }

    #[test]
    fn not_leader_errors_use_retryable_sqlstate() {
        let error = OpenDbError::NotLeader {
            leader_id: Some(1),
            leader_addr: Some("opendb-1.opendb-peer:7000".to_string()),
        };

        assert_eq!(sqlstate(&error), "57P03");
    }

    #[test]
    fn sql_errors_use_syntax_error_sqlstate() {
        assert_eq!(
            sqlstate(&OpenDbError::Sql("bad query".to_string())),
            "42601"
        );
    }
}
