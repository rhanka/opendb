#[derive(Debug, thiserror::Error)]
pub enum OpenDbError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("not leader for root range: leader_id={leader_id:?}, leader_addr={leader_addr:?}")]
    NotLeader {
        leader_id: Option<u64>,
        leader_addr: Option<String>,
    },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("sql error: {0}")]
    Sql(String),
}

pub type OpenDbResult<T> = Result<T, OpenDbError>;
