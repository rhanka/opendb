use crate::root_range::{OpenDbRaftNodeId, RootRange, RootRangeCommand};
use opendb_common::{OpenDbError, OpenDbResult};
use openraft::error::{
    CheckIsLeaderError, ClientWriteError, InstallSnapshotError, NetworkError, PayloadTooLarge,
    RPCError, RaftError, RemoteError,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::Adaptor;
use openraft::{
    BasicNode, Config, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, Raft,
    RaftLogReader, RaftNetwork, RaftNetworkFactory, RaftSnapshotBuilder, RaftStorage, Snapshot,
    SnapshotMeta, SnapshotPolicy, StorageError, StoredMembership, Vote,
};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Cursor, ErrorKind};
use std::net::SocketAddr;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MAX_RAFT_RPC_FRAME_LEN: usize = 16 * 1024 * 1024;
const RAFT_STORE_VERSION: u16 = 1;
const RAFT_STORE_FILE: &str = "raft-state.json";

openraft::declare_raft_types!(
    RootRangeTypeConfig:
        D = RootRangeCommand,
        R = RootRangeResponse,
        NodeId = OpenDbRaftNodeId,
        Node = BasicNode,
        Entry = Entry<RootRangeTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum RootRangeResponse {
    Applied,
    Noop,
}

pub(crate) struct RootRangeRaftHarness {
    raft: Raft<RootRangeTypeConfig>,
    members: BTreeMap<OpenDbRaftNodeId, BasicNode>,
}

impl RootRangeRaftHarness {
    pub(crate) async fn new(
        node_id: OpenDbRaftNodeId,
        data_dir: impl AsRef<Path>,
        members: BTreeMap<OpenDbRaftNodeId, BasicNode>,
    ) -> OpenDbResult<Self> {
        let data_dir = data_dir.as_ref();
        let bootstrap_replica_node_ids = members.keys().copied().collect::<Vec<_>>();
        let root_range = RootRange::new_with_authority_and_bootstrap_replicas(
            data_dir,
            crate::root_range::RootRangeAuthority::Standalone,
            bootstrap_replica_node_ids,
        );
        let store = RootRangeRaftStore::open(root_range.clone(), data_dir).await?;
        let (log_store, state_machine) = Adaptor::new(store);
        let config = Arc::new(root_range_raft_config()?);
        let raft = Raft::new(
            node_id,
            config,
            TcpRaftNetworkFactory,
            log_store,
            state_machine,
        )
        .await
        .map_err(|err| OpenDbError::Storage(format!("start root range raft: {err}")))?;

        Ok(Self { raft, members })
    }

    #[cfg(test)]
    pub(crate) async fn new_single_node(
        node_id: OpenDbRaftNodeId,
        data_dir: impl AsRef<Path>,
    ) -> OpenDbResult<Self> {
        let mut members = BTreeMap::new();
        members.insert(
            node_id,
            BasicNode::new(format!("in-process-root-range-{node_id}")),
        );

        Self::new(node_id, data_dir, members).await
    }

    pub(crate) async fn initialize_cluster(&self) -> OpenDbResult<()> {
        if self.raft.is_initialized().await.map_err(|err| {
            OpenDbError::Storage(format!("read root range raft init state: {err}"))
        })? {
            return Ok(());
        }

        self.raft
            .initialize(self.members.clone())
            .await
            .map_err(|err| OpenDbError::Storage(format!("initialize root range raft: {err}")))
    }

    #[cfg(test)]
    pub(crate) async fn initialize_single_node(&self) -> OpenDbResult<()> {
        self.initialize_cluster().await
    }

    pub(crate) async fn submit(
        &self,
        command: RootRangeCommand,
    ) -> OpenDbResult<RootRangeResponse> {
        let response = self
            .raft
            .client_write(command)
            .await
            .map_err(map_client_write_error)?;
        Ok(response.data)
    }

    pub(crate) async fn is_leader(&self, node_id: OpenDbRaftNodeId) -> bool {
        self.raft.current_leader().await == Some(node_id)
    }

    pub(crate) async fn ensure_linearizable_leader(
        &self,
    ) -> Result<(), RaftError<OpenDbRaftNodeId, CheckIsLeaderError<OpenDbRaftNodeId, BasicNode>>>
    {
        self.raft.ensure_linearizable().await.map(|_| ())
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_leader(
        &self,
        node_id: OpenDbRaftNodeId,
        timeout: std::time::Duration,
    ) -> OpenDbResult<()> {
        self.raft
            .wait(Some(timeout))
            .current_leader(node_id, "root range leader")
            .await
            .map_err(|err| OpenDbError::Storage(format!("wait for root range leader: {err}")))
            .map(|_| ())
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_any_leader(
        &self,
        timeout: std::time::Duration,
    ) -> OpenDbResult<OpenDbRaftNodeId> {
        let metrics = self
            .raft
            .wait(Some(timeout))
            .metrics(
                |metrics| metrics.current_leader.is_some(),
                "root range any leader",
            )
            .await
            .map_err(|err| OpenDbError::Storage(format!("wait for root range leader: {err}")))?;
        metrics
            .current_leader
            .ok_or_else(|| OpenDbError::Storage("root range leader missing".to_string()))
    }

    pub(crate) async fn serve_peer_rpc(self: Arc<Self>, addr: SocketAddr) -> OpenDbResult<()> {
        let listener = TcpListener::bind(addr).await.map_err(|err| {
            OpenDbError::Storage(format!(
                "bind root range raft RPC listener on {addr}: {err}"
            ))
        })?;

        loop {
            let (stream, _) = listener.accept().await.map_err(|err| {
                OpenDbError::Storage(format!("accept root range raft RPC connection: {err}"))
            })?;
            let node = Arc::clone(&self);
            tokio::spawn(async move {
                let _ = node.handle_peer_rpc(stream).await;
            });
        }
    }

    async fn handle_peer_rpc(&self, mut stream: TcpStream) -> OpenDbResult<()> {
        match read_json_frame::<RootRangeRaftRpcRequest>(&mut stream).await? {
            RootRangeRaftRpcRequest::AppendEntries(rpc) => {
                let response = self.raft.append_entries(*rpc).await;
                write_json_frame(
                    &mut stream,
                    &RootRangeRaftRpcResponse::AppendEntries(response),
                )
                .await
            }
            RootRangeRaftRpcRequest::InstallSnapshot(rpc) => {
                let response = self.raft.install_snapshot(*rpc).await;
                write_json_frame(
                    &mut stream,
                    &RootRangeRaftRpcResponse::InstallSnapshot(response),
                )
                .await
            }
            RootRangeRaftRpcRequest::Vote(rpc) => {
                let response = self.raft.vote(rpc).await;
                write_json_frame(&mut stream, &RootRangeRaftRpcResponse::Vote(response)).await
            }
        }
    }

    pub(crate) async fn shutdown(&self) -> OpenDbResult<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|err| OpenDbError::Storage(format!("shutdown root range raft: {err}")))
    }
}

fn map_client_write_error(
    err: RaftError<OpenDbRaftNodeId, ClientWriteError<OpenDbRaftNodeId, BasicNode>>,
) -> OpenDbError {
    if let Some(forward) = err.forward_to_leader() {
        return OpenDbError::NotLeader {
            leader_id: forward.leader_id,
            leader_addr: forward.leader_node.as_ref().map(|node| node.addr.clone()),
        };
    }

    OpenDbError::Storage(format!("root range raft client_write failed: {err}"))
}

fn root_range_raft_config() -> OpenDbResult<Config> {
    Config {
        cluster_name: "opendb-root-range".to_string(),
        snapshot_policy: SnapshotPolicy::Never,
        ..Config::default()
    }
    .validate()
    .map_err(|err| OpenDbError::Storage(format!("invalid root range raft config: {err}")))
}

fn response_for_entry(entry: &Entry<RootRangeTypeConfig>) -> RootRangeResponse {
    match &entry.payload {
        EntryPayload::Normal(_) => RootRangeResponse::Applied,
        EntryPayload::Blank | EntryPayload::Membership(_) => RootRangeResponse::Noop,
    }
}

#[derive(Clone, Debug)]
struct SharedRaftLogStore(Arc<tokio::sync::Mutex<RootRangeRaftStoreState>>);

impl SharedRaftLogStore {
    fn new(state: RootRangeRaftStoreState) -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(state)))
    }
}

#[derive(Debug)]
struct RootRangeRaftStore {
    state: SharedRaftLogStore,
}

#[derive(Debug)]
struct RootRangeRaftStoreState {
    root_range: RootRange,
    state_path: PathBuf,
    logs: BTreeMap<u64, Entry<RootRangeTypeConfig>>,
    vote: Option<Vote<OpenDbRaftNodeId>>,
    committed: Option<LogId<OpenDbRaftNodeId>>,
    last_applied: Option<LogId<OpenDbRaftNodeId>>,
    last_membership: StoredMembership<OpenDbRaftNodeId, BasicNode>,
    current_snapshot: Option<SnapshotMeta<OpenDbRaftNodeId, BasicNode>>,
    last_purged_log_id: Option<LogId<OpenDbRaftNodeId>>,
    applied_normal_count: usize,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PersistedRootRangeRaftStoreState {
    version: u16,
    logs: BTreeMap<u64, Entry<RootRangeTypeConfig>>,
    vote: Option<Vote<OpenDbRaftNodeId>>,
    committed: Option<LogId<OpenDbRaftNodeId>>,
    last_applied: Option<LogId<OpenDbRaftNodeId>>,
    last_membership: StoredMembership<OpenDbRaftNodeId, BasicNode>,
    current_snapshot: Option<SnapshotMeta<OpenDbRaftNodeId, BasicNode>>,
    last_purged_log_id: Option<LogId<OpenDbRaftNodeId>>,
    applied_normal_count: usize,
}

impl RootRangeRaftStore {
    async fn open(root_range: RootRange, data_dir: &Path) -> OpenDbResult<Self> {
        let state = RootRangeRaftStoreState::load(
            root_range,
            data_dir.join("root-range").join(RAFT_STORE_FILE),
        )
        .await?;
        Ok(Self {
            state: SharedRaftLogStore::new(state),
        })
    }
}

impl RootRangeRaftStoreState {
    async fn load(root_range: RootRange, state_path: PathBuf) -> OpenDbResult<Self> {
        let persisted = match fs::read(&state_path).await {
            Ok(bytes) => Some(
                serde_json::from_slice::<PersistedRootRangeRaftStoreState>(&bytes).map_err(
                    |err| {
                        OpenDbError::Storage(format!(
                            "decode root range raft state {}: {err}",
                            state_path.display()
                        ))
                    },
                )?,
            ),
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => {
                return Err(OpenDbError::Storage(format!(
                    "read root range raft state {}: {err}",
                    state_path.display()
                )));
            }
        };

        match persisted {
            Some(persisted) => Self::from_persisted(root_range, state_path, persisted).await,
            None => {
                let existing_records = root_range.replay().await?;
                if !existing_records.is_empty() {
                    return Err(OpenDbError::Storage(format!(
                        "root range WAL has {} records but raft state {} is missing",
                        existing_records.len(),
                        state_path.display()
                    )));
                }
                Ok(Self::new_empty(root_range, state_path))
            }
        }
    }

    async fn from_persisted(
        root_range: RootRange,
        state_path: PathBuf,
        persisted: PersistedRootRangeRaftStoreState,
    ) -> OpenDbResult<Self> {
        if persisted.version != RAFT_STORE_VERSION {
            return Err(OpenDbError::Storage(format!(
                "unsupported root range raft state version {}, expected {}",
                persisted.version, RAFT_STORE_VERSION
            )));
        }
        validate_persisted_state_matches_wal(&root_range, &state_path, &persisted).await?;

        Ok(Self {
            root_range,
            state_path,
            logs: persisted.logs,
            vote: persisted.vote,
            committed: persisted.committed,
            last_applied: persisted.last_applied,
            last_membership: persisted.last_membership,
            current_snapshot: persisted.current_snapshot,
            last_purged_log_id: persisted.last_purged_log_id,
            applied_normal_count: persisted.applied_normal_count,
        })
    }

    fn new_empty(root_range: RootRange, state_path: PathBuf) -> Self {
        Self {
            root_range,
            state_path,
            logs: BTreeMap::new(),
            vote: None,
            committed: None,
            last_applied: None,
            last_membership: StoredMembership::default(),
            current_snapshot: None,
            last_purged_log_id: None,
            applied_normal_count: 0,
        }
    }

    async fn persist(
        &self,
        subject: ErrorSubject<OpenDbRaftNodeId>,
        verb: ErrorVerb,
    ) -> Result<(), StorageError<OpenDbRaftNodeId>> {
        let persisted = PersistedRootRangeRaftStoreState {
            version: RAFT_STORE_VERSION,
            logs: self.logs.clone(),
            vote: self.vote,
            committed: self.committed,
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            current_snapshot: self.current_snapshot.clone(),
            last_purged_log_id: self.last_purged_log_id,
            applied_normal_count: self.applied_normal_count,
        };

        write_json_file_atomic(&self.state_path, &persisted)
            .await
            .map_err(|err| Self::storage_error(subject, verb, err))
    }

    fn storage_error(
        subject: ErrorSubject<OpenDbRaftNodeId>,
        verb: ErrorVerb,
        message: impl ToString,
    ) -> StorageError<OpenDbRaftNodeId> {
        StorageError::from_io_error(subject, verb, std::io::Error::other(message.to_string()))
    }

    fn apply_error(
        log_id: LogId<OpenDbRaftNodeId>,
        err: OpenDbError,
    ) -> StorageError<OpenDbRaftNodeId> {
        Self::storage_error(ErrorSubject::Apply(log_id), ErrorVerb::Write, err)
    }

    fn last_log_id(&self) -> Option<LogId<OpenDbRaftNodeId>> {
        self.logs
            .values()
            .next_back()
            .map(|entry| entry.log_id)
            .or(self.last_purged_log_id)
    }

    fn already_applied(&self, log_id: LogId<OpenDbRaftNodeId>) -> bool {
        self.last_applied
            .map(|last_applied| last_applied >= log_id)
            .unwrap_or(false)
    }

    async fn apply_normal_entry(
        &mut self,
        entry: &Entry<RootRangeTypeConfig>,
        command: &RootRangeCommand,
    ) -> Result<(), StorageError<OpenDbRaftNodeId>> {
        let records = self
            .root_range
            .replay()
            .await
            .map_err(|err| Self::apply_error(entry.log_id, err))?;

        match records.get(self.applied_normal_count) {
            Some(existing) if existing == &command.record => {}
            Some(existing) => {
                return Err(Self::storage_error(
                    ErrorSubject::Apply(entry.log_id),
                    ErrorVerb::Write,
                    format!(
                        "root range WAL record {} differs from raft log {}: existing {:?}, proposed {:?}",
                        self.applied_normal_count, entry.log_id, existing, command.record
                    ),
                ));
            }
            None if records.len() == self.applied_normal_count => {
                self.root_range
                    .apply_committed(&command.record)
                    .await
                    .map_err(|err| Self::apply_error(entry.log_id, err))?;
            }
            None => {
                return Err(Self::storage_error(
                    ErrorSubject::Apply(entry.log_id),
                    ErrorVerb::Write,
                    format!(
                        "root range WAL has {} records but raft expected applied normal offset {}",
                        records.len(),
                        self.applied_normal_count
                    ),
                ));
            }
        }

        self.applied_normal_count += 1;
        Ok(())
    }
}

async fn validate_persisted_state_matches_wal(
    root_range: &RootRange,
    state_path: &Path,
    persisted: &PersistedRootRangeRaftStoreState,
) -> OpenDbResult<()> {
    let records = root_range.replay().await?;
    if records.len() < persisted.applied_normal_count {
        return Err(OpenDbError::Storage(format!(
            "root range raft state {} reports {} applied normal records but WAL has {} records",
            state_path.display(),
            persisted.applied_normal_count,
            records.len()
        )));
    }

    let mut applied_normal_records = Vec::new();
    if let Some(last_applied) = persisted.last_applied {
        for entry in persisted.logs.values() {
            if entry.log_id <= last_applied
                && let EntryPayload::Normal(command) = &entry.payload
            {
                applied_normal_records.push(&command.record);
            }
        }
    }

    if applied_normal_records.len() != persisted.applied_normal_count {
        return Err(OpenDbError::Storage(format!(
            "root range raft state {} reports {} applied normal records but contains {} applied normal log entries",
            state_path.display(),
            persisted.applied_normal_count,
            applied_normal_records.len()
        )));
    }

    for (index, expected_record) in applied_normal_records.into_iter().enumerate() {
        let wal_record = records.get(index).ok_or_else(|| {
            OpenDbError::Storage(format!(
                "root range raft state {} applied normal offset {index} is missing from WAL",
                state_path.display()
            ))
        })?;
        if wal_record != expected_record {
            return Err(OpenDbError::Storage(format!(
                "root range raft state {} applied normal offset {index} differs from WAL: wal {:?}, raft {:?}",
                state_path.display(),
                wal_record,
                expected_record
            )));
        }
    }

    Ok(())
}

impl RaftLogReader<RootRangeTypeConfig> for SharedRaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RootRangeTypeConfig>>, StorageError<OpenDbRaftNodeId>> {
        let state = self.0.lock().await;
        Ok(state
            .logs
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogReader<RootRangeTypeConfig> for RootRangeRaftStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RootRangeTypeConfig>>, StorageError<OpenDbRaftNodeId>> {
        self.state.try_get_log_entries(range).await
    }
}

impl RaftStorage<RootRangeTypeConfig> for RootRangeRaftStore {
    type LogReader = SharedRaftLogStore;
    type SnapshotBuilder = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<OpenDbRaftNodeId>,
    ) -> Result<(), StorageError<OpenDbRaftNodeId>> {
        let mut state = self.state.0.lock().await;
        state.vote = Some(*vote);
        state.persist(ErrorSubject::Vote, ErrorVerb::Write).await?;
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<OpenDbRaftNodeId>>, StorageError<OpenDbRaftNodeId>> {
        Ok(self.state.0.lock().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<OpenDbRaftNodeId>>,
    ) -> Result<(), StorageError<OpenDbRaftNodeId>> {
        let mut state = self.state.0.lock().await;
        state.committed = committed;
        state.persist(ErrorSubject::Store, ErrorVerb::Write).await?;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<OpenDbRaftNodeId>>, StorageError<OpenDbRaftNodeId>> {
        Ok(self.state.0.lock().await.committed)
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<RootRangeTypeConfig>, StorageError<OpenDbRaftNodeId>> {
        let state = self.state.0.lock().await;
        Ok(LogState {
            last_purged_log_id: state.last_purged_log_id,
            last_log_id: state.last_log_id(),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.state.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<OpenDbRaftNodeId>>
    where
        I: IntoIterator<Item = Entry<RootRangeTypeConfig>> + openraft::OptionalSend,
    {
        let mut state = self.state.0.lock().await;
        for entry in entries {
            state.logs.insert(entry.log_id.index, entry);
        }
        state.persist(ErrorSubject::Logs, ErrorVerb::Write).await?;
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<OpenDbRaftNodeId>,
    ) -> Result<(), StorageError<OpenDbRaftNodeId>> {
        let mut state = self.state.0.lock().await;
        state.logs.retain(|index, _| *index < log_id.index);
        state
            .persist(ErrorSubject::Log(log_id), ErrorVerb::Delete)
            .await?;
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<OpenDbRaftNodeId>,
    ) -> Result<(), StorageError<OpenDbRaftNodeId>> {
        let mut state = self.state.0.lock().await;
        state.logs.retain(|index, _| *index > log_id.index);
        state.last_purged_log_id = Some(log_id);
        state
            .persist(ErrorSubject::Log(log_id), ErrorVerb::Delete)
            .await?;
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<OpenDbRaftNodeId>>,
            StoredMembership<OpenDbRaftNodeId, BasicNode>,
        ),
        StorageError<OpenDbRaftNodeId>,
    > {
        let state = self.state.0.lock().await;
        Ok((state.last_applied, state.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<RootRangeTypeConfig>],
    ) -> Result<Vec<RootRangeResponse>, StorageError<OpenDbRaftNodeId>> {
        let mut responses = Vec::with_capacity(entries.len());
        let mut state = self.state.0.lock().await;

        for entry in entries {
            if state.already_applied(entry.log_id) {
                responses.push(response_for_entry(entry));
                continue;
            }

            match &entry.payload {
                EntryPayload::Blank => {
                    state.last_applied = Some(entry.log_id);
                    state
                        .persist(ErrorSubject::Apply(entry.log_id), ErrorVerb::Write)
                        .await?;
                    responses.push(RootRangeResponse::Noop);
                }
                EntryPayload::Membership(membership) => {
                    state.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    state.last_applied = Some(entry.log_id);
                    state
                        .persist(ErrorSubject::Apply(entry.log_id), ErrorVerb::Write)
                        .await?;
                    responses.push(RootRangeResponse::Noop);
                }
                EntryPayload::Normal(command) => {
                    state.apply_normal_entry(entry, command).await?;
                    state.last_applied = Some(entry.log_id);
                    state
                        .persist(ErrorSubject::Apply(entry.log_id), ErrorVerb::Write)
                        .await?;
                    responses.push(RootRangeResponse::Applied);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self {
            state: self.state.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<OpenDbRaftNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<OpenDbRaftNodeId, BasicNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<OpenDbRaftNodeId>> {
        Err(RootRangeRaftStoreState::storage_error(
            ErrorSubject::Snapshot(Some(meta.signature())),
            ErrorVerb::Write,
            "root range raft snapshots are disabled in milestone 1",
        ))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RootRangeTypeConfig>>, StorageError<OpenDbRaftNodeId>> {
        Ok(None)
    }
}

impl RaftSnapshotBuilder<RootRangeTypeConfig> for RootRangeRaftStore {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<RootRangeTypeConfig>, StorageError<OpenDbRaftNodeId>> {
        Err(RootRangeRaftStoreState::storage_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            "root range raft snapshots are disabled in milestone 1",
        ))
    }
}

#[derive(Clone, Debug, Default)]
struct TcpRaftNetworkFactory;

impl RaftNetworkFactory<RootRangeTypeConfig> for TcpRaftNetworkFactory {
    type Network = TcpRaftNetwork;

    async fn new_client(&mut self, target: OpenDbRaftNodeId, node: &BasicNode) -> Self::Network {
        TcpRaftNetwork {
            target,
            node: node.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct TcpRaftNetwork {
    target: OpenDbRaftNodeId,
    node: BasicNode,
}

impl TcpRaftNetwork {
    async fn call(
        &self,
        request: RootRangeRaftRpcRequest,
    ) -> Result<
        RootRangeRaftRpcResponse,
        RPCError<OpenDbRaftNodeId, BasicNode, RaftError<OpenDbRaftNodeId>>,
    > {
        let mut stream = TcpStream::connect(&self.node.addr)
            .await
            .map_err(|err| self.network_error(err))?;
        write_json_frame(&mut stream, &request)
            .await
            .map_err(|err| self.network_error(std::io::Error::other(err.to_string())))?;
        read_json_frame(&mut stream)
            .await
            .map_err(|err| self.network_error(std::io::Error::other(err.to_string())))
    }

    fn network_error<E>(
        &self,
        err: E,
    ) -> RPCError<OpenDbRaftNodeId, BasicNode, RaftError<OpenDbRaftNodeId>>
    where
        E: std::error::Error + 'static,
    {
        RPCError::Network(NetworkError::new(&err))
    }

    fn remote_error<E>(&self, err: E) -> RPCError<OpenDbRaftNodeId, BasicNode, E>
    where
        E: std::error::Error,
    {
        RPCError::RemoteError(RemoteError::new_with_node(
            self.target,
            self.node.clone(),
            err,
        ))
    }

    fn unexpected_response<E>(&self, message: &str) -> RPCError<OpenDbRaftNodeId, BasicNode, E>
    where
        E: std::error::Error,
    {
        let err = std::io::Error::other(format!(
            "root range raft peer {} at {} returned {message}",
            self.target, self.node.addr
        ));
        RPCError::Network(NetworkError::new(&err))
    }

    fn append_payload_too_large(
        &self,
        payload_len: usize,
        entries_len: usize,
    ) -> RPCError<OpenDbRaftNodeId, BasicNode, RaftError<OpenDbRaftNodeId>> {
        let err = std::io::Error::other(format!(
            "root range raft append_entries payload to {} at {} is too large: {} bytes",
            self.target, self.node.addr, payload_len
        ));
        let entries_hint = entries_len.saturating_sub(1).max(1) as u64;
        RPCError::PayloadTooLarge(
            PayloadTooLarge::new_entries_hint(entries_hint).with_source_error(&err),
        )
    }
}

impl RaftNetwork<RootRangeTypeConfig> for TcpRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<RootRangeTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<OpenDbRaftNodeId>,
        RPCError<OpenDbRaftNodeId, BasicNode, RaftError<OpenDbRaftNodeId>>,
    > {
        let entries_len = rpc.entries.len();
        let request = RootRangeRaftRpcRequest::AppendEntries(Box::new(rpc));
        let payload_len = encoded_json_len(&request).map_err(|err| self.network_error(err))?;
        if payload_len > MAX_RAFT_RPC_FRAME_LEN {
            return Err(self.append_payload_too_large(payload_len, entries_len));
        }

        match self.call(request).await? {
            RootRangeRaftRpcResponse::AppendEntries(Ok(response)) => Ok(response),
            RootRangeRaftRpcResponse::AppendEntries(Err(err)) => Err(self.remote_error(err)),
            _ => Err(self.unexpected_response("unexpected append_entries response")),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<RootRangeTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<OpenDbRaftNodeId>,
        RPCError<OpenDbRaftNodeId, BasicNode, RaftError<OpenDbRaftNodeId, InstallSnapshotError>>,
    > {
        let response = self
            .call(RootRangeRaftRpcRequest::InstallSnapshot(Box::new(rpc)))
            .await
            .map_err(|err| match err {
                RPCError::Timeout(err) => RPCError::Timeout(err),
                RPCError::Unreachable(err) => RPCError::Unreachable(err),
                RPCError::PayloadTooLarge(err) => RPCError::PayloadTooLarge(err),
                RPCError::Network(err) => RPCError::Network(err),
                RPCError::RemoteError(err) => RPCError::Network(NetworkError::new(&err)),
            })?;

        match response {
            RootRangeRaftRpcResponse::InstallSnapshot(Ok(response)) => Ok(response),
            RootRangeRaftRpcResponse::InstallSnapshot(Err(err)) => Err(self.remote_error(err)),
            _ => Err(self.unexpected_response("unexpected install_snapshot response")),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<OpenDbRaftNodeId>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<OpenDbRaftNodeId>,
        RPCError<OpenDbRaftNodeId, BasicNode, RaftError<OpenDbRaftNodeId>>,
    > {
        match self.call(RootRangeRaftRpcRequest::Vote(rpc)).await? {
            RootRangeRaftRpcResponse::Vote(Ok(response)) => Ok(response),
            RootRangeRaftRpcResponse::Vote(Err(err)) => Err(self.remote_error(err)),
            _ => Err(self.unexpected_response("unexpected vote response")),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "method", content = "payload")]
enum RootRangeRaftRpcRequest {
    AppendEntries(Box<AppendEntriesRequest<RootRangeTypeConfig>>),
    InstallSnapshot(Box<InstallSnapshotRequest<RootRangeTypeConfig>>),
    Vote(VoteRequest<OpenDbRaftNodeId>),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "method", content = "payload")]
enum RootRangeRaftRpcResponse {
    AppendEntries(Result<AppendEntriesResponse<OpenDbRaftNodeId>, RaftError<OpenDbRaftNodeId>>),
    InstallSnapshot(
        Result<
            InstallSnapshotResponse<OpenDbRaftNodeId>,
            RaftError<OpenDbRaftNodeId, InstallSnapshotError>,
        >,
    ),
    Vote(Result<VoteResponse<OpenDbRaftNodeId>, RaftError<OpenDbRaftNodeId>>),
}

async fn read_json_frame<T>(stream: &mut TcpStream) -> OpenDbResult<T>
where
    T: serde::de::DeserializeOwned,
{
    let len = read_frame_len(stream).await?;
    if len > MAX_RAFT_RPC_FRAME_LEN {
        return Err(OpenDbError::Storage(format!(
            "root range raft RPC frame too large: {len}"
        )));
    }

    let mut payload = vec![0_u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|err| OpenDbError::Storage(format!("read root range raft RPC frame: {err}")))?;
    serde_json::from_slice(&payload)
        .map_err(|err| OpenDbError::Storage(format!("decode root range raft RPC frame: {err}")))
}

async fn read_frame_len(stream: &mut TcpStream) -> OpenDbResult<usize> {
    let mut len = [0_u8; 4];
    stream.read_exact(&mut len).await.map_err(|err| {
        OpenDbError::Storage(format!("read root range raft RPC frame length: {err}"))
    })?;
    let len = u32::from_be_bytes(len);
    usize::try_from(len).map_err(|err| {
        OpenDbError::Storage(format!("root range raft RPC frame length overflow: {err}"))
    })
}

async fn write_json_frame<T>(stream: &mut TcpStream, value: &T) -> OpenDbResult<()>
where
    T: serde::Serialize,
{
    let payload = serde_json::to_vec(value)
        .map_err(|err| OpenDbError::Storage(format!("encode root range raft RPC frame: {err}")))?;
    if payload.len() > MAX_RAFT_RPC_FRAME_LEN {
        return Err(OpenDbError::Storage(format!(
            "root range raft RPC frame too large: {}",
            payload.len()
        )));
    }
    let len = u32::try_from(payload.len()).map_err(|err| {
        OpenDbError::Storage(format!("root range raft RPC frame length overflow: {err}"))
    })?;
    stream.write_all(&len.to_be_bytes()).await.map_err(|err| {
        OpenDbError::Storage(format!("write root range raft RPC frame length: {err}"))
    })?;
    stream
        .write_all(&payload)
        .await
        .map_err(|err| OpenDbError::Storage(format!("write root range raft RPC frame: {err}")))
}

fn encoded_json_len<T>(value: &T) -> Result<usize, std::io::Error>
where
    T: serde::Serialize,
{
    serde_json::to_vec(value)
        .map(|payload| payload.len())
        .map_err(|err| std::io::Error::other(format!("encode root range raft RPC frame: {err}")))
}

async fn write_json_file_atomic<T>(path: &Path, value: &T) -> OpenDbResult<()>
where
    T: serde::Serialize,
{
    let parent = containing_dir(path);
    fs::create_dir_all(&parent).await.map_err(|err| {
        OpenDbError::Storage(format!(
            "create root range raft state directory {}: {err}",
            parent.display()
        ))
    })?;

    let temp_path = temp_state_path(path);
    let payload = serde_json::to_vec(value)
        .map_err(|err| OpenDbError::Storage(format!("encode root range raft state json: {err}")))?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)
        .await
        .map_err(|err| storage_file_error(&temp_path, "open temp state file", err))?;

    file.write_all(&payload)
        .await
        .map_err(|err| storage_file_error(&temp_path, "write temp state file", err))?;
    file.sync_data()
        .await
        .map_err(|err| storage_file_error(&temp_path, "sync temp state file", err))?;
    drop(file);

    fs::rename(&temp_path, path).await.map_err(|err| {
        OpenDbError::Storage(format!(
            "replace root range raft state {} with {}: {err}",
            path.display(),
            temp_path.display()
        ))
    })?;
    sync_directory(path, parent).await
}

fn temp_state_path(path: &Path) -> PathBuf {
    let mut temp_path = path.as_os_str().to_owned();
    temp_path.push(".tmp");
    PathBuf::from(temp_path)
}

fn containing_dir(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

async fn sync_directory(path: &Path, dir: PathBuf) -> OpenDbResult<()> {
    let path = path.to_path_buf();
    let task_path = path.clone();
    tokio::task::spawn_blocking(move || {
        std::fs::File::open(&dir)
            .and_then(|file| file.sync_all())
            .map_err(|err| {
                OpenDbError::Storage(format!(
                    "sync root range raft state directory {} for {}: {err}",
                    dir.display(),
                    task_path.display()
                ))
            })
    })
    .await
    .map_err(|err| {
        OpenDbError::Storage(format!(
            "sync root range raft state directory task for {}: {err}",
            path.display()
        ))
    })?
}

fn storage_file_error(path: &Path, operation: &str, err: impl std::fmt::Display) -> OpenDbError {
    OpenDbError::Storage(format!(
        "{operation} for root range raft state {}: {err}",
        path.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::root_range::RootRangeAuthority;
    use openraft::CommittedLeaderId;

    #[tokio::test]
    async fn raft_store_open_rejects_state_ahead_of_wal() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new_with_authority_and_bootstrap_replicas(
            temp_dir.path(),
            RootRangeAuthority::Standalone,
            vec![0],
        );
        let log_id = LogId::new(CommittedLeaderId::new(1, 0), 1);
        let state_path = temp_dir.path().join("root-range").join(RAFT_STORE_FILE);
        let persisted = PersistedRootRangeRaftStoreState {
            version: RAFT_STORE_VERSION,
            logs: BTreeMap::new(),
            vote: None,
            committed: Some(log_id),
            last_applied: Some(log_id),
            last_membership: StoredMembership::default(),
            current_snapshot: None,
            last_purged_log_id: None,
            applied_normal_count: 1,
        };
        write_json_file_atomic(&state_path, &persisted)
            .await
            .expect("write raft state");

        let error = RootRangeRaftStore::open(root_range, temp_dir.path())
            .await
            .expect_err("reject raft state ahead of WAL");

        assert!(
            error
                .to_string()
                .contains("reports 1 applied normal records but WAL has 0 records"),
            "unexpected error: {error}"
        );
    }
}
