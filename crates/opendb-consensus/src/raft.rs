use crate::root_range::{OpenDbRaftNodeId, RootRange, RootRangeCommand};
use opendb_common::{OpenDbError, OpenDbResult};
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
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
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

openraft::declare_raft_types!(
    pub RootRangeTypeConfig:
        D = RootRangeCommand,
        R = RootRangeResponse,
        NodeId = OpenDbRaftNodeId,
        Node = BasicNode,
        Entry = Entry<RootRangeTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RootRangeResponse {
    Applied,
    Noop,
}

pub struct RootRangeRaftHarness {
    node_id: OpenDbRaftNodeId,
    raft: Raft<RootRangeTypeConfig>,
}

impl RootRangeRaftHarness {
    pub async fn new_single_node(
        node_id: OpenDbRaftNodeId,
        data_dir: impl AsRef<Path>,
    ) -> OpenDbResult<Self> {
        let root_range = RootRange::new(data_dir);
        let store = RootRangeRaftStore::new(root_range.clone());
        let (log_store, state_machine) = Adaptor::new(store);
        let config = Arc::new(root_range_raft_config()?);
        let raft = Raft::new(
            node_id,
            config,
            UnreachableRaftNetworkFactory,
            log_store,
            state_machine,
        )
        .await
        .map_err(|err| OpenDbError::Storage(format!("start root range raft: {err}")))?;

        Ok(Self { node_id, raft })
    }

    pub async fn initialize_single_node(&self) -> OpenDbResult<()> {
        let mut members = BTreeMap::new();
        members.insert(
            self.node_id,
            BasicNode::new(format!("in-process-root-range-{}", self.node_id)),
        );

        self.raft
            .initialize(members)
            .await
            .map_err(|err| OpenDbError::Storage(format!("initialize root range raft: {err}")))
    }

    pub fn raft(&self) -> &Raft<RootRangeTypeConfig> {
        &self.raft
    }

    pub async fn shutdown(
        &self,
    ) -> Result<(), <openraft::TokioRuntime as openraft::AsyncRuntime>::JoinError> {
        self.raft.shutdown().await
    }
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

#[derive(Clone, Debug)]
struct SharedRaftLogStore(Arc<tokio::sync::Mutex<RootRangeRaftStoreState>>);

impl SharedRaftLogStore {
    fn new(root_range: RootRange) -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(
            RootRangeRaftStoreState::new(root_range),
        )))
    }
}

#[derive(Debug)]
struct RootRangeRaftStore {
    state: SharedRaftLogStore,
}

#[derive(Debug)]
struct RootRangeRaftStoreState {
    root_range: RootRange,
    logs: BTreeMap<u64, Entry<RootRangeTypeConfig>>,
    vote: Option<Vote<OpenDbRaftNodeId>>,
    committed: Option<LogId<OpenDbRaftNodeId>>,
    last_applied: Option<LogId<OpenDbRaftNodeId>>,
    last_membership: StoredMembership<OpenDbRaftNodeId, BasicNode>,
    current_snapshot: Option<SnapshotMeta<OpenDbRaftNodeId, BasicNode>>,
    last_purged_log_id: Option<LogId<OpenDbRaftNodeId>>,
}

impl RootRangeRaftStore {
    fn new(root_range: RootRange) -> Self {
        Self {
            state: SharedRaftLogStore::new(root_range),
        }
    }
}

impl RootRangeRaftStoreState {
    fn new(root_range: RootRange) -> Self {
        Self {
            root_range,
            logs: BTreeMap::new(),
            vote: None,
            committed: None,
            last_applied: None,
            last_membership: StoredMembership::default(),
            current_snapshot: None,
            last_purged_log_id: None,
        }
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
        self.state.0.lock().await.vote = Some(*vote);
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
        self.state.0.lock().await.committed = committed;
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
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<OpenDbRaftNodeId>,
    ) -> Result<(), StorageError<OpenDbRaftNodeId>> {
        self.state
            .0
            .lock()
            .await
            .logs
            .retain(|index, _| *index < log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<OpenDbRaftNodeId>,
    ) -> Result<(), StorageError<OpenDbRaftNodeId>> {
        let mut state = self.state.0.lock().await;
        state.logs.retain(|index, _| *index > log_id.index);
        state.last_purged_log_id = Some(log_id);
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
            match &entry.payload {
                EntryPayload::Blank => {
                    state.last_applied = Some(entry.log_id);
                    responses.push(RootRangeResponse::Noop);
                }
                EntryPayload::Membership(membership) => {
                    state.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    state.last_applied = Some(entry.log_id);
                    responses.push(RootRangeResponse::Noop);
                }
                EntryPayload::Normal(command) => {
                    state
                        .root_range
                        .apply_committed(&command.record)
                        .await
                        .map_err(|err| RootRangeRaftStoreState::apply_error(entry.log_id, err))?;
                    state.last_applied = Some(entry.log_id);
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
        let mut state = self.state.0.lock().await;
        state.current_snapshot = Some(meta.clone());
        state.last_applied = meta.last_log_id;
        state.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RootRangeTypeConfig>>, StorageError<OpenDbRaftNodeId>> {
        Ok(self
            .state
            .0
            .lock()
            .await
            .current_snapshot
            .clone()
            .map(|meta| Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(Vec::new())),
            }))
    }
}

impl RaftSnapshotBuilder<RootRangeTypeConfig> for RootRangeRaftStore {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<RootRangeTypeConfig>, StorageError<OpenDbRaftNodeId>> {
        let mut state = self.state.0.lock().await;
        let last_applied = state.last_applied;
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: state.last_membership.clone(),
            snapshot_id: last_applied
                .map(|log_id| format!("root-range-{}", log_id.index))
                .unwrap_or_else(|| "root-range-empty".to_string()),
        };
        state.current_snapshot = Some(meta.clone());

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}

#[derive(Clone, Debug, Default)]
struct UnreachableRaftNetworkFactory;

impl RaftNetworkFactory<RootRangeTypeConfig> for UnreachableRaftNetworkFactory {
    type Network = UnreachableRaftNetwork;

    async fn new_client(&mut self, target: OpenDbRaftNodeId, node: &BasicNode) -> Self::Network {
        UnreachableRaftNetwork {
            target,
            node: node.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct UnreachableRaftNetwork {
    target: OpenDbRaftNodeId,
    node: BasicNode,
}

impl UnreachableRaftNetwork {
    fn error<E>(&self) -> RPCError<OpenDbRaftNodeId, BasicNode, E>
    where
        E: std::error::Error,
    {
        let err = std::io::Error::other(format!(
            "root range raft peer RPC is not implemented: target={} addr={}",
            self.target, self.node.addr
        ));
        RPCError::Unreachable(Unreachable::new(&err))
    }
}

impl RaftNetwork<RootRangeTypeConfig> for UnreachableRaftNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<RootRangeTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<OpenDbRaftNodeId>,
        RPCError<OpenDbRaftNodeId, BasicNode, RaftError<OpenDbRaftNodeId>>,
    > {
        Err(self.error())
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<RootRangeTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<OpenDbRaftNodeId>,
        RPCError<OpenDbRaftNodeId, BasicNode, RaftError<OpenDbRaftNodeId, InstallSnapshotError>>,
    > {
        Err(self.error())
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<OpenDbRaftNodeId>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<OpenDbRaftNodeId>,
        RPCError<OpenDbRaftNodeId, BasicNode, RaftError<OpenDbRaftNodeId>>,
    > {
        Err(self.error())
    }
}
