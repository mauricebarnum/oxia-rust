// Copyright 2025-2026 Maurice S. Barnum
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use futures::stream::Stream;
use futures::stream::StreamExt;
use rand::SeedableRng;
use rand::distr::Distribution;
use rand::distr::Uniform;
use rand::rngs::StdRng;
use tokio::sync::Notify;
use tokio::sync::OnceCell;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::Streaming;
use tonic::metadata::MetadataValue;
use tracing::debug;
use tracing::info;
use tracing::trace;
use tracing::warn;

use mauricebarnum_oxia_common::proto as oxia_proto;

use crate::DeleteOptions;
use crate::DeleteRangeOptions;
use crate::Error;
use crate::GetOptions;
use crate::GetResponse;
use crate::GrpcClient;
use crate::ListOptions;
use crate::ListResponse;
use crate::OxiaError;
use crate::PutOptions;
use crate::PutResponse;
use crate::RangeScanOptions;
use crate::RangeScanResponse;
use crate::Result;
use crate::SecondaryIndex;
use crate::batch_get;
use crate::config;
use crate::create_grpc_client;
use crate::pool::ChannelPool;

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ShardMapEpoch(u64);

impl ShardMapEpoch {
    fn new() -> Self {
        Self::default()
    }

    const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

pub type ShardMapUpdateResult = std::result::Result<ShardMapEpoch, Error>;

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ShardId(i64);

impl ShardId {
    pub const fn new(val: i64) -> Self {
        Self(val)
    }

    // A canonical invalid value
    pub const INVALID: Self = Self(-1);

    pub const fn is_invalid(self) -> bool {
        self.0 < 0
    }
}

impl From<i64> for ShardId {
    #[inline]
    fn from(val: i64) -> Self {
        Self(val)
    }
}

impl From<ShardId> for i64 {
    #[inline]
    fn from(my: ShardId) -> Self {
        my.0
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn get_response_as_result(r: oxia_proto::GetResponse) -> Result<Option<GetResponse>> {
    use oxia_proto::Status;
    match Status::try_from(r.status) {
        Ok(Status::Ok) => Ok(Some(GetResponse::from_proto(r))),
        Ok(Status::KeyNotFound) => Ok(None),
        _ => Err(OxiaError::from(r.status).into()),
    }
}

const DIRECT_ID_SLOTS: u64 = 20;

// ShardIdMap<T> we map shard ids to things in seveal "hot" places.  This map should be suitable
// for all, or most, of those uses.
//
// Shard ids should be small integers, nearly contiguous.  Or so we think:  the ids are generated
// by the Oxia server, so we need to be careful to avoid being poisoned with data that causes
// memory consumption to go crazy or leading to bogus indexing.  The approach here is to hope we
// have a small set of contigous ids and they are directly mapped to values.
pub struct ShardIdMap<T> {
    // Array for O(1) direct lookup (Small, dense IDs)
    direct: [Option<T>; DIRECT_ID_SLOTS as usize],
    // BTreeMap for O(log N) lookup (Larger, sparse IDs)
    spilled: BTreeMap<ShardId, T>,
    count: usize,
}

impl<T: fmt::Debug> fmt::Debug for ShardIdMap<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardIdMap")
            .field("direct", &self.direct)
            .field("spilled", &self.spilled)
            .field("count", &self.count)
            .finish()
    }
}

impl<T> Default for ShardIdMap<T> {
    fn default() -> Self {
        Self {
            direct: std::array::from_fn(|_| None),
            spilled: BTreeMap::new(),
            count: 0,
        }
    }
}

impl<T> ShardIdMap<T> {
    const fn len(&self) -> usize {
        self.count
    }

    // Check if ID is small enough for the direct array
    #[inline]
    fn direct_index(id: ShardId) -> Option<usize> {
        #[allow(clippy::cast_sign_loss)]
        // let u = id.0 as usize;
        // (u < DIRECT_ID_SLOTS).then_some(u)
        let u = id.0 as u64;
        (u < DIRECT_ID_SLOTS).then_some(u as usize)
    }

    /// insert `id` with `value` unless `id` is invalid
    pub fn insert(&mut self, id: ShardId, value: T) -> Option<T> {
        if id.is_invalid() {
            return None;
        }

        let old = if let Some(idx) = Self::direct_index(id) {
            self.direct[idx].replace(value)
        } else {
            self.spilled.insert(id, value)
        };

        if old.is_none() {
            self.count += 1;
        }
        old
    }

    pub fn get(&self, id: ShardId) -> Option<&T> {
        if let Some(idx) = Self::direct_index(id) {
            return self.direct[idx].as_ref();
        }
        self.spilled.get(&id)
    }

    pub fn remove(&mut self, id: ShardId) -> Option<T> {
        let old = if let Some(idx) = Self::direct_index(id) {
            self.direct[idx].take()
        } else {
            self.spilled.remove(&id)
        };

        if old.is_some() {
            self.count -= 1;
        }

        old
    }

    pub fn iter(&self) -> impl Iterator<Item = (ShardId, &T)> + '_ {
        fn idx_as_id(idx: usize) -> ShardId {
            debug_assert!(idx < DIRECT_ID_SLOTS as usize);
            #[allow(clippy::cast_possible_wrap)]
            ShardId(idx as i64)
        }

        let direct_iter = self.direct.iter().enumerate().filter_map(|(idx, slot)| {
            let val = slot.as_ref()?;
            Some((idx_as_id(idx), val))
        });

        let spilled_iter = self.spilled.iter().map(|(&id, val)| (id, val));
        direct_iter.chain(spilled_iter)
    }
}

/// Constants for Oxia protocol
const STATUS_OK: i32 = 0;

#[derive(Debug)]
struct Session {
    id: i64,
    heartbeater: JoinHandle<()>,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.heartbeater.abort();
    }
}

/// Shared directory mapping shard IDs to pre-formatted leader URLs (`Arc<str>`).
/// Updated atomically when shard assignments change; clients look up dynamically.
///
/// URLs are formatted once during assignment processing, eliminating per-RPC
/// `format!("http://...")` allocations.
pub type LeaderDirectory = Arc<ArcSwap<ShardIdMap<Arc<str>>>>;

fn format_leader_url(a: &oxia_proto::ShardAssignment) -> Arc<str> {
    Arc::from(format!("http://{}", a.leader))
}

#[derive(Debug)]
struct ClientData {
    config: Arc<config::Config>,
    channel_pool: ChannelPool,
    shard_id: ShardId,
    /// Shared reference to leader directory - enables dynamic leader lookup.
    /// When leader changes, clients automatically use the new leader on next RPC.
    leaders: LeaderDirectory,
    meta: tonic::metadata::MetadataMap,
    cancel_token: CancellationToken,
    // Note: No gRPC client caching - we rely on ChannelPool which already caches
    // connections by URL. This simplifies wrong-leader handling since there's no
    // stale client to invalidate.
}

impl ClientData {
    const fn new(
        config: Arc<config::Config>,
        channel_pool: ChannelPool,
        shard_id: ShardId,
        leaders: LeaderDirectory,
        meta: tonic::metadata::MetadataMap,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            config,
            channel_pool,
            shard_id,
            leaders,
            meta,
            cancel_token,
        }
    }

    /// Get the current leader address for this shard, if known.
    fn get_leader(&self) -> Option<Arc<str>> {
        self.leaders.load().get(self.shard_id).cloned()
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    data: Arc<ClientData>,
    session: Arc<OnceCell<Option<Session>>>,
}

impl Client {
    /// Look up current leader address from the shared leader directory.
    fn get_leader(&self) -> Option<Arc<str>> {
        self.data.leaders.load().get(self.data.shard_id).cloned()
    }

    /// Get a gRPC client for the current leader.
    ///
    /// Performs dynamic leader lookup on each call - no caching at this level.
    /// The `ChannelPool` handles connection caching by URL, so repeated calls
    /// to the same leader reuse the existing connection.
    async fn get_grpc_client(&self) -> crate::Result<GrpcClient> {
        let leader = self
            .get_leader()
            .ok_or_else(|| Error::NoShardMapping(self.data.shard_id))?;
        let channel = self.data.channel_pool.get(&leader).await?;
        Ok(GrpcClient::new(channel))
    }

    /// Invalidate the cached channel for the current leader.
    async fn invalidate_channel(&self) {
        if let Some(leader) = self.get_leader() {
            self.data.channel_pool.remove(&leader).await;
        }
    }

    /// Execute an RPC with automatic channel invalidation on connection errors.
    async fn rpc<T, F, Fut>(&self, f: F) -> Result<T>
    where
        F: FnOnce(GrpcClient) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    {
        let client = self.get_grpc_client().await?;
        let result: Result<T> = f(client).await.map_err(Into::into);
        if let Err(ref e) = result
            && e.is_connection_error()
        {
            if let Some(leader) = self.get_leader() {
                debug!(leader = %leader, "connection error, invalidating channel");
            }
            self.invalidate_channel().await;
        }
        result
    }
}

/// Enum to indicate the expected type of write response
#[derive(Copy, Clone)]
enum ExpectedWriteResponse {
    Put,
    Delete,
    DeleteRange,
}

/// Checks if a write response matches expectations
fn check_write_response(r: &oxia_proto::WriteResponse, x: ExpectedWriteResponse) -> Option<Error> {
    let (nputs, ndeletes, nranges): (usize, usize, usize) = match x {
        ExpectedWriteResponse::Put => (1, 0, 0),
        ExpectedWriteResponse::Delete => (0, 1, 0),
        ExpectedWriteResponse::DeleteRange => (0, 0, 1),
    };
    if r.puts.len() != nputs || r.deletes.len() != ndeletes || r.delete_ranges.len() != nranges {
        return Some(Error::Custom(format!(
            "Expected puts:{}={}, deletes:{}={}, ranges:{}={}",
            r.puts.len(),
            nputs,
            r.deletes.len(),
            ndeletes,
            r.delete_ranges.len(),
            nranges
        )));
    }
    None
}

/// Validates a put response - ensures it has the expected structure
fn check_put_response(r: &oxia_proto::WriteResponse) -> Option<Error> {
    if let Some(e) = check_write_response(r, ExpectedWriteResponse::Put) {
        return Some(e);
    }

    if let Some(p) = r.puts.first()
        && p.status != STATUS_OK
    {
        return Some(OxiaError::from(p.status).into());
    }

    None
}

/// Validates a delete response - ensures it has the expected structure and status
fn check_delete_response(r: &oxia_proto::WriteResponse) -> Option<Error> {
    if let Some(e) = check_write_response(r, ExpectedWriteResponse::Delete) {
        return Some(e);
    }

    if let Some(d) = r.deletes.first()
        && d.status != STATUS_OK
    {
        return Some(OxiaError::from(d.status).into());
    }
    None
}

fn check_delete_range_response(r: &oxia_proto::WriteResponse) -> Option<Error> {
    if let Some(e) = check_write_response(r, ExpectedWriteResponse::DeleteRange) {
        return Some(e);
    }

    if let Some(d) = r.delete_ranges.first()
        && d.status != STATUS_OK
    {
        return Some(OxiaError::from(d.status).into());
    }
    None
}

#[inline]
fn no_response_error(server: Option<&str>, shard_id: ShardId) -> Error {
    let server = server.unwrap_or("unknown");
    let msg = format!("server={server} shard={shard_id}");
    Error::NoResponseFromServer(msg)
}

// const INTERNAL_KEY_PREFIX: &str = "__oxia/";

impl Client {
    fn start_heartbeat(&self, session_id: i64) -> JoinHandle<()> {
        let session_timeout_ms = self.data.config.session_timeout().as_millis();
        assert!(
            session_timeout_ms <= u128::from(u32::MAX),
            "bug: time out out of range {} max {}",
            session_timeout_ms,
            u32::MAX
        );

        let client = self.clone();

        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).unwrap();
        let mut rng = StdRng::from_seed(seed);

        tokio::spawn(async move {
            // We'll send heartbeats a random time +/- X% of one quarter of session timeout.
            // This will allow us miss a few heartbeats without losing the session
            let distr = {
                const JITTER: f64 = 0.03;
                let target_ms = session_timeout_ms as f64 / 4.0;
                #[allow(clippy::cast_sign_loss)]
                let min_ms = (target_ms * (1.0 - JITTER)).round() as u32;
                #[allow(clippy::cast_sign_loss)]
                let max_ms = (target_ms * (1.0 + JITTER)).round() as u32;
                Uniform::new_inclusive(min_ms, max_ms).unwrap()
            };
            loop {
                tokio::select! {
                    () = client.data.cancel_token.cancelled() => break,
                    () = async {
                        let req = client.create_request(oxia_proto::SessionHeartbeat {
                            session_id,
                            shard: client.data.shard_id.into(),
                        });
                        let result = client
                            .rpc(|mut grpc| async move { grpc.keep_alive(req).await })
                        .await;
                        debug!(?result, "grpc.keep_alive");

                        if let Err(ref e) = result {
                            warn!(?e, "heartbeat failed");
                        }

                        let sleep_ms = distr.sample(&mut rng);
                        let timeout = Duration::from_millis(sleep_ms.into());
                        trace!("sleeping for {} ms", sleep_ms);
                        sleep(timeout).await;
                    } => {}
                }
            }
        })
    }

    /// Creates a new Client instance for a specific shard.
    ///
    /// The client looks up the current leader dynamically from the shared `leaders`
    /// directory on each RPC, enabling automatic recovery when leaders change.
    pub(crate) fn new(
        channel_pool: ChannelPool,
        config: Arc<config::Config>,
        cancel_token: CancellationToken,
        shard_id: ShardId,
        leaders: LeaderDirectory,
    ) -> Result<Self> {
        // Create metadata with shard ID and namespace
        let mut meta = tonic::metadata::MetadataMap::new();
        meta.insert("shard-id", MetadataValue::from(shard_id.0));
        meta.insert(
            "namespace",
            MetadataValue::from_str(config.namespace())
                .map_err(|e| Error::Custom(format!("Invalid metadata value: {e}")))?,
        );

        Ok(Self {
            data: Arc::new(ClientData::new(
                config,
                channel_pool,
                shard_id,
                leaders,
                meta,
                cancel_token,
            )),
            session: Arc::new(OnceCell::new()),
        })
    }

    pub(crate) fn id(&self) -> ShardId {
        self.data.shard_id
    }

    async fn create_session(&self) -> Result<Session> {
        let config = &self.data.config;
        let req = self.create_request(oxia_proto::CreateSessionRequest {
            session_timeout_ms: config.session_timeout().as_millis() as u32,
            client_identity: config.identity().into(),
            shard: self.data.shard_id.0,
        });
        let rsp = self
            .rpc(|mut grpc| async move { grpc.create_session(req).await })
            .await?
            .into_inner();
        Ok(Session {
            id: rsp.session_id,
            heartbeater: self.start_heartbeat(rsp.session_id),
        })
    }

    async fn get_session_id(&self) -> Result<Option<i64>> {
        let s = self
            .session
            .get_or_try_init(|| async {
                let rsp = self.create_session().await;
                debug!(?rsp, "get_session_id");
                rsp.map(Some)
            })
            .await?;
        Ok(s.as_ref().map(|session| session.id))
    }

    fn make_proto_get_req(opts: &GetOptions, k: impl Into<String>) -> oxia_proto::GetRequest {
        oxia_proto::GetRequest {
            key: k.into(),
            include_value: opts.include_value,
            comparison_type: opts.comparison_type.into(),
            secondary_index_name: None,
        }
    }

    /// Creates a `GetRequest` with the appropriate options
    fn make_get_req(&self, opts: &GetOptions, k: impl Into<String>) -> oxia_proto::ReadRequest {
        let data = &self.data;
        oxia_proto::ReadRequest {
            shard: Some(data.shard_id.0),
            gets: vec![Self::make_proto_get_req(opts, k)],
        }
    }

    /// Creates a `PutRequest` with the appropriate options
    fn make_put_req(
        &self,
        session_id: Option<i64>,
        opts: &PutOptions,
        k: &str,
        v: Bytes,
    ) -> oxia_proto::WriteRequest {
        let data = &self.data;
        oxia_proto::WriteRequest {
            shard: Some(data.shard_id.0),
            puts: vec![oxia_proto::PutRequest {
                key: k.to_string(),
                value: v,
                expected_version_id: opts.expected_version_id,
                session_id,
                client_identity: Some(data.config.identity().into()),
                partition_key: opts.partition_key.clone(),
                sequence_key_delta: opts
                    .sequence_key_deltas
                    .as_ref()
                    .map_or_else(Vec::new, |x| (*x).to_vec()),
                secondary_indexes: SecondaryIndex::to_proto(opts.secondary_indexes.clone()),
            }],
            deletes: vec![],
            delete_ranges: vec![],
        }
    }

    /// Creates a `DeleteRequest` with the appropriate options
    fn make_delete_req(&self, opts: &DeleteOptions, k: &str) -> oxia_proto::WriteRequest {
        let data = &self.data;
        oxia_proto::WriteRequest {
            shard: Some(data.shard_id.0),
            puts: vec![],
            deletes: vec![oxia_proto::DeleteRequest {
                key: k.to_string(),
                expected_version_id: opts.expected_version_id,
            }],
            delete_ranges: vec![],
        }
    }

    /// Creates a `DeleteRangeRequest` with the appropriate options
    fn make_delete_range_req(
        &self,
        _opts: &DeleteRangeOptions,
        start_inclusive: &str,
        end_exclusive: &str,
    ) -> oxia_proto::WriteRequest {
        let data = &self.data;
        oxia_proto::WriteRequest {
            shard: Some(data.shard_id.0),
            puts: vec![],
            deletes: vec![],
            delete_ranges: vec![oxia_proto::DeleteRangeRequest {
                start_inclusive: start_inclusive.to_string(),
                end_exclusive: end_exclusive.to_string(),
            }],
        }
    }

    /// Creates a `ListRequest` with the appropriate options
    fn make_list_req(
        &self,
        opts: &ListOptions,
        start_inclusive: &str,
        end_exclusive: &str,
    ) -> oxia_proto::ListRequest {
        let data = &self.data;
        oxia_proto::ListRequest {
            shard: Some(data.shard_id.0),
            start_inclusive: start_inclusive.to_string(),
            end_exclusive: end_exclusive.to_string(),
            secondary_index_name: opts.secondary_index_name.clone(),
            include_internal_keys: opts.include_internal_keys,
        }
    }

    /// Creates a `RangeScanRequest` with the appropriate options
    fn make_range_scan_req(
        &self,
        opts: &RangeScanOptions,
        start_inclusive: &str,
        end_exclusive: &str,
    ) -> oxia_proto::RangeScanRequest {
        let data = &self.data;

        oxia_proto::RangeScanRequest {
            shard: Some(data.shard_id.0),
            start_inclusive: start_inclusive.to_string(),
            end_exclusive: end_exclusive.to_string(),
            secondary_index_name: opts.secondary_index_name.clone(),
            include_internal_keys: opts.include_internal_keys,
        }
    }

    fn create_request<T>(&self, payload: T) -> tonic::Request<T> {
        let mut req = tonic::Request::from_parts(
            self.data.meta.clone(),
            tonic::Extensions::default(),
            payload,
        );
        if let Some(timeout) = self.data.config.request_timeout() {
            req.set_timeout(timeout);
        }
        req
    }

    /// Processes a write request through the gRPC stream
    async fn process_write(
        &self,
        write_req: oxia_proto::WriteRequest,
    ) -> Result<oxia_proto::WriteResponse> {
        let data = &self.data;

        let write_req = oxia_proto::WriteRequest {
            shard: Some(data.shard_id.0),
            ..write_req
        };

        // Create a channel for the write stream with a buffer of 1 (only sending one request)
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        // Send the request
        tx.send(write_req)
            .await
            .map_err(|e| Error::Custom(format!("Failed to send write request: {e}")))?;

        // Create stream from the receiver
        let write_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        // Create request with proper metadata
        let req = self.create_request(write_stream);

        // Get response stream with better error handling
        let mut resp_stream = self
            .rpc(|mut grpc| async move { grpc.write_stream(req).await })
            .await?
            .into_inner();

        // Get first response
        let write_response = match resp_stream.next().await {
            Some(Ok(response)) => response,
            Some(Err(err)) => return Err(err.into()),
            None => {
                return Err(Error::Custom(
                    "Server returned no write response".to_string(),
                ));
            }
        };

        // Close the stream by dropping the sender
        drop(tx);

        // Verify we received only one response
        if let Some(response) = resp_stream.next().await {
            return Err(Error::Custom(format!(
                "Expected exactly one response, but got additional response: {response:?}"
            )));
        }

        Ok(write_response)
    }

    /// Retrieves a key with the given options
    pub(crate) async fn get(&self, k: &str, opts: &GetOptions) -> Result<Option<GetResponse>> {
        let req = self.create_request(self.make_get_req(opts, k));
        let mut rsp_stream = self
            .rpc(|mut grpc| async move { grpc.read(req).await })
            .await?
            .into_inner();

        let rsp = match rsp_stream.next().await {
            Some(Ok(response)) => response,
            Some(Err(err)) => return Err(err.into()),
            None => {
                return Err(no_response_error(
                    self.data.get_leader().as_deref(),
                    self.data.shard_id,
                ));
            }
        };

        // Verify we received only one response
        if let Some(response) = rsp_stream.next().await {
            return Err(Error::Custom(format!(
                "Expected only one ReadResponse, but got additional response: {response:?}"
            )));
        }

        // Check that we got exactly one GetResponse
        if rsp.gets.len() != 1 {
            return Err(Error::Custom(format!(
                "Expected one GetResponse, got {}",
                rsp.gets.len()
            )));
        }

        get_response_as_result(rsp.gets.into_iter().next().unwrap())
    }

    async fn demux_stream(
        context: Arc<ClientData>,
        keys: Vec<Arc<str>>,
        read_response: Result<tonic::Response<tonic::Streaming<oxia_proto::ReadResponse>>>,
    ) -> impl Stream<Item = batch_get::ResponseItem> {
        enum State {
            Demuxing {
                context: Arc<ClientData>,
                keys_iter: std::vec::IntoIter<Arc<str>>,
                gets_iter: std::vec::IntoIter<oxia_proto::GetResponse>,
            },
            Failing {
                keys_iter: std::vec::IntoIter<Arc<str>>,
                err: Error,
            },
        }

        let keys_iter = keys.into_iter();
        let state = match read_response {
            Ok(s) => match s.into_inner().next().await {
                Some(Ok(rr)) => State::Demuxing {
                    context,
                    keys_iter,
                    gets_iter: rr.gets.into_iter(),
                },
                Some(Err(status)) => State::Failing {
                    keys_iter,
                    err: status.into(),
                },
                None => State::Failing {
                    keys_iter,
                    err: no_response_error(context.get_leader().as_deref(), context.shard_id),
                },
            },
            Err(err) => State::Failing { keys_iter, err },
        };

        futures::stream::unfold(state, move |mut state| async move {
            loop {
                match state {
                    State::Demuxing {
                        context,
                        mut keys_iter,
                        mut gets_iter,
                    } => {
                        let Some(r) = gets_iter.next() else {
                            if keys_iter.len() == 0 {
                                return None;
                            }
                            // gets exhausted early; fail remaining keys
                            let err = no_response_error(
                                context.get_leader().as_deref(),
                                context.shard_id,
                            );
                            state = State::Failing { keys_iter, err };
                            continue;
                        };

                        let Some(k) = keys_iter.next() else {
                            warn!(?r, remaining = %gets_iter.len(), "protocol error: response without matching request key");
                            if gets_iter.len() > 0 {
                                debug!("unmatched responses: {:?}", gets_iter.collect::<Vec<_>>());
                            }
                            return None;
                        };

                        let item = batch_get::ResponseItem::new(
                            k,
                            crate::shard::get_response_as_result(r),
                        );
                        let next = State::Demuxing {
                            context,
                            keys_iter,
                            gets_iter,
                        };
                        return Some((item, next));
                    }
                    State::Failing { mut keys_iter, err } => {
                        let key = keys_iter.next()?;
                        let failed = batch_get::ResponseItem::failed(key, err.clone());
                        return Some((failed, State::Failing { keys_iter, err }));
                    }
                };
            }
        })
    }

    pub(crate) async fn batch_get(
        &self,
        batch_req: batch_get::Request,
    ) -> impl Stream<Item = batch_get::ResponseItem> + use<> {
        let opts = match batch_req.opts {
            Some(o) => o.into(),
            None => GetOptions::default(),
        };

        let gets: Vec<oxia_proto::GetRequest> = batch_req
            .keys
            .iter()
            .map(|key| Self::make_proto_get_req(&opts, key.to_string()))
            .collect();

        let read_req = oxia_proto::ReadRequest {
            shard: Some(self.data.shard_id.into()),
            gets,
        };

        let read_result = self
            .rpc(|mut grpc| async move { grpc.read(read_req).await })
            .await;

        Self::demux_stream(Arc::clone(&self.data), batch_req.keys, read_result).await
    }

    /// Puts a value with the given options
    pub(crate) async fn put(
        &self,
        key: &str,
        value: Bytes,
        options: &PutOptions,
    ) -> Result<PutResponse> {
        let session_id = if options.ephemeral {
            self.get_session_id().await?
        } else {
            None
        };

        let req = self.make_put_req(session_id, options, key, value);
        let rsp = self.process_write(req).await?;

        if let Some(e) = check_put_response(&rsp) {
            return Err(e);
        }

        let p = rsp.puts.into_iter().next().unwrap();
        Ok(PutResponse::from_proto(p))
    }

    /// Deletes a key with the given options
    pub(crate) async fn delete(&self, key: &str, options: &DeleteOptions) -> Result<()> {
        let req = self.make_delete_req(options, key);
        let rsp = self.process_write(req).await?;

        if let Some(e) = check_delete_response(&rsp) {
            return Err(e);
        }

        Ok(())
    }

    /// Deletes a range of keys with the given options
    pub(crate) async fn delete_range(
        &self,
        start_inclusive: &str,
        end_exclusive: &str,
        options: &DeleteRangeOptions,
    ) -> Result<()> {
        let req = self.make_delete_range_req(options, start_inclusive, end_exclusive);
        let rsp = self.process_write(req).await?;

        check_delete_range_response(&rsp).map_or_else(|| Ok(()), Err)
    }

    /// Lists keys in the given range with options
    pub(crate) async fn list(
        &self,
        start_inclusive: &str,
        end_exclusive: &str,
        opts: &ListOptions,
    ) -> Result<ListResponse> {
        let partial_ok = opts.partial_ok;
        let req = self.create_request(self.make_list_req(opts, start_inclusive, end_exclusive));

        let mut rsp = ListResponse::default();
        let mut rsp_stream = self
            .rpc(|mut grpc| async move { grpc.list(req).await })
            .await?
            .into_inner();

        while let Some(r) = rsp_stream.next().await {
            match r {
                Ok(r) => rsp.accum(r),
                Err(err) => {
                    if partial_ok {
                        rsp.partial = true;
                    } else {
                        return Err(err.into());
                    }
                }
            }
        }

        Ok(rsp)
    }

    /// Gets a list with timeout handling
    #[allow(dead_code)]
    pub(crate) async fn list_with_timeout(
        &self,
        start_inclusive: &str,
        end_exclusive: &str,
        opts: &ListOptions,
        timeout: Duration,
    ) -> Result<ListResponse> {
        // Apply timeout to the list operation
        match tokio::time::timeout(timeout, self.list(start_inclusive, end_exclusive, opts)).await {
            Ok(result) => result,
            Err(_) => Err(Error::Custom(format!(
                "List request timed out after {timeout:?}"
            ))),
        }
    }

    /// Scans for records in the given range with options
    pub(crate) async fn range_scan(
        &self,
        start_inclusive: &str,
        end_exclusive: &str,
        opts: &RangeScanOptions,
    ) -> Result<RangeScanResponse> {
        let partial_ok = opts.partial_ok;
        let req =
            self.create_request(self.make_range_scan_req(opts, start_inclusive, end_exclusive));

        let mut rsp = RangeScanResponse::default();
        let mut rsp_stream = self
            .rpc(|mut grpc| async move { grpc.range_scan(req).await })
            .await?
            .into_inner();

        while let Some(r) = rsp_stream.next().await {
            match r {
                Ok(r) => rsp.accum(r),
                Err(err) => {
                    if partial_ok {
                        rsp.partial = true;
                    } else {
                        return Err(err.into());
                    }
                }
            }
        }

        Ok(rsp)
    }

    pub(crate) async fn get_notifications(
        &self,
        start_offset_exclusive: Option<i64>,
    ) -> Result<NotificationsStream> {
        let req = oxia_proto::NotificationsRequest {
            shard: self.data.shard_id.0,
            start_offset_exclusive,
        };

        let rsp = self
            .rpc(|mut grpc| async move { grpc.get_notifications(req).await })
            .await?;
        Ok(NotificationsStream::from_proto(rsp.into_inner()))
    }
}

/// Trait for a shardfmapping strategy.
///
/// Note: this trait is object-safe, despite being using with static dispatch.  This is to
/// facilitate possibly supporting more than one mapping strategy later.
trait ShardMapper {
    /// Builder type that can construct this map.
    type Builder: ShardMapperBuilder<Mapper = Self>;

    /// Create a new builder for this map.
    fn new_builder(n: &oxia_proto::NamespaceShardsAssignment) -> Result<Self::Builder>;

    /// Get the shard ID for a given key.
    fn get_shard_id(&self, key: &str) -> Option<ShardId>;
}

/// Trait for building a `ShardMapper`.
trait ShardMapperBuilder {
    /// The resulting map type produced by this builder.
    type Mapper: ShardMapper<Builder = Self>;

    /// Incorporate this assiggment in the map, or fail if there's a problem such as the data in
    /// illogical, etc.
    fn accept(&mut self, a: &oxia_proto::ShardAssignment) -> Result<()>;

    /// Build the shard map.
    fn build(self) -> Result<Self::Mapper>;
}

mod int32_hash_range {
    use crate::{OverlappingRanges, ServerError, ShardId};

    use super::{Result, ShardMapper, ShardMapperBuilder};
    use mauricebarnum_oxia_common::proto as oxia_proto;
    use std::{cmp::Ordering, collections::HashSet};
    use xxhash_rust::xxh3::xxh3_64;

    #[derive(Debug)]
    struct Range {
        min: u32,
        max: u32,
        id: ShardId,
    }

    #[derive(Debug, Default)]
    pub struct Mapper {
        ranges: Vec<Range>,
    }

    impl Mapper {
        fn hash(key: &str) -> u32 {
            let h = xxh3_64(key.as_bytes());
            (h & 0xffff_ffff) as u32
        }
    }

    impl ShardMapper for Mapper {
        type Builder = Builder;

        fn new_builder(n: &oxia_proto::NamespaceShardsAssignment) -> Result<Self::Builder> {
            let r = oxia_proto::ShardKeyRouter::try_from(n.shard_key_router)
                .map_err(|e| ServerError::BadShardKeyRouter(e.to_string()))?;

            if r != oxia_proto::ShardKeyRouter::Xxhash3 {
                return Err(ServerError::BadShardKeyRouter(format!(
                    "unsupported shard_key_router {r:?}"
                ))
                .into());
            }

            Ok(Builder {
                ranges: Vec::with_capacity(n.assignments.len()),
            })
        }

        fn get_shard_id(&self, key: &str) -> Option<ShardId> {
            let h = Self::hash(key);
            let found = self.ranges.binary_search_by(|x| {
                if x.max < h {
                    Ordering::Less
                } else if x.min > h {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            });
            found.ok().map(|i| self.ranges[i].id)
        }
    }

    #[derive(Debug)]
    pub struct Builder {
        ranges: Vec<Range>,
    }

    impl Builder {
        fn extract_range(a: &oxia_proto::ShardAssignment) -> Result<&oxia_proto::Int32HashRange> {
            use oxia_proto::shard_assignment::ShardBoundaries::Int32HashRange;
            let b = a
                .shard_boundaries
                .as_ref()
                .ok_or(ServerError::NoShardBoundaries)?;

            match b {
                Int32HashRange(r) => Ok(r),
            }
        }

        fn validate(&self) -> Result<()> {
            // Validate the ranges are not overlapping, and there are no duplicate shards

            if self.ranges.is_empty() {
                return Ok(());
            }

            let mut p = &self.ranges[0];
            let mut seen = HashSet::with_capacity(self.ranges.len());

            for r in &self.ranges[1..] {
                if r.min <= p.max {
                    let overlap = OverlappingRanges {
                        range1_min: p.min,
                        range1_max: p.max,
                        range2_min: r.min,
                        range2_max: r.max,
                    };
                    return Err(ServerError::OverlappingRanges(overlap).into());
                }
                p = r;

                if !seen.insert(r.id) {
                    return Err(ServerError::DuplicateShardId(r.id).into());
                }
            }

            Ok(())
        }
    }

    impl ShardMapperBuilder for Builder {
        type Mapper = Mapper;

        fn accept(&mut self, a: &oxia_proto::ShardAssignment) -> Result<()> {
            let r = Self::extract_range(a)?;
            self.ranges.push(Range {
                min: r.min_hash_inclusive,
                max: r.max_hash_inclusive,
                id: a.shard.into(),
            });
            Ok(())
        }

        fn build(mut self) -> Result<Self::Mapper> {
            self.ranges.sort_unstable_by_key(|x| x.min);
            self.validate()?;
            Ok(Mapper {
                ranges: self.ranges,
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        #[should_panic(expected = "called `Result::unwrap()` on an `Err` value")]
        fn test_builder_overlaps() {
            Builder {
                ranges: vec![
                    Range {
                        min: 0,
                        max: 2,
                        id: ShardId::new(0),
                    },
                    Range {
                        min: 1,
                        max: 3,
                        id: ShardId::new(1),
                    },
                ],
            }
            .build()
            .unwrap();
        }

        #[test]
        #[should_panic(expected = "called `Result::unwrap()` on an `Err` value")]
        fn test_builder_duplicate_shard_id() {
            Builder {
                ranges: vec![
                    Range {
                        min: 0,
                        max: 1_431_655_765u32,
                        id: ShardId::new(0),
                    },
                    Range {
                        min: 1_431_655_766,
                        max: 2_863_311_531,
                        id: ShardId::new(1),
                    },
                    Range {
                        min: 2_863_311_532,
                        max: 3_015_596_446,
                        id: ShardId::new(2),
                    },
                    // hole! [3_015_596_447, 3_015_596_448]
                    Range {
                        min: 3_015_596_449,
                        max: 4_294_967_295,
                        id: ShardId::new(2),
                    },
                ],
            }
            .build()
            .unwrap();
        }

        #[test]
        fn test_get_shard_id() {
            let mapper = Builder {
                ranges: vec![
                    Range {
                        min: 0,
                        max: 1_431_655_765,
                        id: ShardId::new(0),
                    },
                    Range {
                        min: 1_431_655_766,
                        max: 2_863_311_531,
                        id: ShardId::new(1),
                    },
                    Range {
                        min: 2_863_311_532,
                        max: 3_015_596_446,
                        id: ShardId::new(2),
                    },
                    // hole! [3_015_596_447, 3_015_596_448]
                    Range {
                        min: 3_015_596_449,
                        max: 4_294_967_295,
                        id: ShardId::new(3),
                    },
                ],
            }
            .build()
            .unwrap();

            assert_eq!(1, mapper.get_shard_id("foo-0").unwrap().0);
            assert_eq!(3, mapper.get_shard_id("foo-4").unwrap().0);
            assert_eq!(0, mapper.get_shard_id("foo-5").unwrap().0);
            assert_eq!(2, mapper.get_shard_id("e199").unwrap().0);
            assert!(mapper.get_shard_id("foo-3").is_none());
        }
    }
} // mod int32_hash_range

mod searchable {
    use crate::shard::ShardMapEpoch;

    use super::{Client, Error, Result, ShardId, ShardIdMap, ShardMapper, int32_hash_range};

    #[derive(Debug, Default)]
    pub struct Shards {
        #[allow(clippy::struct_field_names)]
        shards: ShardIdMap<Client>,
        mapper: int32_hash_range::Mapper,
        epoch: ShardMapEpoch,
    }

    impl Shards {
        pub(super) const fn new(
            shards: ShardIdMap<Client>,
            mapper: int32_hash_range::Mapper,
            epoch: ShardMapEpoch,
        ) -> Self {
            Self {
                shards,
                mapper,
                epoch,
            }
        }

        pub(super) const fn epoch(&self) -> ShardMapEpoch {
            self.epoch
        }

        pub(crate) fn find_by_id(&self, id: ShardId) -> Result<Client> {
            self.shards
                .get(id)
                .cloned()
                .ok_or_else(|| Error::NoShardMapping(id))
        }

        pub(crate) fn get_shard_id(&self, key: &str) -> Option<ShardId> {
            self.mapper.get_shard_id(key)
        }

        pub(super) fn find_by_key(&self, key: &str) -> Result<Client> {
            let id = self
                .get_shard_id(key)
                .ok_or_else(|| Error::NoShardMappingForKey(key.into()))?;
            self.find_by_id(id)
        }

        pub(super) fn iter(&self) -> impl Iterator<Item = (ShardId, &Client)> + '_ {
            self.shards.iter()
        }

        pub(super) const fn len(&self) -> usize {
            self.shards.len()
        }

        pub(crate) fn get(&self, id: ShardId) -> Option<&Client> {
            self.shards.get(id)
        }
    }
}

#[derive(Debug)]
pub struct NotificationsStream {
    inner: tonic::Streaming<oxia_proto::NotificationBatch>,
}

impl NotificationsStream {
    const fn from_proto(inner: tonic::Streaming<oxia_proto::NotificationBatch>) -> Self {
        Self { inner }
    }
}

impl Stream for NotificationsStream {
    type Item = crate::Result<crate::NotificationBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.get_mut().inner.poll_next_unpin(cx) {
            Poll::Ready(Some(item)) => {
                let r = match item {
                    Ok(b) => Ok(crate::NotificationBatch::from_proto(b)),
                    Err(e) => Err(e.into()),
                };
                Poll::Ready(Some(r))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct ShardAssignmentTask {
    config: Arc<config::Config>,
    channel_pool: ChannelPool,
    shards: Arc<ArcSwap<searchable::Shards>>,
    /// Shared leader directory - updated atomically when assignments change.
    /// Clients hold a reference to this and look up leaders dynamically.
    leaders: LeaderDirectory,
    token: CancellationToken,
    changed: watch::Sender<ShardMapUpdateResult>,
    refresh_signal: Arc<Notify>,
    last_refresh: Instant,
}

impl ShardAssignmentTask {
    fn new(
        config: Arc<config::Config>,
        channel_pool: ChannelPool,
        shards: Arc<ArcSwap<searchable::Shards>>,
        leaders: LeaderDirectory,
        token: CancellationToken,
        changed: watch::Sender<ShardMapUpdateResult>,
        refresh_signal: Arc<Notify>,
    ) -> Self {
        Self {
            config,
            channel_pool,
            shards,
            leaders,
            token,
            changed,
            refresh_signal,
            last_refresh: Instant::now(),
        }
    }

    fn make_mapper(ns: &oxia_proto::NamespaceShardsAssignment) -> Result<int32_hash_range::Mapper> {
        let mut mb = int32_hash_range::Mapper::new_builder(ns)?;
        for a in &ns.assignments {
            mb.accept(a)?;
        }
        mb.build()
    }

    fn process_assignments(&self, mut ns: oxia_proto::NamespaceShardsAssignment) -> Result<()> {
        // Create shard mappings and bail out if we find anything silly.
        let mapper = Self::make_mapper(&ns)?;
        let current = self.shards.load();

        // Build new leaders map from assignments
        let mut new_leaders = ShardIdMap::<Arc<str>>::default();
        for a in &mut ns.assignments {
            new_leaders.insert(a.shard.into(), format_leader_url(a));
        }
        // Update leaders atomically - clients will see new leaders on next RPC
        self.leaders.store(Arc::new(new_leaders));

        let mut shards = ShardIdMap::<Client>::default();

        for a in &ns.assignments {
            info!(?a, "processing assignments");
            let id = a.shard.into();
            // Reuse existing client if present - it will look up leader dynamically.
            // No need to compare leaders since clients don't cache them anymore.
            let client = if let Some(c) = current.get(id).cloned() {
                c
            } else {
                Client::new(
                    self.channel_pool.clone(),
                    Arc::clone(&self.config),
                    self.token.clone(),
                    a.shard.into(),
                    Arc::clone(&self.leaders),
                )?
            };
            shards.insert(a.shard.into(), client);
        }

        let new_epoch = current.epoch().next();
        self.shards
            .store(Arc::new(searchable::Shards::new(shards, mapper, new_epoch)));

        Ok(())
    }

    async fn process_assignments_stream(&self, mut s: Streaming<oxia_proto::ShardAssignments>) {
        let namespace = self.config.namespace();
        while let Some(r) = s.next().await {
            debug!(?r, "assignment");
            match r {
                Ok(mut r) => {
                    if let Some(a) = r.namespaces.get_mut(namespace) {
                        // We're throwing this map away, so grab the value contents rather
                        // than clone it
                        let a = std::mem::take(a);
                        let pr = self.process_assignments(a);
                        let _ = self.changed.send(pr.map(|()| self.shards.load().epoch()));
                    }
                }
                Err(e) => {
                    let _ = self.changed.send(Err(e.into()));
                }
            }
        }
    }

    async fn run(&mut self, mut cluster: GrpcClient) {
        const MIN_REFRESH_INTERVAL: Duration = Duration::from_millis(50);

        #[derive(Debug)]
        enum ExitReason {
            Cancelled,
        }

        let mut interval = tokio::time::interval(Duration::from_millis(600));
        let reason = loop {
            tokio::select! {
            () = self.token.cancelled() => {
                    break ExitReason::Cancelled;
                }
                    () = self.refresh_signal.notified() => {
                    if self.last_refresh.elapsed() < MIN_REFRESH_INTERVAL {
                        continue; // Rate limit
                    }
                }
                _ = interval.tick() => {}
            }
            let req = oxia_proto::ShardAssignmentsRequest {
                namespace: self.config.namespace().into(),
            };
            match cluster.get_shard_assignments(req).await {
                Ok(rsp) => self.process_assignments_stream(rsp.into_inner()).await,
                Err(err) => {
                    warn!(?err, "get_shard_assignments");
                    let _ = self.changed.send(Err(err.into()));
                }
            }
            self.last_refresh = Instant::now();
            trace!(?self.shards);
        };
        info!(?reason, "ShardAssignmentTask exiting");
    }
}

#[derive(Debug)]
pub struct Manager {
    shards: Arc<ArcSwap<searchable::Shards>>,
    #[allow(dead_code)]
    leaders: LeaderDirectory,
    cancel_token: CancellationToken,
    task_handle: JoinHandle<()>,
    changed: watch::Receiver<ShardMapUpdateResult>,
    refresh_signal: Arc<Notify>,
}

impl Manager {
    /// Creates a new shard manager
    pub(crate) async fn new(config: &Arc<config::Config>) -> Result<Self> {
        let channel_pool = ChannelPool::new(config);
        let shards = Arc::new(ArcSwap::from_pointee(searchable::Shards::default()));
        let leaders: LeaderDirectory = Arc::new(ArcSwap::from_pointee(ShardIdMap::default()));
        let cancel_token = CancellationToken::new();
        let refresh_signal = Arc::new(Notify::new());
        let (task_handle, changed) = Self::start_shard_assignment_task(
            config,
            &channel_pool,
            &cancel_token,
            &shards,
            &leaders,
            &refresh_signal,
        )
        .await?;
        Ok(Self {
            shards,
            leaders,
            cancel_token,
            task_handle,
            changed,
            refresh_signal,
        })
    }

    /// Maps a key to a shard id.  This is a volatile mapping, callers must be prepared for the
    /// mapping to be invalid by the time it is used.
    pub(crate) fn get_shard_id(&self, key: &str) -> Option<ShardId> {
        self.shards.load().get_shard_id(key)
    }

    /// Gets a client by shard id
    pub(crate) fn get_client_by_shard_id(&self, id: ShardId) -> Result<Client> {
        self.shards.load().find_by_id(id)
    }

    /// Gets a client for the shard that handles the given key
    pub(crate) fn get_client(&self, key: &str) -> Result<Client> {
        self.shards.load().find_by_key(key)
    }

    /// Returns the current clients sorted by shard id.  The shard mapping is volatile, so callers
    /// must be prepared.
    pub(crate) fn get_shard_clients(&self) -> Vec<Client> {
        let shards = self.shards.load();
        let mut clients = Vec::with_capacity(shards.len());
        clients.extend(shards.iter().map(|(_, c)| c.clone()));
        clients
    }

    /// Returns the number of shards at the moment
    /// This is mostly usable as hint, as the number of shards may change over time
    #[allow(dead_code)]
    pub(crate) fn num_shards(&self) -> usize {
        self.shards.load().len()
    }

    #[allow(dead_code)]
    pub(crate) fn recv_changed(&self) -> watch::Receiver<ShardMapUpdateResult> {
        self.changed.clone()
    }

    /// Returns the current shard map epoch. Epoch increments with each shard assignment update.
    pub fn epoch(&self) -> ShardMapEpoch {
        self.shards.load().epoch()
    }

    /// Signals the background task to refresh shard assignments.
    /// Rate-limited to prevent excessive refreshes.
    pub fn trigger_refresh(&self) {
        self.refresh_signal.notify_one();
    }

    /// Waits for the shard map to be updated to an epoch greater than `after_epoch`.
    ///
    /// Returns immediately if the current epoch is already greater than `after_epoch`.
    /// On success, returns the new epoch.
    /// Logs errors from failed shard assignment fetches but continues waiting.
    pub async fn wait_for_update(
        &self,
        after_epoch: ShardMapEpoch,
        timeout: Duration,
    ) -> Result<ShardMapEpoch> {
        // Fast path: already past the requested epoch
        if self.epoch() > after_epoch {
            return Ok(self.epoch());
        }

        let mut rx = self.changed.clone();
        tokio::time::timeout(timeout, async {
            loop {
                rx.changed().await.map_err(|_| Error::Cancelled)?;

                match rx.borrow_and_update().as_ref() {
                    Err(e) => {
                        warn!(?e, "shard assignment update failed");
                    }
                    Ok(&epoch) => {
                        if epoch > after_epoch {
                            return Ok(epoch);
                        }
                    }
                }
            }
        })
        .await
        .map_err(|_| Error::RequestTimeout)?
    }

    async fn start_shard_assignment_task(
        config: &Arc<config::Config>,
        channel_pool: &ChannelPool,
        token: &CancellationToken,
        shards: &Arc<ArcSwap<searchable::Shards>>,
        leaders: &LeaderDirectory,
        refresh_signal: &Arc<Notify>,
    ) -> Result<(JoinHandle<()>, watch::Receiver<ShardMapUpdateResult>)> {
        let (tx, mut rx) = watch::channel::<ShardMapUpdateResult>(Ok(ShardMapEpoch::new()));
        // Consume the initial value
        rx.borrow_and_update();

        let cluster = create_grpc_client(config.service_addr(), channel_pool).await?;
        let mut task = ShardAssignmentTask::new(
            Arc::clone(config),
            channel_pool.clone(),
            Arc::clone(shards),
            Arc::clone(leaders),
            token.clone(),
            tx,
            Arc::clone(refresh_signal),
        );
        let handle = tokio::spawn(async move {
            task.run(cluster).await;
        });

        // Wait for the notification that the task has started up
        rx.changed().await.map_err(Error::other)?;
        if let Err(e) = rx.borrow_and_update().as_ref() {
            return Err(e.to_owned());
        }
        Ok((handle, rx))
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.task_handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_shard_id(x: u64) -> ShardId {
        #[allow(clippy::cast_possible_wrap)]
        (x as i64).into()
    }

    #[test]
    fn test_shard_id_map() {
        use crate::ShardId;
        let mut offsets = ShardIdMap::<i32>::default();

        let id_negative: ShardId = (-3_i64).into();
        assert!(offsets.get(id_negative).is_none());
        assert!(offsets.insert(id_negative, 42).is_none());
        assert!(offsets.remove(id_negative).is_none());

        let id_zero: ShardId = 0.into();
        assert!(offsets.get(id_zero).is_none());
        offsets.insert(id_zero, 42);
        assert_eq!(*offsets.get(id_zero).unwrap(), 42);
        offsets.remove(id_zero);
        assert!(offsets.get(id_zero).is_none());

        let id_direct = make_shard_id(DIRECT_ID_SLOTS - 2);
        assert!(offsets.get(id_direct).is_none());
        offsets.insert(id_direct, 42);
        assert_eq!(*offsets.get(id_direct).unwrap(), 42);
        offsets.remove(id_direct);
        assert!(offsets.get(id_direct).is_none());

        let id_spilled = make_shard_id(DIRECT_ID_SLOTS + 13);
        assert!(offsets.get(id_spilled).is_none());
        offsets.insert(id_spilled, 42);
        assert_eq!(*offsets.get(id_spilled).unwrap(), 42);
        offsets.remove(id_spilled);
        assert!(offsets.get(id_spilled).is_none());

        let id_very_large: ShardId = i64::MAX.into();
        assert!(offsets.get(id_very_large).is_none());
        offsets.insert(id_very_large, 42);
        assert_eq!(*offsets.get(id_very_large).unwrap(), 42);
        offsets.remove(id_very_large);
        assert!(offsets.get(id_very_large).is_none());
    }

    #[test]
    fn test_shard_id_map_order() {
        let mut items = ShardIdMap::<i32>::default();
        items.insert((-1000).into(), -1000);
        items.insert((-100).into(), -100);
        items.insert((-10).into(), -10);
        items.insert((-1).into(), -1);
        items.insert(0.into(), 0);
        items.insert(1.into(), 1);
        items.insert(10.into(), 10);
        items.insert(100.into(), 100);
        items.insert(1000.into(), 1000);
        assert!(items.iter().is_sorted());
    }

    mod client_tests {
        use super::*;
        use crate::{config::Config, pool::ChannelPool};

        fn make_test_config() -> Arc<config::Config> {
            Config::builder().service_addr("localhost:1234").build()
        }

        fn make_test_leaders(shard_id: ShardId, leader: &str) -> LeaderDirectory {
            let mut leaders = ShardIdMap::default();
            leaders.insert(shard_id, Arc::from(format!("http://{leader}")));
            Arc::new(ArcSwap::from_pointee(leaders))
        }

        fn make_test_client(config: &Arc<config::Config>) -> Client {
            let pool = ChannelPool::new(config);
            let shard_id = ShardId::new(1);
            let leaders = make_test_leaders(shard_id, "localhost:1234");
            Client::new(
                pool,
                Arc::clone(config),
                CancellationToken::new(),
                shard_id,
                leaders,
            )
            .unwrap()
        }

        #[test_log::test(tokio::test)]
        async fn test_client_get_leader() {
            let config = make_test_config();
            let client = make_test_client(&config);

            // Client should be able to look up its leader
            assert_eq!(
                client.get_leader().as_deref(),
                Some("http://localhost:1234")
            );
        }

        #[cfg(not(miri))] // Miri doesn't support network operations (getaddrinfo)
        #[test_log::test(tokio::test)]
        async fn test_client_get_grpc_client_fails_on_unreachable() {
            let config = make_test_config();
            let client = make_test_client(&config);

            // get_grpc_client should fail when server is unreachable
            let result = client.get_grpc_client().await;
            assert!(result.is_err());
        }

        #[test_log::test(tokio::test)]
        async fn test_client_invalidate_on_empty_pool() {
            let config = make_test_config();
            let client = make_test_client(&config);

            // invalidate_channel should not panic even when pool is empty
            client.invalidate_channel().await;
        }

        #[cfg(not(miri))] // Miri doesn't support network operations (getaddrinfo)
        #[test_log::test(tokio::test)]
        async fn test_rpc_handles_connection_error() {
            let config = make_test_config();
            let client = make_test_client(&config);

            // rpc() should fail when server is unreachable
            let result: crate::Result<()> = client
                .rpc(|mut grpc| async move {
                    grpc.keep_alive(oxia_proto::SessionHeartbeat {
                        session_id: 0,
                        shard: 0,
                    })
                    .await
                    .map(|_| ())
                })
                .await;
            assert!(result.is_err());
        }

        #[test]
        fn test_is_connection_error_classification() {
            use std::io;

            // Test various IO errors that should trigger invalidation
            let io_conn_errors = [
                io::ErrorKind::ConnectionReset,
                io::ErrorKind::BrokenPipe,
                io::ErrorKind::ConnectionAborted,
                io::ErrorKind::NotConnected,
            ];

            for kind in io_conn_errors {
                let err = crate::Error::from(io::Error::from(kind));
                assert!(
                    err.is_connection_error(),
                    "{kind:?} should be a connection error"
                );
            }

            // Test IO errors that should NOT trigger invalidation
            let io_non_conn_errors = [
                io::ErrorKind::NotFound,
                io::ErrorKind::PermissionDenied,
                io::ErrorKind::WouldBlock,
            ];

            for kind in io_non_conn_errors {
                let err = crate::Error::from(io::Error::from(kind));
                assert!(
                    !err.is_connection_error(),
                    "{kind:?} should NOT be a connection error"
                );
            }
        }
    }
}
