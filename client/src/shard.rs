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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

use arc_swap::ArcSwap;
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use futures::stream::Stream;
use futures::stream::StreamExt;
use rand::SeedableRng;
use rand::distr::Distribution;
use rand::distr::Uniform;
use rand::rngs::StdRng;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
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
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ShardId(i64);

impl ShardId {
    pub const fn new(val: i64) -> Self {
        ShardId(val)
    }

    // A canonical invalid value
    pub const INVALID: ShardId = ShardId(-1);

    pub fn is_invalid(self) -> bool {
        self.0 < 0
    }
}

impl From<i64> for ShardId {
    #[inline]
    fn from(val: i64) -> Self {
        ShardId(val)
    }
}

impl From<ShardId> for i64 {
    #[inline]
    fn from(my: ShardId) -> Self {
        my.0
    }
}

impl From<ShardId> for usize {
    #[inline]
    fn from(my: ShardId) -> Self {
        my.0 as usize
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

const DIRECT_ID_MAX: usize = 20;

// ShardIdMap<T> we map shard ids to things in seveal "hot" places.  This map should be suitable
// for all, or most, of those uses.
//
// Shard ids should be small integers, nearly contiguous.  Or so we think:  the ids are generated
// by the Oxia server, so we need to be careful to avoid being poisoned with data that causes
// memory consumption to go crazy or leading to bogus indexing.  The approach here is to hope we
// have a small set of contigous ids and they are directly mapped to values.
pub struct ShardIdMap<T> {
    // Array for O(1) direct lookup (Small, dense IDs)
    direct: [Option<T>; DIRECT_ID_MAX],
    // BTreeMap for O(log N) lookup (Larger, sparse IDs)
    spilled: BTreeMap<ShardId, T>,
    #[cfg(accept_invalid_shard_ids)]
    // BTreeMap for invalid IDs
    invalid: BTreeMap<ShardId, T>,
}

impl<T: fmt::Debug> fmt::Debug for ShardIdMap<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(not(accept_invalid_shard_ids))]
        return f
            .debug_struct("ShardIdMap")
            .field("direct", &self.direct)
            .field("spilled", &self.spilled)
            .finish();

        #[cfg(accept_invalid_shard_ids)]
        return f
            .debug_struct("ShardIdMap")
            .field("direct", &self.direct)
            .field("spilled", &self.spilled)
            .field("invalid", &self.invalid)
            .finish();
    }
}

impl<T> Default for ShardIdMap<T> {
    fn default() -> Self {
        Self {
            direct: std::array::from_fn(|_| None),
            spilled: BTreeMap::new(),
            #[cfg(accept_invalid_shard_ids)]
            invalid: BTreeMap::new(),
        }
    }
}

impl<T> ShardIdMap<T> {
    fn len(&self) -> usize {
        let result = self.direct.iter().filter(|x| x.is_some()).count() + self.spilled.len();
        #[cfg(accept_invalid_shard_ids)]
        return result + self.invalid.len();
        #[cfg(not(accept_invalid_shard_ids))]
        return result;
    }

    // Check if ID is small enough for the direct array
    #[inline]
    fn direct_index(id: ShardId) -> Option<usize> {
        let idx: usize = id.into();
        (idx < DIRECT_ID_MAX).then_some(idx)
    }

    pub fn insert(&mut self, id: ShardId, value: T) -> bool {
        if id.is_invalid() {
            #[cfg(accept_invalid_shard_ids)]
            {
                self.invalid.insert(id, value);
                return true;
            }
            #[cfg(not(accept_invalid_shard_ids))]
            return false;
        }
        if let Some(i) = Self::direct_index(id) {
            self.direct[i] = Some(value);
        } else {
            self.spilled.insert(id, value);
        }
        true
    }

    pub fn get(&self, id: ShardId) -> Option<&T> {
        if let Some(i) = Self::direct_index(id) {
            self.direct[i].as_ref()
        } else {
            #[cfg(accept_invalid_shard_ids)]
            if id.is_invalid() {
                let v = self.invalid.get(&id);
                if v.is_some() {
                    return v;
                }
            }
            self.spilled.get(&id)
        }
    }

    pub fn remove(&mut self, id: ShardId) {
        if let Some(i) = Self::direct_index(id) {
            self.direct[i] = None;
        } else {
            #[cfg(accept_invalid_shard_ids)]
            if id.is_invalid() {
                self.invalid.remove(&id);
                return;
            }
            self.spilled.remove(&id);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (ShardId, &T)> + '_ {
        let direct_iter = self
            .direct
            .iter()
            .enumerate()
            .filter_map(|(idx, opt_val)| opt_val.as_ref().map(|val| (ShardId(idx as i64), val)))
            .fuse(); // Keep .fuse() for correctness

        // The order of chaining these is important to ensure the order is preserved among the
        // buckets.  `invalid` comes first, if it exists, as those are the negative ids.  Next is
        // are the directly mapped ids, and finally the spilled arena.

        let spilled_iter = self.spilled.iter().map(|(&id, val)| (id, val));
        let chained_iter = direct_iter.chain(spilled_iter);

        #[cfg(accept_invalid_shard_ids)]
        let chained_iter = {
            let invalid_iter = self.invalid.iter().map(|(&id, val)| (id, val));
            invalid_iter.chain(chained_iter)
        };

        chained_iter
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

#[derive(Debug)]
struct ClientData {
    config: Arc<config::Config>,
    channel_pool: ChannelPool,
    shard_id: ShardId,
    leader: String,
    meta: tonic::metadata::MetadataMap,
    /// Cached gRPC client. Set to None when invalidated, lazily populated on first use.
    /// Using ArcSwapOption for lock-free reads on the hot path.
    grpc: ArcSwapOption<GrpcClient>,
    /// Mutex to serialize initialization attempts (prevents thundering herd on cache miss).
    grpc_init: Mutex<()>,
}

impl ClientData {
    fn new(
        config: Arc<config::Config>,
        channel_pool: ChannelPool,
        shard_id: ShardId,
        leader: impl Into<String>,
        meta: tonic::metadata::MetadataMap,
    ) -> Self {
        Self {
            config,
            channel_pool,
            shard_id,
            leader: leader.into(),
            meta,
            grpc: ArcSwapOption::empty(),
            grpc_init: Mutex::new(()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    data: Arc<ClientData>,
    session: Arc<OnceCell<Option<Session>>>,
}

impl Client {
    /// Get the cached gRPC client, or create one from the pool if not cached.
    async fn get_grpc_client(&self) -> crate::Result<GrpcClient> {
        // Fast path: lock-free check if we have a cached client
        if let Some(client) = self.data.grpc.load_full() {
            return Ok((*client).clone());
        }

        // Slow path: acquire init lock to prevent thundering herd
        let _guard = self.data.grpc_init.lock().await;

        // Double-check after acquiring lock (another task may have initialized)
        if let Some(client) = self.data.grpc.load_full() {
            return Ok((*client).clone());
        }

        // Create new client from pool
        let url = format!("http://{}", self.data.leader);
        let channel = self.data.channel_pool.get(&url).await?;
        let client = GrpcClient::new(channel);
        self.data.grpc.store(Some(Arc::new(client.clone())));
        Ok(client)
    }

    /// Invalidate the cached gRPC client for this shard's leader.
    async fn invalidate_channel(&self) {
        self.data.grpc.store(None);
        let url = format!("http://{}", self.data.leader);
        self.data.channel_pool.remove(&url).await;
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
            debug!(leader = %self.data.leader, "connection error, invalidating channel");
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
fn no_response_error(server: &str, shard_id: ShardId) -> Error {
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
            }
        })
    }

    /// Creates a new Client instance for a specific shard
    pub(crate) fn new(
        channel_pool: ChannelPool,
        config: Arc<config::Config>,
        shard_id: ShardId,
        dest: impl Into<String>,
    ) -> Result<Self> {
        // Create metadata with shard ID and namespace
        let mut meta = tonic::metadata::MetadataMap::new();
        meta.insert("shard-id", MetadataValue::from(shard_id.0));
        meta.insert(
            "namespace",
            MetadataValue::from_str(config.namespace())
                .map_err(|e| Error::Custom(format!("Invalid metadata value: {e}")))?,
        );

        Ok(Client {
            data: Arc::new(ClientData::new(config, channel_pool, shard_id, dest, meta)),
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
                return Err(no_response_error(&self.data.leader, self.data.shard_id));
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
                    err: no_response_error(&context.leader, context.shard_id),
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
                            let err = no_response_error(&context.leader, context.shard_id);
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
        let def_opts = GetOptions::default();

        let keys: Vec<Arc<str>> = batch_req
            .items
            .iter()
            .map(|item| item.key.clone())
            .collect();

        let gets: Vec<oxia_proto::GetRequest> = batch_req
            .items
            .into_iter()
            .map(|item| {
                let opts = item
                    .opts
                    .as_ref()
                    .unwrap_or_else(|| batch_req.opts.as_ref().unwrap_or(&def_opts));
                Self::make_proto_get_req(opts, item.key.to_string())
            })
            .collect();

        let read_req = oxia_proto::ReadRequest {
            shard: Some(self.data.shard_id.into()),
            gets,
        };

        let read_result = self
            .rpc(|mut grpc| async move { grpc.read(read_req).await })
            .await;

        Self::demux_stream(self.data.clone(), keys, read_result).await
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
    pub(crate) struct Mapper {
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
    pub(crate) struct Builder {
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
    use super::{Client, Error, Result, ShardId, ShardIdMap, ShardMapper, int32_hash_range};

    #[derive(Debug, Default)]
    pub(super) struct Shards {
        shards: ShardIdMap<Client>,
        mapper: int32_hash_range::Mapper,
    }

    impl Shards {
        pub(super) fn new(shards: ShardIdMap<Client>, mapper: int32_hash_range::Mapper) -> Self {
            Self { shards, mapper }
        }

        pub(super) fn find_by_id(&self, id: ShardId) -> Result<Client> {
            self.shards
                .get(id)
                .cloned()
                .ok_or_else(|| Error::NoShardMapping(id))
        }

        pub(super) fn get_shard_id(&self, key: &str) -> Option<ShardId> {
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

        pub(super) fn len(&self) -> usize {
            self.shards.len()
        }
    }
}

#[derive(Debug)]
pub(crate) struct NotificationsStream {
    inner: tonic::Streaming<oxia_proto::NotificationBatch>,
}

impl NotificationsStream {
    fn from_proto(inner: tonic::Streaming<oxia_proto::NotificationBatch>) -> Self {
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
    changed: watch::Sender<std::result::Result<(), Error>>,
}

impl ShardAssignmentTask {
    fn new(
        config: Arc<config::Config>,
        channel_pool: ChannelPool,
        shards: Arc<ArcSwap<searchable::Shards>>,
        changed: watch::Sender<std::result::Result<(), Error>>,
    ) -> Self {
        Self {
            config,
            channel_pool,
            shards,
            changed,
        }
    }

    fn make_mapper(ns: &oxia_proto::NamespaceShardsAssignment) -> Result<int32_hash_range::Mapper> {
        let mut mb = int32_hash_range::Mapper::new_builder(ns)?;
        for a in &ns.assignments {
            mb.accept(a)?;
        }
        mb.build()
    }

    async fn process_assignments(&self, ns: oxia_proto::NamespaceShardsAssignment) -> Result<()> {
        // Create shard mappings and bail out if we find anything silly.
        let mapper = Self::make_mapper(&ns)?;

        // Now build the shard clients. Each Client holds a reference to the channel pool
        // and will fetch channels on demand, allowing for reconnection on errors.
        let mut shards = ShardIdMap::<Client>::default();
        for a in &ns.assignments {
            info!(?a, "processing assignments");
            let client = Client::new(
                self.channel_pool.clone(),
                self.config.clone(),
                a.shard.into(),
                &a.leader,
            )?;
            shards.insert(a.shard.into(), client);
        }

        self.shards
            .store(Arc::new(searchable::Shards::new(shards, mapper)));

        Ok(())
    }

    async fn process_assignments_stream(&mut self, mut s: Streaming<oxia_proto::ShardAssignments>) {
        let namespace = self.config.namespace();
        while let Some(r) = s.next().await {
            debug!(?r, "assignment");
            match r {
                Ok(mut r) => {
                    if let Some(a) = r.namespaces.get_mut(namespace) {
                        // We're throwing this map away, so grab the value contents rather
                        // than clone it
                        let a = std::mem::take(a);
                        let pr = self.process_assignments(a).await;
                        let _ = self.changed.send(pr);
                    }
                }
                Err(e) => {
                    let _ = self.changed.send(Err(e.into()));
                }
            }
        }
    }

    async fn run(&mut self, mut cluster: GrpcClient, shutdown_requested: Arc<AtomicBool>) {
        let sleep_time = Duration::from_millis(600);
        loop {
            if shutdown_requested.load(Ordering::Relaxed) {
                info!("ShardAssignmentTask exiting");
                return;
            }

            let req = oxia_proto::ShardAssignmentsRequest {
                namespace: self.config.namespace().into(),
            };
            match cluster.get_shard_assignments(req).await {
                Ok(rsp) => self.process_assignments_stream(rsp.into_inner()).await,
                Err(err) => {
                    warn!(?err, "get_shard_assignments");
                }
            }
            debug!(?self.shards);
            trace!(?sleep_time, "sleeping in retrieve_shards");
            tokio::time::sleep(sleep_time).await;
        }
    }
}

#[derive(Debug)]
pub(crate) struct Manager {
    shards: Arc<ArcSwap<searchable::Shards>>,
    shutdown_requested: Arc<AtomicBool>,
    task_handle: JoinHandle<()>,
    changed: watch::Receiver<std::result::Result<(), Error>>,
}

impl Manager {
    /// Creates a new shard manager
    pub(crate) async fn new(config: &Arc<config::Config>) -> Result<Self> {
        let channel_pool = ChannelPool::new(config);
        let shards = Arc::new(ArcSwap::from_pointee(searchable::Shards::default()));
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let (task_handle, changed) =
            Self::start_shard_assignment_task(config, &channel_pool, &shards, &shutdown_requested)
                .await?;
        Ok(Manager {
            shards,
            shutdown_requested,
            task_handle,
            changed,
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
    pub(crate) fn recv_changed(&self) -> watch::Receiver<std::result::Result<(), Error>> {
        self.changed.clone()
    }

    async fn start_shard_assignment_task(
        config: &Arc<config::Config>,
        channel_pool: &ChannelPool,
        shards: &Arc<ArcSwap<searchable::Shards>>,
        shutdown_requested: &Arc<AtomicBool>,
    ) -> Result<(
        JoinHandle<()>,
        watch::Receiver<std::result::Result<(), Error>>,
    )> {
        let (tx, mut rx) = watch::channel(Ok(()));
        // Consume the initial value
        rx.borrow_and_update();

        let cluster = create_grpc_client(config.service_addr(), channel_pool).await?;
        let mut task =
            ShardAssignmentTask::new(config.clone(), channel_pool.clone(), shards.clone(), tx);
        let shutdown_requested = shutdown_requested.clone();
        let handle = tokio::spawn(async move {
            task.run(cluster, shutdown_requested).await;
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
        self.shutdown_requested.store(true, Ordering::Relaxed);
        self.task_handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_id_map() {
        use crate::ShardId;
        let mut offsets = ShardIdMap::<i32>::default();

        let id_negative: ShardId = (-3_i64).into();
        assert!(offsets.get(id_negative).is_none());
        let r = offsets.insert(id_negative, 42);
        #[cfg(accept_invalid_shard_ids)]
        {
            assert!(r);
            assert_eq!(*offsets.get(id_negative).unwrap(), 42);
        }
        #[cfg(not(accept_invalid_shard_ids))]
        assert!(!r);

        offsets.remove(id_negative);
        assert!(offsets.get(id_negative).is_none());

        let id_zero: ShardId = 0.into();
        assert!(offsets.get(id_zero).is_none());
        offsets.insert(id_zero, 42);
        assert_eq!(*offsets.get(id_zero).unwrap(), 42);
        offsets.remove(id_zero);
        assert!(offsets.get(id_zero).is_none());

        let id_direct: ShardId = (DIRECT_ID_MAX as i64 - 2).into();
        assert!(offsets.get(id_direct).is_none());
        offsets.insert(id_direct, 42);
        assert_eq!(*offsets.get(id_direct).unwrap(), 42);
        offsets.remove(id_direct);
        assert!(offsets.get(id_direct).is_none());

        let id_spilled: ShardId = (DIRECT_ID_MAX as i64 + 13).into();
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
        use crate::pool::ChannelPool;

        fn make_test_config() -> Arc<config::Config> {
            config::Builder::new()
                .service_addr("localhost:1234")
                .build()
                .unwrap()
        }

        fn make_test_client(config: &Arc<config::Config>) -> Client {
            let pool = ChannelPool::new(config);
            Client::new(pool, config.clone(), ShardId::new(1), "localhost:1234").unwrap()
        }

        #[test_log::test(tokio::test)]
        async fn test_client_initial_state() {
            let config = make_test_config();
            let client = make_test_client(&config);

            // Initially, no gRPC client should be cached
            assert!(client.data.grpc.load().is_none());
        }

        #[cfg(not(miri))] // Miri doesn't support network operations (getaddrinfo)
        #[test_log::test(tokio::test)]
        async fn test_client_get_grpc_client_fails_on_unreachable() {
            let config = make_test_config();
            let client = make_test_client(&config);

            // get_grpc_client should fail when server is unreachable
            let result = client.get_grpc_client().await;
            assert!(result.is_err());

            // Cache should still be empty since connection failed
            assert!(client.data.grpc.load().is_none());
        }

        #[test_log::test(tokio::test)]
        async fn test_client_invalidate_on_empty_cache() {
            let config = make_test_config();
            let client = make_test_client(&config);

            // invalidate_channel on empty cache should not panic
            client.invalidate_channel().await;

            // Cache should still be None
            assert!(client.data.grpc.load().is_none());
        }

        #[cfg(not(miri))] // Miri doesn't support network operations (getaddrinfo)
        #[test_log::test(tokio::test)]
        async fn test_rpc_invalidates_on_connection_error() {
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
