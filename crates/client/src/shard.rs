use async_once_cell::OnceCell;
use bytes::Bytes;
// use futures::{StreamExt, stream};
use mauricebarnum_oxia_common::proto as oxia_proto;
use rand::SeedableRng;
use rand::distr::Distribution;
use rand::distr::Uniform;
use rand::rngs::StdRng;
use std::time::Duration;
use std::{str::FromStr, sync::Arc};
use tokio::sync::{Mutex, oneshot};
use tokio::{task::JoinHandle, time::sleep};
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;
use tonic::Streaming;
use tonic::metadata::MetadataValue;
// use mauricebarnum_oxia_common::proto::ShardAssignments;

use crate::{
    DeleteOptions, Error, GetOptions, GetResponse, GrpcClient, GrpcClientCache, ListOptions,
    ListResponse, OxiaError, PutOptions, PutResponse, RangeScanOptions, RangeScanResponse, Result,
    config,
};

/// Constants for Oxia protocol
const STATUS_OK: i32 = 0;

// ============= Concrete Implementations =============

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
    grpc: GrpcClient,
    shard_id: i64,
    leader: String,
    meta: tonic::metadata::MetadataMap,
}

impl ClientData {
    pub fn new(
        config: Arc<config::Config>,
        grpc: GrpcClient,
        shard_id: i64,
        leader: impl Into<String>,
        meta: tonic::metadata::MetadataMap,
    ) -> Self {
        Self {
            config,
            grpc,
            shard_id,
            leader: leader.into(),
            meta,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    data: Arc<ClientData>,
    session: Arc<OnceCell<Option<Session>>>,
}

/// Enum to indicate the expected type of write response
enum ExpectedWriteResponse {
    Put,
    Delete,
    // DeleteRange,
}

/// Checks if a write response matches expectations
fn check_write_response(r: &oxia_proto::WriteResponse, x: ExpectedWriteResponse) -> Option<Error> {
    let (nputs, ndeletes, nranges): (usize, usize, usize) = match x {
        ExpectedWriteResponse::Put => (1, 0, 0),
        ExpectedWriteResponse::Delete => (0, 1, 0),
        // ExpectedWriteResponse::DeleteRange => (0, 0, 1),
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
    check_write_response(r, ExpectedWriteResponse::Put)
}

/// Validates a delete response - ensures it has the expected structure and status
fn check_delete_response(r: &oxia_proto::WriteResponse) -> Option<Error> {
    if let Some(e) = check_write_response(r, ExpectedWriteResponse::Delete) {
        return Some(e);
    }

    if let Some(d) = r.deletes.first() {
        if d.status != STATUS_OK {
            return Some(OxiaError::from(d.status).into());
        }
    }
    None
}

const INTERNAL_KEY_PREFIX: &str = "__oxia/";

/// Helper method to create a fully-formed tonic request with metadata
impl Client {
    fn start_heartbeat(&self, session_id: i64) -> JoinHandle<()> {
        let session_timeout_ms = self.data.config.session_timeout.as_millis();
        if session_timeout_ms > u32::MAX as u128 {
            panic!(
                "bug: time out out of range {} max {}",
                session_timeout_ms,
                u32::MAX
            );
        }

        let meta = self.data.meta.clone();
        let shard = self.data.shard_id;
        let mut grpc = self.data.grpc.clone();

        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).unwrap();
        let mut rng = StdRng::from_seed(seed);

        tokio::spawn(async move {
            // We'll seen heartbeats a random time +/- X% of one quarter of session timeout.
            // This will allow us miss a few heartbeats without losing the session
            let distr = {
                const JITTER: f64 = 0.03;
                let target_ms = session_timeout_ms as f64 / 4.0;
                let min_ms = (target_ms * (1.0 - JITTER)).round() as u32;
                let max_ms = (target_ms * (1.0 + JITTER)).round() as u32;
                Uniform::new_inclusive(min_ms, max_ms).unwrap()
            };
            loop {
                // FIXME: create_request() isn't usable here because we can't capture `self`. consider getting rid of it.
                let req = tonic::Request::from_parts(
                    meta.clone(),
                    tonic::Extensions::default(),
                    oxia_proto::SessionHeartbeat { session_id, shard },
                );
                let rsp = grpc.keep_alive(req).await;
                dbg!(&rsp);

                let sleep_ms = distr.sample(&mut rng);
                let timeout = Duration::from_millis(sleep_ms.into());
                println!("sleeping for {} ms", sleep_ms);
                sleep(timeout).await;
            }
        })
    }

    /// Creates a new Client instance for a specific shard
    pub(super) fn new(
        grpc: GrpcClient,
        config: Arc<config::Config>,
        shard_id: i64,
        dest: &str,
    ) -> Result<Self> {
        // Create metadata with shard ID and namespace
        let mut meta = tonic::metadata::MetadataMap::new();
        meta.insert("shard-id", MetadataValue::from(shard_id));
        meta.insert(
            "namespace",
            MetadataValue::from_str(&config.namespace)
                .map_err(|e| Error::Custom(format!("Invalid metadata value: {}", e)))?,
        );

        Ok(Client {
            data: Arc::new(ClientData::new(
                config,
                grpc,
                shard_id,
                dest.to_string(),
                meta,
            )),
            session: Arc::new(OnceCell::new()),
        })
    }

    async fn create_session(&self) -> Result<Session> {
        let data = &self.data;
        let config = &data.config;
        let mut grpc = data.grpc.clone();
        // Call create_session
        let rsp = grpc
            .create_session(self.create_request(oxia_proto::CreateSessionRequest {
                session_timeout_ms: config.session_timeout.as_millis() as u32,
                client_identity: config.identity.clone(),
                shard: data.shard_id,
            }))
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
            .get_or_try_init(async {
                let rsp = self.create_session().await;
                dbg!(&rsp);
                rsp.map(Some)
            })
            .await?;
        Ok(s.as_ref().map(|session| session.id))
    }

    /// Creates a GetRequest with the appropriate options
    fn make_get_req(&self, opts: &GetOptions, k: impl Into<String>) -> oxia_proto::ReadRequest {
        let data = &self.data;
        let ct = opts.comparison_type as i32;
        oxia_proto::ReadRequest {
            shard: Some(data.shard_id),
            gets: vec![oxia_proto::GetRequest {
                key: k.into(),
                include_value: opts.include_value,
                comparison_type: ct,
                secondary_index_name: None,
            }],
        }
    }

    /// Creates a PutRequest with the appropriate options
    fn make_put_req(
        &self,
        session_id: Option<i64>,
        opts: PutOptions,
        k: impl Into<String>,
        v: Bytes,
    ) -> oxia_proto::WriteRequest {
        let data = &self.data;
        oxia_proto::WriteRequest {
            shard: Some(data.shard_id),
            puts: vec![oxia_proto::PutRequest {
                key: k.into(),
                value: v,
                expected_version_id: opts.expected_version_id,
                session_id,
                client_identity: Some(data.config.identity.clone()),
                partition_key: opts.partition_key,
                sequence_key_delta: opts.sequence_key_deltas,
                secondary_indexes: opts.secondary_indexes,
            }],
            deletes: vec![],
            delete_ranges: vec![],
        }
    }

    /// Creates a DeleteRequest with the appropriate options
    fn make_delete_req(
        &self,
        opts: &DeleteOptions,
        k: impl Into<String>,
    ) -> oxia_proto::WriteRequest {
        let data = &self.data;
        oxia_proto::WriteRequest {
            shard: Some(data.shard_id),
            puts: vec![],
            deletes: vec![oxia_proto::DeleteRequest {
                key: k.into(),
                expected_version_id: opts.expected_version_id,
            }],
            delete_ranges: vec![],
        }
    }

    /// Creates a ListRequest with the appropriate options
    fn make_list_req(
        &self,
        opts: ListOptions,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
    ) -> oxia_proto::ListRequest {
        let data = &self.data;
        oxia_proto::ListRequest {
            shard: Some(data.shard_id),
            start_inclusive: start_inclusive.into(),
            end_exclusive: end_exclusive.into(),
            secondary_index_name: opts.secondary_index_name,
        }
    }

    /// Creates a RangeScanRequest with the appropriate options
    fn make_range_scan_req(
        &self,
        opts: RangeScanOptions,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
    ) -> oxia_proto::RangeScanRequest {
        let data = &self.data;

        // If end key is empty, use the internal key prefix to scan all keys
        let effective_end = {
            let s = end_exclusive.into();
            if s.is_empty() {
                INTERNAL_KEY_PREFIX.to_string()
            } else {
                s
            }
        };

        oxia_proto::RangeScanRequest {
            shard: Some(data.shard_id),
            start_inclusive: start_inclusive.into(),
            end_exclusive: effective_end,
            secondary_index_name: opts.secondary_index_name,
        }
    }

    fn create_request<T>(&self, payload: T) -> tonic::Request<T> {
        tonic::Request::from_parts(
            self.data.meta.clone(),
            tonic::Extensions::default(),
            payload,
        )
    }

    /// Processes a write request through the gRPC stream
    async fn process_write(
        &self,
        write_req: oxia_proto::WriteRequest,
    ) -> Result<oxia_proto::WriteResponse> {
        let data = &self.data;

        let write_req = oxia_proto::WriteRequest {
            shard: Some(data.shard_id),
            ..write_req
        };

        // Create a channel for the write stream with a buffer of 1 (only sending one request)
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        // Send the request
        tx.send(write_req)
            .await
            .map_err(|e| Error::Custom(format!("Failed to send write request: {}", e)))?;

        // Create stream from the receiver
        let write_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        // Create request with proper metadata
        let req = self.create_request(write_stream);

        // Get response stream with better error handling
        let mut resp_stream = data.grpc.clone().write_stream(req).await?.into_inner();

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
                "Expected exactly one response, but got additional response: {:?}",
                response
            )));
        }

        Ok(write_response)
    }

    /// Retrieves a key with the given options
    pub(super) async fn get(
        &self,
        k: impl Into<String>,
        opts: GetOptions,
    ) -> Result<Option<GetResponse>> {
        let req = self.create_request(self.make_get_req(&opts, k));
        let mut rsp_stream = self.data.grpc.clone().read(req).await?.into_inner();

        let rsp = match rsp_stream.next().await {
            Some(Ok(response)) => response,
            Some(Err(err)) => return Err(err.into()),
            None => {
                return Err(Error::Custom(
                    "Server returned no response for get".to_string(),
                ));
            }
        };

        // Verify we received only one response
        if let Some(response) = rsp_stream.next().await {
            return Err(Error::Custom(format!(
                "Expected only one ReadResponse, but got additional response: {:?}",
                response
            )));
        }

        // Check that we got exactly one GetResponse
        if rsp.gets.len() != 1 {
            return Err(Error::Custom(format!(
                "Expected one GetResponse, got {}",
                rsp.gets.len()
            )));
        }

        let r = rsp.gets.into_iter().next().unwrap();
        use oxia_proto::Status;
        match Status::try_from(r.status) {
            Ok(Status::Ok) => Ok(Some(r.into())),
            Ok(Status::KeyNotFound) => Ok(None),
            _ => Err(OxiaError::from(r.status).into()),
        }
    }

    // /// Gets multiple keys in a single batch request
    // pub(super) async fn batch_get(
    //     &self,
    //     keys: Vec<String>,
    //     opts: GetOptions,
    // ) -> Result<Vec<Result<GetResponse>>> {
    //     // Create get requests for each key
    //     let mut gets = Vec::with_capacity(keys.len());
    //     for key in keys {
    //         gets.push(oxia_proto::GetRequest {
    //             key,
    //             include_value: opts.include_value,
    //             comparison_type: opts.comparison_type as i32,
    //             secondary_index_name: None,
    //         });
    //     }
    //
    //     let data = &self.data;
    //
    //     // Create the read request with all gets
    //     let read_req = oxia_proto::ReadRequest {
    //         shard: Some(data.shard_id),
    //         gets,
    //     };
    //
    //     // Send the request
    //     let req = self.create_request(read_req);
    //     let mut results = Vec::new();
    //     let mut rsp_stream = data.grpc.clone().read(req).await?.into_inner();
    //
    //     // Process all responses
    //     while let Some(rsp) = rsp_stream.next().await {
    //         match rsp {
    //             Ok(rsp) => {
    //                 for get in rsp.gets {
    //                     if get.status != STATUS_OK {
    //                         results.push(Err(OxiaError::from(get.status).into()));
    //                     } else {
    //                         results.push(Ok(get.into()));
    //                     }
    //                 }
    //             }
    //             Err(err) => return Err(err.into()),
    //         }
    //     }
    //
    //     Ok(results)
    // }

    /// Puts a value with the given options
    pub(super) async fn put(
        &mut self,
        key: impl Into<String>,
        value: Vec<u8>,
        options: PutOptions,
    ) -> Result<PutResponse> {
        let session_id = match options.ephemeral {
            true => self.get_session_id().await?,
            false => None,
        };

        let req = self.make_put_req(session_id, options, key, value.into());
        let rsp = self.process_write(req).await?;

        if let Some(e) = check_put_response(&rsp) {
            return Err(e);
        }

        let p = rsp.puts.into_iter().next().unwrap();
        Ok(p.into())
    }

    /// Deletes a key with the given options
    pub(super) async fn delete(
        &self,
        key: impl Into<String>,
        options: DeleteOptions,
    ) -> Result<()> {
        let req = self.make_delete_req(&options, key);
        let rsp = self.process_write(req).await?;

        if let Some(e) = check_delete_response(&rsp) {
            return Err(e);
        }

        Ok(())
    }

    /// Lists keys in the given range with options
    pub(super) async fn list(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
        opts: ListOptions,
    ) -> Result<ListResponse> {
        let partial_ok = opts.partial_ok;

        let data = &self.data;

        let req = self.create_request(self.make_list_req(opts, start_inclusive, end_exclusive));

        let mut rsp = ListResponse::default();
        let mut rsp_stream = data.grpc.clone().list(req).await?.into_inner();

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
    pub(super) async fn list_with_timeout(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
        opts: ListOptions,
        timeout: Duration,
    ) -> Result<ListResponse> {
        // Apply timeout to the list operation
        match tokio::time::timeout(timeout, self.list(start_inclusive, end_exclusive, opts)).await {
            Ok(result) => result,
            Err(_) => Err(Error::Custom(format!(
                "List request timed out after {:?}",
                timeout
            ))),
        }
    }

    /// Scans for records in the given range with options
    pub(super) async fn range_scan(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
        opts: RangeScanOptions,
    ) -> Result<RangeScanResponse> {
        let partial_ok = opts.partial_ok;

        let data = &self.data;
        let req =
            self.create_request(self.make_range_scan_req(opts, start_inclusive, end_exclusive));

        let mut rsp = RangeScanResponse::default();
        let mut rsp_stream = data.grpc.clone().range_scan(req).await?.into_inner();

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
}

/// Trait for a shard mapping strategy.
///
/// Note: this trait is object-safe, despite being using with static dispatch.  This is to
/// facilitate possibly supporting more than one mapping strategy later.
trait ShardMapper {
    /// Builder type that can construct this map.
    type Builder: ShardMapperBuilder<Mapper = Self>;

    /// Create a new builder for this map.
    fn new_builder(n: &oxia_proto::NamespaceShardsAssignment) -> Result<Self::Builder>;

    /// Get the shard ID for a given key.
    fn get_shard_id(&self, key: &str) -> Option<i64>;
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
    use crate::UnexpectedServerResponse;

    use super::{Result, ShardMapper, ShardMapperBuilder};
    use mauricebarnum_oxia_common::proto as oxia_proto;
    use std::{cmp::Ordering, collections::HashSet};
    use xxhash_rust::xxh3::xxh3_64;

    #[derive(Debug)]
    struct Range {
        min: u32,
        max: u32,
        id: i64,
    }

    #[derive(Debug, Default)]
    pub(crate) struct Mapper {
        ranges: Vec<Range>,
    }

    impl Mapper {
        fn hash(key: &str) -> u32 {
            let h = xxh3_64(key.as_bytes());
            (h & 0xffffffff) as u32
        }
    }

    impl ShardMapper for Mapper {
        type Builder = Builder;

        fn new_builder(n: &oxia_proto::NamespaceShardsAssignment) -> Result<Self::Builder> {
            let r = oxia_proto::ShardKeyRouter::try_from(n.shard_key_router)
                .map_err(|e| UnexpectedServerResponse::BadShardKeyRouter(e.to_string()))?;

            if r != oxia_proto::ShardKeyRouter::Xxhash3 {
                return Err(UnexpectedServerResponse::BadShardKeyRouter(format!(
                    "unsupported shard_key_router {:?}",
                    r
                ))
                .into());
            }

            Ok(Builder {
                ranges: Vec::with_capacity(n.assignments.len()),
            })
        }

        fn get_shard_id(&self, key: &str) -> Option<i64> {
            let h = Self::hash(key);
            self.ranges
                .binary_search_by(|x| match h {
                    _ if h < x.min => Ordering::Less,
                    _ if h > x.max => Ordering::Greater,
                    _ => Ordering::Equal,
                })
                .ok()
                .map(|i| self.ranges[i].id)
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
                .ok_or(UnexpectedServerResponse::NoShardBoundaries)?;

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
                    return Err(UnexpectedServerResponse::OverlappingRanges(Box::new(
                        crate::OverlappingRangesData {
                            range1_min: p.min,
                            range1_max: p.max,
                            range2_min: r.min,
                            range2_max: r.max,
                        },
                    ))
                    .into());
                }
                p = r;

                if !seen.insert(r.id) {
                    return Err(UnexpectedServerResponse::DuplicateShardId(r.id).into());
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
                id: a.shard,
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
} // mod int32_hash_range

#[derive(Debug, Default)]
struct Shards {
    clients: Vec<(i64, Client)>,
    mapper: int32_hash_range::Mapper,
}

struct ShardAssignmentTask {
    config: Arc<config::Config>,
    cache: GrpcClientCache,
    shards: Arc<Mutex<Shards>>,
    ready: Option<oneshot::Sender<Result<()>>>,
}

impl ShardAssignmentTask {
    fn new(
        config: Arc<config::Config>,
        cache: GrpcClientCache,
        shards: Arc<Mutex<Shards>>,
        ready: oneshot::Sender<Result<()>>,
    ) -> Self {
        Self {
            config,
            cache,
            shards,
            ready: Some(ready),
        }
    }

    fn make_mapper(ns: &oxia_proto::NamespaceShardsAssignment) -> Result<int32_hash_range::Mapper> {
        let mut mb = int32_hash_range::Mapper::new_builder(ns)?;
        for a in &ns.assignments {
            mb.accept(a)?;
        }
        mb.build()
    }

    async fn process_assignments(&self, ns: &oxia_proto::NamespaceShardsAssignment) -> Result<()> {
        // Create shard mappings and bail out if we find anything silly.
        let mapper = Self::make_mapper(ns)?;

        // Now potentially make network calls to connect clients.
        let mut clients = Vec::with_capacity(ns.assignments.len());
        for a in &ns.assignments {
            let grpc = self.cache.get(&a.leader).await?;
            let client = Client::new(grpc, self.config.clone(), a.shard, &a.leader)?;
            clients.push((a.shard, client));
        }
        clients.sort_unstable_by_key(|c| c.0);

        let mut guard = self.shards.lock().await;
        guard.mapper = mapper;
        guard.clients = clients;

        Ok(())
    }

    async fn process_assignments_stream(&mut self, mut s: Streaming<oxia_proto::ShardAssignments>) {
        while let Some(r) = s.next().await {
            dbg!(&r);
            match r {
                Ok(r) => {
                    if let Some(a) = r.namespaces.get(&self.config.namespace) {
                        // TODO: log errors
                        let pr = self.process_assignments(a).await;
                        if let Some(tx) = self.ready.take() {
                            let _ = tx.send(pr);
                        }
                    }
                }
                Err(e) => {
                    dbg!("ERR", &e); // TODO: we'll want to log this an keep going
                    if let Some(tx) = self.ready.take() {
                        let _ = tx.send(Err(e.into()));
                    }
                }
            }
        }
    }

    async fn run(&mut self, mut cluster: GrpcClient) {
        let sleep_time = Duration::from_millis(600);
        loop {
            let req = oxia_proto::ShardAssignmentsRequest {
                namespace: self.config.namespace.clone(),
            };
            match cluster.get_shard_assignments(req).await {
                Ok(rsp) => self.process_assignments_stream(rsp.into_inner()).await,
                Err(err) => {
                    dbg!(&err);
                }
            }
            dbg!(&self.shards);
            println!("sleeping in retrieve_shards {:?}", sleep_time);
            tokio::time::sleep(sleep_time).await;
        }
    }
}

#[derive(Debug)]
pub(super) struct Manager {
    cache: GrpcClientCache,
    shards: Arc<Mutex<Shards>>,
    task_handle: JoinHandle<()>,
}

impl Manager {
    /// Creates a new shard manager
    pub(super) async fn new(
        config: &Arc<config::Config>,
        cluster: &GrpcClient,
        cache: &GrpcClientCache,
    ) -> Result<Self> {
        let shards = Arc::new(Mutex::new(Shards::default()));
        let task_handle =
            Self::start_shard_assignment_task(config, cluster, cache, &shards).await?;
        Ok(Manager {
            cache: cache.clone(),
            shards,
            task_handle,
        })
    }

    /// Gets a client for the shard that handles the given key
    pub(super) async fn get_client(&self, _namespace: &str, key: &str) -> Result<Client> {
        // TODO: figure out if this is worth the trouble. I suspect not until the Rust gets smarter about eliding the state machine from unnecessary
        // paths. But leave it here to ponder and play with.

        let guard = if let Ok(guard) = self.shards.try_lock() {
            guard
        } else {
            self.shards.lock().await
        };

        let shard_id = guard
            .mapper
            .get_shard_id(key)
            .ok_or_else(|| Error::Custom(format!("unable to map {key} to shard")))?;

        // Linear or binary search?  It depends.  Since it's easy let's just code up both and make
        // it someone else's problem to pick the crossover
        const BINARY_SEARCH_CROSSOVER: usize = 16;
        let c = if guard.clients.len() > BINARY_SEARCH_CROSSOVER {
            guard
                .clients
                .binary_search_by_key(&shard_id, |(k, _)| *k)
                .ok()
                .map(|i| &guard.clients[i])
        } else {
            guard.clients.iter().find(|x| x.0 == shard_id)
        }
        .ok_or_else(|| Error::NoShardMapping(shard_id))?;

        Ok(c.1.clone())
    }

    /// Executes a function on each shard client
    pub(super) async fn for_each_shard<R, F>(&self, _namespace: &str, mut f: F) -> Option<R>
    where
        F: FnMut(&Client) -> Option<R>,
    {
        let lock = self.shards.lock().await;
        for s in lock.clients.iter() {
            if let Some(r) = f(&s.1) {
                return Some(r);
            }
        }
        None
    }

    /// Returns a stream containing a snapshot of the current clients.  Use `for_each_shard` to
    /// work on a locked shards.
    pub(super) async fn shard_stream(&self, _namespace: &str) -> impl Stream<Item = Client> {
        let clients: Vec<Client> = {
            let lock = self.shards.lock().await;
            lock.clients
                .iter()
                .map(|(_, client)| client.clone())
                .collect()
        };
        tokio_stream::iter(clients)
    }

    /// Returns the number of shards at the moment
    /// This is mostly usable as hint, as the number of shards may change over time
    pub(super) async fn num_shards(&self) -> usize {
        self.shards.lock().await.clients.len()
    }

    pub(super) async fn reconnect(&mut self, dest: impl AsRef<str>) -> Result<()> {
        self.cache
            .reconnect(dest.as_ref(), |grpc| {
                let shards = self.shards.clone();
                let dest = dest.as_ref().to_string();
                let grpc = grpc.clone();
                async move {
                    // let mut guard = shards.lock().await;
                    for c in shards.lock().await.clients.iter_mut() {
                        let data = &mut c.1.data;
                        if data.leader == dest {
                            *data = Arc::new(ClientData::new(
                                data.config.clone(),
                                grpc.clone(),
                                data.shard_id,
                                data.leader.clone(),
                                data.meta.clone(),
                            ))
                        }
                    }
                }
            })
            .await?;
        Ok(())
    }

    /// blah
    async fn start_shard_assignment_task(
        config: &Arc<config::Config>,
        cluster: &GrpcClient,
        cache: &GrpcClientCache,
        shards: &Arc<Mutex<Shards>>,
    ) -> Result<JoinHandle<()>> {
        let (tx, rx) = oneshot::channel();
        let cluster = cluster.clone();
        let mut task = ShardAssignmentTask::new(config.clone(), cache.clone(), shards.clone(), tx);
        let handle = tokio::spawn(async move {
            task.run(cluster).await;
        });

        // TODO: time this out
        rx.await.map_err(|e| Error::Boxed(Box::new(e)))??;
        Ok(handle)
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}
