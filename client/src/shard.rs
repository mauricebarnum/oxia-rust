// Copyright 2025 Maurice S. Barnum
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

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use rand::SeedableRng;
use rand::distr::Distribution;
use rand::distr::Uniform;
use rand::rngs::StdRng;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::Stream;
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
use crate::config;
use crate::create_grpc_client;
use crate::pool::ChannelPool;

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
    grpc: GrpcClient,
    shard_id: i64,
    #[allow(dead_code)] // this is actually used
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

const INTERNAL_KEY_PREFIX: &str = "__oxia/";

/// Helper method to create a fully-formed tonic request with metadata
impl Client {
    fn start_heartbeat(&self, session_id: i64) -> JoinHandle<()> {
        let session_timeout_ms = self.data.config.session_timeout().as_millis();
        assert!(
            session_timeout_ms <= u128::from(u32::MAX),
            "bug: time out out of range {} max {}",
            session_timeout_ms,
            u32::MAX
        );

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
                #[allow(clippy::cast_sign_loss)]
                let min_ms = (target_ms * (1.0 - JITTER)).round() as u32;
                #[allow(clippy::cast_sign_loss)]
                let max_ms = (target_ms * (1.0 + JITTER)).round() as u32;
                Uniform::new_inclusive(min_ms, max_ms).unwrap()
            };
            loop {
                let req = tonic::Request::from_parts(
                    meta.clone(),
                    tonic::Extensions::default(),
                    oxia_proto::SessionHeartbeat { session_id, shard },
                );
                let rsp = grpc.keep_alive(req).await;
                debug!(?rsp, "grpc.keep_alive");

                let sleep_ms = distr.sample(&mut rng);
                let timeout = Duration::from_millis(sleep_ms.into());
                trace!("sleeping for {} ms", sleep_ms);
                sleep(timeout).await;
            }
        })
    }

    /// Creates a new Client instance for a specific shard
    pub(crate) fn new(
        grpc: GrpcClient,
        config: Arc<config::Config>,
        shard_id: i64,
        dest: impl Into<String>,
    ) -> Result<Self> {
        // Create metadata with shard ID and namespace
        let mut meta = tonic::metadata::MetadataMap::new();
        meta.insert("shard-id", MetadataValue::from(shard_id));
        meta.insert(
            "namespace",
            MetadataValue::from_str(config.namespace())
                .map_err(|e| Error::Custom(format!("Invalid metadata value: {e}")))?,
        );

        Ok(Client {
            data: Arc::new(ClientData::new(config, grpc, shard_id, dest, meta)),
            session: Arc::new(OnceCell::new()),
        })
    }

    pub(crate) fn id(&self) -> i64 {
        self.data.shard_id
    }

    async fn create_session(&self) -> Result<Session> {
        let data = &self.data;
        let config = &data.config;
        let mut grpc = data.grpc.clone();
        // Call create_session
        let rsp = grpc
            .create_session(self.create_request(oxia_proto::CreateSessionRequest {
                session_timeout_ms: config.session_timeout().as_millis() as u32,
                client_identity: config.identity().into(),
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
            .get_or_try_init(|| async {
                let rsp = self.create_session().await;
                debug!(?rsp, "get_session_id");
                rsp.map(Some)
            })
            .await?;
        Ok(s.as_ref().map(|session| session.id))
    }

    /// Creates a `GetRequest` with the appropriate options
    fn make_get_req(&self, opts: &GetOptions, k: &str) -> oxia_proto::ReadRequest {
        let data = &self.data;
        oxia_proto::ReadRequest {
            shard: Some(data.shard_id),
            gets: vec![oxia_proto::GetRequest {
                key: k.to_string(),
                include_value: opts.include_value,
                comparison_type: opts.comparison_type.into(),
                secondary_index_name: None,
            }],
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
            shard: Some(data.shard_id),
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
                secondary_indexes: SecondaryIndex::into_proto_vec(opts.secondary_indexes.clone()),
            }],
            deletes: vec![],
            delete_ranges: vec![],
        }
    }

    /// Creates a `DeleteRequest` with the appropriate options
    fn make_delete_req(&self, opts: &DeleteOptions, k: &str) -> oxia_proto::WriteRequest {
        let data = &self.data;
        oxia_proto::WriteRequest {
            shard: Some(data.shard_id),
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
            shard: Some(data.shard_id),
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
            shard: Some(data.shard_id),
            start_inclusive: start_inclusive.to_string(),
            end_exclusive: end_exclusive.to_string(),
            secondary_index_name: opts.secondary_index_name.clone(),
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

        // If end key is empty, use the internal key prefix to scan all keys
        let effective_end = {
            if end_exclusive.is_empty() {
                INTERNAL_KEY_PREFIX.to_string()
            } else {
                end_exclusive.to_string()
            }
        };

        oxia_proto::RangeScanRequest {
            shard: Some(data.shard_id),
            start_inclusive: start_inclusive.to_string(),
            end_exclusive: effective_end,
            secondary_index_name: opts.secondary_index_name.clone(),
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
            shard: Some(data.shard_id),
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
                "Expected exactly one response, but got additional response: {response:?}"
            )));
        }

        Ok(write_response)
    }

    /// Retrieves a key with the given options
    pub(crate) async fn get(&self, k: &str, opts: &GetOptions) -> Result<Option<GetResponse>> {
        use oxia_proto::Status;

        let req = self.create_request(self.make_get_req(opts, k));
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

        let r = rsp.gets.into_iter().next().unwrap();
        match Status::try_from(r.status) {
            Ok(Status::Ok) => Ok(Some(GetResponse::from_proto(r))),
            Ok(Status::KeyNotFound) => Ok(None),
            _ => Err(OxiaError::from(r.status).into()),
        }
    }

    // /// Gets multiple keys in a single batch request
    // pub(crate) async fn batch_get(
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

    pub(crate) async fn get_notifications(
        &self,
        start_offset_exclusive: Option<i64>,
    ) -> Result<NotificationsStream> {
        let req = oxia_proto::NotificationsRequest {
            shard: self.data.shard_id,
            start_offset_exclusive,
        };

        self.data
            .grpc
            .clone()
            .get_notifications(req)
            .await
            .map(|r| NotificationsStream::from_proto(r.into_inner()))
            .map_err(std::convert::Into::into)
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
            (h & 0xffff_ffff) as u32
        }
    }

    impl ShardMapper for Mapper {
        type Builder = Builder;

        fn new_builder(n: &oxia_proto::NamespaceShardsAssignment) -> Result<Self::Builder> {
            let r = oxia_proto::ShardKeyRouter::try_from(n.shard_key_router)
                .map_err(|e| UnexpectedServerResponse::BadShardKeyRouter(e.to_string()))?;

            if r != oxia_proto::ShardKeyRouter::Xxhash3 {
                return Err(UnexpectedServerResponse::BadShardKeyRouter(format!(
                    "unsupported shard_key_router {r:?}"
                ))
                .into());
            }

            Ok(Builder {
                ranges: Vec::with_capacity(n.assignments.len()),
            })
        }

        fn get_shard_id(&self, key: &str) -> Option<i64> {
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
                        id: 0,
                    },
                    Range {
                        min: 1,
                        max: 3,
                        id: 1,
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
                        id: 0,
                    },
                    Range {
                        min: 1_431_655_766,
                        max: 2_863_311_531,
                        id: 1,
                    },
                    Range {
                        min: 2_863_311_532,
                        max: 3_015_596_446,
                        id: 2,
                    },
                    // hole! [3_015_596_447, 3_015_596_448]
                    Range {
                        min: 3_015_596_449,
                        max: 4_294_967_295,
                        id: 2,
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
                        id: 0,
                    },
                    Range {
                        min: 1_431_655_766,
                        max: 2_863_311_531,
                        id: 1,
                    },
                    Range {
                        min: 2_863_311_532,
                        max: 3_015_596_446,
                        id: 2,
                    },
                    // hole! [3_015_596_447, 3_015_596_448]
                    Range {
                        min: 3_015_596_449,
                        max: 4_294_967_295,
                        id: 3,
                    },
                ],
            }
            .build()
            .unwrap();

            assert_eq!(1, mapper.get_shard_id("foo-0").unwrap());
            assert_eq!(3, mapper.get_shard_id("foo-4").unwrap());
            assert_eq!(0, mapper.get_shard_id("foo-5").unwrap());
            assert_eq!(2, mapper.get_shard_id("e199").unwrap());
            assert!(mapper.get_shard_id("foo-3").is_none());
        }
    }
} // mod int32_hash_range

#[derive(Clone, Debug)]
struct Shard {
    id: i64,
    client: Client,
}

mod searchable {
    use super::{Client, Error, Result, Shard, ShardMapper, int32_hash_range};

    // Linear or binary search for shards?  It depends.  Since it's easy let's just code up both and make
    // it someone else's problem to pick the crossover
    const BINARY_SEARCH_CROSSOVER: usize = 16;

    #[derive(Debug, Default)]
    pub(super) struct Shards {
        shards: Vec<super::Shard>,
        mapper: int32_hash_range::Mapper,
    }

    impl Shards {
        pub(super) fn new(mut shards: Vec<Shard>, mapper: int32_hash_range::Mapper) -> Self {
            if shards.len() > BINARY_SEARCH_CROSSOVER {
                shards.sort_unstable_by_key(|c| c.id);
            }
            Self { shards, mapper }
        }

        pub(super) fn find_by_id(&self, id: i64) -> Result<Client> {
            let s = if self.shards.len() > BINARY_SEARCH_CROSSOVER {
                self.shards
                    .binary_search_by_key(&id, |s| s.id)
                    .ok()
                    .map(|i| &self.shards[i])
            } else {
                self.shards.iter().find(|s| s.id == id)
            };
            s.map(|s| s.client.clone())
                .ok_or_else(|| Error::NoShardMapping(id))
        }

        pub(super) fn find_by_key(&self, key: &str) -> Result<Client> {
            let id = self
                .mapper
                .get_shard_id(key)
                .ok_or_else(|| Error::Custom(format!("unable to map {key} to shard")))?;
            self.find_by_id(id)
        }
    }

    impl std::ops::Deref for Shards {
        type Target = [Shard];
        fn deref(&self) -> &Self::Target {
            &self.shards
        }
    }
}

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
    shards: Arc<Mutex<searchable::Shards>>,
    changed: watch::Sender<std::result::Result<(), Arc<Error>>>,
}

impl ShardAssignmentTask {
    fn new(
        config: Arc<config::Config>,
        channel_pool: ChannelPool,
        shards: Arc<Mutex<searchable::Shards>>,
        changed: watch::Sender<std::result::Result<(), Arc<Error>>>,
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

        // Now potentially make network calls to connect clients.
        let mut shards = Vec::with_capacity(ns.assignments.len());
        for a in &ns.assignments {
            info!(?a, "processing assignments");
            let grpc = create_grpc_client(&a.leader, &self.channel_pool).await?;
            let client = Client::new(grpc, self.config.clone(), a.shard, &a.leader)?;
            shards.push(Shard {
                id: a.shard,
                client,
            });
        }

        let mut guard = self.shards.lock().await;
        *guard = searchable::Shards::new(shards, mapper);

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
                        let _ = self.changed.send(pr.map_err(Arc::new));
                    }
                }
                Err(e) => {
                    let _ = self.changed.send(Err(Arc::new(e.into())));
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
    // channel_pool: ChannelPool,
    shards: Arc<Mutex<searchable::Shards>>,
    shutdown_requested: Arc<AtomicBool>,
    task_handle: JoinHandle<()>,
    changed: watch::Receiver<std::result::Result<(), Arc<Error>>>,
}

impl Manager {
    /// Creates a new shard manager
    pub(crate) async fn new(config: &Arc<config::Config>) -> Result<Self> {
        let channel_pool = ChannelPool::new(config);
        let shards = Arc::new(Mutex::new(searchable::Shards::default()));
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let (task_handle, changed) =
            Self::start_shard_assignment_task(config, &channel_pool, &shards, &shutdown_requested)
                .await?;
        Ok(Manager {
            // channel_pool,
            shards,
            shutdown_requested,
            task_handle,
            changed,
        })
    }

    /// Gets a client by shard id
    pub(crate) async fn get_client_by_shard_id(&self, id: i64) -> Result<Client> {
        self.shards.lock().await.find_by_id(id)
    }

    /// Gets a client for the shard that handles the given key
    pub(crate) async fn get_client(&self, key: &str) -> Result<Client> {
        self.shards.lock().await.find_by_key(key)
    }

    /// Returns a stream containing a snapshot of the current clients.  Use `for_each_shard` to
    /// work on a locked shards.
    pub(crate) async fn shard_stream(&self) -> impl Stream<Item = Client> + use<> {
        let clients: Vec<Client> = {
            let lock = self.shards.lock().await;
            lock.iter().map(|s| s.client.clone()).collect()
        };
        tokio_stream::iter(clients)
    }

    /// Returns the number of shards at the moment
    /// This is mostly usable as hint, as the number of shards may change over time
    #[allow(dead_code)]
    pub(crate) async fn num_shards(&self) -> usize {
        self.shards.lock().await.len()
    }

    #[allow(dead_code)]
    pub(crate) fn recv_changed(&self) -> watch::Receiver<std::result::Result<(), Arc<Error>>> {
        self.changed.clone()
    }

    async fn start_shard_assignment_task(
        config: &Arc<config::Config>,
        channel_pool: &ChannelPool,
        shards: &Arc<Mutex<searchable::Shards>>,
        shutdown_requested: &Arc<AtomicBool>,
    ) -> Result<(
        JoinHandle<()>,
        watch::Receiver<std::result::Result<(), Arc<Error>>>,
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
        rx.changed().await.map_err(|e| Error::Boxed(Box::new(e)))?;
        rx.borrow_and_update()
            .as_ref()
            .map_err(|e| Error::Shared(e.clone()))?;
        Ok((handle, rx))
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        self.shutdown_requested.store(true, Ordering::Relaxed);
        self.task_handle.abort();
    }
}
