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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use bon::Builder;
use bytes::Bytes;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;
use tonic::transport::Channel;

use mauricebarnum_oxia_common::proto as oxia_proto;

use crate::notification::NotificationsStream;
use crate::pool::ChannelPool;

pub mod batch_get;
pub mod config;
pub mod errors;
mod notification;
mod pool;
mod shard;
mod util;

pub use errors::*;
pub use shard::ShardId;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct SecondaryIndex(oxia_proto::SecondaryIndex);

impl SecondaryIndex {
    pub fn new(name: impl Into<String>, key: impl Into<String>) -> Self {
        Self(oxia_proto::SecondaryIndex {
            index_name: name.into(),
            secondary_key: key.into(),
        })
    }

    pub fn index_name(&self) -> &str {
        &self.0.index_name
    }
    pub fn secondary_key(&self) -> &str {
        &self.0.secondary_key
    }

    // --- crate-only helpers; do not leak oxia_proto in public API ---
    #[inline]
    pub(crate) fn take_inner(&mut self) -> oxia_proto::SecondaryIndex {
        // Cheap dummy: String::new() does not allocate.
        std::mem::replace(
            &mut self.0,
            oxia_proto::SecondaryIndex {
                index_name: String::new(),
                secondary_key: String::new(),
            },
        )
    }

    #[inline]
    pub(crate) fn clone_inner(&self) -> oxia_proto::SecondaryIndex {
        self.0.clone()
    }

    pub(crate) fn to_proto(
        mut si: Option<Arc<[SecondaryIndex]>>,
    ) -> Vec<oxia_proto::SecondaryIndex> {
        match &mut si {
            None => Vec::new(),
            Some(arc) => {
                if let Some(slice) = Arc::get_mut(arc) {
                    // Unique: move out by replacing with dummies.
                    let mut out = Vec::with_capacity(slice.len());
                    for s in slice.iter_mut() {
                        out.push(s.take_inner());
                    }
                    out
                } else {
                    arc.iter().map(SecondaryIndex::clone_inner).collect()
                }
            }
        }
    }
}

#[derive(Builder, Clone, Debug)]
pub struct PutOptions {
    expected_version_id: Option<i64>,
    #[builder(into)]
    partition_key: Option<String>,
    #[builder(into)]
    sequence_key_deltas: Option<Arc<[u64]>>,
    #[builder(into)]
    secondary_indexes: Option<Arc<[SecondaryIndex]>>,
    #[builder(default = false)]
    ephemeral: bool,
}

impl PutOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for PutOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[derive(Clone, Debug)]
pub struct PutResponse {
    pub version: RecordVersion,
    pub key: Option<String>,
}

impl PutResponse {
    fn from_proto(x: oxia_proto::PutResponse) -> Self {
        PutResponse {
            version: RecordVersion::from_proto(x.version),
            key: x.key,
        }
    }
}

#[repr(i32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum KeyComparisonType {
    /// The stored key must be equal to the requested key
    Equal = oxia_proto::KeyComparisonType::Equal as i32,
    /// Search for a key that is the highest key that is <= to the requested key
    Floor = oxia_proto::KeyComparisonType::Floor as i32,
    /// Search for a key that is the lowest key that is >= to the requested key
    Ceiling = oxia_proto::KeyComparisonType::Ceiling as i32,
    /// Search for a key that is the highest key that is < to the requested key
    Lower = oxia_proto::KeyComparisonType::Lower as i32,
    /// Search for a key that is the lowest key that is > to the requested key
    Higher = oxia_proto::KeyComparisonType::Higher as i32,
}

impl KeyComparisonType {
    fn to_proto(self) -> oxia_proto::KeyComparisonType {
        match self {
            KeyComparisonType::Equal => oxia_proto::KeyComparisonType::Equal,
            KeyComparisonType::Floor => oxia_proto::KeyComparisonType::Floor,
            KeyComparisonType::Ceiling => oxia_proto::KeyComparisonType::Ceiling,
            KeyComparisonType::Lower => oxia_proto::KeyComparisonType::Lower,
            KeyComparisonType::Higher => oxia_proto::KeyComparisonType::Higher,
        }
    }
}

impl From<KeyComparisonType> for i32 {
    fn from(k: KeyComparisonType) -> i32 {
        k as i32
    }
}

impl Display for KeyComparisonType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_proto().as_str_name())
    }
}

#[derive(Builder, Clone, Debug)]
#[builder(on(String, into))]
pub struct GetOptions {
    #[builder(default = true)]
    include_value: bool, // default: true
    #[builder(default = KeyComparisonType::Equal)]
    comparison_type: KeyComparisonType, // default: Equal
    partition_key: Option<String>,        // default: None
    secondary_index_name: Option<String>, // default: None
}

impl GetOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for GetOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl<S: get_options_builder::State> GetOptionsBuilder<S> {
    pub fn exclude_value(self) -> GetOptionsBuilder<get_options_builder::SetIncludeValue<S>>
    where
        S::IncludeValue: get_options_builder::IsUnset,
    {
        self.include_value(false)
    }
}

#[derive(Clone, Debug, Default, Eq)]
pub struct RecordVersion {
    pub version_id: i64,
    pub modifications_count: i64,
    pub created_timestamp: u64,
    pub modified_timestamp: u64,
    pub session_id: Option<i64>,         // ignored by comparision
    pub client_identity: Option<String>, // ignored by comparison traits
}

impl RecordVersion {
    fn from_proto(value: Option<oxia_proto::Version>) -> Self {
        if let Some(v) = value {
            RecordVersion {
                version_id: v.version_id,
                modifications_count: v.modifications_count,
                created_timestamp: v.created_timestamp,
                modified_timestamp: v.modified_timestamp,
                session_id: v.session_id,
                client_identity: v.client_identity,
            }
        } else {
            RecordVersion::default()
        }
    }

    pub fn is_ephemeral(&self) -> bool {
        self.session_id.is_some()
    }
}

impl PartialEq for RecordVersion {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for RecordVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RecordVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        self.version_id
            .cmp(&other.version_id)
            .then_with(|| self.modifications_count.cmp(&other.modifications_count))
            .then_with(|| self.created_timestamp.cmp(&other.created_timestamp))
            .then_with(|| self.modified_timestamp.cmp(&other.modified_timestamp))
    }
}

#[derive(Clone, Debug, Default, Eq)]
pub struct GetResponse {
    pub value: Option<Bytes>,
    pub version: RecordVersion,
    pub key: Option<String>,
    pub secondary_index_key: Option<String>,
}

impl GetResponse {
    fn from_proto(x: oxia_proto::GetResponse) -> Self {
        GetResponse {
            value: x.value,
            version: RecordVersion::from_proto(x.version),
            key: x.key,
            secondary_index_key: x.secondary_index_key,
        }
    }
}

impl PartialEq for GetResponse {
    fn eq(&self, other: &Self) -> bool {
        // We want to compare `value` last, hence not using `#[derive(PartialEq)]`.  A manual
        // implementation might be more efficient, but let's start simple so we don't need to worry
        // about divergence.
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for GetResponse {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GetResponse {
    fn cmp(&self, other: &Self) -> Ordering {
        self.secondary_index_key
            .cmp(&other.secondary_index_key)
            .then_with(|| self.key.cmp(&other.key))
            .then_with(|| self.version.cmp(&other.version))
            .then_with(|| self.value.as_ref().cmp(&other.value.as_ref()))
    }
}

#[derive(Builder, Clone, Debug)]
pub struct DeleteOptions {
    expected_version_id: Option<i64>,
    #[builder(into)]
    partition_key: Option<String>,
}

impl DeleteOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for DeleteOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[derive(Builder, Clone, Debug)]
#[builder(on(String, into))]
pub struct ListOptions {
    #[allow(dead_code)]
    shard: Option<i64>,
    secondary_index_name: Option<String>,
    #[builder(default = true)]
    sort: bool,
    #[builder(default = true)]
    partial_ok: bool,
    partition_key: Option<String>,
    #[builder(default = false)]
    include_internal_keys: bool,
}

impl ListOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for ListOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[derive(Clone, Debug)]
pub struct ListResponse {
    pub keys: Vec<String>,
    pub sorted: bool,
    pub partial: bool,
}

impl ListResponse {
    fn accum(&mut self, x: oxia_proto::ListResponse) {
        if !x.keys.is_empty() {
            self.sorted = self.keys.is_empty();
            self.keys.extend(x.keys.into_iter().map(|x| x.to_string()));
        }
    }

    fn merge(&mut self, x: ListResponse) {
        self.keys.extend(x.keys);
        self.sorted = false;
        if x.partial {
            self.partial = true;
        }
    }

    fn sort(&mut self) {
        if !self.sorted {
            self.keys.sort_unstable();
            self.sorted = true;
        }
    }
}

impl Default for ListResponse {
    fn default() -> Self {
        Self {
            keys: Vec::new(),
            sorted: true,
            partial: false,
        }
    }
}

#[derive(Builder, Clone, Debug)]
#[builder(on(String, into))]
pub struct RangeScanOptions {
    #[allow(dead_code)]
    shard: Option<i64>,
    secondary_index_name: Option<String>,
    #[builder(default = true)]
    sort: bool,
    #[builder(default = true)]
    partial_ok: bool,
    partition_key: Option<String>,
    #[builder(default = false)]
    include_internal_keys: bool,
}

impl RangeScanOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for RangeScanOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[derive(Clone, Debug)]
pub struct RangeScanResponse {
    pub records: Vec<GetResponse>,
    pub sorted: bool,
    pub partial: bool,
}

impl RangeScanResponse {
    fn accum(&mut self, x: oxia_proto::RangeScanResponse) {
        if !x.records.is_empty() {
            self.sorted = self.records.is_empty();
            self.records
                .extend(x.records.into_iter().map(GetResponse::from_proto));
        }
    }

    fn merge(&mut self, x: RangeScanResponse) {
        self.records.extend(x.records);
        self.sorted = false;
        if x.partial {
            self.partial = true;
        }
    }

    fn sort(&mut self) {
        if !self.sorted {
            self.records.sort_unstable_by(|a, b| a.key.cmp(&b.key));
            self.sorted = true;
        }
    }
}

impl Default for RangeScanResponse {
    fn default() -> Self {
        Self {
            records: vec![],
            sorted: true,
            partial: false,
        }
    }
}

#[derive(Builder, Clone, Debug)]
pub struct DeleteRangeOptions {
    shard: Option<i64>,
    #[builder(into)]
    partition_key: Option<String>,
}

impl DeleteRangeOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for DeleteRangeOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Options for creating a notifications stream.
///
/// By default, the stream will not automatically reconnect when a shard stream
/// closes or encounters an error. Use the reconnect options to enable automatic
/// reconnection behavior.
#[derive(Builder, Clone, Debug)]
pub struct NotificationsOptions {
    /// Enable automatic reconnection when a shard stream closes normally.
    ///
    /// When enabled, the notifications stream will automatically attempt to
    /// reconnect to a shard when its stream ends without an error, resuming
    /// from the last received offset.
    #[builder(default = false)]
    pub(crate) reconnect_on_close: bool,

    /// Enable automatic reconnection when a shard stream encounters an error.
    ///
    /// When enabled, the notifications stream will automatically attempt to
    /// reconnect to a shard when an error occurs, resuming from the last
    /// received offset.
    ///
    /// Note: Errors indicating the shard is no longer available (e.g., shard
    /// reassignment) will not trigger a reconnection attempt.
    #[builder(default = false)]
    pub(crate) reconnect_on_error: bool,
}

impl NotificationsOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for NotificationsOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum NotificationType {
    KeyCreated = oxia_proto::NotificationType::KeyCreated as i32,
    KeyModified = oxia_proto::NotificationType::KeyModified as i32,
    KeyDeleted = oxia_proto::NotificationType::KeyDeleted as i32,
    KeyRangeDeleted = oxia_proto::NotificationType::KeyRangeDeleted as i32,
    Unknown(i32),
}

impl NotificationType {
    fn try_to_proto(self) -> Option<oxia_proto::NotificationType> {
        match self {
            NotificationType::KeyCreated => Some(oxia_proto::NotificationType::KeyCreated),
            NotificationType::KeyModified => Some(oxia_proto::NotificationType::KeyModified),
            NotificationType::KeyDeleted => Some(oxia_proto::NotificationType::KeyDeleted),
            NotificationType::KeyRangeDeleted => {
                Some(oxia_proto::NotificationType::KeyRangeDeleted)
            }
            NotificationType::Unknown(_) => None,
        }
    }
}

impl From<i32> for NotificationType {
    fn from(x: i32) -> Self {
        use oxia_proto::NotificationType as p;
        match x {
            x if x == p::KeyCreated as i32 => NotificationType::KeyCreated,
            x if x == p::KeyModified as i32 => NotificationType::KeyModified,
            x if x == p::KeyDeleted as i32 => NotificationType::KeyDeleted,
            x if x == p::KeyRangeDeleted as i32 => NotificationType::KeyRangeDeleted,
            other => NotificationType::Unknown(other),
        }
    }
}

impl Display for NotificationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let NotificationType::Unknown(x) = *self {
            write!(f, "Uknownn({x}")
        } else {
            write!(f, "{}", self.try_to_proto().unwrap().as_str_name())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Notification {
    pub type_: NotificationType,
    pub version_id: Option<i64>,
    pub key_range_last: Option<String>,
}

impl Notification {
    fn from_proto(x: oxia_proto::Notification) -> Self {
        Self {
            type_: x.r#type.into(),
            version_id: x.version_id,
            key_range_last: x.key_range_last,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NotificationBatch {
    pub shard: ShardId,
    pub offset: i64,
    pub timestamp: u64,
    pub notifications: HashMap<String, Notification>,
}

impl NotificationBatch {
    fn from_proto(x: oxia_proto::NotificationBatch) -> Self {
        Self {
            shard: x.shard.into(),
            offset: x.offset,
            timestamp: x.timestamp,
            notifications: x
                .notifications
                .into_iter()
                .map(|(k, v)| (k, Notification::from_proto(v)))
                .collect(),
        }
    }
}

type GrpcClient = oxia_proto::OxiaClientClient<Channel>;

pub(crate) async fn create_grpc_client(
    dest: &str,
    channel_pool: &ChannelPool,
) -> Result<GrpcClient> {
    let url = {
        // TODO: http v https
        const SCHEME: &str = "http://";
        let mut url = String::with_capacity(SCHEME.len() + dest.len());
        url.push_str(SCHEME);
        url.push_str(dest);
        url
    };
    let channel = channel_pool.get(&url).await?;
    Ok(GrpcClient::new(channel))
}

pub(crate) async fn execute_with_retry<Fut, R>(
    config: &Arc<config::Config>,
    op: impl Fn() -> Fut,
) -> Result<R>
where
    Fut: std::future::Future<Output = Result<R>> + Send,
    R: Send,
{
    let timeout = config.request_timeout();
    let retry_config = config.retry();
    let start = if retry_config.is_some() && timeout.unwrap_or_default() > Duration::ZERO {
        Some(Instant::now())
    } else {
        None
    };

    let result = util::with_timeout(timeout, op()).await;

    // Happy path: call just worked
    if result.is_ok() {
        return result;
    }

    // Or... our time budget was consumed, and we're done
    if matches!(&result, Err(Error::RequestTimeout)) {
        return result;
    }

    // Or... no retries are requested
    if retry_config.is_none() {
        return result;
    }

    // Account for the time we've used so far for the next round of retries
    let timeout = timeout
        .zip(start)
        .map(|(t, s)| t.saturating_sub(s.elapsed()));

    let retry_op = move || op();
    util::with_timeout(timeout, util::with_retry(retry_config, retry_op)).await
}

#[derive(Clone, Debug)]
pub struct Client {
    shard_manager: Option<Arc<shard::Manager>>,
    config: Arc<config::Config>,
}

impl Client {
    pub fn new(config: Arc<config::Config>) -> Self {
        Self {
            shard_manager: None,
            config,
        }
    }

    pub fn config(&self) -> &Arc<config::Config> {
        &self.config
    }

    #[inline]
    pub fn is_connected(&self) -> bool {
        self.shard_manager.is_some()
    }

    pub async fn connect(&mut self) -> Result<()> {
        if !self.is_connected() {
            let sm = shard::Manager::new(&self.config).await?;
            self.shard_manager = Some(Arc::new(sm));
        }
        Ok(())
    }

    pub async fn reconnect(&mut self) -> Result<()> {
        self.shard_manager = None;
        self.connect().await
    }

    #[inline]
    fn get_shard_manager(&self) -> Result<Arc<shard::Manager>> {
        self.shard_manager
            .clone()
            .ok_or_else(|| crate::Error::Custom("Shard manager not initialized".to_string()))
    }

    fn get_shard(&self, k: &str) -> Result<shard::Client> {
        self.get_shard_manager()?.get_client(k)
    }

    fn get_shard_by_id(&self, id: ShardId) -> Result<shard::Client> {
        self.get_shard_manager()?.get_client_by_shard_id(id)
    }

    pub async fn get_with_options(
        &self,
        key: impl Into<String>,
        options: GetOptions,
    ) -> Result<Option<GetResponse>> {
        let key = Arc::from(key.into());

        // Define the core operation with retry/timeout wrapper
        let execute_get = |shard: shard::Client, key: Arc<str>, options: GetOptions| async move {
            execute_with_retry(&self.config, move || {
                let shard = shard.clone();
                let key = Arc::clone(&key);
                let options = options.clone();
                async move { shard.get(&key, &options).await }
            })
            .await
        };

        // Single shard case: exact match with partition key OR exact match without secondary index
        let use_single_shard = options.partition_key.is_some()
            || (options.comparison_type == KeyComparisonType::Equal
                && options.secondary_index_name.is_none());

        if use_single_shard {
            let selector = options.partition_key.as_deref().unwrap_or(&key);
            let shard = self.get_shard(selector)?;
            return execute_get(shard, key, options).await;
        }

        // Multi-shard case: query all shards and select best response
        let max_parallel = match self.config.max_parallel_requests() {
            0 => usize::MAX,
            n => n,
        };

        let best_response = futures::stream::iter(self.get_shard_manager()?.get_shard_clients())
            .map(|shard| execute_get(shard, Arc::clone(&key), options.clone()))
            .buffer_unordered(max_parallel)
            .try_fold(None, |best, response| async {
                Ok(match response {
                    Some(candidate) => Some(match best {
                        None => candidate,
                        Some(prev) => {
                            util::select_response(Some(prev), candidate, options.comparison_type)
                        }
                    }),
                    None => best,
                })
            })
            .await?;

        Ok(best_response)
    }

    pub async fn get(&self, k: impl Into<String>) -> Result<Option<GetResponse>> {
        self.get_with_options(k, GetOptions::default()).await
    }

    /// batch_get request multiple keys, minimizing requests to the backend
    ///
    /// On success, the result stream will yield one item per key, even in the event
    /// of a network failure or timeout.  The order of returned keys is not specified.
    ///
    /// Only KeyComparisonType::Equal is accepted.
    pub async fn batch_get(
        &self,
        req: batch_get::Request,
    ) -> Result<impl futures::stream::Stream<Item = batch_get::ResponseItem> + use<>> {
        let shard_manager = self.get_shard_manager()?;
        let (batch_get_futures, failures) = batch_get::prepare_requests(req, shard_manager).await?;

        // Send the request to all of the shards in parallel.  This will block until the initial
        // backend calls return either a stream or an error.  A remote error will be demuxed into
        // per-key errors, see shard::Client::batch_get()
        let batch_get_stream = if batch_get_futures.is_empty() {
            None
        } else {
            Some(
                futures::stream::iter(batch_get_futures)
                    .buffer_unordered(self.config.max_parallel_requests())
                    .flatten_unordered(None)
                    .boxed(),
            )
        };

        let failures_stream = if failures.is_empty() {
            None
        } else {
            Some(futures::stream::iter(failures))
        };

        // Finally, return a stream that will collect results from each shard as they become
        // available.  Per-shard ordering is as defined by the Oxia server (as of this writing,
        // they should be in the same order).  The ordering of responses between shards and the early failures
        // already collected is unspecified.
        //
        // All of this syntatic mess lets us return precisely the stream we need without boxing it.
        // The only overhead is reading it, which hopefully will be done just this once.

        use futures::future::Either::Left;
        use futures::future::Either::Right;

        let final_stream = match (batch_get_stream, failures_stream) {
            // Requests, no early failures
            (Some(b), None) => Left(b),

            // Requests and early failures
            (Some(b), Some(f)) => Right(Left(Left(futures::stream::select(b, f)))),

            // All failures
            (None, Some(f)) => Right(Left(Right(f))),

            // Empty requests? Should be rare
            (None, None) => Right(Right(futures::stream::empty())),
        };

        Ok(final_stream)
    }

    pub async fn put_with_options(
        &self,
        key: impl Into<String>,
        value: impl Into<Bytes>,
        options: PutOptions,
    ) -> Result<PutResponse> {
        let key = Arc::from(key.into());
        let value = value.into();
        let selector = options.partition_key.as_deref().unwrap_or(&key);
        let shard = self.get_shard(selector)?;
        execute_with_retry(&self.config, move || {
            let shard = shard.clone();
            let key = Arc::clone(&key);
            let value = value.clone();
            let options = options.clone();
            async move { shard.put(&key, value, &options).await }
        })
        .await
    }

    pub async fn put(
        &self,
        key: impl Into<String>,
        value: impl Into<Bytes>,
    ) -> Result<PutResponse> {
        self.put_with_options(key, value, PutOptions::new()).await
    }

    pub async fn delete_with_options(
        &self,
        key: impl Into<String>,
        options: DeleteOptions,
    ) -> Result<()> {
        let key = Arc::from(key.into());
        let selector = options.partition_key.as_deref().unwrap_or(&key);
        let shard = self.get_shard(selector)?;
        execute_with_retry(&self.config, move || {
            let shard = shard.clone();
            let key = Arc::clone(&key);
            let options = options.clone();
            async move { shard.delete(&key, &options).await }
        })
        .await
    }

    pub async fn delete(&self, key: impl Into<String>) -> Result<()> {
        self.delete_with_options(key, DeleteOptions::default())
            .await
    }

    pub async fn delete_range_with_options(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
        options: DeleteRangeOptions,
    ) -> Result<()> {
        let do_delete_range = |shard: shard::Client,
                               start: Arc<str>,
                               end: Arc<str>,
                               options: DeleteRangeOptions| async move {
            execute_with_retry(&self.config, move || {
                let shard = shard.clone();
                let start = Arc::clone(&start);
                let end = Arc::clone(&end);
                let options = options.clone();
                async move { shard.delete_range(&start, &end, &options).await }
            })
            .await
        };

        let start = Arc::from(start_inclusive.into());
        let end = Arc::from(end_exclusive.into());

        if let Some(shard) = {
            if let Some(pk) = options.partition_key.as_deref() {
                Some(self.get_shard(pk)?)
            } else if let Some(id) = options.shard {
                Some(self.get_shard_by_id(id.into())?)
            } else {
                None
            }
        } {
            return do_delete_range(shard, start, end, options).await;
        }

        let n = match self.config.max_parallel_requests() {
            0 => usize::MAX,
            n => n,
        };

        let mut result_stream =
            futures::stream::iter(self.get_shard_manager()?.get_shard_clients())
                .map(|shard| {
                    do_delete_range(shard, Arc::clone(&start), Arc::clone(&end), options.clone())
                })
                .buffer_unordered(n);

        let mut errs = Vec::new();
        while let Some(shard_result) = result_stream.next().await {
            match shard_result {
                Err(Error::ShardError(e)) => errs.push(e),
                Err(_) => return shard_result,
                Ok(()) => (),
            }
        }

        if !errs.is_empty() {
            return Err(Error::MultipleShardError(errs.into()));
        }

        Ok(())
    }

    pub async fn delete_range(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
    ) -> Result<()> {
        self.delete_range_with_options(
            start_inclusive,
            end_exclusive,
            DeleteRangeOptions::default(),
        )
        .await
    }

    pub async fn list_with_options(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
        options: ListOptions,
    ) -> Result<ListResponse> {
        let do_list = |shard: shard::Client,
                       start: Arc<str>,
                       end: Arc<str>,
                       options: ListOptions| async move {
            execute_with_retry(&self.config, move || {
                let shard = shard.clone();
                let start = Arc::clone(&start);
                let end = Arc::clone(&end);
                let options = options.clone();
                async move { shard.list(&start, &end, &options).await }
            })
            .await
        };

        let start = Arc::from(start_inclusive.into());
        let end = Arc::from(end_exclusive.into());

        if let Some(pk) = options.partition_key.as_deref() {
            let shard = self.get_shard(pk)?;
            return do_list(shard, start, end, options).await;
        }

        let n = match self.config.max_parallel_requests() {
            0 => usize::MAX,
            n => n,
        };

        let mut result_stream =
            futures::stream::iter(self.get_shard_manager()?.get_shard_clients())
                .map(|shard| do_list(shard, Arc::clone(&start), Arc::clone(&end), options.clone()))
                .buffer_unordered(n);

        let mut response = ListResponse::default();
        while let Some(shard_result) = result_stream.next().await {
            if let Ok(shard_response) = shard_result {
                response.merge(shard_response);
            } else if options.partial_ok {
                response.partial = true;
            } else {
                return shard_result;
            }
        }

        if options.sort {
            response.sort();
        }

        Ok(response)
    }

    pub async fn list(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
    ) -> Result<ListResponse> {
        self.list_with_options(start_inclusive, end_exclusive, ListOptions::default())
            .await
    }

    pub async fn range_scan_with_options(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
        options: RangeScanOptions,
    ) -> Result<RangeScanResponse> {
        let do_range_scan = |shard: shard::Client,
                             start: Arc<str>,
                             end: Arc<str>,
                             options: RangeScanOptions| async move {
            execute_with_retry(&self.config, move || {
                let shard = shard.clone();
                let start = Arc::clone(&start);
                let end = Arc::clone(&end);
                let options = options.clone();
                async move { shard.range_scan(&start, &end, &options).await }
            })
            .await
        };

        let start = Arc::from(start_inclusive.into());
        let end = Arc::from(end_exclusive.into());

        if let Some(pk) = options.partition_key.as_deref() {
            let shard = self.get_shard(pk)?;
            return do_range_scan(shard, start, end, options).await;
        }

        let n = match self.config.max_parallel_requests() {
            0 => usize::MAX,
            n => n,
        };

        let mut result_stream =
            futures::stream::iter(self.get_shard_manager()?.get_shard_clients())
                .map(|shard| {
                    do_range_scan(shard, Arc::clone(&start), Arc::clone(&end), options.clone())
                })
                .buffer_unordered(n);

        let mut response = RangeScanResponse::default();
        while let Some(shard_result) = result_stream.next().await {
            if let Ok(shard_response) = shard_result {
                response.merge(shard_response);
            } else if options.partial_ok {
                response.partial = true;
            } else {
                return shard_result;
            }
        }

        if options.sort {
            response.sort();
        }

        Ok(response)
    }

    pub async fn range_scan(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
    ) -> Result<RangeScanResponse> {
        self.range_scan_with_options(start_inclusive, end_exclusive, RangeScanOptions::default())
            .await
    }

    /// Create a notifications stream with default options.
    pub fn create_notifications_stream(&self) -> Result<notification::NotificationsStream> {
        self.create_notifications_stream_with_options(NotificationsOptions::default())
    }

    /// Create a notifications stream with custom options.
    pub fn create_notifications_stream_with_options(
        &self,
        options: NotificationsOptions,
    ) -> Result<notification::NotificationsStream> {
        let shard_manager = self.get_shard_manager()?;
        Ok(NotificationsStream::new(
            Arc::clone(&self.config),
            Arc::clone(&shard_manager),
            options,
        ))
    }
}
