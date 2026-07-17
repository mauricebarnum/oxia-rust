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
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::time::Duration;

use backon::FibonacciBuilder;
use backon::Retryable;
use bon::Builder;
use bytes::Bytes;
use futures_core::stream::BoxStream;
use futures_util::stream::StreamExt;
use futures_util::stream::TryStreamExt;
use mauricebarnum_oxia_common::proto as oxia_proto;
use tonic::transport::Channel;
use tracing::Instrument;

use crate::notification::NotificationsStream;
use crate::pool::ChannelPool;
use crate::shard::ShardMapEpoch;

pub mod batch_get;
pub mod config;
pub mod discovery;
pub mod errors;
mod metrics;
mod notification;
mod pool;
mod shard;
mod util;

pub use errors::*;
pub use shard::ShardId;

pub type Result<T> = std::result::Result<T, Error>;

#[inline]
fn instrument_stream<S>(stream: S, span: tracing::Span) -> impl futures_core::Stream<Item = S::Item>
where
    S: futures_core::Stream,
{
    let mut stream = Box::pin(stream);
    futures_util::stream::poll_fn(move |context: &mut Context<'_>| {
        let _entered = span.enter();
        Pin::as_mut(&mut stream).poll_next(context)
    })
}

#[derive(Clone, Debug)]
pub struct SecondaryIndex(oxia_proto::SecondaryIndex);

impl SecondaryIndex {
    pub fn new(name: impl Into<String>, key: impl Into<String>) -> Self {
        Self(oxia_proto::SecondaryIndex {
            index_name: name.into(),
            secondary_key: key.into(),
        })
    }

    #[inline]
    pub fn index_name(&self) -> &str {
        &self.0.index_name
    }

    #[inline]
    pub fn secondary_key(&self) -> &str {
        &self.0.secondary_key
    }

    // --- crate-only helpers; do not leak oxia_proto in public API ---
    #[inline]
    pub(crate) const fn take_inner(&mut self) -> oxia_proto::SecondaryIndex {
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

    pub(crate) fn take_proto_indices(
        mut si: Option<Arc<[Self]>>,
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
                    arc.iter().map(Self::clone_inner).collect()
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
    override_version_id: Option<i64>,
    override_modifications_count: Option<i64>,
}

impl PutOptions {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for PutOptions {
    #[inline]
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
        Self {
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
    const fn to_proto(self) -> oxia_proto::KeyComparisonType {
        match self {
            Self::Equal => oxia_proto::KeyComparisonType::Equal,
            Self::Floor => oxia_proto::KeyComparisonType::Floor,
            Self::Ceiling => oxia_proto::KeyComparisonType::Ceiling,
            Self::Lower => oxia_proto::KeyComparisonType::Lower,
            Self::Higher => oxia_proto::KeyComparisonType::Higher,
        }
    }
}

impl From<KeyComparisonType> for i32 {
    #[inline]
    fn from(k: KeyComparisonType) -> Self {
        k as Self
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
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for GetOptions {
    #[inline]
    fn default() -> Self {
        Self::builder().build()
    }
}

impl<S: get_options_builder::State> GetOptionsBuilder<S> {
    #[inline]
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
            Self {
                version_id: v.version_id,
                modifications_count: v.modifications_count,
                created_timestamp: v.created_timestamp,
                modified_timestamp: v.modified_timestamp,
                session_id: v.session_id,
                client_identity: v.client_identity,
            }
        } else {
            Self::default()
        }
    }

    #[inline]
    pub const fn is_ephemeral(&self) -> bool {
        self.session_id.is_some()
    }
}

impl PartialEq for RecordVersion {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for RecordVersion {
    #[inline]
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
        Self {
            value: x.value,
            version: RecordVersion::from_proto(x.version),
            key: x.key,
            secondary_index_key: x.secondary_index_key,
        }
    }
}

impl PartialEq for GetResponse {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        // We want to compare `value` last, hence not using `#[derive(PartialEq)]`.  A manual
        // implementation might be more efficient, but let's start simple so we don't need to worry
        // about divergence.
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for GetResponse {
    #[inline]
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
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for DeleteOptions {
    #[inline]
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
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for ListOptions {
    #[inline]
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
    fn accum(&mut self, mut x: oxia_proto::ListResponse) {
        if !x.keys.is_empty() {
            self.sorted = self.keys.is_empty();
            self.keys.append(&mut x.keys);
        }
    }

    fn merge(&mut self, x: Self) {
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
    #[inline]
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
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for RangeScanOptions {
    #[inline]
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

    fn merge(&mut self, x: Self) {
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
    #[inline]
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
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for DeleteRangeOptions {
    #[inline]
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

#[derive(Clone, Debug)]
pub struct SequenceUpdate {
    pub highest_sequence_key: String,
    pub shard: ShardId,
}

pub type SequenceUpdateStream = BoxStream<'static, Result<SequenceUpdate>>;

#[derive(Clone, Debug)]
pub enum SequenceUpdateRouting {
    PartitionKey(String),
    Shard(i64),
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
    const fn try_to_proto(self) -> Option<oxia_proto::NotificationType> {
        match self {
            Self::KeyCreated => Some(oxia_proto::NotificationType::KeyCreated),
            Self::KeyModified => Some(oxia_proto::NotificationType::KeyModified),
            Self::KeyDeleted => Some(oxia_proto::NotificationType::KeyDeleted),
            Self::KeyRangeDeleted => Some(oxia_proto::NotificationType::KeyRangeDeleted),
            Self::Unknown(_) => None,
        }
    }
}

impl From<i32> for NotificationType {
    fn from(x: i32) -> Self {
        use oxia_proto::NotificationType as p;
        match x {
            x if x == p::KeyCreated as i32 => Self::KeyCreated,
            x if x == p::KeyModified as i32 => Self::KeyModified,
            x if x == p::KeyDeleted as i32 => Self::KeyDeleted,
            x if x == p::KeyRangeDeleted as i32 => Self::KeyRangeDeleted,
            other => Self::Unknown(other),
        }
    }
}

impl Display for NotificationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Self::Unknown(x) = *self {
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
                .map(|entry| {
                    (
                        entry.key.unwrap_or_default(),
                        Notification::from_proto(entry.value.unwrap_or_default()),
                    )
                })
                .collect(),
        }
    }
}

type GrpcClient = oxia_proto::OxiaClientClient<Channel>;

fn validate_dest(dest: &str) -> Result<()> {
    if dest.contains('#') {
        let e = ClientError::DestinationContainsUnexpectedComponent {
            component: "fragment".to_string(),
        };
        return Err(e.into());
    }
    // To properly validate, we need a scheme.
    let url = format!("http://{dest}");
    let uri = url
        .parse::<http::Uri>()
        .map_err(|e| ClientError::InvalidDestinationFormat {
            dest: dest.to_string(),
            reason: e.to_string(),
        })?;

    if let Some(auth) = uri.authority() {
        if auth.as_str().contains('@') {
            let e = ClientError::DestinationContainsUserinfo {};
            return Err(e.into());
        }
    } else {
        let e = ClientError::DestinationMissingAuthority {};
        return Err(e.into());
    }

    if uri.path() != "" && uri.path() != "/" {
        let e = ClientError::DestinationContainsUnexpectedComponent {
            component: format!("path: '{}'", uri.path()),
        };
        return Err(e.into());
    }

    if uri.query().is_some() {
        return Err(Error::Client(Arc::new(
            ClientError::DestinationContainsUnexpectedComponent {
                component: "query string".to_string(),
            },
        )));
    }

    Ok(())
}

/// Maximum size for a single incoming gRPC response message.
/// Protects against OOM from oversized server responses (the Go server
/// allows up to 256 MB per frame).
pub(crate) const MAX_GRPC_DECODING_SIZE: usize = 64 * 1024 * 1024;

pub(crate) async fn create_grpc_client(
    dest: &str,
    channel_pool: &ChannelPool,
) -> Result<GrpcClient> {
    validate_dest(dest)?;
    let url = {
        // TODO: http v https
        const SCHEME: &str = "http://";
        let mut url = String::with_capacity(SCHEME.len() + dest.len());
        url.push_str(SCHEME);
        url.push_str(dest);
        url
    };
    let channel = channel_pool.get(&url).await?;
    Ok(GrpcClient::new(channel).max_decoding_message_size(MAX_GRPC_DECODING_SIZE))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetrySafety {
    Idempotent,
    NonIdempotent,
}

impl RetrySafety {
    #[inline]
    fn should_retry(self, error: &Error) -> bool {
        error.is_retryable() || matches!((self, error), (Self::Idempotent, Error::RequestTimeout))
    }
}

pub(crate) async fn execute_with_retry<F, Fut, R>(
    config: &Arc<config::Config>,
    shard_manager: Option<&Arc<shard::Manager>>,
    retry_safety: RetrySafety,
    op: F,
) -> Result<R>
where
    Fut: Future<Output = Result<R>> + Send,
    R: Send,
    F: Fn() -> Fut + Send + Sync,
{
    let timeout = config.request_timeout();
    util::with_timeout(timeout, async {
        let result = op().await;

        // Happy path: call just worked
        if result.is_ok() {
            return result;
        }

        // Something went wrong.  We'll enter the retry path now if we're confident that
        // the operation can be retried safely.  Unconditional writes, for example,
        // shouldn't be retried upon a timeout since we don't know if the previous write
        // was applied or not.
        if let Err(ref error) = result
            && !retry_safety.should_retry(error)
        {
            return result;
        }

        // Stale shard map: refresh and retry before falling through to transient retry
        if config.retry_on_stale_shard_map()
            && let Some(sm) = shard_manager
            && let Err(ref e) = result
            && e.is_wrong_leader()
        {
            return stale_shard_map_retry(config, sm, &op).await;
        }

        let Some(retry_config) = config.retry() else {
            return result;
        };

        let backoff = FibonacciBuilder::default()
            .with_min_delay(retry_config.initial_delay)
            .with_max_delay(retry_config.max_delay)
            .with_max_times(retry_config.attempts)
            .with_jitter();

        op.retry(backoff)
            .when(|e: &Error| retry_safety.should_retry(e))
            .await
    })
    .await
}

/// Retry loop for stale shard map (wrong leader) errors.
///
/// Each iteration: capture epoch → trigger refresh → wait for update → retry op.
/// Bounded by retry attempts (or 1 if no retry config) and the outer request timeout.
async fn stale_shard_map_retry<F, Fut, R>(
    config: &Arc<config::Config>,
    shard_manager: &Arc<shard::Manager>,
    op: F,
) -> Result<R>
where
    Fut: Future<Output = Result<R>> + Send,
    R: Send,
    F: Fn() -> Fut + Send + Sync,
{
    let max_attempts = config.retry().map_or(1, |r| r.attempts);
    let wait_timeout = config
        .request_timeout()
        .filter(|timeout| *timeout > Duration::ZERO)
        .unwrap_or(Duration::from_secs(30));

    for _ in 0..max_attempts {
        let epoch = shard_manager.epoch();
        shard_manager.trigger_refresh();
        shard_manager.wait_for_update(epoch, wait_timeout).await?;

        let result = op().await;

        #[allow(clippy::match_same_arms)]
        match &result {
            Ok(_) => return result,
            Err(Error::RequestTimeout) => return result,
            Err(e) if e.is_wrong_leader() => {}
            Err(_) => return result,
        }
    }

    // Final attempt after exhausting retry budget
    op().await
}

#[derive(Clone, Debug)]
pub struct Client {
    shard_manager: Option<Arc<shard::Manager>>,
    config: Arc<config::Config>,
    metrics: metrics::Metrics,
}

impl Client {
    #[inline]
    pub fn new(config: Arc<config::Config>) -> Self {
        let metrics = metrics::Metrics::new(config.meter_provider(), config.namespace());
        Self {
            shard_manager: None,
            config,
            metrics,
        }
    }

    #[inline]
    pub const fn config(&self) -> &Arc<config::Config> {
        &self.config
    }

    #[inline]
    pub const fn is_connected(&self) -> bool {
        self.shard_manager.is_some()
    }

    pub async fn connect(&mut self) -> Result<()> {
        if self.is_connected() {
            return Ok(());
        }

        let span = tracing::info_span!(
            target: metrics::SCOPE_NAME,
            "client.connect",
            "db.system" = metrics::DB_SYSTEM,
            "db.name" = self.config.namespace(),
            "error.type" = tracing::field::Empty,
        );
        async {
            let result = shard::Manager::new(&self.config).await.map(|sm| {
                self.shard_manager = Some(Arc::new(sm));
            });
            metrics::record_error_type(&result);
            result
        }
        .instrument(span)
        .await
    }

    #[inline]
    pub fn reconnect(&mut self) -> impl Future<Output = Result<()>> {
        self.shard_manager = None;
        self.connect()
    }

    /// Returns the current shard map epoch.
    ///
    /// Epoch increments with each shard assignment update within this client's lifetime.
    #[inline]
    pub fn epoch(&self) -> Result<ShardMapEpoch> {
        Ok(self.get_shard_manager()?.epoch())
    }

    /// Signals the background task to refresh shard assignments.
    #[inline]
    pub fn trigger_refresh(&self) -> Result<()> {
        self.get_shard_manager()?.trigger_refresh();
        Ok(())
    }

    /// Waits for the shard map to be updated to an epoch greater than `after_epoch`.
    ///
    /// Returns immediately if the current epoch is already greater than `after_epoch`.
    /// On success, returns the new epoch.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let epoch = client.epoch()?;
    /// match client.put("key", "value").await {
    ///     Err(e) if e.is_wrong_leader() => {
    ///         client.trigger_refresh()?;
    ///         client.wait_for_update(epoch, Duration::from_millis(500)).await?;
    ///         // Retry the operation
    ///     }
    ///     result => result
    /// }
    /// ```
    #[inline]
    pub async fn wait_for_update(
        &self,
        after_epoch: ShardMapEpoch,
        timeout: Duration,
    ) -> Result<ShardMapEpoch> {
        self.get_shard_manager()?
            .wait_for_update(after_epoch, timeout)
            .await
    }

    #[inline]
    fn get_shard_manager(&self) -> Result<Arc<shard::Manager>> {
        self.shard_manager
            .clone()
            .ok_or_else(|| Error::Custom("Shard manager not initialized".to_string()))
    }

    fn get_shard(&self, k: &str) -> Result<shard::Client> {
        self.get_shard_manager()?.get_client(k)
    }

    fn get_shard_by_id(&self, id: ShardId) -> Result<shard::Client> {
        self.get_shard_manager()?.get_client_by_shard_id(id)
    }

    async fn execute_on_shard<F, Fut, R>(
        &self,
        shard: shard::Client,
        retry_safety: RetrySafety,
        op: F,
    ) -> Result<R>
    where
        Fut: Future<Output = Result<R>> + Send,
        R: Send,
        F: Fn(shard::Client) -> Fut + Send + Sync,
    {
        execute_with_retry(
            &self.config,
            self.shard_manager.as_ref(),
            retry_safety,
            move || op(shard.clone()),
        )
        .await
    }

    #[inline]
    pub fn get(&self, k: impl Into<String>) -> impl Future<Output = Result<Option<GetResponse>>> {
        self.get_with_options(k, GetOptions::default())
    }

    #[inline]
    pub fn get_with_options(
        &self,
        key: impl Into<String>,
        options: GetOptions,
    ) -> impl Future<Output = Result<Option<GetResponse>>> {
        self.get_internal(Arc::from(key.into()), options)
    }

    #[allow(clippy::large_futures)]
    #[tracing::instrument(
        target = "mauricebarnum_oxia_client",
        name = "db.operation.get",
        skip(self, key, options),
        fields(
            db.system = metrics::DB_SYSTEM,
            db.name = %self.config.namespace(),
            db.operation = metrics::Operation::Get.as_str(),
            db.oxia.shard_id = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn get_internal(
        &self,
        key: Arc<str>,
        options: GetOptions,
    ) -> Result<Option<GetResponse>> {
        let start = metrics::operation_start();
        let result = async {
            let execute_get = |shard, key: Arc<str>, options: GetOptions| {
                self.execute_on_shard(shard, RetrySafety::Idempotent, move |shard| {
                    let key = Arc::clone(&key);
                    let options = options.clone();
                    async move { shard.get(&key, &options).await }
                })
            };

            // Single shard case: exact match with partition key OR exact match without secondary index
            let use_single_shard = options.partition_key.is_some()
                || (options.comparison_type == KeyComparisonType::Equal
                    && options.secondary_index_name.is_none());

            if use_single_shard {
                let selector = options.partition_key.as_deref().unwrap_or(&key);
                let shard = self.get_shard(selector)?;
                tracing::Span::current().record("db.oxia.shard_id", i64::from(shard.id()));
                return execute_get(shard, Arc::clone(&key), options.clone()).await;
            }

            // Multi-shard case: query all shards and select the best response
            let max_parallel = match self.config.max_parallel_requests() {
                0 => usize::MAX,
                n => n,
            };

            futures_util::stream::iter(self.get_shard_manager()?.get_shard_clients())
                .map(|shard| execute_get(shard, Arc::clone(&key), options.clone()))
                .buffer_unordered(max_parallel)
                .try_fold(None, |best, response| async {
                    Ok(match response {
                        Some(candidate) => Some(match best {
                            None => candidate,
                            Some(prev) => util::select_response(
                                Some(prev),
                                candidate,
                                options.comparison_type,
                            ),
                        }),
                        None => best,
                    })
                })
                .await
        }
        .await;
        metrics::record_operation_result_with_size(
            &self.metrics,
            metrics::Operation::Get,
            metrics::get_result_kind,
            metrics::get_response_size,
            start,
            &result,
        );
        result
    }

    /// `batch_get` request multiple keys, minimizing requests to the backend
    ///
    /// On success, the result stream will yield one item per key, even in the event
    /// of a network failure or timeout.  The order of returned keys is not specified.
    ///
    /// Only `KeyComparisonType::Equal` is accepted.
    pub fn batch_get(
        &self,
        req: batch_get::Request,
    ) -> Result<impl futures_core::Stream<Item = batch_get::ResponseItem> + use<>> {
        use futures_util::future::Either::Left;
        use futures_util::future::Either::Right;

        let span = tracing::info_span!(
            target: metrics::SCOPE_NAME,
            "db.operation.batch_get",
            "db.system" = metrics::DB_SYSTEM,
            "db.name" = self.config.namespace(),
            "db.operation" = metrics::Operation::BatchGet.as_str(),
            "error.type" = tracing::field::Empty,
        );
        let (start, final_stream) = {
            let span: &tracing::Span = &span;
            span.in_scope(|| {
                let start = metrics::operation_start();
                let shard_manager = match self.get_shard_manager() {
                    Ok(manager) => manager,
                    Err(error) => {
                        let result = Err(error);
                        metrics::record_operation_result(
                            &self.metrics,
                            metrics::Operation::BatchGet,
                            metrics::result_kind,
                            start,
                            &result,
                        );
                        return result;
                    }
                };
                let (batch_get_futures, failures) =
                    batch_get::prepare_requests(req, &shard_manager);

                let batch_get_stream = if batch_get_futures.is_empty() {
                    None
                } else {
                    Some(
                        futures_util::stream::iter(batch_get_futures)
                            .buffer_unordered(self.config.max_parallel_requests())
                            .flatten_unordered(None)
                            .boxed(),
                    )
                };

                let failures_stream = if failures.is_empty() {
                    None
                } else {
                    Some(futures_util::stream::iter(failures))
                };

                let final_stream = match (batch_get_stream, failures_stream) {
                    (Some(b), None) => Left(b),
                    (Some(b), Some(f)) => Right(Left(Left(futures_util::stream::select(b, f)))),
                    (None, Some(f)) => Right(Left(Right(f))),
                    (None, None) => Right(Right(futures_util::stream::empty())),
                };
                Ok((start, final_stream))
            })
        }?;
        let final_stream = instrument_stream(final_stream, span);

        Ok(metrics::record_batch_get_stream(
            self.metrics.clone(),
            start,
            final_stream,
        ))
    }

    #[inline]
    pub fn put(
        &self,
        key: impl Into<String>,
        value: impl Into<Bytes>,
    ) -> impl Future<Output = Result<PutResponse>> {
        self.put_with_options(key, value, PutOptions::new())
    }

    #[inline]
    pub fn put_with_options(
        &self,
        key: impl Into<String>,
        value: impl Into<Bytes>,
        options: PutOptions,
    ) -> impl Future<Output = Result<PutResponse>> {
        self.put_internal(Arc::from(key.into()), value.into(), options)
    }

    #[allow(clippy::large_futures)]
    #[tracing::instrument(
        target = "mauricebarnum_oxia_client",
        name = "db.operation.put",
        skip(self, key, value, options),
        fields(
            db.system = metrics::DB_SYSTEM,
            db.name = %self.config.namespace(),
            db.operation = metrics::Operation::Put.as_str(),
            db.oxia.shard_id = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn put_internal(
        &self,
        key: Arc<str>,
        value: Bytes,
        options: PutOptions,
    ) -> Result<PutResponse> {
        let value_size = value.len() as u64;
        let start = metrics::operation_start();
        let result = async {
            let selector = options.partition_key.as_deref().unwrap_or(&key);
            let shard = self.get_shard(selector)?;
            tracing::Span::current().record("db.oxia.shard_id", i64::from(shard.id()));
            // TODO: inspect PutOptions and retry timeouts when the request is provably
            // idempotent, such as a put constrained by an expected version.
            self.execute_on_shard(shard, RetrySafety::NonIdempotent, move |shard| {
                let key = Arc::clone(&key);
                let value = value.clone();
                let options = options.clone();
                async move { shard.put(&key, value, &options).await }
            })
            .await
        }
        .await;
        metrics::record_operation_result_with_size(
            &self.metrics,
            metrics::Operation::Put,
            metrics::result_kind,
            |_| value_size,
            start,
            &result,
        );
        result
    }

    #[inline]
    pub fn delete(&self, key: impl Into<String>) -> impl Future<Output = Result<()>> {
        self.delete_with_options(key, DeleteOptions::default())
    }

    #[inline]
    pub fn delete_with_options(
        &self,
        key: impl Into<String>,
        options: DeleteOptions,
    ) -> impl Future<Output = Result<()>> {
        self.delete_internal(Arc::from(key.into()), options)
    }

    #[allow(clippy::large_futures)]
    #[tracing::instrument(
        target = "mauricebarnum_oxia_client",
        name = "db.operation.delete",
        skip(self, key, options),
        fields(
            db.system = metrics::DB_SYSTEM,
            db.name = %self.config.namespace(),
            db.operation = metrics::Operation::Delete.as_str(),
            db.oxia.shard_id = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn delete_internal(&self, key: Arc<str>, options: DeleteOptions) -> Result<()> {
        let start = metrics::operation_start();
        let result = async {
            let selector = options.partition_key.as_deref().unwrap_or(&key);
            let shard = self.get_shard(selector)?;
            tracing::Span::current().record("db.oxia.shard_id", i64::from(shard.id()));
            // TODO: inspect DeleteOptions and retry timeouts when the request is provably
            // idempotent, such as a delete constrained by an expected version.
            self.execute_on_shard(shard, RetrySafety::NonIdempotent, move |shard| {
                let key = Arc::clone(&key);
                let options = options.clone();
                async move { shard.delete(&key, &options).await }
            })
            .await
        }
        .await;
        metrics::record_operation_result(
            &self.metrics,
            metrics::Operation::Delete,
            metrics::result_kind,
            start,
            &result,
        );
        result
    }

    #[inline]
    pub fn delete_range(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
    ) -> impl Future<Output = Result<()>> {
        self.delete_range_with_options(
            start_inclusive,
            end_exclusive,
            DeleteRangeOptions::default(),
        )
    }

    #[inline]
    pub fn delete_range_with_options(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
        options: DeleteRangeOptions,
    ) -> impl Future<Output = Result<()>> {
        self.delete_range_internal(
            Arc::from(start_inclusive.into()),
            Arc::from(end_exclusive.into()),
            options,
        )
    }

    #[allow(clippy::large_futures)]
    #[tracing::instrument(
        target = "mauricebarnum_oxia_client",
        name = "db.operation.delete_range",
        skip(self, start_inclusive, end_exclusive, options),
        fields(
            db.system = metrics::DB_SYSTEM,
            db.name = %self.config.namespace(),
            db.operation = metrics::Operation::DeleteRange.as_str(),
            db.oxia.shard_id = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn delete_range_internal(
        &self,
        start_inclusive: Arc<str>,
        end_exclusive: Arc<str>,
        options: DeleteRangeOptions,
    ) -> Result<()> {
        let start = metrics::operation_start();
        let result = async {
            // TODO: inspect DeleteRangeOptions and retry timeouts when the request is
            // provably idempotent.
            let do_delete_range = |shard,
                                   start_inclusive: Arc<str>,
                                   end_exclusive: Arc<str>,
                                   options: DeleteRangeOptions| {
                self.execute_on_shard(shard, RetrySafety::NonIdempotent, move |shard| {
                    let start = Arc::clone(&start_inclusive);
                    let end = Arc::clone(&end_exclusive);
                    let options = options.clone();
                    async move { shard.delete_range(&start, &end, &options).await }
                })
            };

            if let Some(shard) = {
                if let Some(pk) = options.partition_key.as_deref() {
                    Some(self.get_shard(pk)?)
                } else if let Some(id) = options.shard {
                    Some(self.get_shard_by_id(id.into())?)
                } else {
                    None
                }
            } {
                tracing::Span::current().record("db.oxia.shard_id", i64::from(shard.id()));
                return do_delete_range(
                    shard,
                    Arc::clone(&start_inclusive),
                    Arc::clone(&end_exclusive),
                    options.clone(),
                )
                .await;
            }

            let n = match self.config.max_parallel_requests() {
                0 => usize::MAX,
                n => n,
            };

            let mut result_stream =
                futures_util::stream::iter(self.get_shard_manager()?.get_shard_clients())
                    .map(|shard| {
                        do_delete_range(
                            shard,
                            Arc::clone(&start_inclusive),
                            Arc::clone(&end_exclusive),
                            options.clone(),
                        )
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
        .await;
        metrics::record_operation_result(
            &self.metrics,
            metrics::Operation::DeleteRange,
            metrics::result_kind,
            start,
            &result,
        );
        result
    }

    #[inline]
    pub fn list(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
    ) -> impl Future<Output = Result<ListResponse>> {
        self.list_with_options(start_inclusive, end_exclusive, ListOptions::default())
    }

    #[inline]
    pub fn list_with_options(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
        options: ListOptions,
    ) -> impl Future<Output = Result<ListResponse>> {
        self.list_internal(
            Arc::from(start_inclusive.into()),
            Arc::from(end_exclusive.into()),
            options,
        )
    }

    #[allow(clippy::large_futures)]
    #[tracing::instrument(
        target = "mauricebarnum_oxia_client",
        name = "db.operation.list",
        skip(self, start_inclusive, end_exclusive, options),
        fields(
            db.system = metrics::DB_SYSTEM,
            db.name = %self.config.namespace(),
            db.operation = metrics::Operation::List.as_str(),
            db.oxia.shard_id = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn list_internal(
        &self,
        start_inclusive: Arc<str>,
        end_exclusive: Arc<str>,
        options: ListOptions,
    ) -> Result<ListResponse> {
        let start = metrics::operation_start();
        let result = async {
            let do_list = |shard,
                           start_inclusive: Arc<str>,
                           end_exclusive: Arc<str>,
                           options: ListOptions| {
                self.execute_on_shard(shard, RetrySafety::Idempotent, move |shard| {
                    let start = Arc::clone(&start_inclusive);
                    let end = Arc::clone(&end_exclusive);
                    let options = options.clone();
                    async move { shard.list(&start, &end, &options).await }
                })
            };

            if let Some(pk) = options.partition_key.as_deref() {
                let shard = self.get_shard(pk)?;
                tracing::Span::current().record("db.oxia.shard_id", i64::from(shard.id()));
                return do_list(
                    shard,
                    Arc::clone(&start_inclusive),
                    Arc::clone(&end_exclusive),
                    options.clone(),
                )
                .await;
            }

            let n = match self.config.max_parallel_requests() {
                0 => usize::MAX,
                n => n,
            };

            let mut result_stream =
                futures_util::stream::iter(self.get_shard_manager()?.get_shard_clients())
                    .map(|shard| {
                        do_list(
                            shard,
                            Arc::clone(&start_inclusive),
                            Arc::clone(&end_exclusive),
                            options.clone(),
                        )
                    })
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
        .await;
        metrics::record_operation_result(
            &self.metrics,
            metrics::Operation::List,
            metrics::result_kind,
            start,
            &result,
        );
        result
    }

    #[inline]
    pub fn range_scan(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
    ) -> impl Future<Output = Result<RangeScanResponse>> {
        self.range_scan_with_options(start_inclusive, end_exclusive, RangeScanOptions::default())
    }

    #[inline]
    pub fn range_scan_with_options(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
        options: RangeScanOptions,
    ) -> impl Future<Output = Result<RangeScanResponse>> {
        self.range_scan_internal(
            Arc::from(start_inclusive.into()),
            Arc::from(end_exclusive.into()),
            options,
        )
    }

    #[allow(clippy::large_futures)]
    #[tracing::instrument(
        target = "mauricebarnum_oxia_client",
        name = "db.operation.range_scan",
        skip(self, start_inclusive, end_exclusive, options),
        fields(
            db.system = metrics::DB_SYSTEM,
            db.name = %self.config.namespace(),
            db.operation = metrics::Operation::RangeScan.as_str(),
            db.oxia.shard_id = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn range_scan_internal(
        &self,
        start_inclusive: Arc<str>,
        end_exclusive: Arc<str>,
        options: RangeScanOptions,
    ) -> Result<RangeScanResponse> {
        let start = metrics::operation_start();
        let result = async {
            let do_range_scan = |shard,
                                 start_inclusive: Arc<str>,
                                 end_exclusive: Arc<str>,
                                 options: RangeScanOptions| {
                self.execute_on_shard(shard, RetrySafety::Idempotent, move |shard| {
                    let start = Arc::clone(&start_inclusive);
                    let end = Arc::clone(&end_exclusive);
                    let options = options.clone();
                    async move { shard.range_scan(&start, &end, &options).await }
                })
            };

            if let Some(pk) = options.partition_key.as_deref() {
                let shard = self.get_shard(pk)?;
                tracing::Span::current().record("db.oxia.shard_id", i64::from(shard.id()));
                return do_range_scan(
                    shard,
                    Arc::clone(&start_inclusive),
                    Arc::clone(&end_exclusive),
                    options.clone(),
                )
                .await;
            }

            let n = match self.config.max_parallel_requests() {
                0 => usize::MAX,
                n => n,
            };

            let mut result_stream =
                futures_util::stream::iter(self.get_shard_manager()?.get_shard_clients())
                    .map(|shard| {
                        do_range_scan(
                            shard,
                            Arc::clone(&start_inclusive),
                            Arc::clone(&end_exclusive),
                            options.clone(),
                        )
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
        .await;
        metrics::record_operation_result_with_size(
            &self.metrics,
            metrics::Operation::RangeScan,
            metrics::result_kind,
            metrics::range_scan_response_size,
            start,
            &result,
        );
        result
    }

    /// Create a notifications stream with default options.
    #[inline]
    pub fn create_notifications_stream(&self) -> Result<NotificationsStream> {
        self.create_notifications_stream_with_options(NotificationsOptions::default())
    }

    /// Create a notifications stream with custom options.
    pub fn create_notifications_stream_with_options(
        &self,
        options: NotificationsOptions,
    ) -> Result<NotificationsStream> {
        let shard_manager = self.get_shard_manager()?;
        Ok(NotificationsStream::new(
            Arc::clone(&self.config),
            Arc::clone(&shard_manager),
            options,
        ))
    }

    #[inline]
    pub fn get_sequence_updates_shard(
        &self,
        key: impl Into<String>,
        shard: i64,
    ) -> impl Future<Output = Result<SequenceUpdateStream>> {
        self.get_sequence_updates(key, SequenceUpdateRouting::Shard(shard))
    }

    #[inline]
    pub fn get_sequence_updates_partition(
        &self,
        key: impl Into<String>,
        partition: impl Into<String>,
    ) -> impl Future<Output = Result<SequenceUpdateStream>> {
        self.get_sequence_updates(key, SequenceUpdateRouting::PartitionKey(partition.into()))
    }

    #[inline]
    pub fn get_sequence_updates(
        &self,
        key: impl Into<String>,
        routing: SequenceUpdateRouting,
    ) -> impl Future<Output = Result<SequenceUpdateStream>> {
        self.get_sequence_updates_internal(key.into(), routing)
    }

    #[tracing::instrument(
        target = "mauricebarnum_oxia_client",
        name = "db.operation.get_sequence_updates",
        skip(self, key),
        fields(
            db.system = metrics::DB_SYSTEM,
            db.name = %self.config.namespace(),
            db.operation = metrics::Operation::GetSequenceUpdates.as_str(),
            db.oxia.shard_id = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn get_sequence_updates_internal(
        &self,
        key: String,
        routing: SequenceUpdateRouting,
    ) -> Result<SequenceUpdateStream> {
        let start = metrics::operation_start();
        let shard = match routing {
            SequenceUpdateRouting::PartitionKey(pk) => self.get_shard(&pk),
            SequenceUpdateRouting::Shard(s) => self.get_shard_by_id(s.into()),
        };

        let result = match shard {
            Ok(shard) => {
                tracing::Span::current().record("db.oxia.shard_id", i64::from(shard.id()));
                shard.get_sequence_updates(key).await
            }
            Err(error) => Err(error),
        };
        metrics::record_operation_result(
            &self.metrics,
            metrics::Operation::GetSequenceUpdates,
            metrics::result_kind,
            start,
            &result,
        );
        result.map(|stream| instrument_stream(stream, tracing::Span::current()).boxed())
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use super::*;

    const RETRIES: usize = 3;
    const EXPECTED_RETRYABLE_CALLS: usize = RETRIES + 2;
    const EXPECTED_IDEMPOTENT_TIMEOUT_CALLS: usize = RETRIES + 2;

    #[test_log::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn execute_with_retry_retries_retryable_errors() -> Result<()> {
        let config = config::Config::builder()
            .service_addr("localhost:1234")
            .retry(config::RetryConfig::new(RETRIES, Duration::from_millis(1)))
            .build();
        let count = Arc::new(AtomicUsize::new(0));

        let result = execute_with_retry(&config, None, RetrySafety::NonIdempotent, {
            let count = Arc::clone(&count);
            move || {
                let count = Arc::clone(&count);
                async move {
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Err::<(), _>(Error::Io(
                        io::Error::new(io::ErrorKind::ConnectionReset, "").into(),
                    ))
                }
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(
            EXPECTED_RETRYABLE_CALLS,
            count.load(std::sync::atomic::Ordering::Relaxed)
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn execute_with_retry_retries_request_timeout_for_idempotent_ops() -> Result<()> {
        let config = config::Config::builder()
            .service_addr("localhost:1234")
            .retry(config::RetryConfig::new(RETRIES, Duration::from_millis(1)))
            .build();
        let count = Arc::new(AtomicUsize::new(0));

        let result = execute_with_retry(&config, None, RetrySafety::Idempotent, {
            let count = Arc::clone(&count);
            move || {
                let count = Arc::clone(&count);
                async move {
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Err::<(), _>(Error::RequestTimeout)
                }
            }
        })
        .await;

        assert!(matches!(result, Err(Error::RequestTimeout)));
        assert_eq!(
            EXPECTED_IDEMPOTENT_TIMEOUT_CALLS,
            count.load(std::sync::atomic::Ordering::Relaxed)
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn execute_with_retry_does_not_retry_request_timeout_for_non_idempotent_ops() -> Result<()>
    {
        let config = config::Config::builder()
            .service_addr("localhost:1234")
            .retry(config::RetryConfig::new(3, Duration::from_millis(1)))
            .build();
        let count = Arc::new(AtomicUsize::new(0));

        let result = execute_with_retry(&config, None, RetrySafety::NonIdempotent, {
            let count = Arc::clone(&count);
            move || {
                let count = Arc::clone(&count);
                async move {
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Err::<(), _>(Error::RequestTimeout)
                }
            }
        })
        .await;

        assert!(matches!(result, Err(Error::RequestTimeout)));
        assert_eq!(1, count.load(std::sync::atomic::Ordering::Relaxed));
        Ok(())
    }

    #[test_log::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn execute_with_retry_without_retry_config_runs_once() -> Result<()> {
        let config = config::Config::builder()
            .service_addr("localhost:1234")
            .build();
        let count = Arc::new(AtomicUsize::new(0));

        let result = execute_with_retry(&config, None, RetrySafety::NonIdempotent, {
            let count = Arc::clone(&count);
            move || {
                let count = Arc::clone(&count);
                async move {
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Err::<(), _>(Error::Io(
                        io::Error::new(io::ErrorKind::ConnectionReset, "").into(),
                    ))
                }
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(1, count.load(std::sync::atomic::Ordering::Relaxed));
        Ok(())
    }

    #[test_log::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn execute_with_retry_timeout_bounds_retry_sequence() -> Result<()> {
        let config = config::Config::builder()
            .service_addr("localhost:1234")
            .retry(config::RetryConfig::new(3, Duration::from_millis(50)))
            .request_timeout(Duration::from_millis(10))
            .build();
        let count = Arc::new(AtomicUsize::new(0));

        let result = execute_with_retry(&config, None, RetrySafety::Idempotent, {
            let count = Arc::clone(&count);
            move || {
                let count = Arc::clone(&count);
                async move {
                    count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Err::<(), _>(Error::Io(
                        io::Error::new(io::ErrorKind::ConnectionReset, "").into(),
                    ))
                }
            }
        })
        .await;

        assert!(matches!(result, Err(Error::RequestTimeout)));
        assert_eq!(2, count.load(std::sync::atomic::Ordering::Relaxed));
        Ok(())
    }

    #[test]
    fn test_validate_dest() {
        assert!(validate_dest("localhost:6648").is_ok());
        assert!(validate_dest("127.0.0.1:6648").is_ok());
        assert!(validate_dest("[::1]:6648").is_ok());

        // userinfo
        let r = validate_dest("user:pass@localhost:6648");
        assert!(matches!(
            r,
            Err(Error::Client(e)) if matches!(*e, ClientError::DestinationContainsUserinfo)
        ));

        let r = validate_dest("user@localhost:6648");
        assert!(matches!(
            r,
            Err(Error::Client(e)) if matches!(*e, ClientError::DestinationContainsUserinfo)
        ));

        // path
        let r = validate_dest("localhost:6648/path");
        if let Err(Error::Client(e)) = r {
            if let ClientError::DestinationContainsUnexpectedComponent { component } = e.as_ref() {
                assert!(component.contains("path"));
            } else {
                panic!("Expected DestinationContainsUnexpectedComponent, got {e:?}");
            }
        } else {
            panic!("Expected Error::Client, got {r:?}");
        }

        assert!(validate_dest("localhost:6648/").is_ok());

        // query
        let r = validate_dest("localhost:6648?query=1");
        if let Err(Error::Client(e)) = r {
            if let ClientError::DestinationContainsUnexpectedComponent { component } = e.as_ref() {
                assert!(component.contains("query"));
            } else {
                panic!("Expected DestinationContainsUnexpectedComponent, got {e:?}");
            }
        } else {
            panic!("Expected Error::Client, got {r:?}");
        }

        // Invalid formats
        assert!(validate_dest(" ").is_err());
        let r = validate_dest("localhost:6648#frag");
        if let Err(Error::Client(e)) = r {
            if let ClientError::DestinationContainsUnexpectedComponent { component } = e.as_ref() {
                assert!(component.contains("fragment"));
            } else {
                panic!("Expected DestinationContainsUnexpectedComponent, got {e:?}");
            }
        } else {
            panic!("Expected Error::Client, got {r:?}");
        }
    }
}
