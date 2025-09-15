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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::TryStreamExt;
use tonic::transport::Channel;

use mauricebarnum_oxia_common::proto as oxia_proto;

use crate::notification::NotificationsStream;
use crate::pool::ChannelPool;

pub mod config;
pub mod errors;
mod notification;
mod pool;
mod shard;
mod util;

pub use errors::*;

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

    pub(crate) fn into_proto_vec(
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

#[derive(Clone, Debug, Default)]
pub struct PutOptions {
    expected_version_id: Option<i64>,
    partition_key: Option<String>,
    sequence_key_deltas: Option<Arc<[u64]>>,
    secondary_indexes: Option<Arc<[SecondaryIndex]>>,
    ephemeral: bool,
}

impl PutOptions {
    pub fn new() -> Self {
        PutOptions::default()
    }

    #[must_use]
    pub fn expected_version_id(mut self, value: i64) -> Self {
        self.expected_version_id = Some(value);
        self
    }

    #[must_use]
    pub fn partition_key(mut self, value: impl Into<String>) -> Self {
        self.partition_key = Some(value.into());
        self
    }

    #[must_use]
    pub fn sequence_key_deltas(mut self, value: impl Into<Arc<[u64]>>) -> Self {
        self.sequence_key_deltas = Some(value.into());
        self
    }

    #[must_use]
    pub fn secondary_indexes(mut self, value: impl Into<Arc<[SecondaryIndex]>>) -> Self {
        self.secondary_indexes = Some(value.into());
        self
    }

    #[must_use]
    pub fn ephemeral(mut self) -> Self {
        self.ephemeral = true;
        self
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

#[derive(Clone, Debug)]
pub struct GetOptions {
    include_value: bool,                  // default: true
    comparison_type: KeyComparisonType,   // default: Equal
    partition_key: Option<String>,        // default: None
    secondary_index_name: Option<String>, // default: None
}

impl GetOptions {
    pub fn new() -> Self {
        Self {
            include_value: true,
            comparison_type: KeyComparisonType::Equal,
            partition_key: None,
            secondary_index_name: None,
        }
    }

    #[must_use]
    pub fn include_value(mut self) -> Self {
        self.include_value = true;
        self
    }

    #[must_use]
    pub fn exclude_value(mut self) -> Self {
        self.include_value = false;
        self
    }

    #[must_use]
    pub fn comparison_type(mut self, value: KeyComparisonType) -> Self {
        self.comparison_type = value;
        self
    }

    #[must_use]
    pub fn partition_key(mut self, value: impl Into<String>) -> Self {
        self.partition_key = Some(value.into());
        self
    }

    #[must_use]
    pub fn secondary_index_name(mut self, value: impl Into<String>) -> Self {
        self.secondary_index_name = Some(value.into());
        self
    }
}

impl Default for GetOptions {
    fn default() -> Self {
        GetOptions::new()
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

#[derive(Clone, Debug, Default)]
pub struct DeleteOptions {
    expected_version_id: Option<i64>,
    partition_key: Option<String>, // default: None
}

impl DeleteOptions {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn expected_version_id(mut self, value: i64) -> Self {
        self.expected_version_id = Some(value);
        self
    }

    #[must_use]
    pub fn partition_key(mut self, value: impl Into<String>) -> Self {
        self.partition_key = Some(value.into());
        self
    }
}

#[derive(Clone, Debug)]
pub struct ListOptions {
    shard: Option<i64>,
    secondary_index_name: Option<String>,
    sort: bool,
    partial_ok: bool,
    partition_key: Option<String>,
}

impl ListOptions {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn shard(mut self, value: i64) -> Self {
        self.shard = Some(value);
        self
    }

    #[must_use]
    pub fn secondary_index_name(mut self, value: impl Into<String>) -> Self {
        self.secondary_index_name = Some(value.into());
        self
    }

    #[must_use]
    pub fn sort(mut self, value: bool) -> Self {
        self.sort = value;
        self
    }

    #[must_use]
    pub fn partial_ok(mut self, value: bool) -> Self {
        self.partial_ok = value;
        self
    }

    #[must_use]
    pub fn partition_key(mut self, value: impl Into<String>) -> Self {
        self.partition_key = Some(value.into());
        self
    }
}

impl Default for ListOptions {
    fn default() -> Self {
        Self {
            shard: None,
            secondary_index_name: None,
            sort: true,
            partial_ok: true,
            partition_key: None,
        }
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

#[derive(Clone, Debug)]
pub struct RangeScanOptions {
    shard: Option<i64>,
    secondary_index_name: Option<String>,
    sort: bool,
    partial_ok: bool,
    partition_key: Option<String>,
}

impl RangeScanOptions {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn shard(mut self, value: i64) -> Self {
        self.shard = Some(value);
        self
    }

    #[must_use]
    pub fn sort(mut self, value: bool) -> Self {
        self.sort = value;
        self
    }

    #[must_use]
    pub fn partial_ok(mut self, value: bool) -> Self {
        self.partial_ok = value;
        self
    }

    #[must_use]
    pub fn secondary_index_name(mut self, value: impl Into<String>) -> Self {
        self.secondary_index_name = Some(value.into());
        self
    }

    #[must_use]
    pub fn partition_key(mut self, value: impl Into<String>) -> Self {
        self.partition_key = Some(value.into());
        self
    }
}

impl Default for RangeScanOptions {
    fn default() -> Self {
        Self {
            shard: None,
            secondary_index_name: None,
            sort: true,
            partial_ok: false,
            partition_key: None,
        }
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

#[derive(Clone, Debug, Default)]
pub struct DeleteRangeOptions {
    shard: Option<i64>,
    partition_key: Option<String>,
}

impl DeleteRangeOptions {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn shard(mut self, value: i64) -> Self {
        self.shard = Some(value);
        self
    }

    #[must_use]
    pub fn partition_key(mut self, value: impl Into<String>) -> Self {
        self.partition_key = Some(value.into());
        self
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
    pub shard: i64,
    pub offset: i64,
    pub timestamp: u64,
    pub notifications: HashMap<String, Notification>,
}

impl NotificationBatch {
    fn from_proto(x: oxia_proto::NotificationBatch) -> Self {
        Self {
            shard: x.shard,
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
    let retry_config = config.retry();

    let retry_op = move || op();
    util::with_timeout(
        config.request_timeout(),
        util::with_retry(retry_config, retry_op),
    )
    .await
}

#[derive(Debug)]
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

    pub async fn connect(&mut self) -> Result<()> {
        let shard_manager = shard::Manager::new(&self.config).await?;
        self.shard_manager = Some(Arc::new(shard_manager));
        Ok(())
    }

    #[inline]
    fn get_shard_manager(&self) -> Result<Arc<shard::Manager>> {
        self.shard_manager
            .clone()
            .ok_or_else(|| crate::Error::Custom("Shard manager not initialized".to_string()))
    }

    async fn get_shard(&self, k: &str) -> Result<shard::Client> {
        let shard = self.get_shard_manager()?.get_client(k).await?;
        Ok(shard)
    }

    async fn get_shard_by_id(&self, id: i64) -> Result<shard::Client> {
        let shard = self.get_shard_manager()?.get_client_by_shard_id(id).await?;
        Ok(shard)
    }

    pub async fn get_with_options(
        &self,
        key: impl Into<String>,
        options: GetOptions,
    ) -> Result<Option<GetResponse>> {
        let key = key.into();

        // Define the core operation with retry/timeout wrapper
        let execute_get = |shard: shard::Client, key: String, options: GetOptions| async move {
            execute_with_retry(&self.config, move || {
                let shard = shard.clone();
                let key = key.clone();
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
            let shard = self.get_shard(selector).await?;
            return execute_get(shard, key, options).await;
        }

        // Multi-shard case: query all shards and select best response
        let max_parallel = match self.config.max_parallel_requests() {
            0 => usize::MAX,
            n => n,
        };

        let best_response = self
            .get_shard_manager()?
            .shard_stream()
            .await
            .map(|shard| execute_get(shard, key.clone(), options.clone()))
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

    pub async fn put_with_options(
        &self,
        key: impl Into<String>,
        value: impl Into<Bytes>,
        options: PutOptions,
    ) -> Result<PutResponse> {
        let key = key.into();
        let value = value.into();
        let selector = options.partition_key.as_deref().unwrap_or(&key);
        let shard = self.get_shard(selector).await?;
        execute_with_retry(&self.config, move || {
            let shard = shard.clone();
            let key = key.clone();
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
        self.put_with_options(key, value, PutOptions::default())
            .await
    }

    pub async fn delete_with_options(
        &self,
        key: impl Into<String>,
        options: DeleteOptions,
    ) -> Result<()> {
        let key = key.into();
        let selector = options.partition_key.as_deref().unwrap_or(&key);
        let shard = self.get_shard(selector).await?;
        execute_with_retry(&self.config, move || {
            let shard = shard.clone();
            let key = key.clone();
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
                               start: String,
                               end: String,
                               options: DeleteRangeOptions| async move {
            execute_with_retry(&self.config, move || {
                let shard = shard.clone();
                let start = start.clone();
                let end = end.clone();
                let options = options.clone();
                async move { shard.delete_range(&start, &end, &options).await }
            })
            .await
        };

        let start = start_inclusive.into();
        let end = end_exclusive.into();

        if let Some(shard) = {
            if let Some(pk) = options.partition_key.as_deref() {
                Some(self.get_shard(pk).await?)
            } else if let Some(id) = options.shard {
                Some(self.get_shard_by_id(id).await?)
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

        let mut result_stream = self
            .get_shard_manager()?
            .shard_stream()
            .await
            .map(|shard| do_delete_range(shard, start.clone(), end.clone(), options.clone()))
            .buffer_unordered(n);

        let mut errs = Vec::new();
        while let Some(shard_result) = result_stream.next().await {
            match shard_result {
                Err(Error::ShardError(e)) => errs.push(*e),
                Err(_) => return shard_result,
                Ok(()) => (),
            }
        }

        if !errs.is_empty() {
            return Err(Error::MultipleShardError(errs));
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
        let do_list = |shard: shard::Client, start: String, end: String, options: ListOptions| async move {
            execute_with_retry(&self.config, move || {
                let shard = shard.clone();
                let start = start.clone();
                let end = end.clone();
                let options = options.clone();
                async move { shard.list(&start, &end, &options).await }
            })
            .await
        };

        let start = start_inclusive.into();
        let end = end_exclusive.into();

        if let Some(pk) = options.partition_key.as_deref() {
            let shard = self.get_shard(pk).await?;
            return do_list(shard, start, end, options).await;
        }

        let n = match self.config.max_parallel_requests() {
            0 => usize::MAX,
            n => n,
        };

        let mut result_stream = self
            .get_shard_manager()?
            .shard_stream()
            .await
            .map(|shard| do_list(shard, start.clone(), end.clone(), options.clone()))
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
                             start: String,
                             end: String,
                             options: RangeScanOptions| async move {
            execute_with_retry(&self.config, move || {
                let shard = shard.clone();
                let start = start.clone();
                let end = end.clone();
                let options = options.clone();
                async move { shard.range_scan(&start, &end, &options).await }
            })
            .await
        };

        let start = start_inclusive.into();
        let end = end_exclusive.into();

        if let Some(pk) = options.partition_key.as_deref() {
            let shard = self.get_shard(pk).await?;
            return do_range_scan(shard, start, end, options).await;
        }

        let n = match self.config.max_parallel_requests() {
            0 => usize::MAX,
            n => n,
        };

        let mut result_stream = self
            .get_shard_manager()?
            .shard_stream()
            .await
            .map(|shard| do_range_scan(shard, start.clone(), end.clone(), options.clone()))
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

    pub fn create_notifications_stream(&self) -> Result<notification::NotificationsStream> {
        let shard_manager = self.get_shard_manager()?;
        Ok(NotificationsStream::new(
            self.config.clone(),
            shard_manager.clone(),
        ))
    }
}
