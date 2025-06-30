use std::{
    cmp::Ordering,
    collections::{HashMap, hash_map::Entry},
    fmt::Display,
    sync::Arc,
};

use bytes::Bytes;

use futures::{StreamExt, TryStreamExt};

use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

pub mod config;
pub mod errors;
mod shard;
mod util;

use mauricebarnum_oxia_common::proto as oxia_proto;

pub use errors::*;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Default)]
pub struct PutOptions {
    expected_version_id: Option<i64>,
    partition_key: Option<String>,
    sequence_key_deltas: Option<Arc<[u64]>>,
    secondary_indexes: Option<Arc<[oxia_proto::SecondaryIndex]>>,
    ephemeral: bool,
}

impl PutOptions {
    pub fn new() -> Self {
        PutOptions::default()
    }

    pub fn expected_version_id(mut self, value: i64) -> Self {
        self.expected_version_id = Some(value);
        self
    }

    pub fn partition_key(mut self, value: impl Into<String>) -> Self {
        self.partition_key = Some(value.into());
        self
    }

    pub fn sequence_key_deltas(mut self, value: impl Into<Arc<[u64]>>) -> Self {
        self.sequence_key_deltas = Some(value.into());
        self
    }

    pub fn secondary_indexes(
        mut self,
        value: impl Into<Arc<[oxia_proto::SecondaryIndex]>>,
    ) -> Self {
        self.secondary_indexes = Some(value.into());
        self
    }

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

impl From<oxia_proto::PutResponse> for PutResponse {
    fn from(x: oxia_proto::PutResponse) -> Self {
        PutResponse {
            version: x.version.into(),
            key: x.key,
        }
    }
}

#[repr(i32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum KeyComparisonType {
    /// The stored key must be equal to the requested key
    Equal = 0,
    /// Search for a key that is the highest key that is <= to the requested key
    Floor = 1,
    /// Search for a key that is the lowest key that is >= to the requested key
    Ceiling = 2,
    /// Search for a key that is the highest key that is < to the requested key
    Lower = 3,
    /// Search for a key that is the lowest key that is > to the requested key
    Higher = 4,
}

impl Display for KeyComparisonType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                KeyComparisonType::Ceiling => "Ceiling",
                KeyComparisonType::Equal => "Equal",
                KeyComparisonType::Floor => "Floor",
                KeyComparisonType::Higher => "Higher",
                KeyComparisonType::Lower => "Lower",
            }
        )
    }
}

impl From<oxia_proto::KeyComparisonType> for KeyComparisonType {
    fn from(value: oxia_proto::KeyComparisonType) -> Self {
        type X = oxia_proto::KeyComparisonType;
        type Y = KeyComparisonType;
        match value {
            X::Equal => Y::Equal,
            X::Floor => Y::Floor,
            X::Ceiling => Y::Ceiling,
            X::Lower => Y::Lower,
            X::Higher => Y::Higher,
        }
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

    pub fn include_value(mut self) -> Self {
        self.include_value = true;
        self
    }

    pub fn exclude_value(mut self) -> Self {
        self.include_value = false;
        self
    }

    pub fn comparison_type(mut self, value: KeyComparisonType) -> Self {
        self.comparison_type = value;
        self
    }

    pub fn partition_key(mut self, value: impl Into<String>) -> Self {
        self.partition_key = Some(value.into());
        self
    }

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

impl From<Option<oxia_proto::Version>> for RecordVersion {
    fn from(value: Option<oxia_proto::Version>) -> Self {
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
}

#[derive(Clone, Debug, Default, Eq)]
pub struct GetResponse {
    pub value: Option<Bytes>,
    pub version: RecordVersion,
    pub key: Option<String>,
    pub secondary_index_key: Option<String>,
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

impl From<oxia_proto::GetResponse> for GetResponse {
    fn from(x: oxia_proto::GetResponse) -> Self {
        GetResponse {
            value: x.value,
            version: x.version.into(),
            key: x.key,
            secondary_index_key: x.secondary_index_key,
        }
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

    pub fn expected_version_id(mut self, value: i64) -> Self {
        self.expected_version_id = Some(value);
        self
    }

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

    pub fn shard(mut self, value: i64) -> Self {
        self.shard = Some(value);
        self
    }

    pub fn secondary_index_name(mut self, value: impl Into<String>) -> Self {
        self.secondary_index_name = Some(value.into());
        self
    }

    pub fn sort(mut self, value: bool) -> Self {
        self.sort = value;
        self
    }

    pub fn partial_ok(mut self, value: bool) -> Self {
        self.partial_ok = value;
        self
    }

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

impl From<oxia_proto::ListResponse> for ListResponse {
    fn from(x: oxia_proto::ListResponse) -> Self {
        Self {
            keys: x.keys.into_iter().map(|x| x.to_string()).collect(),
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

    pub fn shard(mut self, value: i64) -> Self {
        self.shard = Some(value);
        self
    }

    pub fn secondary_index_name(mut self, value: impl Into<String>) -> Self {
        self.secondary_index_name = Some(value.into());
        self
    }

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
            self.records.extend(x.records.into_iter().map(|x| x.into()));
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

impl From<oxia_proto::RangeScanResponse> for RangeScanResponse {
    fn from(x: oxia_proto::RangeScanResponse) -> Self {
        Self {
            records: x.records.into_iter().map(GetResponse::from).collect(),
            sorted: true,
            partial: false,
        }
    }
}

type GrpcClient = oxia_proto::OxiaClientClient<Channel>;

#[derive(Clone, Debug, Default)]
struct GrpcClientCache(Arc<RwLock<HashMap<String, GrpcClient>>>);

impl GrpcClientCache {
    fn new() -> Self {
        GrpcClientCache(Arc::new(RwLock::new(HashMap::new())))
    }

    async fn try_get(&self, dest: &str) -> Option<GrpcClient> {
        // See if we already have the client.  This should be the common case
        let guard = self.0.read().await;
        guard.get(dest).cloned()
    }

    async fn new_client(dest: &str) -> Result<GrpcClient> {
        let endpoint = Endpoint::from_shared(dest.to_string())?;
        let channel = endpoint.connect().await?;
        Ok(GrpcClient::new(channel))
    }

    async fn get(&self, dest: &str) -> Result<GrpcClient> {
        let url = "http://".to_string() + dest;
        if let Some(c) = self.try_get(&url).await {
            return Ok(c);
        }

        let mut guard = self.0.write().await;
        let c = match guard.entry(url.clone()) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => e.insert(Self::new_client(&url).await?).clone(),
        };
        Ok(c)
    }

    #[allow(dead_code)]
    async fn reconnect<F, Fut>(&self, dest: &str, callback: F) -> Result<()>
    where
        F: FnOnce(&GrpcClient) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        // Connect to the destination without blocking access to the cache.  If there are
        // interleaved calls to this method, it's not deterministic which
        let c = Self::new_client(dest).await?;

        // Update the cache for future lookups to get the new client
        let mut guard = self.0.write().await;
        guard.insert(dest.to_string(), c.clone());
        callback(&c).await;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Client {
    grpc_cache: GrpcClientCache,
    cluster: Option<GrpcClient>,
    shards: Option<shard::Manager>,
    config: Arc<config::Config>,
}

impl Client {
    pub fn new(config: Arc<config::Config>) -> Self {
        Self {
            grpc_cache: GrpcClientCache::new(),
            cluster: None,
            shards: None,
            config,
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        let grpc = self.grpc_cache.get(self.config.service_addr()).await?;
        self.shards = Some(shard::Manager::new(&self.config, &grpc, &self.grpc_cache).await?);
        self.cluster = Some(grpc.clone());
        Ok(())
    }

    fn get_shards(&self) -> Result<&shard::Manager> {
        self.shards
            .as_ref()
            .ok_or_else(|| crate::Error::Custom("Shard manager not initialized".to_string()))
    }

    async fn get_shard(&self, k: &str) -> Result<shard::Client> {
        let namespace = self.config.namespace();
        let shard = self.get_shards()?.get_client(namespace, k).await?;
        Ok(shard)
    }

    async fn execute_with_retry<F, Fut, Args, R>(&self, op: F, args: Args) -> Result<R>
    where
        F: Fn(Args) -> Fut + Clone + Send + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send,
        Args: Clone + Send + 'static,
        R: Send,
    {
        let timeout = self.config.request_timeout();
        let do_with_timeout = move || {
            let args = args.clone();
            let op = op.clone();
            async move {
                let fut = async move { op(args).await };
                util::with_timeout(timeout, fut).await
            }
        };
        util::with_retry(self.config.retry(), do_with_timeout).await
    }

    pub async fn get_with_options(
        &self,
        key: impl Into<String>,
        options: GetOptions,
    ) -> Result<Option<GetResponse>> {
        type Args = (shard::Client, String, GetOptions);
        let do_get = |(shard, key, options): Args| async move { shard.get(key, options).await };

        let key = key.into();

        if options.partition_key.is_some()
            || (options.comparison_type == KeyComparisonType::Equal
                && options.secondary_index_name.is_none())
        {
            let selector = options.partition_key.as_deref().unwrap_or(&key);
            let shard = self.get_shard(selector).await?;
            return self.execute_with_retry(do_get, (shard, key, options)).await;
        }

        let n = match self.config.max_parallel_requests() {
            0 => usize::MAX,
            n => n,
        };

        let stream = self
            .get_shards()?
            .shard_stream(self.config.namespace())
            .await
            .map(|shard| {
                let args: Args = (shard, key.clone(), options.clone());
                async move { self.execute_with_retry(do_get, args).await }
            })
            .buffer_unordered(n);

        stream
            .try_fold(None, |acc, r| async {
                Ok(match r {
                    Some(r) => Some(util::select_response(acc, r, options.comparison_type)),
                    None => acc,
                })
            })
            .await
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
        type Args = (shard::Client, String, Bytes, PutOptions);
        let do_put = |(shard, key, value, options): Args| async move {
            shard.put(key, value, options).await
        };
        let key = key.into();
        let selector = options.partition_key.as_deref().unwrap_or(&key);
        let shard = self.get_shard(selector).await?;
        self.execute_with_retry(do_put, (shard, key, value.into(), options))
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
        type Args = (shard::Client, String, DeleteOptions);
        let do_delete =
            |(shard, key, options): Args| async move { shard.delete(key, options).await };

        let key = key.into();
        let selector = options.partition_key.as_deref().unwrap_or(&key);
        let shard = self.get_shard(selector).await?;
        self.execute_with_retry(do_delete, (shard, key, options))
            .await
    }

    pub async fn delete(&self, key: impl Into<String>) -> Result<()> {
        self.delete_with_options(key, DeleteOptions::default())
            .await
    }

    pub async fn list_with_options(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
        options: ListOptions,
    ) -> Result<ListResponse> {
        type Args = (shard::Client, String, String, ListOptions);
        let do_list = |(shard, start, end, options): Args| async move {
            shard.list(start, end, options).await
        };

        let start = start_inclusive.into();
        let end = end_exclusive.into();

        if let Some(pk) = options.partition_key.as_deref() {
            let shard = self.get_shard(pk).await?;
            return self
                .execute_with_retry(do_list, (shard, start, end, options))
                .await;
        }

        let n = match self.config.max_parallel_requests() {
            0 => usize::MAX,
            n => n,
        };

        let mut result_stream = self
            .get_shards()?
            .shard_stream(self.config.namespace())
            .await
            .map(|shard| {
                let args: Args = (shard, start.clone(), end.clone(), options.clone());
                async move { self.execute_with_retry(do_list, args).await }
            })
            .buffer_unordered(n);

        let mut rsp = ListResponse::default();
        while let Some(s) = result_stream.next().await {
            if let Ok(r) = s {
                rsp.merge(r);
            } else if options.partial_ok {
                rsp.partial = true;
            } else {
                return s;
            }
        }

        if options.sort {
            rsp.sort();
        }

        Ok(rsp)
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
        type Args = (shard::Client, String, String, RangeScanOptions);
        let do_range_scan = |(shard, start, end, options): Args| async move {
            shard.range_scan(start, end, options).await
        };

        let start = start_inclusive.into();
        let end = end_exclusive.into();

        if let Some(pk) = options.partition_key.as_deref() {
            let shard = self.get_shard(pk).await?;
            return self
                .execute_with_retry(do_range_scan, (shard, start, end, options))
                .await;
        }

        let n = match self.config.max_parallel_requests() {
            0 => usize::MAX,
            n => n,
        };

        let mut result_stream = self
            .get_shards()?
            .shard_stream(self.config.namespace())
            .await
            .map(|shard| {
                let args: Args = (shard, start.clone(), end.clone(), options.clone());
                async move { self.execute_with_retry(do_range_scan, args).await }
            })
            .buffer_unordered(n);

        let mut rsp = RangeScanResponse::default();
        while let Some(s) = result_stream.next().await {
            if let Ok(r) = s {
                rsp.merge(r);
            } else if options.partial_ok {
                rsp.partial = true;
            } else {
                return s;
            }
        }

        if options.sort {
            rsp.sort();
        }

        Ok(rsp)
    }

    pub async fn range_scan(
        &self,
        start_inclusive: impl Into<String>,
        end_exclusive: impl Into<String>,
    ) -> Result<RangeScanResponse> {
        self.range_scan_with_options(start_inclusive, end_exclusive, RangeScanOptions::default())
            .await
    }
}
