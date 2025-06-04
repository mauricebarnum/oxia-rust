use bytes::Bytes;
use futures::{StreamExt, stream};
use std::fmt::Display;
use std::future::Future;
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

pub mod config;
pub mod errors;
mod shard;
mod util;

use mauricebarnum_oxia_common::proto as oxia_proto;

pub use errors::*;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Default)]
pub struct PutOptions {
    expected_version_id: Option<i64>,
    partition_key: Option<String>,
    sequence_key_deltas: Vec<u64>,
    secondary_indexes: Vec<oxia_proto::SecondaryIndex>,
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

    pub fn partition_key(mut self, value: String) -> Self {
        self.partition_key = Some(value);
        self
    }

    pub fn sequence_key_deltas(mut self, value: Vec<u64>) -> Self {
        self.sequence_key_deltas = value;
        self
    }

    pub fn secondary_indexes(mut self, value: Vec<oxia_proto::SecondaryIndex>) -> Self {
        self.secondary_indexes = value;
        self
    }

    pub fn ephemeral(mut self) -> Self {
        self.ephemeral = true;
        self
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct GetOptions {
    include_value: bool,                // default: true
    comparison_type: KeyComparisonType, // default: Equal
    partition_key: Option<String>,      // default: None
}

impl GetOptions {
    pub fn new() -> Self {
        Self {
            include_value: true,
            comparison_type: KeyComparisonType::Equal,
            partition_key: None,
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

    pub fn partition_key(mut self, value: String) -> Self {
        self.partition_key = Some(value.into());
        self
    }
}

impl Default for GetOptions {
    fn default() -> Self {
        GetOptions::new()
    }
}

#[derive(Debug, Default)]
pub struct RecordVersion {
    pub version_id: i64,
    pub modifications_count: i64,
    pub created_timestamp: u64,
    pub modified_timestamp: u64,
    pub session_id: Option<i64>,
    pub client_identity: Option<String>,
}

impl RecordVersion {
    fn is_ephemeral(&self) -> bool {
        self.session_id.is_some()
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

#[derive(Debug)]
pub struct GetResponse {
    pub value: Option<Bytes>,
    pub version: RecordVersion,
    pub key: Option<String>,
}

impl From<oxia_proto::GetResponse> for GetResponse {
    fn from(x: oxia_proto::GetResponse) -> Self {
        GetResponse {
            value: x.value,
            version: x.version.into(),
            key: x.key,
        }
    }
}

#[derive(Debug, Default)]
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

    pub fn partition_key(mut self, value: String) -> Self {
        self.partition_key = Some(value);
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

    pub fn secondary_index_name(mut self, value: String) -> Self {
        self.secondary_index_name = Some(value);
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

    pub fn partition_key(mut self, value: String) -> Self {
        self.partition_key = Some(value);
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

#[derive(Debug)]
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

    pub fn secondary_index_name(mut self, value: String) -> Self {
        self.secondary_index_name = Some(value);
        self
    }

    pub fn partition_key(mut self, value: String) -> Self {
        self.partition_key = Some(value);
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

#[derive(Debug)]
pub struct RangeScanResponse {
    pub records: Vec<GetResponse>,
    pub sorted: bool,
    pub partial: bool,
}

impl RangeScanResponse {
    fn new() -> Self {
        Self::default()
    }

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

    fn ok(self) -> bool {
        !self.partial
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

    async fn clear(&mut self) {
        self.0.write().await.clear();
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

    async fn reconnect<F, Fut>(&self, dest: &str, callback: F) -> Result<()>
    where
        F: FnOnce(&GrpcClient) -> Fut,
        Fut: Future<Output = ()>,
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
        let grpc = self.grpc_cache.get(&self.config.service_address).await?;
        self.shards = Some(shard::Manager::new(&self.config, &grpc, &self.grpc_cache).await?);
        self.cluster = Some(grpc.clone());
        Ok(())
    }

    // TODO: consider connecting to the shard manager
    fn get_shards(&self) -> Result<&shard::Manager> {
        self.shards.as_ref().ok_or(crate::Error::Custom(
            "Shard manager not initialized".to_string(),
        ))
    }

    async fn get_shard(&self, k: &str) -> Result<shard::Client> {
        let namespace = &self.config.namespace;
        let shard = self.get_shards()?.get_client(namespace, k).await?;
        Ok(shard)
    }

    pub async fn get_with_options(&self, k: String, options: GetOptions) -> Result<GetResponse> {
        if options.comparison_type != KeyComparisonType::Equal {
            return Err(ClientError::UnsupportedKeyComparator(options.comparison_type).into());
        }

        let selector = options.partition_key.as_ref().unwrap_or(&k);
        self.get_shard(selector).await?.get(k, options).await
    }

    pub async fn get(&self, k: String) -> Result<GetResponse> {
        self.get_with_options(k, GetOptions::default()).await
    }

    pub async fn put_with_options(
        &self,
        key: String,
        value: Vec<u8>,
        options: PutOptions,
    ) -> Result<PutResponse> {
        let selector = options.partition_key.as_ref().unwrap_or(&key);
        self.get_shard(selector)
            .await?
            .put(key, value, options)
            .await
    }

    pub async fn put(&self, key: String, value: Vec<u8>) -> Result<PutResponse> {
        self.put_with_options(key, value, PutOptions::default())
            .await
    }

    pub async fn delete_with_options(&self, key: String, options: DeleteOptions) -> Result<()> {
        let selector = options.partition_key.as_ref().unwrap_or(&key);
        self.get_shard(selector).await?.delete(key, options).await
    }

    pub async fn delete(&self, key: String) -> Result<()> {
        self.delete_with_options(key, DeleteOptions::default())
            .await
    }

    pub async fn list_with_options(
        &self,
        start_inclusive: String,
        end_exclusive: String,
        options: ListOptions,
    ) -> Result<ListResponse> {
        if let Some(pk) = options.partition_key.as_ref() {
            return self
                .get_shard(pk)
                .await?
                .list(start_inclusive, end_exclusive, options)
                .await;
        }

        let shards = self.get_shards()?;

        // Here we need some sort of scatter-gather, but also then to assemble results in order.
        // An option would be to allow partial ordering so that list results can be returned as we receive them.
        // For now, keep this kinda dumb
        let mut rsp = ListResponse::default();
        let mut tasks = Vec::with_capacity(shards.num_shards().await);

        shards
            .for_each_shard("default", |shard| -> Option<()> {
                let start_inclusive = start_inclusive.clone();
                let end_exclusive = end_exclusive.clone();
                let options = options.clone();
                let shard = shard.clone();
                tasks
                    .push(async move { shard.list(start_inclusive, end_exclusive, options).await });
                None
            })
            .await;

        let mut result_stream =
            stream::iter(tasks).buffer_unordered(self.config.max_parallel_requests);
        while let Some(r) = result_stream.next().await {
            match r {
                Ok(r) => {
                    rsp.merge(r);
                }
                Err(e) => {
                    if options.partial_ok {
                        rsp.partial = true;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        if options.sort {
            rsp.sort();
        }

        Ok(rsp)
    }

    pub async fn list(
        &self,
        start_inclusive: String,
        end_exclusive: String,
    ) -> Result<ListResponse> {
        self.list_with_options(start_inclusive, end_exclusive, ListOptions::default())
            .await
    }

    pub async fn range_scan_with_options(
        &self,
        start_inclusive: String,
        end_exclusive: String,
        options: RangeScanOptions,
    ) -> Result<RangeScanResponse> {
        if let Some(pk) = options.partition_key.as_ref() {
            return self
                .get_shard(pk)
                .await?
                .range_scan(start_inclusive, end_exclusive, options)
                .await;
        }

        let shards = self.get_shards()?;

        // Here we need some sort of scatter-gather, but also then to assemble results in order.
        // An option would be to allow partial ordering so that list results can be returned as we receive them.
        // For now, keep this kinda dumb
        let mut rsp = RangeScanResponse::default();
        let mut tasks = Vec::with_capacity(shards.num_shards().await);

        shards
            .for_each_shard("default", |shard| -> Option<()> {
                let start_inclusive = start_inclusive.clone();
                let end_exclusive = end_exclusive.clone();
                let options = options.clone();
                let shard = shard.clone();
                tasks.push(async move {
                    shard
                        .range_scan(start_inclusive, end_exclusive, options)
                        .await
                });
                None
            })
            .await;

        let mut result_stream =
            stream::iter(tasks).buffer_unordered(self.config.max_parallel_requests);
        while let Some(r) = result_stream.next().await {
            match r {
                // Ok(r) => match r {
                Ok(r) => {
                    rsp.merge(r);
                }
                Err(e) => {
                    if options.partial_ok {
                        rsp.partial = true;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        if options.sort {
            rsp.sort();
        }

        Ok(rsp)
    }

    pub async fn range_scan(
        &self,
        start_inclusive: String,
        end_exclusive: String,
    ) -> Result<RangeScanResponse> {
        self.range_scan_with_options(start_inclusive, end_exclusive, RangeScanOptions::default())
            .await
    }
}
