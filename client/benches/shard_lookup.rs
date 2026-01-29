// Copyright 2026 Maurice S. Barnum
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

// Benchmark comparing sync primitives for hot paths
//
// Run with: cargo bench --package mauricebarnum-oxia-client --bench shard_lookup

use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::{ArcSwap, ArcSwapOption};
use tokio::sync::{Mutex, RwLock};

// Simulate the searchable::Shards structure
#[derive(Clone, Default)]
struct MockShards {
    // Simulates ShardIdMap lookup + hash computation
    data: Vec<i64>,
}

impl MockShards {
    fn new(num_shards: usize) -> Self {
        Self {
            data: (0..num_shards as i64).collect(),
        }
    }

    fn find_by_key(&self, key: &str) -> Option<i64> {
        // Simulate hash + lookup (similar to real code path)
        let hash = key.len() % self.data.len();
        self.data.get(hash).copied()
    }
}

// Current implementation pattern: Mutex
struct MutexManager {
    shards: Arc<Mutex<MockShards>>,
}

impl MutexManager {
    fn new(shards: MockShards) -> Self {
        Self {
            shards: Arc::new(Mutex::new(shards)),
        }
    }

    async fn get_client(&self, key: &str) -> Option<i64> {
        self.shards.lock().await.find_by_key(key)
    }
}

// Proposed implementation pattern: ArcSwap
struct ArcSwapManager {
    shards: ArcSwap<MockShards>,
}

impl ArcSwapManager {
    fn new(shards: MockShards) -> Self {
        Self {
            shards: ArcSwap::new(Arc::new(shards)),
        }
    }

    fn get_client(&self, key: &str) -> Option<i64> {
        self.shards.load().find_by_key(key)
    }
}

const ITERATIONS: u64 = 100_000;
const CONCURRENT_TASKS: usize = 16;

fn format_duration(d: Duration, iterations: u64) -> String {
    let nanos_per_op = d.as_nanos() as f64 / iterations as f64;
    if nanos_per_op < 1000.0 {
        format!("{:.1} ns/op", nanos_per_op)
    } else if nanos_per_op < 1_000_000.0 {
        format!("{:.2} µs/op", nanos_per_op / 1000.0)
    } else {
        format!("{:.2} ms/op", nanos_per_op / 1_000_000.0)
    }
}

async fn bench_mutex_single(manager: &MutexManager, iterations: u64) -> Duration {
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("key-{}", i);
        std::hint::black_box(manager.get_client(&key).await);
    }
    start.elapsed()
}

async fn bench_arcswap_single(manager: &ArcSwapManager, iterations: u64) -> Duration {
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("key-{}", i);
        std::hint::black_box(manager.get_client(&key));
    }
    start.elapsed()
}

async fn bench_mutex_concurrent(
    manager: Arc<MutexManager>,
    tasks: usize,
    iterations: u64,
) -> Duration {
    let per_task = iterations / tasks as u64;
    let start = Instant::now();

    let handles: Vec<_> = (0..tasks)
        .map(|t| {
            let m = manager.clone();
            tokio::spawn(async move {
                for i in 0..per_task {
                    let key = format!("key-{}-{}", t, i);
                    std::hint::black_box(m.get_client(&key).await);
                }
            })
        })
        .collect();

    for h in handles {
        h.await.unwrap();
    }
    start.elapsed()
}

async fn bench_arcswap_concurrent(
    manager: Arc<ArcSwapManager>,
    tasks: usize,
    iterations: u64,
) -> Duration {
    let per_task = iterations / tasks as u64;
    let start = Instant::now();

    let handles: Vec<_> = (0..tasks)
        .map(|t| {
            let m = manager.clone();
            tokio::spawn(async move {
                for i in 0..per_task {
                    let key = format!("key-{}-{}", t, i);
                    std::hint::black_box(m.get_client(&key));
                }
            })
        })
        .collect();

    for h in handles {
        h.await.unwrap();
    }
    start.elapsed()
}

#[tokio::main]
async fn main() {
    let shards = MockShards::new(8); // Typical shard count

    println!("Shard Lookup Benchmark: Mutex vs ArcSwap");
    println!("=========================================");
    println!("Iterations: {}", ITERATIONS);
    println!();

    // Single-threaded benchmarks
    println!("Single-threaded (no contention):");
    println!("---------------------------------");

    let mutex_mgr = MutexManager::new(shards.clone());
    let arcswap_mgr = ArcSwapManager::new(shards.clone());

    // Warmup
    for _ in 0..1000 {
        std::hint::black_box(mutex_mgr.get_client("warmup").await);
        std::hint::black_box(arcswap_mgr.get_client("warmup"));
    }

    let mutex_time = bench_mutex_single(&mutex_mgr, ITERATIONS).await;
    let arcswap_time = bench_arcswap_single(&arcswap_mgr, ITERATIONS).await;

    println!(
        "  Mutex:   {} (total: {:?})",
        format_duration(mutex_time, ITERATIONS),
        mutex_time
    );
    println!(
        "  ArcSwap: {} (total: {:?})",
        format_duration(arcswap_time, ITERATIONS),
        arcswap_time
    );
    println!(
        "  Speedup: {:.2}x",
        mutex_time.as_nanos() as f64 / arcswap_time.as_nanos() as f64
    );
    println!();

    // Concurrent benchmarks
    println!(
        "Concurrent ({} tasks, {} total ops):",
        CONCURRENT_TASKS, ITERATIONS
    );
    println!("----------------------------------------------");

    let mutex_mgr = Arc::new(MutexManager::new(shards.clone()));
    let arcswap_mgr = Arc::new(ArcSwapManager::new(shards));

    let mutex_time = bench_mutex_concurrent(mutex_mgr, CONCURRENT_TASKS, ITERATIONS).await;
    let arcswap_time = bench_arcswap_concurrent(arcswap_mgr, CONCURRENT_TASKS, ITERATIONS).await;

    println!(
        "  Mutex:   {} (total: {:?})",
        format_duration(mutex_time, ITERATIONS),
        mutex_time
    );
    println!(
        "  ArcSwap: {} (total: {:?})",
        format_duration(arcswap_time, ITERATIONS),
        arcswap_time
    );
    println!(
        "  Speedup: {:.2}x",
        mutex_time.as_nanos() as f64 / arcswap_time.as_nanos() as f64
    );

    println!();
    println!();
    bench_dynamic_leader_lookup().await;
    println!();
    println!();
    bench_grpc_client_cache().await;
}

// =============================================================================
// Benchmark 2: gRPC Client Cache (RwLock vs ArcSwapOption)
// =============================================================================

// Simulates a cached gRPC client (like tonic Channel)
#[derive(Clone)]
struct MockGrpcClient {
    #[allow(dead_code)]
    id: u64,
}

// Current implementation: RwLock with double-checked locking
struct RwLockCache {
    client: RwLock<Option<MockGrpcClient>>,
}

impl RwLockCache {
    fn new() -> Self {
        Self {
            client: RwLock::new(Some(MockGrpcClient { id: 1 })),
        }
    }

    async fn get_client(&self) -> MockGrpcClient {
        // Fast path: read lock
        {
            let guard = self.client.read().await;
            if let Some(ref client) = *guard {
                return client.clone();
            }
        }
        // Slow path would acquire write lock - but for cache hit benchmark we skip this
        unreachable!("cache should be populated")
    }
}

// Proposed implementation: ArcSwapOption
struct ArcSwapCache {
    client: ArcSwapOption<MockGrpcClient>,
}

impl ArcSwapCache {
    fn new() -> Self {
        Self {
            client: ArcSwapOption::new(Some(Arc::new(MockGrpcClient { id: 1 }))),
        }
    }

    fn get_client(&self) -> MockGrpcClient {
        // Lock-free load
        self.client
            .load()
            .as_ref()
            .map(|arc| (**arc).clone())
            .unwrap()
    }
}

// =============================================================================
// Benchmark 3: Dynamic Leader Lookup (for wrong-leader resilience)
// =============================================================================
// Compares four approaches:
//   1. Cached leader + format! per RPC (old baseline)
//   2. Dynamic ArcSwap lookup + format! per RPC (old dynamic)
//   3. Dynamic ArcSwap lookup + String::clone (heap alloc, no format)
//   4. Dynamic ArcSwap lookup with pre-formatted Arc<str> URLs (current impl)

struct CachedLeaderClient {
    leader: String,
}

impl CachedLeaderClient {
    fn new(leader: &str) -> Self {
        Self {
            leader: leader.to_string(),
        }
    }

    fn get_url(&self) -> String {
        format!("http://{}", self.leader)
    }
}

/// Old approach: store raw addresses, format URL on every lookup.
struct DynamicFormatClient {
    shard_id: i64,
    leaders: Arc<ArcSwap<std::collections::HashMap<i64, String>>>,
}

impl DynamicFormatClient {
    fn new(shard_id: i64, leaders: Arc<ArcSwap<std::collections::HashMap<i64, String>>>) -> Self {
        Self { shard_id, leaders }
    }

    fn get_url(&self) -> Option<String> {
        let guard = self.leaders.load();
        guard.get(&self.shard_id).map(|l| format!("http://{}", l))
    }
}

/// Hypothetical approach: store pre-formatted URLs as String, clone on each lookup.
/// Isolates allocation cost (String::clone) from refcount cost (Arc::clone).
struct DynamicStringCloneClient {
    shard_id: i64,
    leaders: Arc<ArcSwap<std::collections::HashMap<i64, String>>>,
}

impl DynamicStringCloneClient {
    fn new(shard_id: i64, leaders: Arc<ArcSwap<std::collections::HashMap<i64, String>>>) -> Self {
        Self { shard_id, leaders }
    }

    fn get_url(&self) -> Option<String> {
        let guard = self.leaders.load();
        guard.get(&self.shard_id).cloned() // String::clone — heap alloc, no format
    }
}

/// Current approach: store pre-formatted URLs as Arc<str>, no per-RPC allocation.
struct DynamicPreformattedClient {
    shard_id: i64,
    leaders: Arc<ArcSwap<std::collections::HashMap<i64, Arc<str>>>>,
}

impl DynamicPreformattedClient {
    fn new(shard_id: i64, leaders: Arc<ArcSwap<std::collections::HashMap<i64, Arc<str>>>>) -> Self {
        Self { shard_id, leaders }
    }

    fn get_url(&self) -> Option<Arc<str>> {
        let guard = self.leaders.load();
        guard.get(&self.shard_id).cloned()
    }
}

async fn bench_dynamic_leader_lookup() {
    println!("Dynamic Leader Lookup Benchmark");
    println!("================================");
    println!("Compares cached leader vs dynamic lookup (format vs String clone vs Arc<str>)");
    println!("Iterations: {}", ITERATIONS);
    println!();

    // Setup - old style (raw addresses)
    let mut raw_leaders = std::collections::HashMap::new();
    raw_leaders.insert(0i64, "localhost:6648".to_string());
    raw_leaders.insert(1i64, "localhost:6649".to_string());
    raw_leaders.insert(2i64, "localhost:6650".to_string());
    let raw_shards = Arc::new(ArcSwap::from_pointee(raw_leaders));

    // Setup - string clone style (pre-formatted URLs as String)
    let mut str_leaders = std::collections::HashMap::new();
    str_leaders.insert(0i64, "http://localhost:6648".to_string());
    str_leaders.insert(1i64, "http://localhost:6649".to_string());
    str_leaders.insert(2i64, "http://localhost:6650".to_string());
    let str_shards = Arc::new(ArcSwap::from_pointee(str_leaders));

    // Setup - new style (pre-formatted URLs)
    let mut pre_leaders: std::collections::HashMap<i64, Arc<str>> =
        std::collections::HashMap::new();
    pre_leaders.insert(0i64, Arc::from("http://localhost:6648"));
    pre_leaders.insert(1i64, Arc::from("http://localhost:6649"));
    pre_leaders.insert(2i64, Arc::from("http://localhost:6650"));
    let pre_shards = Arc::new(ArcSwap::from_pointee(pre_leaders));

    let cached_client = CachedLeaderClient::new("localhost:6649");
    let format_client = DynamicFormatClient::new(1, raw_shards.clone());
    let string_clone_client = DynamicStringCloneClient::new(1, str_shards.clone());
    let preformatted_client = DynamicPreformattedClient::new(1, pre_shards.clone());

    // Warmup
    for _ in 0..1000 {
        std::hint::black_box(cached_client.get_url());
        std::hint::black_box(format_client.get_url());
        std::hint::black_box(string_clone_client.get_url());
        std::hint::black_box(preformatted_client.get_url());
    }

    println!("Single-threaded:");
    println!("---------------------------------");

    // Cached benchmark
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        std::hint::black_box(cached_client.get_url());
    }
    let cached_time = start.elapsed();

    // Dynamic + format benchmark (old)
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        std::hint::black_box(format_client.get_url());
    }
    let format_time = start.elapsed();

    // Dynamic + String clone benchmark
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        std::hint::black_box(string_clone_client.get_url());
    }
    let string_clone_time = start.elapsed();

    // Dynamic + pre-formatted benchmark (current)
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        std::hint::black_box(preformatted_client.get_url());
    }
    let preformatted_time = start.elapsed();

    println!(
        "  Cached + format:            {} (total: {:?})",
        format_duration(cached_time, ITERATIONS),
        cached_time
    );
    println!(
        "  Dynamic + format (old):     {} (total: {:?})",
        format_duration(format_time, ITERATIONS),
        format_time
    );
    println!(
        "  Dynamic + String clone:     {} (total: {:?})",
        format_duration(string_clone_time, ITERATIONS),
        string_clone_time
    );
    println!(
        "  Dynamic + Arc<str>:         {} (total: {:?})",
        format_duration(preformatted_time, ITERATIONS),
        preformatted_time
    );
    let old_overhead_ns =
        (format_time.as_nanos() as f64 - cached_time.as_nanos() as f64) / ITERATIONS as f64;
    let str_clone_overhead_ns =
        (string_clone_time.as_nanos() as f64 - cached_time.as_nanos() as f64) / ITERATIONS as f64;
    let new_overhead_ns =
        (preformatted_time.as_nanos() as f64 - cached_time.as_nanos() as f64) / ITERATIONS as f64;
    println!(
        "  Old overhead (vs cached):        {:.1} ns",
        old_overhead_ns
    );
    println!(
        "  String clone overhead (vs cached): {:.1} ns",
        str_clone_overhead_ns
    );
    println!(
        "  Arc<str> overhead (vs cached):    {:.1} ns",
        new_overhead_ns
    );
    println!(
        "  Pre-formatted speedup vs format: {:.2}x",
        format_time.as_nanos() as f64 / preformatted_time.as_nanos() as f64
    );
    println!();

    // Concurrent benchmark
    println!(
        "Concurrent ({} tasks, {} total ops):",
        CONCURRENT_TASKS, ITERATIONS
    );
    println!("----------------------------------------------");

    let cached_client = Arc::new(CachedLeaderClient::new("localhost:6649"));
    let format_client = Arc::new(DynamicFormatClient::new(1, raw_shards));
    let string_clone_client = Arc::new(DynamicStringCloneClient::new(1, str_shards));
    let preformatted_client = Arc::new(DynamicPreformattedClient::new(1, pre_shards));

    let per_task = ITERATIONS / CONCURRENT_TASKS as u64;

    // Cached concurrent
    let start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_TASKS)
        .map(|_| {
            let c = cached_client.clone();
            tokio::spawn(async move {
                for _ in 0..per_task {
                    std::hint::black_box(c.get_url());
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    let cached_time = start.elapsed();

    // Dynamic + format concurrent (old)
    let start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_TASKS)
        .map(|_| {
            let c = format_client.clone();
            tokio::spawn(async move {
                for _ in 0..per_task {
                    std::hint::black_box(c.get_url());
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    let format_time = start.elapsed();

    // Dynamic + String clone concurrent
    let start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_TASKS)
        .map(|_| {
            let c = string_clone_client.clone();
            tokio::spawn(async move {
                for _ in 0..per_task {
                    std::hint::black_box(c.get_url());
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    let string_clone_time = start.elapsed();

    // Dynamic + pre-formatted concurrent (current)
    let start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_TASKS)
        .map(|_| {
            let c = preformatted_client.clone();
            tokio::spawn(async move {
                for _ in 0..per_task {
                    std::hint::black_box(c.get_url());
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    let preformatted_time = start.elapsed();

    println!(
        "  Cached + format:            {} (total: {:?})",
        format_duration(cached_time, ITERATIONS),
        cached_time
    );
    println!(
        "  Dynamic + format (old):     {} (total: {:?})",
        format_duration(format_time, ITERATIONS),
        format_time
    );
    println!(
        "  Dynamic + String clone:     {} (total: {:?})",
        format_duration(string_clone_time, ITERATIONS),
        string_clone_time
    );
    println!(
        "  Dynamic + Arc<str>:         {} (total: {:?})",
        format_duration(preformatted_time, ITERATIONS),
        preformatted_time
    );
    let old_overhead_ns =
        (format_time.as_nanos() as f64 - cached_time.as_nanos() as f64) / ITERATIONS as f64;
    let str_clone_overhead_ns =
        (string_clone_time.as_nanos() as f64 - cached_time.as_nanos() as f64) / ITERATIONS as f64;
    let new_overhead_ns =
        (preformatted_time.as_nanos() as f64 - cached_time.as_nanos() as f64) / ITERATIONS as f64;
    println!(
        "  Old overhead (vs cached):        {:.1} ns",
        old_overhead_ns
    );
    println!(
        "  String clone overhead (vs cached): {:.1} ns",
        str_clone_overhead_ns
    );
    println!(
        "  Arc<str> overhead (vs cached):    {:.1} ns",
        new_overhead_ns
    );
    println!(
        "  Pre-formatted speedup vs format: {:.2}x",
        format_time.as_nanos() as f64 / preformatted_time.as_nanos() as f64
    );
}

async fn bench_grpc_client_cache() {
    println!("gRPC Client Cache Benchmark: RwLock vs ArcSwapOption");
    println!("=====================================================");
    println!("Iterations: {} (cache hit path only)", ITERATIONS);
    println!();

    // Single-threaded benchmark
    println!("Single-threaded (no contention):");
    println!("---------------------------------");

    let rwlock_cache = RwLockCache::new();
    let arcswap_cache = ArcSwapCache::new();

    // Warmup
    for _ in 0..1000 {
        std::hint::black_box(rwlock_cache.get_client().await);
        std::hint::black_box(arcswap_cache.get_client());
    }

    // RwLock benchmark
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        std::hint::black_box(rwlock_cache.get_client().await);
    }
    let rwlock_time = start.elapsed();

    // ArcSwapOption benchmark
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        std::hint::black_box(arcswap_cache.get_client());
    }
    let arcswap_time = start.elapsed();

    println!(
        "  RwLock:       {} (total: {:?})",
        format_duration(rwlock_time, ITERATIONS),
        rwlock_time
    );
    println!(
        "  ArcSwapOption: {} (total: {:?})",
        format_duration(arcswap_time, ITERATIONS),
        arcswap_time
    );
    println!(
        "  Speedup: {:.2}x",
        rwlock_time.as_nanos() as f64 / arcswap_time.as_nanos() as f64
    );
    println!();

    // Concurrent benchmark
    println!(
        "Concurrent ({} tasks, {} total ops):",
        CONCURRENT_TASKS, ITERATIONS
    );
    println!("----------------------------------------------");

    let rwlock_cache = Arc::new(RwLockCache::new());
    let arcswap_cache = Arc::new(ArcSwapCache::new());

    // RwLock concurrent
    let per_task = ITERATIONS / CONCURRENT_TASKS as u64;
    let start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_TASKS)
        .map(|_| {
            let cache = rwlock_cache.clone();
            tokio::spawn(async move {
                for _ in 0..per_task {
                    std::hint::black_box(cache.get_client().await);
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    let rwlock_time = start.elapsed();

    // ArcSwapOption concurrent
    let start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_TASKS)
        .map(|_| {
            let cache = arcswap_cache.clone();
            tokio::spawn(async move {
                for _ in 0..per_task {
                    std::hint::black_box(cache.get_client());
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    let arcswap_time = start.elapsed();

    println!(
        "  RwLock:       {} (total: {:?})",
        format_duration(rwlock_time, ITERATIONS),
        rwlock_time
    );
    println!(
        "  ArcSwapOption: {} (total: {:?})",
        format_duration(arcswap_time, ITERATIONS),
        arcswap_time
    );
    println!(
        "  Speedup: {:.2}x",
        rwlock_time.as_nanos() as f64 / arcswap_time.as_nanos() as f64
    );
}
