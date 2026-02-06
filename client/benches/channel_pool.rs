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

// Benchmark comparing ChannelPool data structure alternatives.
//
// Measures cache-hit reads, invalidation, cross-client sharing semantics,
// and mixed workloads for four pool implementations:
//   1. RwLock<HashMap<String, Channel>>  (current production code)
//   2. DashMap<String, Channel>          (lock-free concurrent map)
//   3. ArcSwap<HashMap<String, Channel>> (lock-free snapshot reads)
//   4. ArcSwap<Vec<(Arc<str>, Channel)>> (lock-free linear scan, small N)
//
// Each benchmark runs at two scales:
//   - Small:  3 leaders  (typical deployment)
//   - Large: 48 leaders  (scale scenario — thousands of shards, dozens of leaders)
//
// The ChannelPool is keyed by leader URL, not shard ID. Thousands of shards
// funnel through dozens of leaders, so the pool size tracks leader count.
//
// Run with: cargo bench --package mauricebarnum-oxia-client --bench channel_pool
// Include DashMap: cargo bench --package mauricebarnum-oxia-client --bench channel_pool --features bench-dashmap

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use arc_swap::ArcSwap;
#[cfg(feature = "bench-dashmap")]
use dashmap::DashMap;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Mock channel — stands in for tonic::transport::Channel (Arc-based handle)
// ---------------------------------------------------------------------------

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
struct MockChannel {
    id: u64,
}

impl MockChannel {
    fn new() -> Self {
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Leader URL generation
// ---------------------------------------------------------------------------

fn generate_leaders(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| format!("http://node-{i:03}.cluster.local:6648"))
        .collect()
}

// ---------------------------------------------------------------------------
// Pool variant 1: RwLock<HashMap> (current production pattern)
// ---------------------------------------------------------------------------

struct RwLockPool {
    clients: RwLock<HashMap<String, MockChannel>>,
}

impl RwLockPool {
    fn populated(leaders: &[String]) -> Self {
        let mut map = HashMap::new();
        for url in leaders {
            map.insert(url.clone(), MockChannel::new());
        }
        Self {
            clients: RwLock::new(map),
        }
    }

    async fn get(&self, target: &str) -> Option<MockChannel> {
        self.clients.read().await.get(target).cloned()
    }

    async fn remove(&self, target: &str) -> Option<MockChannel> {
        self.clients.write().await.remove(target)
    }

    async fn insert(&self, target: &str, ch: MockChannel) {
        self.clients.write().await.insert(target.to_string(), ch);
    }
}

// ---------------------------------------------------------------------------
// Pool variant 2: DashMap (lock-free concurrent map)
// Gated behind the "bench-dashmap" feature to avoid pulling in dashmap
// when not needed.
// ---------------------------------------------------------------------------

#[cfg(feature = "bench-dashmap")]
struct DashMapPool {
    clients: DashMap<String, MockChannel>,
}

#[cfg(feature = "bench-dashmap")]
impl DashMapPool {
    fn populated(leaders: &[String]) -> Self {
        let map = DashMap::new();
        for url in leaders {
            map.insert(url.clone(), MockChannel::new());
        }
        Self { clients: map }
    }

    fn get(&self, target: &str) -> Option<MockChannel> {
        self.clients.get(target).map(|r| r.value().clone())
    }

    fn remove(&self, target: &str) -> Option<MockChannel> {
        self.clients.remove(target).map(|(_, v)| v)
    }

    fn insert(&self, target: &str, ch: MockChannel) {
        self.clients.insert(target.to_string(), ch);
    }
}

// ---------------------------------------------------------------------------
// Pool variant 3: ArcSwap<HashMap> (lock-free snapshot reads)
// ---------------------------------------------------------------------------

struct ArcSwapHashMapPool {
    clients: ArcSwap<HashMap<String, MockChannel>>,
}

impl ArcSwapHashMapPool {
    fn populated(leaders: &[String]) -> Self {
        let mut map = HashMap::new();
        for url in leaders {
            map.insert(url.clone(), MockChannel::new());
        }
        Self {
            clients: ArcSwap::from_pointee(map),
        }
    }

    fn get(&self, target: &str) -> Option<MockChannel> {
        self.clients.load().get(target).cloned()
    }

    fn remove(&self, target: &str) -> Option<MockChannel> {
        let old = self.clients.load();
        let mut new_map = (**old).clone();
        let removed = new_map.remove(target);
        self.clients.store(Arc::new(new_map));
        removed
    }

    fn insert(&self, target: &str, ch: MockChannel) {
        let old = self.clients.load();
        let mut new_map = (**old).clone();
        new_map.insert(target.to_string(), ch);
        self.clients.store(Arc::new(new_map));
    }
}

// ---------------------------------------------------------------------------
// Pool variant 4: ArcSwap<Vec<(Arc<str>, Channel)>> (lock-free, adaptive)
//
// Below LINEAR_SCAN_THRESHOLD: unsorted, plain linear scan, append on insert.
// At or above: sorted, binary search, sorted insert.
// ---------------------------------------------------------------------------

const LINEAR_SCAN_THRESHOLD: usize = 8;

struct ArcSwapVecPool {
    clients: ArcSwap<Vec<(Arc<str>, MockChannel)>>,
}

/// Search result for the adaptive Vec pool.
enum Found {
    /// Key found at this index.
    Hit(usize),
    /// Key not found. For sorted vecs, the insertion point; for unsorted, ignored.
    Miss(usize),
}

impl ArcSwapVecPool {
    fn populated(leaders: &[String]) -> Self {
        let mut entries: Vec<_> = leaders
            .iter()
            .map(|url| (Arc::<str>::from(url.as_str()), MockChannel::new()))
            .collect();
        if entries.len() >= LINEAR_SCAN_THRESHOLD {
            entries.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        }
        Self {
            clients: ArcSwap::from_pointee(entries),
        }
    }

    #[inline]
    fn find(entries: &[(Arc<str>, MockChannel)], target: &str) -> Found {
        if entries.len() < LINEAR_SCAN_THRESHOLD {
            // Unsorted linear scan
            for (i, (url, _)) in entries.iter().enumerate() {
                if url.as_ref() == target {
                    return Found::Hit(i);
                }
            }
            Found::Miss(entries.len())
        } else {
            // Sorted binary search
            match entries.binary_search_by(|(url, _)| url.as_ref().cmp(target)) {
                Ok(i) => Found::Hit(i),
                Err(i) => Found::Miss(i),
            }
        }
    }

    fn get(&self, target: &str) -> Option<MockChannel> {
        let guard = self.clients.load();
        match Self::find(&guard, target) {
            Found::Hit(i) => Some(guard[i].1.clone()),
            Found::Miss(_) => None,
        }
    }

    fn remove(&self, target: &str) -> Option<MockChannel> {
        let old = self.clients.load();
        let mut new_vec: Vec<_> = (**old).clone();
        match Self::find(&new_vec, target) {
            Found::Hit(pos) => {
                let removed = new_vec.remove(pos).1;
                self.clients.store(Arc::new(new_vec));
                Some(removed)
            }
            Found::Miss(_) => None,
        }
    }

    fn insert(&self, target: &str, ch: MockChannel) {
        let old = self.clients.load();
        let mut new_vec: Vec<_> = (**old).clone();
        match Self::find(&new_vec, target) {
            Found::Hit(pos) => {
                new_vec[pos].1 = ch;
            }
            Found::Miss(pos) => {
                if new_vec.len() < LINEAR_SCAN_THRESHOLD {
                    // Unsorted: append
                    new_vec.push((Arc::from(target), ch));
                } else if new_vec.len() == LINEAR_SCAN_THRESHOLD.saturating_sub(1) {
                    // Crossing threshold: append then sort
                    new_vec.push((Arc::from(target), ch));
                    new_vec.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
                } else {
                    // Sorted: insert at position
                    new_vec.insert(pos, (Arc::from(target), ch));
                }
            }
        }
        self.clients.store(Arc::new(new_vec));
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

const ITERATIONS: u64 = 100_000;
const CONCURRENT_TASKS: usize = 16;

fn format_duration(d: Duration, iterations: u64) -> String {
    let nanos_per_op = d.as_nanos() as f64 / iterations as f64;
    if nanos_per_op < 1000.0 {
        format!("{nanos_per_op:.1} ns/op")
    } else if nanos_per_op < 1_000_000.0 {
        format!("{:.2} µs/op", nanos_per_op / 1000.0)
    } else {
        format!("{:.2} ms/op", nanos_per_op / 1_000_000.0)
    }
}

fn print_result(label: &str, d: Duration, iterations: u64) {
    println!(
        "  {label:<38} {:<16} (total: {d:?})",
        format_duration(d, iterations)
    );
}

const fn vec_strategy(n: usize) -> &'static str {
    if n < LINEAR_SCAN_THRESHOLD {
        "linear"
    } else {
        "bsearch"
    }
}

// ---------------------------------------------------------------------------
// Benchmark: single-threaded cache-hit reads
// ---------------------------------------------------------------------------

async fn bench_read_single(leaders: &[String]) {
    let n = leaders.len();
    let vec_label = format!("ArcSwap<Vec> ({n}, {})", vec_strategy(n));
    println!("Cache-Hit Reads (single-threaded, {n} leaders, {ITERATIONS} iterations)");
    println!("{}", "-".repeat(74));

    // Look up the first leader (simulates a shard client's hot path)
    let target = &leaders[0];

    // RwLock<HashMap>
    let pool = RwLockPool::populated(leaders);
    for _ in 0..1000 {
        std::hint::black_box(pool.get(target).await);
    }
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        std::hint::black_box(pool.get(target).await);
    }
    let rwlock_time = start.elapsed();

    // DashMap
    #[cfg(feature = "bench-dashmap")]
    let dashmap_time = {
        let pool = DashMapPool::populated(leaders);
        for _ in 0..1000 {
            std::hint::black_box(pool.get(target));
        }
        let start = Instant::now();
        for _ in 0..ITERATIONS {
            std::hint::black_box(pool.get(target));
        }
        start.elapsed()
    };

    // ArcSwap<HashMap>
    let pool = ArcSwapHashMapPool::populated(leaders);
    for _ in 0..1000 {
        std::hint::black_box(pool.get(target));
    }
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        std::hint::black_box(pool.get(target));
    }
    let arcswap_hm_time = start.elapsed();

    // ArcSwap<Vec>
    let pool = ArcSwapVecPool::populated(leaders);
    for _ in 0..1000 {
        std::hint::black_box(pool.get(target));
    }
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        std::hint::black_box(pool.get(target));
    }
    let arcswap_vec_time = start.elapsed();

    print_result("RwLock<HashMap> (current)", rwlock_time, ITERATIONS);
    #[cfg(feature = "bench-dashmap")]
    print_result("DashMap", dashmap_time, ITERATIONS);
    print_result("ArcSwap<HashMap>", arcswap_hm_time, ITERATIONS);
    print_result(&vec_label, arcswap_vec_time, ITERATIONS);
    println!();
}

// ---------------------------------------------------------------------------
// Benchmark: concurrent cache-hit reads
// ---------------------------------------------------------------------------

async fn bench_read_concurrent(leaders: &[String]) {
    let n = leaders.len();
    let vec_label = format!("ArcSwap<Vec> ({n}, {})", vec_strategy(n));
    println!(
        "Cache-Hit Reads (concurrent, {n} leaders, {CONCURRENT_TASKS} tasks, {ITERATIONS} total ops)"
    );
    println!("{}", "-".repeat(74));

    let target = &leaders[0];
    let per_task = ITERATIONS / CONCURRENT_TASKS as u64;

    // RwLock<HashMap>
    let pool = Arc::new(RwLockPool::populated(leaders));
    let start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_TASKS)
        .map(|_| {
            let p = Arc::clone(&pool);
            let t = target.clone();
            tokio::spawn(async move {
                for _ in 0..per_task {
                    std::hint::black_box(p.get(&t).await);
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    let rwlock_time = start.elapsed();

    // DashMap
    #[cfg(feature = "bench-dashmap")]
    let dashmap_time = {
        let pool = Arc::new(DashMapPool::populated(leaders));
        let start = Instant::now();
        let handles: Vec<_> = (0..CONCURRENT_TASKS)
            .map(|_| {
                let p = Arc::clone(&pool);
                let t = target.clone();
                tokio::spawn(async move {
                    for _ in 0..per_task {
                        std::hint::black_box(p.get(&t));
                    }
                })
            })
            .collect();
        for h in handles {
            h.await.unwrap();
        }
        start.elapsed()
    };

    // ArcSwap<HashMap>
    let pool = Arc::new(ArcSwapHashMapPool::populated(leaders));
    let start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_TASKS)
        .map(|_| {
            let p = Arc::clone(&pool);
            let t = target.clone();
            tokio::spawn(async move {
                for _ in 0..per_task {
                    std::hint::black_box(p.get(&t));
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    let arcswap_hm_time = start.elapsed();

    // ArcSwap<Vec>
    let pool = Arc::new(ArcSwapVecPool::populated(leaders));
    let start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_TASKS)
        .map(|_| {
            let p = Arc::clone(&pool);
            let t = target.clone();
            tokio::spawn(async move {
                for _ in 0..per_task {
                    std::hint::black_box(p.get(&t));
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    let arcswap_vec_time = start.elapsed();

    print_result("RwLock<HashMap> (current)", rwlock_time, ITERATIONS);
    #[cfg(feature = "bench-dashmap")]
    print_result("DashMap", dashmap_time, ITERATIONS);
    print_result("ArcSwap<HashMap>", arcswap_hm_time, ITERATIONS);
    print_result(&vec_label, arcswap_vec_time, ITERATIONS);
    println!();
}

// ---------------------------------------------------------------------------
// Benchmark: invalidation cost
// ---------------------------------------------------------------------------

async fn bench_invalidation(leaders: &[String]) {
    const INVAL_ITERS: u64 = 10_000;
    let n = leaders.len();
    let vec_clone_label = format!("ArcSwap<Vec> (clone {n}, {})", vec_strategy(n));

    println!("Invalidation (remove + re-insert, {n} leaders, {INVAL_ITERS} iterations)");
    println!("{}", "-".repeat(74));

    let target = &leaders[0];

    // RwLock<HashMap>
    let pool = RwLockPool::populated(leaders);
    let start = Instant::now();
    for _ in 0..INVAL_ITERS {
        std::hint::black_box(pool.remove(target).await);
        pool.insert(target, MockChannel::new()).await;
    }
    let rwlock_time = start.elapsed();

    // DashMap
    #[cfg(feature = "bench-dashmap")]
    let dashmap_time = {
        let pool = DashMapPool::populated(leaders);
        let start = Instant::now();
        for _ in 0..INVAL_ITERS {
            std::hint::black_box(pool.remove(target));
            pool.insert(target, MockChannel::new());
        }
        start.elapsed()
    };

    // ArcSwap<HashMap> — remove clones the entire map (O(N) in leader count)
    let pool = ArcSwapHashMapPool::populated(leaders);
    let start = Instant::now();
    for _ in 0..INVAL_ITERS {
        std::hint::black_box(pool.remove(target));
        pool.insert(target, MockChannel::new());
    }
    let arcswap_hm_time = start.elapsed();

    // ArcSwap<Vec> — remove clones the entire vec (O(N) in leader count)
    let pool = ArcSwapVecPool::populated(leaders);
    let start = Instant::now();
    for _ in 0..INVAL_ITERS {
        std::hint::black_box(pool.remove(target));
        pool.insert(target, MockChannel::new());
    }
    let arcswap_vec_time = start.elapsed();

    print_result("RwLock<HashMap> (current)", rwlock_time, INVAL_ITERS);
    #[cfg(feature = "bench-dashmap")]
    print_result("DashMap", dashmap_time, INVAL_ITERS);
    print_result(
        &format!("ArcSwap<HashMap> (clone {n})"),
        arcswap_hm_time,
        INVAL_ITERS,
    );
    print_result(&vec_clone_label, arcswap_vec_time, INVAL_ITERS);
    println!();
}

// ---------------------------------------------------------------------------
// Sharing semantics: invalidation from one client affects all sharing clients
// ---------------------------------------------------------------------------

async fn bench_sharing_semantics(leaders: &[String]) {
    let n = leaders.len();
    println!("Sharing Semantics ({n} leaders): invalidation affects all clients sharing a channel");
    println!("{}", "-".repeat(74));

    // Two shard clients both resolve to the same leader URL (leaders[0]).
    // A third shard routes to leaders[1]. After invalidation of leaders[0],
    // both shards 0 and 1 miss, but the shard on leaders[1] is unaffected.
    let shared = &leaders[0];
    let other = &leaders[1];

    // --- RwLock<HashMap> ---
    {
        let pool = RwLockPool::populated(leaders);

        let ch0 = pool.get(shared).await;
        let ch1 = pool.get(shared).await;
        assert!(ch0.is_some(), "shard 0 should find channel");
        assert!(ch1.is_some(), "shard 1 should find channel");
        assert_eq!(ch0.unwrap().id, ch1.unwrap().id, "should be same channel");

        pool.remove(shared).await;

        assert!(
            pool.get(shared).await.is_none(),
            "shard 0 should miss after invalidation"
        );
        assert!(
            pool.get(shared).await.is_none(),
            "shard 1 should miss after invalidation"
        );
        assert!(pool.get(other).await.is_some(), "other leader unaffected");

        println!("  RwLock<HashMap>:  PASS — invalidation affects all sharing clients");
    }

    // --- DashMap ---
    #[cfg(feature = "bench-dashmap")]
    {
        let pool = DashMapPool::populated(leaders);
        let ch0 = pool.get(shared);
        let ch1 = pool.get(shared);
        assert!(ch0.is_some() && ch1.is_some());
        assert_eq!(ch0.unwrap().id, ch1.unwrap().id);
        pool.remove(shared);
        assert!(pool.get(shared).is_none(), "shard 0 miss");
        assert!(pool.get(shared).is_none(), "shard 1 miss");
        assert!(pool.get(other).is_some(), "other unaffected");
        println!("  DashMap:          PASS — invalidation affects all sharing clients");
    }

    // --- ArcSwap<HashMap> ---
    {
        let pool = ArcSwapHashMapPool::populated(leaders);
        let ch0 = pool.get(shared);
        let ch1 = pool.get(shared);
        assert!(ch0.is_some() && ch1.is_some());
        assert_eq!(ch0.unwrap().id, ch1.unwrap().id);
        pool.remove(shared);
        assert!(pool.get(shared).is_none(), "shard 0 miss");
        assert!(pool.get(shared).is_none(), "shard 1 miss");
        assert!(pool.get(other).is_some(), "other unaffected");
        println!("  ArcSwap<HashMap>: PASS — invalidation affects all sharing clients");
    }

    // --- ArcSwap<Vec> ---
    {
        let pool = ArcSwapVecPool::populated(leaders);
        let ch0 = pool.get(shared);
        let ch1 = pool.get(shared);
        assert!(ch0.is_some() && ch1.is_some());
        assert_eq!(ch0.unwrap().id, ch1.unwrap().id);
        pool.remove(shared);
        assert!(pool.get(shared).is_none(), "shard 0 miss");
        assert!(pool.get(shared).is_none(), "shard 1 miss");
        assert!(pool.get(other).is_some(), "other unaffected");
        println!("  ArcSwap<Vec>:     PASS — invalidation affects all sharing clients");
    }

    println!();
    println!("  Note: All variants correctly propagate invalidation to every client");
    println!("  that shares the same leader URL. This is the required behavior —");
    println!("  a connection error to a leader is not shard-specific.");
    println!();
}

// ---------------------------------------------------------------------------
// Mixed workload: reads + occasional invalidation+reconnect, concurrent
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
async fn bench_mixed_workload(leaders: &[String], inval_every: u64) {
    const MIXED_ITERS: u64 = 100_000;
    let read_pct = 100.0 - (100.0 / inval_every as f64);
    let n = leaders.len();
    let vec_label = format!("ArcSwap<Vec> ({n}, {})", vec_strategy(n));

    println!(
        "Mixed Workload ({read_pct:.1}% read, {n} leaders, {CONCURRENT_TASKS} tasks, {MIXED_ITERS} total ops)"
    );
    println!("{}", "-".repeat(74));

    let target = &leaders[0];
    let per_task = MIXED_ITERS / CONCURRENT_TASKS as u64;

    // RwLock<HashMap>
    let pool = Arc::new(RwLockPool::populated(leaders));
    let start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_TASKS)
        .map(|_| {
            let p = Arc::clone(&pool);
            let t = target.clone();
            tokio::spawn(async move {
                for i in 0..per_task {
                    if i % inval_every == (inval_every - 1) {
                        p.remove(&t).await;
                        p.insert(&t, MockChannel::new()).await;
                    } else {
                        std::hint::black_box(p.get(&t).await);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    let rwlock_time = start.elapsed();

    // DashMap
    #[cfg(feature = "bench-dashmap")]
    let dashmap_time = {
        let pool = Arc::new(DashMapPool::populated(leaders));
        let start = Instant::now();
        let handles: Vec<_> = (0..CONCURRENT_TASKS)
            .map(|_| {
                let p = Arc::clone(&pool);
                let t = target.clone();
                tokio::spawn(async move {
                    for i in 0..per_task {
                        if i % inval_every == (inval_every - 1) {
                            p.remove(&t);
                            p.insert(&t, MockChannel::new());
                        } else {
                            std::hint::black_box(p.get(&t));
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.await.unwrap();
        }
        start.elapsed()
    };

    // ArcSwap<HashMap>
    let pool = Arc::new(ArcSwapHashMapPool::populated(leaders));
    let start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_TASKS)
        .map(|_| {
            let p = Arc::clone(&pool);
            let t = target.clone();
            tokio::spawn(async move {
                for i in 0..per_task {
                    if i % inval_every == (inval_every - 1) {
                        p.remove(&t);
                        p.insert(&t, MockChannel::new());
                    } else {
                        std::hint::black_box(p.get(&t));
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    let arcswap_hm_time = start.elapsed();

    let pool = Arc::new(ArcSwapVecPool::populated(leaders));
    let start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_TASKS)
        .map(|_| {
            let p = Arc::clone(&pool);
            let t = target.clone();
            tokio::spawn(async move {
                for i in 0..per_task {
                    if i % inval_every == (inval_every - 1) {
                        p.remove(&t);
                        p.insert(&t, MockChannel::new());
                    } else {
                        std::hint::black_box(p.get(&t));
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    let arcswap_vec_time = start.elapsed();

    print_result("RwLock<HashMap> (current)", rwlock_time, MIXED_ITERS);
    #[cfg(feature = "bench-dashmap")]
    print_result("DashMap", dashmap_time, MIXED_ITERS);
    print_result("ArcSwap<HashMap>", arcswap_hm_time, MIXED_ITERS);
    print_result(&vec_label, arcswap_vec_time, MIXED_ITERS);
    println!();
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    // Small scale: typical deployment (3 leaders)
    let small_leaders = generate_leaders(3);
    // Large scale: thousands of shards funneling through dozens of leaders
    let large_leaders = generate_leaders(48);

    println!();
    println!("ChannelPool Data Structure Benchmark");
    println!("=====================================");
    println!("Simulates the pool lookup in shard::Client::get_grpc_client().");
    println!("The pool is keyed by leader URL — thousands of shards share dozens of leaders.");
    println!();

    // --- Small scale ---
    println!(
        "==== SMALL SCALE: {} leaders (typical) ====",
        small_leaders.len()
    );
    println!();
    bench_read_single(&small_leaders).await;
    bench_read_concurrent(&small_leaders).await;
    bench_invalidation(&small_leaders).await;
    bench_sharing_semantics(&small_leaders).await;
    bench_mixed_workload(&small_leaders, 100).await;
    bench_mixed_workload(&small_leaders, 1000).await;
    bench_mixed_workload(&small_leaders, 10_000).await;

    // --- Large scale ---
    println!();
    println!(
        "==== LARGE SCALE: {} leaders (thousands of shards) ====",
        large_leaders.len()
    );
    println!();
    bench_read_single(&large_leaders).await;
    bench_read_concurrent(&large_leaders).await;
    bench_invalidation(&large_leaders).await;
    bench_sharing_semantics(&large_leaders).await;
    bench_mixed_workload(&large_leaders, 100).await;
    bench_mixed_workload(&large_leaders, 1000).await;
    bench_mixed_workload(&large_leaders, 10_000).await;
}
