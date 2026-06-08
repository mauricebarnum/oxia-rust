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

#![cfg(not(miri))]

mod common;
use common::TestCluster;

#[test_log::test(tokio::test)]
async fn concurrent_clusters_start_on_distinct_ports() -> anyhow::Result<()> {
    let (left, right) = tokio::join!(TestCluster::start(3, 2, 4), TestCluster::start(3, 2, 4),);
    let left = left?;
    let right = right?;

    let left_addrs = left.server_addrs();
    let right_addrs = right.server_addrs();
    assert!(
        left_addrs.iter().all(|addr| !right_addrs.contains(addr)),
        "concurrent clusters must not share public addresses"
    );

    Ok(())
}
