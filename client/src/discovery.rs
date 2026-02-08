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

use std::fmt::Debug;
use std::sync::Arc;

use futures::future::BoxFuture;
use mauricebarnum_oxia_common::proto::ListNodesRequest;
use mauricebarnum_oxia_common::proto::OxiaAdminClient;
use tracing::debug;
use tracing::warn;

/// Provides candidate server addresses for the shard assignment stream.
///
/// On stream failure, the `ShardAssignmentTask` rotates through the addresses
/// returned by this trait rather than retrying a single fixed address.
///
/// FIXME: this abstraction isn't quite right, but I'm leaving it in for now as the plumbing
/// gets put together.  The assumption that what we want is a flat list of candidates servers,
/// is too specific:  what if some hosts are better because they're closer? Or we have a way to weight the choices that
/// aren't captured in this canddiates/selector divide?  A better approach is to have just a
/// "discovery" interface that returns a candidate.  Maybe it gets feedback like last failure
/// error, if any, to guide the next selection.   Then, we can have an implementation that combines
/// a candidate provider and a selector implementation for the "list of addresses" case.  As
/// implemented here.  Anyway, if this is merged as is, consider the API unstable.
pub trait ServiceDiscovery: Send + Sync + Debug {
    /// Returns candidate addresses to try for the shard assignment stream.
    fn addresses(&self) -> BoxFuture<'_, Vec<String>>;
}

/// Conversion into `Arc<dyn ServiceDiscovery>` for the
/// [`Config`](crate::config::Config) builder.
///
/// Accepts a concrete [`ServiceDiscovery`] (auto-wrapped in `Arc`) or a
/// pre-built `Arc<dyn ServiceDiscovery>`.  Analogous to `IntoIterator` /
/// `IntoFuture` — needed because orphan rules block
/// `impl<T: ServiceDiscovery> From<T> for Arc<dyn ServiceDiscovery>`.
pub trait IntoServiceDiscovery {
    fn into_arc(self) -> Arc<dyn ServiceDiscovery>;
}

impl<T: ServiceDiscovery + 'static> IntoServiceDiscovery for T {
    fn into_arc(self) -> Arc<dyn ServiceDiscovery> {
        Arc::new(self)
    }
}

impl IntoServiceDiscovery for Arc<dyn ServiceDiscovery> {
    fn into_arc(self) -> Arc<dyn ServiceDiscovery> {
        self
    }
}

/// A fixed list of server addresses.
#[derive(Debug)]
pub struct StaticServiceDiscovery {
    addrs: Vec<String>,
}

impl StaticServiceDiscovery {
    pub const fn new(addrs: Vec<String>) -> Self {
        Self { addrs }
    }

    pub fn single(addr: impl Into<String>) -> Self {
        Self {
            addrs: vec![addr.into()],
        }
    }
}

impl ServiceDiscovery for StaticServiceDiscovery {
    fn addresses(&self) -> BoxFuture<'_, Vec<String>> {
        Box::pin(std::future::ready(self.addrs.clone()))
    }
}

/// Discovers addresses via the coordinator's `ListNodes` admin API.
#[derive(Debug)]
pub struct CoordinatorServiceDiscovery {
    coordinator_addr: String,
}

impl CoordinatorServiceDiscovery {
    pub const fn new(coordinator_addr: String) -> Self {
        Self { coordinator_addr }
    }
}

impl ServiceDiscovery for CoordinatorServiceDiscovery {
    fn addresses(&self) -> BoxFuture<'_, Vec<String>> {
        Box::pin(async {
            let url = format!("http://{}", self.coordinator_addr);
            match OxiaAdminClient::connect(url).await {
                Ok(mut admin) => match admin.list_nodes(ListNodesRequest {}).await {
                    Ok(resp) => {
                        let nodes = resp.into_inner().nodes;
                        let addrs: Vec<String> = nodes
                            .into_iter()
                            .filter(|n| !n.public_address.is_empty())
                            .map(|n| n.public_address)
                            .collect();
                        if addrs.is_empty() {
                            warn!("ListNodes returned no addresses");
                        } else {
                            debug!(count = addrs.len(), "discovered addresses via ListNodes");
                        }
                        addrs
                    }
                    Err(e) => {
                        warn!(?e, "ListNodes failed");
                        vec![]
                    }
                },
                Err(e) => {
                    warn!(?e, "failed to connect to coordinator");
                    vec![]
                }
            }
        })
    }
}
