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

#![allow(clippy::missing_panics_doc)]

use std::env;
use std::fs;
use std::io;
use std::io::Write;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::net::TcpStream;
use std::num::NonZeroU32;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context as _;
use anyhow::anyhow;
use mauricebarnum_oxia_client::Client;
use mauricebarnum_oxia_client::config;
use mauricebarnum_oxia_client::errors::Error as ClientError;
use mauricebarnum_oxia_common::replication::GetStatusRequest;
use mauricebarnum_oxia_common::replication::ServingStatus;
use mauricebarnum_oxia_common::replication::oxia_coordination_client::OxiaCoordinationClient;
use tempfile::TempDir;
use tokio::time::sleep as async_sleep;

const CLUSTER_PHASE_TIMEOUT: Duration = Duration::from_secs(45);
const CLUSTER_BOOTSTRAP_ATTEMPTS: usize = 3;
const STABLE_TOPOLOGY_POLLS: usize = 2;

#[inline]
#[allow(dead_code)]
pub const fn non_zero(x: u32) -> NonZeroU32 {
    NonZeroU32::new(x).unwrap()
}

pub trait TestResultExt<T> {
    #[allow(dead_code)]
    #[must_use]
    fn trace_err(self) -> Self;
    #[must_use]
    fn trace_err_at(self, target: &'static str, file: &'static str, line: u32) -> Self;
}

impl<T, E: std::fmt::Debug> TestResultExt<T> for Result<T, E> {
    #[allow(dead_code)]
    fn trace_err(self) -> Self {
        if let Err(ref e) = self {
            tracing::error!(?e, "Operation failed");
        }
        self
    }

    fn trace_err_at(self, target: &'static str, file: &'static str, line: u32) -> Self {
        if let Err(ref e) = self {
            tracing::error!(target, file, line, ?e, "Operation failed");
        }
        self
    }
}

#[macro_export]
macro_rules! trace_err {
    ($expr:expr) => {{ $expr.trace_err_at(module_path!(), file!(), line!()) }};
}

pub fn oxia_cli_path() -> &'static Path {
    let p = oxia_bin_util::path();
    assert!(
        p.exists(),
        "oxia binary not found, re-build oxia-bin-util. expected: {}",
        p.display()
    );
    p
}

/// Create a test temp directory. Auto-cleaned unless `OXIA_KEEP_TEST_DATA=1` is set.
fn test_tempdir() -> anyhow::Result<TempDir> {
    let keep = env::var("OXIA_KEEP_TEST_DATA")
        .is_ok_and(|v| matches!(v.as_str(), "" | "1" | "true" | "yes" | "on"));
    let mut builder = tempfile::Builder::new();
    builder.prefix("oxia-test-");
    if keep {
        builder.disable_cleanup(true);
    }
    trace_err!(builder.tempdir().map_err(Into::into))
}

// Attempt to find `n` unallocated ports.  This function is racy:
// by the time it returns, the ports may be re-allocated.
pub fn find_free_ports(n: usize) -> anyhow::Result<Vec<u16>> {
    let sockets: Vec<socket2::Socket> = (0..n)
        .map(|_| -> anyhow::Result<socket2::Socket> {
            let s = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)?;
            s.set_reuse_address(true)?;
            s.bind(&socket2::SockAddr::from(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                0,
            )))?;
            Ok(s)
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .context("Failed to bind test sockets")?;

    let ports = sockets
        .iter()
        .map(|s| {
            s.local_addr()
                .context("Failed to get local address")?
                .as_socket_ipv4()
                .context("Not an IPv4 address")
                .map(|addr| addr.port())
        })
        .collect::<anyhow::Result<Vec<u16>>>()?;

    Ok(ports)
}

/// Wait for the server to be ready by checking TCP connectivity, then adding
/// a short delay for gRPC initialization.
pub fn wait_for_ready(addr: &str, timeout_secs: u64) -> anyhow::Result<()> {
    let start = Instant::now();
    while start.elapsed().as_secs() < timeout_secs {
        if TcpStream::connect(addr).is_ok() {
            // TCP is up, but gRPC/shard leaders may not be ready yet.
            // Add a short delay to allow for gRPC initialization.
            sleep(Duration::from_millis(200));
            return Ok(());
        }
        sleep(Duration::from_millis(100));
    }
    Err(anyhow!("Port did not open in time"))
}

struct TestServerArgs {
    service_addr: String,
    metrics_addr: String,
    db_dir: PathBuf,
    wal_dir: PathBuf,
    nshards: NonZeroU32,
}

impl TestServerArgs {
    fn start(&self) -> anyhow::Result<Child> {
        let mut cmd = Command::new(oxia_cli_path());
        cmd.arg("standalone")
            .arg("--data-dir")
            .arg(self.db_dir.as_path())
            .arg("--wal-dir")
            .arg(self.wal_dir.as_path())
            .arg("-p")
            .arg(self.service_addr.clone())
            .arg("-m")
            .arg(self.metrics_addr.clone())
            .arg("-s")
            .arg(self.nshards.get().to_string())
            .stdin(Stdio::null());
        let child = trace_err!(cmd.spawn())?;
        trace_err!(wait_for_ready(&self.service_addr, 30))?;
        Ok(child)
    }
}

#[allow(dead_code)]
pub struct TestServer {
    pub data_dir: TempDir,
    args: TestServerArgs,
    process: Child,
}

#[allow(dead_code)]
impl TestServer {
    pub fn start_nshards(nshards: NonZeroU32) -> anyhow::Result<Self> {
        let [service_port, metrics_port] = trace_err!(find_free_ports(2))?.try_into().unwrap();
        let data_dir = trace_err!(test_tempdir())?;
        let args = TestServerArgs {
            service_addr: format!("127.0.0.1:{service_port}"),
            metrics_addr: format!("127.0.0.1:{metrics_port}"),
            db_dir: data_dir.path().join("db"),
            wal_dir: data_dir.path().join("wal"),
            nshards,
        };
        let process = trace_err!(args.start())?;
        Ok(Self {
            data_dir,
            args,
            process,
        })
    }

    pub fn start() -> anyhow::Result<Self> {
        Self::start_nshards(NonZeroU32::new(1).unwrap())
    }

    pub fn shutdown(&mut self) -> anyhow::Result<()> {
        self.process.kill()?;
        self.process.wait()?;
        Ok(())
    }

    pub fn restart(&mut self) -> anyhow::Result<()> {
        trace_err!(self.shutdown())?;
        self.process = trace_err!(self.args.start())?;
        Ok(())
    }

    pub async fn connect_with<S>(
        &self,
        opts: config::ConfigBuilder<S>,
    ) -> Result<Client, ClientError>
    where
        S: config::config_builder::State,
        S::ServiceDiscovery: config::config_builder::IsUnset,
    {
        let config = opts.service_addr(self.args.service_addr.clone()).build();
        let mut client = Client::new(config);
        trace_err!(client.connect().await)?;
        Ok(client)
    }

    pub async fn connect(&self) -> Result<Client, ClientError> {
        self.connect_with(config::Config::builder()).await
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

// ---------------------------------------------------------------------------
// Multi-server cluster test infrastructure
// ---------------------------------------------------------------------------

#[allow(dead_code)]
struct ServerNode {
    public_addr: String,
    internal_addr: String,
    metrics_addr: String,
    data_dir: PathBuf,
    wal_dir: PathBuf,
    process: Option<Child>,
}

#[derive(Clone, Copy, Debug)]
struct ShardStatus {
    server: usize,
    status: ServingStatus,
    head_offset: i64,
    commit_offset: i64,
}

struct ClusterProcesses {
    servers: Vec<ServerNode>,
    coordinator: Option<Child>,
}

impl ClusterProcesses {
    fn shutdown(&mut self) {
        if let Some(mut process) = self.coordinator.take() {
            let _ = process.kill();
            let _ = process.wait();
        }
        for server in &mut self.servers {
            if let Some(mut process) = server.process.take() {
                let _ = process.kill();
                let _ = process.wait();
            }
        }
    }
}

impl Drop for ClusterProcesses {
    fn drop(&mut self) {
        self.shutdown();
    }
}

async fn wait_for_process_port(
    process: &mut Child,
    addr: &str,
    name: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = process.try_wait()? {
            return Err(anyhow!("{name} exited before opening {addr}: {status}"));
        }
        if TcpStream::connect(addr).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!("{name} did not open {addr} within {timeout:?}"));
        }
        async_sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_node_port(
    server: &mut ServerNode,
    internal: bool,
    timeout: Duration,
) -> anyhow::Result<()> {
    let addr = if internal {
        server.internal_addr.clone()
    } else {
        server.public_addr.clone()
    };
    let process = server
        .process
        .as_mut()
        .ok_or_else(|| anyhow!("server at {addr} is stopped"))?;
    wait_for_process_port(process, &addr, "data server", timeout).await
}

async fn get_shard_status(addr: &str, shard: u32) -> anyhow::Result<Option<ShardStatus>> {
    let endpoint = format!("http://{addr}");
    let mut client = tokio::time::timeout(
        Duration::from_secs(2),
        OxiaCoordinationClient::connect(endpoint),
    )
    .await??;
    let request = GetStatusRequest {
        shard: i64::from(shard),
    };
    match tokio::time::timeout(Duration::from_secs(2), client.get_status(request)).await? {
        Ok(response) => {
            let response = response.into_inner();
            let status = ServingStatus::try_from(response.status)?;
            Ok(Some(ShardStatus {
                server: 0,
                status,
                head_offset: response.head_offset,
                commit_offset: response.commit_offset,
            }))
        }
        Err(status) if status.code() == tonic::Code::FailedPrecondition => Ok(None),
        Err(status) => Err(status.into()),
    }
}

#[allow(dead_code)]
struct CoordinatorNode {
    internal_addr: String,
    metrics_addr: String,
    admin_addr: String,
    process: Child,
}

#[allow(dead_code)]
pub struct TestCluster {
    pub data_dir: TempDir,
    servers: Vec<ServerNode>,
    coordinator: CoordinatorNode,
    /// The public address of the first server (used for client connections).
    service_addr: String,
    replication_factor: usize,
    initial_shard_count: u32,
}

#[allow(dead_code)]
impl TestCluster {
    /// Start a cluster with `num_servers` data nodes and one coordinator.
    ///
    /// `replication_factor` and `initial_shard_count` configure the namespace.
    #[allow(clippy::too_many_lines)]
    pub async fn start(
        num_servers: usize,
        replication_factor: u32,
        initial_shard_count: u32,
    ) -> anyhow::Result<Self> {
        let mut errors = Vec::new();
        for attempt in 1..=CLUSTER_BOOTSTRAP_ATTEMPTS {
            match Self::start_once(num_servers, replication_factor, initial_shard_count).await {
                Ok(cluster) => return Ok(cluster),
                Err(error) => {
                    errors.push(format!("attempt {attempt}: {error:#}"));
                    async_sleep(Duration::from_millis(100 * attempt as u64)).await;
                }
            }
        }
        Err(anyhow!(
            "cluster bootstrap failed after {CLUSTER_BOOTSTRAP_ATTEMPTS} attempts:\n{}",
            errors.join("\n")
        ))
    }

    #[allow(clippy::too_many_lines)]
    async fn start_once(
        num_servers: usize,
        replication_factor: u32,
        initial_shard_count: u32,
    ) -> anyhow::Result<Self> {
        assert!(num_servers >= 2, "cluster needs at least 2 servers");
        // 3 ports per server (public, internal, metrics) + 3 for coordinator
        let total_ports = num_servers * 3 + 3;
        let ports = trace_err!(find_free_ports(total_ports))?;

        let data_dir = trace_err!(test_tempdir())?;

        // Partition ports: servers first, then coordinator
        let server_ports = &ports[..num_servers * 3];
        let coord_ports = &ports[num_servers * 3..];

        // Build server metadata for YAML config
        #[allow(clippy::items_after_statements)]
        struct ServerAddr {
            public: String,
            internal: String,
            metrics: String,
        }

        let server_addrs: Vec<ServerAddr> = (0..num_servers)
            .map(|i| {
                let base = i * 3;
                ServerAddr {
                    public: format!("127.0.0.1:{}", server_ports[base]),
                    internal: format!("127.0.0.1:{}", server_ports[base + 1]),
                    metrics: format!("127.0.0.1:{}", server_ports[base + 2]),
                }
            })
            .collect();

        let coord_internal = format!("127.0.0.1:{}", coord_ports[0]);
        let coord_metrics = format!("127.0.0.1:{}", coord_ports[1]);
        let coord_admin = format!("127.0.0.1:{}", coord_ports[2]);

        // Write cluster-config.yaml
        let config_path = data_dir.path().join("cluster-config.yaml");
        {
            let mut f = trace_err!(fs::File::create(&config_path))?;
            trace_err!(writeln!(f, "namespaces:"))?;
            trace_err!(writeln!(f, "  - name: \"default\""))?;
            trace_err!(writeln!(f, "    replicationFactor: {replication_factor}"))?;
            trace_err!(writeln!(f, "    initialShardCount: {initial_shard_count}"))?;
            trace_err!(writeln!(f, "servers:"))?;
            for sa in &server_addrs {
                trace_err!(writeln!(f, "  - public: \"{}\"", sa.public))?;
                trace_err!(writeln!(f, "    internal: \"{}\"", sa.internal))?;
            }
        }

        // Start data servers
        let mut processes = ClusterProcesses {
            servers: Vec::with_capacity(num_servers),
            coordinator: None,
        };
        for (i, sa) in server_addrs.iter().enumerate() {
            let srv_data = data_dir.path().join(format!("server-{i}"));
            let srv_db = srv_data.join("db");
            let srv_wal = srv_data.join("wal");

            let child = trace_err!(
                Command::new(oxia_cli_path())
                    .arg("server")
                    .arg("-p")
                    .arg(&sa.public)
                    .arg("-i")
                    .arg(&sa.internal)
                    .arg("-m")
                    .arg(&sa.metrics)
                    .arg("--data-dir")
                    .arg(&srv_db)
                    .arg("--wal-dir")
                    .arg(&srv_wal)
                    .stdin(Stdio::null())
                    .spawn()
            )?;

            processes.servers.push(ServerNode {
                public_addr: sa.public.clone(),
                internal_addr: sa.internal.clone(),
                metrics_addr: sa.metrics.clone(),
                data_dir: srv_db,
                wal_dir: srv_wal,
                process: Some(child),
            });
        }

        // Wait for each server's public port to be reachable
        for index in 0..processes.servers.len() {
            trace_err!(
                wait_for_node_port(&mut processes.servers[index], false, CLUSTER_PHASE_TIMEOUT,)
                    .await
            )?;
        }

        // Start coordinator
        let coord_process = trace_err!(
            Command::new(oxia_cli_path())
                .arg("coordinator")
                .arg("-f")
                .arg(&config_path)
                .arg("--metadata")
                .arg("memory")
                .arg("-i")
                .arg(&coord_internal)
                .arg("-m")
                .arg(&coord_metrics)
                .arg("-a")
                .arg(&coord_admin)
                .stdin(Stdio::null())
                .spawn()
        )?;

        processes.coordinator = Some(coord_process);
        // Wait for coordinator's internal port while the startup guard owns it.
        trace_err!(
            wait_for_process_port(
                processes
                    .coordinator
                    .as_mut()
                    .expect("coordinator process should exist"),
                &coord_internal,
                "coordinator",
                CLUSTER_PHASE_TIMEOUT,
            )
            .await
        )?;

        let coordinator = CoordinatorNode {
            internal_addr: coord_internal,
            metrics_addr: coord_metrics,
            admin_addr: coord_admin,
            process: processes
                .coordinator
                .take()
                .expect("coordinator process should exist"),
        };

        let service_addr = processes.servers[0].public_addr.clone();
        let mut cluster = Self {
            data_dir,
            servers: std::mem::take(&mut processes.servers),
            coordinator,
            service_addr,
            replication_factor: usize::try_from(replication_factor)?,
            initial_shard_count,
        };
        trace_err!(
            cluster
                .wait_for_stable_topology(CLUSTER_PHASE_TIMEOUT)
                .await
        )?;

        Ok(cluster)
    }

    pub fn coordinator_admin_addr(&self) -> &str {
        &self.coordinator.admin_addr
    }

    fn check_processes(&mut self) -> anyhow::Result<()> {
        if let Some(status) = self.coordinator.process.try_wait()? {
            return Err(anyhow!("coordinator exited unexpectedly: {status}"));
        }
        for (index, server) in self.servers.iter_mut().enumerate() {
            if let Some(process) = server.process.as_mut()
                && let Some(status) = process.try_wait()?
            {
                return Err(anyhow!("server {index} exited unexpectedly: {status}"));
            }
        }
        Ok(())
    }

    async fn shard_statuses(&self, shard: u32) -> Vec<ShardStatus> {
        let requests = self
            .servers
            .iter()
            .enumerate()
            .filter_map(|(server, node)| {
                node.process.as_ref()?;
                let addr = node.internal_addr.clone();
                Some(async move {
                    get_shard_status(&addr, shard)
                        .await
                        .ok()
                        .flatten()
                        .map(|mut status| {
                            status.server = server;
                            status
                        })
                })
            });
        futures::future::join_all(requests)
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    async fn topology_state(&self) -> (bool, String) {
        let mut ready = true;
        let mut details = Vec::new();
        for shard in 0..self.initial_shard_count {
            let statuses = self.shard_statuses(shard).await;
            let leaders = statuses
                .iter()
                .filter(|status| status.status == ServingStatus::Leader)
                .count();
            let members = statuses
                .iter()
                .filter(|status| status.status != ServingStatus::NotMember)
                .count();
            if leaders != 1 || members != self.replication_factor {
                ready = false;
            }
            details.push(format!(
                "shard {shard}: leaders={leaders}, members={members}, statuses={statuses:?}"
            ));
        }
        (ready, details.join("; "))
    }

    async fn wait_for_stable_topology(&mut self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        let mut stable_polls = 0;
        loop {
            self.check_processes()?;
            let (ready, state) = self.topology_state().await;
            if ready {
                stable_polls += 1;
                if stable_polls >= STABLE_TOPOLOGY_POLLS {
                    return Ok(());
                }
            } else {
                stable_polls = 0;
            }
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "cluster topology did not stabilize within {timeout:?}: {state}"
                ));
            }
            async_sleep(Duration::from_millis(200)).await;
        }
    }

    async fn wait_for_restarted_node(
        &mut self,
        index: usize,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        let mut stable_polls = 0;
        loop {
            self.check_processes()?;
            let mut ready = true;
            let mut details = Vec::new();
            for shard in 0..self.initial_shard_count {
                let statuses = self.shard_statuses(shard).await;
                let leaders = statuses
                    .iter()
                    .filter(|status| status.status == ServingStatus::Leader)
                    .count();
                let members: Vec<_> = statuses
                    .iter()
                    .filter(|status| status.status != ServingStatus::NotMember)
                    .collect();
                let max_commit = members.iter().map(|status| status.commit_offset).max();
                let restarted = members.iter().find(|status| status.server == index);
                let shard_ready = leaders == 1
                    && members.len() == self.replication_factor
                    && restarted.is_some_and(|status| {
                        max_commit.is_some_and(|commit| status.head_offset >= commit)
                    });
                ready &= shard_ready;
                details.push(format!(
                    "shard {shard}: leaders={leaders}, max_commit={max_commit:?}, statuses={statuses:?}"
                ));
            }
            let state = details.join("; ");
            if ready {
                stable_polls += 1;
                if stable_polls >= STABLE_TOPOLOGY_POLLS {
                    return Ok(());
                }
            } else {
                stable_polls = 0;
            }
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "server {index} did not rejoin and catch up within {timeout:?}: {state}"
                ));
            }
            async_sleep(Duration::from_millis(200)).await;
        }
    }

    pub fn shutdown(&mut self) {
        let _ = self.coordinator.process.kill();
        let _ = self.coordinator.process.wait();
        for srv in &mut self.servers {
            if let Some(ref mut p) = srv.process {
                let _ = p.kill();
                let _ = p.wait();
            }
        }
    }

    /// Number of server nodes (running or stopped).
    pub const fn server_count(&self) -> usize {
        self.servers.len()
    }

    /// Returns the public addresses of all servers.
    pub fn server_addrs(&self) -> Vec<String> {
        self.servers.iter().map(|s| s.public_addr.clone()).collect()
    }

    /// SIGKILL a server (immediate death -> connection errors).
    pub fn kill_server(&mut self, index: usize) -> anyhow::Result<()> {
        let srv = &mut self.servers[index];
        let mut p = srv
            .process
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "server already stopped"))?;
        p.kill()?;
        p.wait()?;
        Ok(())
    }

    /// SIGTERM a server (graceful shutdown -> may produce wrong-leader
    /// errors).
    pub fn stop_server(&mut self, index: usize) -> anyhow::Result<()> {
        let srv = &mut self.servers[index];
        let mut p = srv
            .process
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "server already stopped"))?;
        let pid = nix::unistd::Pid::from_raw(i32::try_from(p.id()).expect("pid does not fit i32"));
        nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM).map_err(io::Error::other)?;
        p.wait()?;
        Ok(())
    }

    /// Restart a previously stopped server on the same ports and data
    /// directory.
    pub async fn restart_server(&mut self, index: usize) -> anyhow::Result<()> {
        let srv = &mut self.servers[index];
        assert!(srv.process.is_none(), "server {index} is still running");
        let child = trace_err!(
            Command::new(oxia_cli_path())
                .arg("server")
                .arg("-p")
                .arg(&srv.public_addr)
                .arg("-i")
                .arg(&srv.internal_addr)
                .arg("-m")
                .arg(&srv.metrics_addr)
                .arg("--data-dir")
                .arg(&srv.data_dir)
                .arg("--wal-dir")
                .arg(&srv.wal_dir)
                .stdin(Stdio::null())
                .spawn()
        )?;
        srv.process = Some(child);
        trace_err!(wait_for_node_port(srv, false, CLUSTER_PHASE_TIMEOUT).await)?;
        trace_err!(
            self.wait_for_restarted_node(index, CLUSTER_PHASE_TIMEOUT)
                .await
        )
    }

    /// Connect a client to the cluster, retrying to handle asynchronous shard
    /// assignment propagation.
    pub async fn connect(&self) -> Result<Client, ClientError> {
        self.connect_with(config::Config::builder()).await
    }

    pub async fn connect_with<S>(
        &self,
        opts: config::ConfigBuilder<S>,
    ) -> Result<Client, ClientError>
    where
        S: config::config_builder::State,
        S::ServiceDiscovery: config::config_builder::IsUnset,
    {
        let cfg = opts.service_addr(self.service_addr.clone()).build();

        let mut last_err = None;
        for attempt in 0..10 {
            let mut client = Client::new(Arc::clone(&cfg));
            match client.connect().await {
                Ok(()) => return Ok(client),
                Err(e) => {
                    tracing::warn!(attempt, ?e, "cluster connect attempt failed, retrying");
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
            }
        }
        Err(last_err.unwrap())
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ---------------------------------------------------------------------------
// TestEnv trait + parameterized test infrastructure
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub trait TestEnv: Send + Sync {
    fn service_addr(&self) -> &str;
    fn needs_connect_retry(&self) -> bool;
    fn test_timeout(&self) -> Duration;
}

impl TestEnv for TestServer {
    fn service_addr(&self) -> &str {
        &self.args.service_addr
    }

    fn needs_connect_retry(&self) -> bool {
        false
    }

    fn test_timeout(&self) -> Duration {
        Duration::from_secs(10)
    }
}

impl TestEnv for TestCluster {
    fn service_addr(&self) -> &str {
        &self.service_addr
    }

    fn needs_connect_retry(&self) -> bool {
        true
    }

    fn test_timeout(&self) -> Duration {
        Duration::from_mins(2)
    }
}

#[allow(dead_code)]
pub async fn connect_env(env: &(impl TestEnv + ?Sized)) -> Result<Client, ClientError> {
    connect_env_with(env, config::Config::builder()).await
}

#[allow(dead_code)]
pub async fn connect_env_with<S>(
    env: &(impl TestEnv + ?Sized),
    opts: config::ConfigBuilder<S>,
) -> Result<Client, ClientError>
where
    S: config::config_builder::State,
    S::ServiceDiscovery: config::config_builder::IsUnset,
{
    let cfg = opts.service_addr(env.service_addr().to_owned()).build();

    if env.needs_connect_retry() {
        let mut last_err = None;
        for attempt in 0..10 {
            let mut client = Client::new(Arc::clone(&cfg));
            match client.connect().await {
                Ok(()) => return Ok(client),
                Err(e) => {
                    tracing::warn!(attempt, ?e, "connect attempt failed, retrying");
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
            }
        }
        Err(last_err.unwrap())
    } else {
        let mut client = Client::new(cfg);
        client.connect().await?;
        Ok(client)
    }
}

#[allow(dead_code)]
pub enum TestConfig {
    Standalone {
        shards: NonZeroU32,
    },
    Cluster {
        servers: usize,
        rf: u32,
        shards: u32,
    },
}

#[allow(dead_code)]
impl TestConfig {
    pub const fn standalone(shards: u32) -> Self {
        Self::Standalone {
            shards: NonZeroU32::new(shards).unwrap(),
        }
    }

    pub const fn cluster(servers: usize, rf: u32, shards: u32) -> Self {
        Self::Cluster {
            servers,
            rf,
            shards,
        }
    }

    pub async fn start(&self) -> anyhow::Result<Box<dyn TestEnv>> {
        match *self {
            Self::Standalone { shards } => {
                let server = TestServer::start_nshards(shards)?;
                Ok(Box::new(server))
            }
            Self::Cluster {
                servers,
                rf,
                shards,
            } => {
                let cluster = TestCluster::start(servers, rf, shards).await?;
                Ok(Box::new(cluster))
            }
        }
    }
}

#[macro_export]
macro_rules! parameterized_test {
    ($test_fn:ident, [ $( $name:ident : $config:expr ),+ $(,)? ]) => {
        mod $test_fn {
            use super::*;
            $(
                #[test_log::test(tokio::test)]
                async fn $name() -> anyhow::Result<()> {
                    let env = $config.start().await?;
                    let timeout = env.test_timeout();
                    tokio::time::timeout(timeout, super::$test_fn(env.as_ref())).await?
                }
            )+
        }
    };
}

/// Invoke `parameterized_test!` with the standard 3-config matrix.
#[macro_export]
macro_rules! parameterized_test_standard {
    ($test_fn:ident) => {
        parameterized_test!($test_fn, [
            standalone_1s:  common::TestConfig::standalone(1),
            standalone_4s:  common::TestConfig::standalone(4),
            cluster_3n_6s:  common::TestConfig::cluster(3, 2, 6),
        ]);
    };
}
