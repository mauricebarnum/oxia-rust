// tests/common/mod.rs

use cargo_metadata::MetadataCommand;
use mauricebarnum_oxia_client::errors::Error as ClientError;
use mauricebarnum_oxia_client::{Client, config};
use once_cell::sync::OnceCell;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::{io, path};
use tempfile::TempDir;
use tracing::info;

pub trait TestResultExt<T> {
    #[allow(dead_code)]
    fn trace_err(self) -> Self;
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

macro_rules! trace_err {
    ($expr:expr) => {{ $expr.trace_err_at(module_path!(), file!(), line!()) }};
}

pub(crate) use trace_err; // Make it available to other modules in the crate

static OXIA_BIN: OnceCell<PathBuf> = OnceCell::new();

fn get_workspace_root() -> Option<PathBuf> {
    let metadata = MetadataCommand::new().exec().ok()?;
    Some(PathBuf::from(metadata.workspace_root))
}

pub fn oxia_cli_path() -> &'static Path {
    OXIA_BIN.get_or_init(|| {
        // Get workspace target dir
        let workspace_root = get_workspace_root().expect("failed to get workspace root");
        let target_dir = workspace_root.join("target");
        let go_tools_target = target_dir.join("go-tools");
        let go_bin_dir = go_tools_target.join("bin");
        let cmd_path = go_bin_dir.join("cmd");

        if !cmd_path.exists() {
            let dirs = [
                ("GOBIN", go_bin_dir),
                ("GOCACHE", go_tools_target.join("cache")),
                ("GOMODCACHE", go_tools_target.join("mod")),
            ];

            let srcdir = workspace_root.join("go-tools");

            dirs.iter().for_each(|(_, d)| {
                std::fs::create_dir_all(d).unwrap_or_else(|_| panic!("unable to create {d:?}"))
            });

            let version =
                std::env::var("OXIA_CLI_VERSION").unwrap_or_else(|_| "v0.14.1".to_string());

            let vendor_path = srcdir.join("vendor/github.com/oxia-db/oxia/cmd");
            let mut install_args = vec!["install".to_string()];
            if vendor_path.exists() {
                install_args.extend(
                    ["-mod=vendor", "./vendor/github.com/oxia-db/oxia/cmd"].map(|x| x.to_string()),
                );
            } else {
                install_args.push(format!("github.com/oxia-db/oxia/cmd@{version}"));
            }
            info!(?version, ?vendor_path, ?install_args, "building oxia cmd");
            let status = Command::new("go")
                .args(install_args)
                .current_dir(srcdir)
                .envs(dirs)
                .status()
                .expect("failed to run `go install` for oxia CLI");
            info!(?status);

            assert!(status.success(), "oxia CLI installation failed");
        }

        cmd_path
    })
}

// Attempt to find `n` unallocated ports.  This function is racy:
// by the time it returns, the ports may be re-allocated.
pub fn find_free_ports(n: usize) -> io::Result<Vec<u16>> {
    use socket2::{Domain, SockAddr, Socket, Type};
    use std::net::{Ipv4Addr, SocketAddrV4};
    let mut sockets: Vec<Socket> = Vec::with_capacity(n);
    for _ in 0..n {
        let s = trace_err!(Socket::new(Domain::IPV4, Type::STREAM, None))?;
        trace_err!(s.set_reuse_address(true))?;
        trace_err!(s.bind(&SockAddr::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))))?;
        sockets.push(s);
    }

    let mut ports: Vec<u16> = Vec::with_capacity(n);
    for s in &sockets {
        let port = trace_err!(s.local_addr())?
            .as_socket_ipv4()
            .expect("Not IPv4 address")
            .port();
        ports.push(port);
    }
    Ok(ports)
}

pub fn wait_for_ready(addr: &str, timeout_secs: u64) -> io::Result<()> {
    let start = Instant::now();
    while start.elapsed().as_secs() < timeout_secs {
        if TcpStream::connect(addr).is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(100));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "Port did not open in time",
    ))
}

struct TestServerArgs {
    service_addr: String,
    metrics_addr: String,
    db_dir: path::PathBuf,
    wal_dir: path::PathBuf,
}

impl TestServerArgs {
    fn start(&self) -> io::Result<Child> {
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
            .stdin(Stdio::null());
        let child = trace_err!(cmd.spawn())?;
        trace_err!(wait_for_ready(&self.service_addr, 30_000))?;
        Ok(child)
    }
}

pub struct TestServer {
    pub data_dir: TempDir,
    args: TestServerArgs,
    process: Child,
}

impl TestServer {
    pub fn start() -> io::Result<Self> {
        let [service_port, metrics_port] = trace_err!(find_free_ports(2))?.try_into().unwrap();
        let data_dir = trace_err!(tempfile::Builder::new().disable_cleanup(true).tempdir())?;
        let args = TestServerArgs {
            service_addr: format!("127.0.0.1:{service_port}"),
            metrics_addr: format!("127.0.0.1:{metrics_port}"),
            db_dir: data_dir.path().join("db"),
            wal_dir: data_dir.path().join("wal"),
        };
        let process = trace_err!(args.start())?;
        Ok(TestServer {
            data_dir,
            args,
            process,
        })
    }

    pub fn shutdown(&mut self) -> io::Result<()> {
        self.process.kill()
    }

    pub fn restart(&mut self) -> io::Result<()> {
        trace_err!(self.shutdown())?;
        self.process = trace_err!(self.args.start())?;
        Ok(())
    }

    pub async fn connect(&self, optc: Option<config::Builder>) -> Result<Client, ClientError> {
        let builder = optc
            .unwrap_or_default()
            .service_addr(self.args.service_addr.clone());
        let mut client = Client::new(trace_err!(builder.build())?);
        trace_err!(client.connect().await)?;
        Ok(client)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
