// tests/common/mod.rs

use mauricebarnum_oxia_client::config::Config;
use mauricebarnum_oxia_client::errors::Error as ClientError;
use mauricebarnum_oxia_client::{Client, config};
use mauricebarnum_oxia_common::proto::oxia_client_client;
use once_cell::sync::OnceCell;
use socket2::Socket;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tempfile::{/*Builder,*/ TempDir};
// #[cfg(unix)]
// use nix::sys::signal::{Signal, kill};
// #[cfg(unix)]
// use nix::unistd::Pid;

static OXIA_BIN: OnceCell<PathBuf> = OnceCell::new();

pub fn oxia_cli_path() -> &'static Path {
    OXIA_BIN.get_or_init(|| {
        // Get workspace target dir
        let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| {
            let crate_dir =
                std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
            let workspace_target = Path::new(&crate_dir)
                .ancestors()
                .nth(2)
                .expect("Failed to find workspace root")
                .join("target");
            workspace_target.to_string_lossy().into_owned()
        });

        // Use absolute path for oxia-cli directory inside workspace target
        let dir = Path::new(&target_dir).join("oxia-cli");
        std::fs::create_dir_all(&dir).expect("failed to create oxia-cli dir");

        let bin_path = dir.join("cmd");
        if !bin_path.exists() {
            let version =
                std::env::var("OXIA_CLI_VERSION").unwrap_or_else(|_| "latest".to_string());

            let status = Command::new("go")
                .args([
                    "install",
                    "-v",
                    &format!("github.com/streamnative/oxia/cmd@{}", version),
                ])
                .env("GOBIN", &dir)
                .status()
                .expect("failed to run `go install` for oxia CLI");

            assert!(status.success(), "oxia CLI installation failed");
        }

        bin_path
    })
}

// Attempt to find `n` unallocated ports.  This function is racy:
// by the time it returns, the ports may be re-allocated.
pub fn find_free_ports(n: usize) -> io::Result<Vec<u16>> {
    use socket2::{Domain, SockAddr, Socket, Type};
    use std::net::{Ipv4Addr, SocketAddrV4};
    let mut sockets: Vec<Socket> = Vec::with_capacity(n);
    for _ in 0..n {
        let socket = Socket::new(Domain::IPV4, Type::STREAM, None)?;
        socket.set_reuse_address(true)?;
        socket.bind(&SockAddr::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))?;
        sockets.push(socket);
    }

    let mut ports: Vec<u16> = Vec::with_capacity(n);
    for socket in &sockets {
        let port = socket
            .local_addr()?
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

pub struct TestServer {
    pub service_addr: String,
    pub metrics_addr: String,
    pub data_dir: TempDir,
    pub process: Child,
}

impl TestServer {
    pub fn start() -> io::Result<Self> {
        let [service_port, metrics_port] = find_free_ports(2)?[..] else {
            unreachable!("wrong number of ports")
        };
        let service_addr = format!("127.0.0.1:{service_port}");
        let metrics_addr = format!("127.0.0.1:{metrics_port}");
        let data_dir = tempfile::Builder::new().disable_cleanup(true).tempdir()?;
        let process = {
            let mut cmd = Command::new(oxia_cli_path());
            cmd.arg("standalone")
                .arg("--data-dir")
                .arg(data_dir.path().join("db"))
                .arg("--wal-dir")
                .arg(data_dir.path().join("wal"))
                .arg("-p")
                .arg(service_addr.clone())
                .arg("-m")
                .arg(metrics_addr.clone())
                .stdin(Stdio::null());
            cmd.spawn()?
        };
        wait_for_ready(&service_addr, 30_000)?;
        Ok(TestServer {
            service_addr,
            metrics_addr,
            data_dir,
            process,
        })
    }

    pub fn shutdown(&mut self) -> io::Result<()> {
        // #[cfg(unix)]
        // {
        //     let pid = self.process.id();
        //     let nix_pid = Pid::from_raw(pid as i32);
        //     // Send SIGTERM to the process
        //     if let Err(e) = kill(nix_pid, Signal::SIGTERM) {
        //         eprintln!("Failed to send signal: {}", e);
        //     }
        // }
        //
        self.process.kill()
    }

    pub async fn connect(&self, optc: Option<config::Builder>) -> Result<Client, ClientError> {
        let builder = optc
            .unwrap_or_else(|| config::Builder::new())
            .service_addr(self.service_addr.clone()); //.service_address(self.service_addr.clone());
        let mut client = Client::new(builder.build()?);
        client.connect().await?;
        Ok(client)
        // Err(ClientError::NoServiceAddress)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
