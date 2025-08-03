use mauricebarnum_oxia_client::{Client, Result, config};
use std::time::Duration;
use tracing_subscriber::{EnvFilter, fmt};

fn main() -> Result<()> {
    unsafe {
        std::env::set_var("RUST_BACKTRACE", "1");
    }
    tokio::runtime::Runtime::new()?.block_on(async {
        const SECONDS: u64 = 5;

        fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .init();
        //     .with_env_filter(EnvFilter::new("tonic=trace,hyper=trace,h2=trace"))
        //     .init();

        // Replace with your Oxia server address.
        // TODO: what's with the scheme?  It's not in the shard assignments return, so I guess
        // we shouldn't use it here
        let server_address = "localhost:6648";

        let config = crate::config::Builder::new()
            .service_addr(server_address)
            .max_parallel_requests(3)
            .session_timeout(Duration::from_millis(2001))
            .build()?;
        let mut client = Client::new(config.clone());

        client.connect().await?;

        println!("letting background processing to run for {SECONDS} seconds");
        tokio::time::sleep(Duration::from_secs(SECONDS)).await;

        println!("bye");

        Ok(())
    })
}
