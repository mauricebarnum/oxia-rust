use std::time::Duration;

use chrono::Utc;
use mauricebarnum_oxia_client::*;
use tracing_subscriber::{EnvFilter, fmt};
// use tracing::{debug, error, info, span, warn};
// use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<()> {
    // fmt()
    //     .with_env_filter(EnvFilter::new("tonic=trace,hyper=trace,h2=trace"))
    //     .init();
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Replace with your Oxia server address
    let server_address = "localhost:6648";

    let config = crate::config::Builder::new()
        .service_addr(server_address)
        .max_parallel_requests(3)
        .session_timeout(Duration::from_millis(2001))
        .build()?;
    let mut client = Client::new(config);
    client.connect().await?;

    let mut key = String::new();
    for i in 0..11 {
        key = format!("foo-{i}");
        for j in 0..7 {
            let result = client.put(&key, format!("value-{i}-{j}")).await?;
            println!("put result: {result:?}");
        }
    }

    // And do a get
    let result = client.get(&key).await?;
    println!("get result: {result:?}");

    // Let's list all of the keys
    let result = client.list("", "").await?;
    for (i, v) in result.keys.iter().enumerate() {
        println!("list {i} {v}");
    }

    // Delete the last key inserted
    client.delete(&key).await?;

    // Scan for all of the keys

    let result = client.range_scan("", "").await?;
    for (i, v) in result.records.iter().enumerate() {
        println!("scan {} {:?}", i, &v);
    }

    // And do another get, this time expecting KeyNotFound
    if let Some(r) = client.get(&key).await? {
        return Err(Error::Custom(format!("unexpected get success: {r:?}")));
    }

    let result = client
        .put_with_options(
            "ephemeral_key",
            Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            crate::PutOptions::new().ephemeral(),
        )
        .await?;
    println!("put result: {result:?}");

    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("done");
    drop(client);
    Ok(())
}
