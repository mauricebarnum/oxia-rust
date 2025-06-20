use chrono::Utc;
use mauricebarnum_oxia_client as client;
use mauricebarnum_oxia_client::config;
use std::fs;
use std::time::Duration;
use tracing::info;

mod common;

#[test_log::test(tokio::test)]
async fn test_basic() -> Result<(), Box<dyn std::error::Error>> {
    // .max_parallel_requests(3)
    //     .session_timeout(Duration::from_millis(2001))
    let server = common::TestServer::start()?;
    let builder = config::Builder::new()
        // .max_parallel_requests(3)
        .session_timeout(Duration::from_millis(2001));
    let client = server.connect(Some(builder)).await?;

    let mut key = String::new();
    for i in 0..11 {
        key = format!("foo-{}", i);
        for j in 0..7 {
            let result = client
                .put(
                    key.clone(),
                    format!("value-{}-{}", i, j).as_bytes().to_vec(),
                )
                .await?;
            info!(op = "put", ?result);
        }
    }

    // And do a get
    let result = client.get(key.clone()).await?;
    info!(op = "get", ?result);

    // Let's list all of the keys
    let result = client.list("", "").await?;
    for (i, k) in result.keys.iter().enumerate() {
        info!(op = "list", i, key = ?k);
    }

    // Delete the last key inserted
    client.delete(key.clone()).await?;

    // Scan for all of the keys

    let result = client.range_scan("", "").await?;
    for (i, v) in result.records.iter().enumerate() {
        info!(op = "scan", i, value = ?v);
    }

    // And do another get, this time expecting KeyNotFound
    if let Some(r) = client.get(key.clone()).await? {
        return Err(format!("unexpected get success: {:?}", r).into());
    }

    let result = client
        .put_with_options(
            "ephemeral_key",
            Utc::now()
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string()
                .as_bytes()
                .to_vec(),
            client::PutOptions::new().ephemeral(),
        )
        .await?;
    info!(op = "put", ?result);

    tokio::time::sleep(Duration::from_secs(5)).await;
    drop(client);

    fs::remove_dir_all(server.data_dir.path())?;
    Ok(())
}
