use chrono::Utc;
use mauricebarnum_oxia_client as client;
use mauricebarnum_oxia_client::config;
use std::fs;
use std::time::Duration;
use tracing::info;

mod common;
use common::TestResultExt;
use common::trace_err;

#[test_log::test(tokio::test)]
async fn test_basic() -> Result<(), Box<dyn std::error::Error>> {
    // .max_parallel_requests(3)
    //     .session_timeout(Duration::from_millis(2001))
    let server = trace_err!(common::TestServer::start())?;
    let builder = config::Builder::new()
        .retry(config::RetryConfig::new(3, Duration::from_millis(23)))
        .max_parallel_requests(3)
        .session_timeout(Duration::from_millis(2001));
    let client = trace_err!(server.connect(Some(builder)).await)?;

    let mut key = String::new();
    for i in 0..11 {
        key = format!("foo-{}", i);
        for j in 0..7 {
            let result = trace_err!(
                client
                    .put(
                        key.clone(),
                        format!("value-{}-{}", i, j).as_bytes().to_vec(),
                    )
                    .await
            )?;
            info!(op = "put", ?result);
        }
    }

    // And do a get
    let result = client.get(key.clone()).await?;
    info!(op = "get", ?result);

    // Let's list all of the keys
    let result = trace_err!(client.list("", "").await)?;
    for (i, k) in result.keys.iter().enumerate() {
        info!(op = "list", i, key = ?k);
    }

    // Delete the last key inserted
    trace_err!(client.delete(key.clone()).await)?;

    // Scan for all of the keys

    let result = trace_err!(client.range_scan("", "").await)?;
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

    trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
    Ok(())
}

async fn do_test_disconnect(retry: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mut server = common::TestServer::start()?;
    let builder = config::Builder::new()
        .max_parallel_requests(3)
        .request_timeout(Duration::from_millis(300))
        .session_timeout(Duration::from_millis(2001));
    let builder = if retry {
        builder.retry(config::RetryConfig::new(3, Duration::from_millis(100)))
    } else {
        builder
    };

    let client = server.connect(Some(builder)).await?;
    assert_eq!(None, trace_err!(client.get("foo").await)?);

    trace_err!(client.put("foo", "bar").await)?;
    assert!(trace_err!(client.get("foo").await)?.is_some());

    info!("restarting test server");
    trace_err!(server.restart())?;

    let r = trace_err!(client.get("foo").await)?;
    info!(op = "get", ?r);
    assert!(r.is_some());

    trace_err!(fs::remove_dir_all(server.data_dir.path()))?;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_disconnect() -> Result<(), Box<dyn std::error::Error>> {
    do_test_disconnect(true).await
}

#[test_log::test(tokio::test)]
async fn test_disconnect_fail() {
    let r = do_test_disconnect(false).await;
    assert!(r.is_err());
}
