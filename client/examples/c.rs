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

use std::time::Duration;

use chrono::Utc;
use mauricebarnum_oxia_client::{Client, Error, PutOptions, Result, config};
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::Config;
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

    let config = Config::builder()
        .service_addr(server_address)
        .max_parallel_requests(3)
        .session_timeout(Duration::from_millis(2001))
        .build();
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
            crate::PutOptions::builder().ephemeral(true).build(),
        )
        .await?;
    println!("put result: {result:?}");

    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("done");
    drop(client);
    Ok(())
}
