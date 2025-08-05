// Copyright 2025 Maurice S. Barnum
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
use mauricebarnum_oxia_client::{Client, PutOptions, Result, config};
// use tracing_subscriber::{EnvFilter, fmt};
// use tracing::{debug, error, info, span, warn};
// use tracing_subscriber::{EnvFilter, fmt};

fn main() -> Result<()> {
    unsafe {
        std::env::set_var("RUST_BACKTRACE", "1");
    }
    tokio::runtime::Runtime::new()?.block_on(async {
        // fmt()
        //     .with_env_filter(EnvFilter::new("tonic=trace,hyper=trace,h2=trace"))
        //     .init();

        // Replace with your Oxia server address
        let server_address = "localhost:6648";

        let config = crate::config::Builder::new()
            .service_addr(server_address)
            .max_parallel_requests(3)
            .session_timeout(Duration::from_millis(2001))
            .build()?;
        let mut client = Client::new(config.clone());
        client.connect().await?;

        let now_str = Utc::now().format("%Y-%m-%dT%H:%M:%SZ");
        let key = format!("{}-{}", "ephemeral_key", now_str);
        let result = client
            .put_with_options(&key, key.clone(), PutOptions::new().ephemeral())
            .await?;
        println!("put result: {result:?}");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let result = client.get(&key).await?;
        println!("get result: {result:?}");
        drop(client);

        println!("waiting for session to expire");
        tokio::time::sleep(config.session_timeout().checked_mul(2).unwrap()).await;

        println!("new initializing new client");
        let mut client = Client::new(config);
        client.connect().await?;

        println!("re-reading key {key}");
        let result = client.get(&key).await?;
        println!("get result: {result:?}");

        Ok(())
    })
}
