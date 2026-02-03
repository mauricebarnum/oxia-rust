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

use mauricebarnum_oxia_client::{Client, Result, config};
use std::{sync::Arc, time::Duration};
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::Config;

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

        let config = Config::builder()
            .service_addr(server_address)
            .max_parallel_requests(3)
            .session_timeout(Duration::from_millis(2001))
            .build();
        let mut client = Client::new(Arc::clone(&config));

        client.connect().await?;

        println!("letting background processing to run for {SECONDS} seconds");
        tokio::time::sleep(Duration::from_secs(SECONDS)).await;

        println!("bye");

        Ok(())
    })
}
