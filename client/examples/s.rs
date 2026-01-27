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

use mauricebarnum_oxia_client::Client;
use mauricebarnum_oxia_client::Result;
use mauricebarnum_oxia_client::config::Config;

// use tracing::{debug, error, info, span, warn};
// use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<()> {
    // fmt()
    //     .with_env_filter(EnvFilter::new("tonic=trace,hyper=trace,h2=trace"))
    //     .init();

    // Replace with your Oxia server address
    let server_address = "localhost:6648";

    let config = Config::builder().service_addr(server_address).build();
    let mut client = Client::new(config);
    client.connect().await?;

    // let key = "k".to_string();
    // let val = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    // println!("{:?}", client.get(&key).await?);
    scan_dump(&client).await?;

    // client.put(&key, val).await?;
    // println!("{:?}", client.get(&key).await?);

    Ok(())
}

async fn scan_dump(client: &Client) -> Result<()> {
    let result = client.range_scan("", "").await?;
    for (i, v) in result.records.iter().enumerate() {
        println!("scan {} {:?}", i, &v);
    }
    Ok(())
}
