use mauricebarnum_oxia_client::*;
// use tracing::{debug, error, info, span, warn};
// use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<()> {
    // fmt()
    //     .with_env_filter(EnvFilter::new("tonic=trace,hyper=trace,h2=trace"))
    //     .init();

    // Replace with your Oxia server address
    let server_address = "localhost:6648";

    let config = crate::config::Builder::new()
        .service_addr(server_address)
        .build()?;
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
