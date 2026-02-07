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

#![cfg(not(miri))]

use mauricebarnum_oxia_client::config;
use std::time::Duration;
use tracing::info;

mod common;
use common::TestResultExt;

async fn do_test_disconnect(retry: bool) -> anyhow::Result<()> {
    let mut server = common::TestServer::start()?;
    let builder = config::Config::builder()
        .max_parallel_requests(3)
        .request_timeout(Duration::from_millis(300))
        .session_timeout(Duration::from_millis(2001))
        .maybe_retry(if retry {
            Some(config::RetryConfig::new(3, Duration::from_millis(100)))
        } else {
            None
        });

    let client = server.connect_with(builder).await?;
    assert_eq!(None, trace_err!(client.get("foo").await)?);

    trace_err!(client.put("foo", "bar").await)?;
    assert!(trace_err!(client.get("foo").await)?.is_some());

    info!("restarting test server");
    trace_err!(server.restart())?;

    let r = trace_err!(client.get("foo").await)?;
    info!(op = "get", ?r);
    assert!(r.is_some());

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_disconnect() -> anyhow::Result<()> {
    do_test_disconnect(true).await
}

#[test_log::test(tokio::test)]
async fn test_disconnect_fail() {
    let r = do_test_disconnect(false).await;
    assert!(r.is_err());
}
