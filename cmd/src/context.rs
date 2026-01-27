// Copyright 2026 Maurice S. Barnum
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

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::client_lib::Client;
use crate::client_lib::Result;
use crate::client_lib::config;

#[derive(Clone, Debug)]
struct State {
    client: Client,
}

#[derive(Clone, Debug)]
pub struct Context {
    state: Arc<Mutex<State>>,
}

impl Context {
    pub(super) fn new(cli: &crate::Cli) -> Self {
        let config = config::Config::builder()
            .service_addr(cli.service_address.clone())
            .max_parallel_requests(cli.max_parallel_requests)
            .session_timeout(cli.session_timeout)
            .request_timeout(cli.request_timeout)
            .build();

        let client = Client::new(config);
        Self {
            state: Arc::new(Mutex::new(State { client })),
        }
    }

    pub async fn client(&self) -> Result<Client> {
        let mut state = self.state.lock().await;
        state.client.connect().await?;
        Ok(state.client.clone())
    }

    pub async fn make_subcontext(&self) -> Context {
        let state = self.state.lock().await;
        let client = Client::new(Arc::clone(state.client.config()));
        Self {
            state: Arc::new(Mutex::new(State { client })),
        }
    }
}
