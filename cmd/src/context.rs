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
    pub(super) fn new(cli: &crate::Cli) -> Result<Self> {
        let config = config::Builder::new()
            .service_addr(cli.service_address.clone())
            .max_parallel_requests(cli.max_parallel_requests)
            .session_timeout(cli.session_timeout)
            .request_timeout(cli.request_timeout)
            .build()?;

        let client = Client::new(config);
        Ok(Self {
            state: Arc::new(Mutex::new(State { client })),
        })
    }

    pub async fn client(&self) -> Result<Client> {
        let mut state = self.state.lock().await;
        state.client.connect().await?;
        Ok(state.client.clone())
    }

    pub async fn make_subcontext(&self) -> Context {
        let state = self.state.lock().await;
        let client = Client::new(state.client.config().clone());
        Self {
            state: Arc::new(Mutex::new(State { client })),
        }
    }
}
