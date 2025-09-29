use crate::client_lib::Client;
use crate::client_lib::Result;
use crate::client_lib::config;

#[derive(Debug)]
pub struct Context {
    client: Client,
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
        Ok(Self { client })
    }

    pub async fn client(&mut self) -> Result<&mut Client> {
        self.client.connect().await?;
        Ok(&mut self.client)
    }

    pub fn make_subcontext(&self) -> Context {
        Self {
            client: Client::new(self.client.config().clone()),
        }
    }
}
