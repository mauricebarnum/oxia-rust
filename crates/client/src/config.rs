use core::time;
use std::sync::Arc;

use crate::Result;
use tonic::transport::ClientTlsConfig;

#[derive(Debug, Default)]
pub struct Config {
    pub service_address: String,         // must not be empty
    pub namespace: String,               // may be empty
    pub identity: String,                // used for ephemeral records, may be empty
    pub session_timeout: time::Duration, // if non-zero, create a session with the specified timeout
    pub max_parallel_requests: usize,    // maximum number of parallel requests
    pub tls_config: ClientTlsConfig,
}

pub struct Builder {
    c: Config,
}

impl Builder {
    pub fn new(service_address: &str) -> Result<Self> {
        if service_address.is_empty() {
            return Err(crate::Error::Custom(
                "Service address cannot be empty".to_string(),
            ));
        }
        Ok(Self {
            c: Config {
                service_address: service_address.to_string(),
                ..Default::default()
            },
        })
    }

    pub fn namespace(mut self, x: impl AsRef<str>) -> Self {
        self.c.namespace = x.as_ref().to_string();
        self
    }

    pub fn identity(mut self, x: impl AsRef<str>) -> Self {
        self.c.identity = x.as_ref().to_string();
        self
    }

    pub fn session_timeout(mut self, x: time::Duration) -> Self {
        self.c.session_timeout = x;
        self
    }

    pub fn max_parallel_requests(mut self, x: usize) -> Self {
        self.c.max_parallel_requests = x;
        self
    }

    pub fn tls_config(mut self, x: ClientTlsConfig) -> Self {
        self.c.tls_config = x;
        self
    }

    pub fn build(mut self) -> Arc<Config> {
        if self.c.namespace.is_empty() {
            self.c.namespace = "default".to_string();
        }
        Arc::new(self.c)
    }
}
