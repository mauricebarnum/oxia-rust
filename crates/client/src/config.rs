use core::time;
use std::sync::Arc;

use crate::{Error, Result};
use tonic::transport::ClientTlsConfig;

#[derive(Clone, Debug)]
pub struct Config {
    service_addr: String,         // must not be empty
    namespace: String,               // may be empty
    identity: String,                // used for ephemeral records, may be empty
    session_timeout: time::Duration, // if non-zero, create a session with the specified timeout
    max_parallel_requests: usize,    // maximum number of parallel requests if non-zero
    tls_config: ClientTlsConfig,
}

impl Config {
    pub fn service_addr(&self) -> &str { &self.service_addr }
    pub fn namespace(&self) -> &str { &self.namespace }
    pub fn identity(&self) -> &str { &self.identity }
    pub fn session_timeout(&self) -> time::Duration { self.session_timeout }
    pub fn max_parallel_requests(&self) -> usize { self.max_parallel_requests }
    pub fn tls_config(&self) -> &ClientTlsConfig { &self.tls_config }
}

#[derive(Clone)]
pub struct Builder {
    c: Config,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            c: Config{
                service_addr: String::new(),
                namespace: String::new(),
                identity: String::new(),
                session_timeout: Default::default(),
                max_parallel_requests: 0,
                tls_config: Default::default(),
                // ..Default::default()
            }
        }
    }

    pub fn new_from(config: &Config) -> Self {
        Self {
            c: config.clone(),
        }
    }
    
    pub fn service_addr(mut self, x: impl Into<String>) -> Self {
        self.c.service_addr = x.into();
        self
    }
    pub fn namespace(mut self, x: impl Into<String>) -> Self {
        self.c.namespace = x.into();
        self
    }

    pub fn identity(mut self, x: impl Into<String>) -> Self {
        self.c.identity = x.into();
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

    pub fn build(mut self) -> Result<Arc<Config>> {
        if self.c.service_addr.is_empty() {
            return Err(Error::NoServiceAddress);
        }
        if self.c.namespace.is_empty() {
            self.c.namespace = "default".to_string();
        }
        Ok(Arc::new(self.c))
    }
}
