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

use std::sync::Arc;
use std::time::Duration;

use crate::Error;
use crate::Result;

#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    pub(crate) attempts: usize,
    pub(crate) initial_delay: Duration,
    pub(crate) max_delay: Duration,
}

impl RetryConfig {
    pub fn new(attempts: usize, initial_delay: Duration) -> Self {
        Self {
            attempts,
            initial_delay,
            max_delay: initial_delay.saturating_mul(10),
        }
    }

    #[must_use]
    pub fn max(mut self, x: Duration) -> Self {
        self.max_delay = x;
        self
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    service_addr: String,              // must not be empty
    namespace: String,                 // may Option<be empty
    identity: String,                  // used for ephemeral records, may be empty
    session_timeout: Duration,         // if non-zero, create a session with the specified timeout
    max_parallel_requests: usize,      // maximum number of parallel requests if non-zero
    request_timeout: Option<Duration>, // timeout if non-zero
    retry: Option<RetryConfig>,
}

impl Config {
    pub fn service_addr(&self) -> &str {
        &self.service_addr
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn identity(&self) -> &str {
        &self.identity
    }
    pub fn session_timeout(&self) -> Duration {
        self.session_timeout
    }
    pub fn max_parallel_requests(&self) -> usize {
        self.max_parallel_requests
    }
    pub fn request_timeout(&self) -> Option<Duration> {
        self.request_timeout
    }
    pub fn retry(&self) -> Option<RetryConfig> {
        self.retry
    }
}

#[derive(Clone)]
pub struct Builder {
    c: Config,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            c: Config {
                service_addr: String::new(),
                namespace: String::new(),
                identity: String::new(),
                session_timeout: Duration::default(),
                max_parallel_requests: 1,
                request_timeout: None,
                retry: None,
            },
        }
    }

    pub fn new_from(config: &Config) -> Self {
        Self { c: config.clone() }
    }

    #[must_use]
    pub fn service_addr(mut self, x: impl Into<String>) -> Self {
        self.c.service_addr = x.into();
        self
    }

    #[must_use]
    pub fn namespace(mut self, x: impl Into<String>) -> Self {
        self.c.namespace = x.into();
        self
    }

    #[must_use]
    pub fn identity(mut self, x: impl Into<String>) -> Self {
        self.c.identity = x.into();
        self
    }

    #[must_use]
    pub fn session_timeout(mut self, x: Duration) -> Self {
        self.c.session_timeout = x;
        self
    }

    #[must_use]
    pub fn max_parallel_requests(mut self, x: usize) -> Self {
        self.c.max_parallel_requests = x.min(1);
        self
    }

    #[must_use]
    pub fn request_timeout(mut self, x: Duration) -> Self {
        self.c.request_timeout = if x.is_zero() { None } else { Some(x) };
        self
    }

    #[must_use]
    pub fn retry(mut self, x: RetryConfig) -> Self {
        self.c.retry = Some(x);
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

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}
