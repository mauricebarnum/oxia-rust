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

use std::fmt::Display;
use std::io;
use std::sync::Arc;

use thiserror::Error as ThisError;

use crate::KeyComparisonType;

/// Errors that map directly from the Oxia gRPC service responses (excluding success/OK)
#[derive(ThisError, Debug, PartialEq, Eq)]
pub enum OxiaError {
    #[error("Key not found")]
    KeyNotFound,

    #[error("Unexpected version ID")]
    UnexpectedVersionId,

    #[error("Session does not exist")]
    SessionDoesNotExist,

    #[error("Unknown Oxia status code: {0}")]
    Code(i32),
}

impl From<i32> for OxiaError {
    fn from(value: i32) -> Self {
        match value {
            1 => OxiaError::KeyNotFound,
            2 => OxiaError::UnexpectedVersionId,
            3 => OxiaError::SessionDoesNotExist,
            n => OxiaError::Code(n),
        }
    }
}

/// Client-side logic or state errors, not originating from the server
#[derive(Debug, ThisError)]
pub enum ClientError {
    #[error("Inconsistent shard assignment: expected {expected}, got {actual}")]
    InconsistentAssignment { expected: String, actual: String },

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Unexpected internal state: {0}")]
    Internal(String),

    #[error("Unsupported key comparison {0}")]
    UnsupportedKeyComparator(KeyComparisonType),
}

#[derive(Debug)]
pub struct OverlappingRangesData {
    pub range1_min: u32,
    pub range1_max: u32,
    pub range2_min: u32,
    pub range2_max: u32,
}

impl Display for OverlappingRangesData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Overlapping shard ranges: [{range1_min}-{range1_max}] overlaps with [{range2_min}-{range2_max}]",
            range1_min = self.range1_min,
            range1_max = self.range1_max,
            range2_min = self.range2_min,
            range2_max = self.range2_max,
        )
    }
}

/// Unexpected server responses
#[derive(Debug, ThisError)]
pub enum UnexpectedServerResponse {
    #[error("Partial response from multiple-shard request")]
    PartialResponse,

    #[error("Unsupported shard key router configuration: {0}")]
    BadShardKeyRouter(String),

    #[error("No shard boundaries defined for a shard assignment")]
    NoShardBoundaries,

    #[error("No shards configured")]
    NoShardsConfigured,

    #[error(
        "Invalid maximum boundary at {boundary}, only the last shard can have the maximum value"
    )]
    InvalidMaxBoundary { boundary: u32 },

    #[error("Duplicate shard ID: {0}")]
    DuplicateShardId(i64),

    #[error("Overlapping shard ranges: {0}")]
    OverlappingRanges(Box<OverlappingRangesData>),
}

#[derive(Debug)]
pub struct ShardError {
    pub shard: i64,
    pub err: Error,
}

#[derive(Debug, ThisError)]
#[non_exhaustive]
pub enum Error {
    #[error("gRPC error: {0}")]
    TonicStatus(#[source] Box<tonic::Status>),

    #[error("gRPC transport error: {0}")]
    TonicTransport(#[from] tonic::transport::Error),

    #[error("I/O error: {0}")]
    Io(#[source] io::Error),

    #[error("Oxia protocol error: {0}")]
    OxiaError(#[from] OxiaError),

    #[error("Client error: {0}")]
    Client(#[from] ClientError),

    #[error("No service addresses")]
    NoServiceAddress,

    #[error("Unexpected server response: {0}")]
    UnexpectedServerResponse(#[from] UnexpectedServerResponse),

    #[error("No shard mapping for {0}")]
    NoShardMapping(i64),

    #[error("Custom error: {0}")]
    Custom(String),

    #[error("Unknown boxed error: {0}")]
    Boxed(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Multiple shard errors")]
    MultipleShardError(Vec<ShardError>),

    #[error("Shard error")]
    ShardError(Box<ShardError>),

    #[error("Request time out")]
    RequestTimeout {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Shared error: {0}")]
    Shared(#[source] Arc<Error>),

    #[error("Cancelled")]
    Cancelled,

    #[error("Invalid KeyComparisonType value {0}")]
    InvalidKeyComparisonTypeValue(i32),

    #[error("No shard mapping for key {0}")]
    NoShardMappingForKey(String),
}

impl Error {
    /// Whether the error is likely transient and worth retrying
    pub fn is_retryable(&self) -> bool {
        if let Error::TonicStatus(e) = self
            && matches!(
                e.code(),
                tonic::Code::Unavailable | tonic::Code::Unknown | tonic::Code::Internal
            )
        {
            return true;
        }

        if let Error::RequestTimeout { .. } = self {
            return false;
        }

        if let Some(e) = self.as_io_error() {
            use io::ErrorKind;
            return matches!(
                e.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::BrokenPipe
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::NotConnected
                    | ErrorKind::WouldBlock
            );
        }

        false
    }

    fn as_io_error(&self) -> Option<&io::Error> {
        if let Error::MultipleShardError(errs) = self {
            return errs
                .iter()
                .find_map(|boxed_error| boxed_error.err.as_io_error());
        }

        let mut source = Some(self as &dyn std::error::Error);
        while let Some(err) = source {
            if let Some(io_err) = err.downcast_ref::<io::Error>() {
                return Some(io_err);
            }
            source = err.source();
        }
        None
    }

    pub(crate) fn from_tokio_elapsed(e: tokio::time::error::Elapsed) -> Self {
        Error::RequestTimeout {
            source: Box::new(e),
        }
    }
}

impl From<tonic::Status> for Error {
    fn from(value: tonic::Status) -> Self {
        let as_timeout = match value.code() {
            tonic::Code::DeadlineExceeded => true,
            tonic::Code::Cancelled => value.message() == "Timeout expired",
            _ => false,
        };
        let boxed = Box::new(value);
        if as_timeout {
            Error::RequestTimeout { source: boxed }
        } else {
            Error::TonicStatus(boxed)
        }
    }
}

// impl From<tokio::time::error::Elapsed> for Error {
//     fn from(value: tokio::time::error::Elapsed) -> Self {
//     }
// }

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        if value.kind() == io::ErrorKind::TimedOut {
            Error::RequestTimeout {
                source: Box::new(value),
            }
        } else {
            Error::Io(value)
        }
    }
}

impl From<Arc<Error>> for Error {
    fn from(value: Arc<Error>) -> Self {
        Error::Shared(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::info;

    #[test_log::test]
    fn test_error_is_retryable_true() {
        let errs = [
            Error::TonicStatus(tonic::Status::new(tonic::Code::Internal, "").into()),
            Error::TonicStatus(tonic::Status::new(tonic::Code::Unavailable, "").into()),
            Error::TonicStatus(tonic::Status::new(tonic::Code::Unknown, "").into()),
            Error::Io(io::ErrorKind::BrokenPipe.into()),
            Error::Io(io::ErrorKind::ConnectionAborted.into()),
            Error::Io(io::ErrorKind::ConnectionReset.into()),
            Error::Io(io::ErrorKind::NotConnected.into()),
            Error::Io(io::ErrorKind::WouldBlock.into()),
        ];
        for e in &errs {
            info!(?e);
            assert!(e.is_retryable());
        }
    }

    #[test_log::test]
    fn test_error_is_retryable_false() {
        let errs = [
            Error::Custom("not retriable".into()),
            Error::NoShardMapping(-1),
            Error::NoServiceAddress,
            Error::Client(ClientError::Internal("client error".into())),
            Error::MultipleShardError(vec![]),
            Error::RequestTimeout {
                source: Box::new(io::Error::new(io::ErrorKind::TimedOut, "")),
            },
        ];
        for e in &errs {
            info!(?e);
            assert!(!e.is_retryable());
        }
    }
}
