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
use crate::ShardId;

/// Errors that map directly from the Oxia gRPC service responses
#[derive(ThisError, Debug, Clone, PartialEq, Eq)]
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

/// Client-side logic or state errors
#[derive(Debug, Clone, ThisError)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverlappingRanges {
    pub range1_min: u32,
    pub range1_max: u32,
    pub range2_min: u32,
    pub range2_max: u32,
}

impl Display for OverlappingRanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}-{}] overlaps with [{}-{}]",
            self.range1_min, self.range1_max, self.range2_min, self.range2_max
        )
    }
}

/// Unexpected server responses
#[derive(Debug, Clone, ThisError)]
pub enum ServerError {
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
    DuplicateShardId(ShardId),

    #[error("Overlapping shard ranges: {0}")]
    OverlappingRanges(OverlappingRanges),
}

#[derive(thiserror::Error, Debug, Clone)]
#[error("Shard {shard} error: {err}")]
pub struct ShardError {
    pub shard: ShardId,
    #[source]
    pub err: Arc<Error>,
}

/// Error error type returned in the public API
///
/// Arc-wrapping strategy:
/// 1. Non-Clone types (tonic::Status, io::Error) → always Arc-wrapped
/// 2. Large or nested types (ClientError, ServerError, Vec<ShardError>) → Arc-wrapped
///    to avoid expensive copies across async boundaries and through error propagation
/// 3. Small Copy types (ShardId, i32) → stored inline
/// 4. Strings → stored inline (already heap-allocated, 24 bytes is reasonable to copy)
#[derive(Debug, Clone, ThisError)]
#[non_exhaustive]
pub enum Error {
    /// Arc-wrapped: tonic::Status is not Clone and is large
    #[error("gRPC error: {0}")]
    Grpc(#[source] Arc<tonic::Status>),

    /// Arc-wrapped: tonic::transport::Error is not Clone
    #[error("gRPC transport error: {0}")]
    Transport(Arc<tonic::transport::Error>),

    /// Arc-wrapped: io::Error is not Clone
    #[error("I/O error: {0}")]
    Io(#[source] Arc<io::Error>),

    #[error(transparent)]
    Oxia(#[from] OxiaError),

    /// Arc-wrapped: contains Strings and can be large
    #[error(transparent)]
    Client(#[from] Arc<ClientError>),

    /// Arc-wrapped: contains nested data structures
    #[error(transparent)]
    Server(Arc<ServerError>),

    #[error("No service addresses configured")]
    NoServiceAddress,

    #[error("No shard mapping for shard {0}")]
    NoShardMapping(ShardId),

    #[error("No shard mapping for key '{0}'")]
    NoShardMappingForKey(String),

    #[error("No response from server for {0}")]
    NoResponseFromServer(String),

    /// Arc-wrapped: Vec of errors can be arbitrarily large
    #[error("Multiple shard errors: {}", format_shard_errors(.0))]
    MultipleShardError(Arc<[ShardError]>),

    #[error("Shard error: {0}")]
    ShardError(#[from] ShardError),

    #[error("Request timed out")]
    RequestTimeout,

    #[error("Operation cancelled")]
    Cancelled,

    #[error("Invalid KeyComparisonType value {0}")]
    InvalidKeyComparisonType(i32),

    #[error("{0}")]
    Custom(String),

    /// Arc-wrapped: for arbitrary external errors
    #[error("Other error: {0}")]
    Other(Arc<dyn std::error::Error + Send + Sync>),
}

fn format_shard_errors(errors: &[ShardError]) -> String {
    if errors.is_empty() {
        return "no errors".to_string();
    }
    if errors.len() == 1 {
        return format!("shard {}: {}", errors[0].shard, errors[0].err);
    }
    format!("{} errors", errors.len())
}

impl Error {
    /// Whether the error is likely transient and worth retrying
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Grpc(status) => matches!(
                status.code(),
                tonic::Code::Unavailable | tonic::Code::Unknown | tonic::Code::Internal
            ),
            Error::Io(err) => matches!(
                err.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::WouldBlock
            ),
            Error::MultipleShardError(errs) => errs.iter().any(|e| e.err.is_retryable()),
            Error::ShardError(e) => e.err.is_retryable(),
            Error::RequestTimeout => false,
            _ => false,
        }
    }

    pub fn custom(msg: impl Into<String>) -> Self {
        Error::Custom(msg.into())
    }

    pub fn shard_error(shard: ShardId, err: Error) -> Self {
        Error::ShardError(ShardError {
            shard,
            err: err.into(),
        })
    }

    pub fn multiple_shard_errors(errors: Vec<ShardError>) -> Self {
        Error::MultipleShardError(errors.into())
    }

    pub fn other(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Error::Other(Arc::new(err))
    }
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        let code = status.code();
        let is_timeout = code == tonic::Code::DeadlineExceeded
            || (code == tonic::Code::Cancelled && status.message() == "Timeout expired");

        if is_timeout {
            Error::RequestTimeout
        } else if code == tonic::Code::Cancelled {
            Error::Cancelled
        } else {
            Error::Grpc(Arc::new(status))
        }
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        Error::Transport(Arc::new(err))
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        if err.kind() == io::ErrorKind::TimedOut {
            Error::RequestTimeout
        } else {
            Error::Io(Arc::new(err))
        }
    }
}

impl From<ClientError> for Error {
    fn from(err: ClientError) -> Self {
        Error::Client(Arc::new(err))
    }
}

impl From<ServerError> for Error {
    fn from(err: ServerError) -> Self {
        Error::Server(Arc::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::info;

    #[test_log::test]
    fn test_error_is_retryable_true() {
        let errs = [
            Error::from(tonic::Status::new(tonic::Code::Internal, "")),
            Error::from(tonic::Status::new(tonic::Code::Unavailable, "")),
            Error::from(tonic::Status::new(tonic::Code::Unknown, "")),
            Error::from(io::Error::from(io::ErrorKind::BrokenPipe)),
            Error::from(io::Error::from(io::ErrorKind::ConnectionAborted)),
            Error::from(io::Error::from(io::ErrorKind::ConnectionReset)),
            Error::from(io::Error::from(io::ErrorKind::NotConnected)),
            Error::from(io::Error::from(io::ErrorKind::WouldBlock)),
        ];
        for e in &errs {
            info!(?e);
            assert!(e.is_retryable());
        }
    }

    #[test_log::test]
    fn test_error_is_retryable_false() {
        let errs = [
            Error::custom("not retriable"),
            Error::NoShardMapping(ShardId::INVALID),
            Error::NoServiceAddress,
            Error::from(ClientError::Internal("client error".into())),
            Error::multiple_shard_errors(vec![]),
            Error::RequestTimeout,
        ];
        for e in &errs {
            info!(?e);
            assert!(!e.is_retryable());
        }
    }

    #[test_log::test]
    fn test_error_cloneable() {
        // Clone without heap allocation (inline variants)
        let err1 = Error::custom("test");
        let cloned1 = err1.clone();
        assert!(matches!(cloned1, Error::Custom(_)));

        let err2 = Error::NoServiceAddress;
        let _cloned2 = err2.clone();

        let io_err = Error::from(io::Error::from(io::ErrorKind::NotFound));
        let cloned_io = io_err.clone();
        if let (Error::Io(arc1), Error::Io(arc2)) = (&io_err, &cloned_io) {
            assert!(Arc::ptr_eq(arc1, arc2));
        }

        // Nested errors are cheap to clone
        let nested = Error::shard_error(ShardId::new(1), Error::custom("nested"));
        let cloned_nested = nested.clone();
        if let (
            Error::ShardError(ShardError { err: s1, .. }),
            Error::ShardError(ShardError { err: s2, .. }),
        ) = (&nested, &cloned_nested)
        {
            assert!(Arc::ptr_eq(s1, s2));
        }
    }

    #[test_log::test]
    fn test_error_conversions() {
        let _: Error = io::Error::from(io::ErrorKind::Other).into();
        let _: Error = OxiaError::KeyNotFound.into();
        let _: Error = ClientError::Internal("test".into()).into();
    }

    #[test_log::test]
    fn test_shard_error_helper() {
        let inner = Error::custom("inner error");
        let shard_err = Error::shard_error(ShardId::new(42), inner);
        assert!(matches!(shard_err, Error::ShardError { .. }));
    }

    #[test_log::test]
    fn test_other_error() {
        let io_err = io::Error::from(io::ErrorKind::Other);
        let err = Error::other(io_err);
        assert!(matches!(err, Error::Other(_)));

        // Verify cloning works and shares Arc
        let cloned = err.clone();
        if let (Error::Other(arc1), Error::Other(arc2)) = (&err, &cloned) {
            assert!(Arc::ptr_eq(arc1, arc2));
        }
    }
}
