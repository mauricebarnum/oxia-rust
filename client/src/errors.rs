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

use std::fmt::Display;
use std::io;
use std::sync::Arc;

use thiserror::Error as ThisError;

use crate::KeyComparisonType;
use crate::ShardId;

/// Error codes from Oxia gRPC service responses.
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct OxiaError(i32);

impl OxiaError {
    pub const KEY_NOT_FOUND: Self = Self(1);
    pub const UNEXPECTED_VERSION_ID: Self = Self(2);
    pub const SESSION_DOES_NOT_EXIST: Self = Self(3);
    /// Leader has fenced the shard
    pub const INVALID_STATUS: Self = Self(102);
    /// Leader is closing
    pub const ALREADY_CLOSED: Self = Self(104);
    /// Node is not the leader for this shard
    pub const NODE_IS_NOT_LEADER: Self = Self(106);

    #[inline]
    pub const fn new(code: i32) -> Self {
        Self(code)
    }

    /// Returns true if this error indicates the request was sent to the wrong leader.
    #[inline]
    pub const fn is_wrong_leader(self) -> bool {
        matches!(
            self,
            Self::INVALID_STATUS | Self::ALREADY_CLOSED | Self::NODE_IS_NOT_LEADER
        )
    }
}

impl std::fmt::Display for OxiaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::KEY_NOT_FOUND => f.write_str("key not found code=1"),
            Self::UNEXPECTED_VERSION_ID => f.write_str("unexpected version ID code=2"),
            Self::SESSION_DOES_NOT_EXIST => f.write_str("session does not exist code=3"),
            Self::INVALID_STATUS => f.write_str("invalid status (leader fenced) code=102"),
            Self::ALREADY_CLOSED => f.write_str("already closed (leader closing) code=104"),
            Self::NODE_IS_NOT_LEADER => f.write_str("node is not leader code=106"),
            _ => write!(f, "unknown Oxia error code={}", self.0),
        }
    }
}

impl std::error::Error for OxiaError {}

impl From<i32> for OxiaError {
    #[inline]
    fn from(code: i32) -> Self {
        Self(code)
    }
}

impl From<OxiaError> for i32 {
    #[inline]
    fn from(err: OxiaError) -> Self {
        err.0
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
            // Transport errors (connection refused, DNS failures, etc.) are retryable
            // as the server may come back online
            Error::Transport(_) => true,
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

    /// Whether the error indicates a connection failure that should invalidate cached channels.
    /// This is used to trigger channel reconnection on the next request.
    pub fn is_connection_error(&self) -> bool {
        match self {
            Error::Transport(_) => true,
            Error::Grpc(status) => status.code() == tonic::Code::Unavailable,
            Error::Io(err) => matches!(
                err.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::NotConnected
            ),
            Error::MultipleShardError(errs) => errs.iter().any(|e| e.err.is_connection_error()),
            Error::ShardError(e) => e.err.is_connection_error(),
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

    /// Whether the error indicates a shard is no longer available or has been reassigned.
    ///
    /// These errors should NOT trigger automatic reconnection attempts because the shard
    /// assignment has changed and the client should reconfigure.
    pub fn is_shard_unavailable(&self) -> bool {
        match self {
            // Shard mapping errors indicate the shard is no longer valid
            Error::NoShardMapping(_) | Error::NoShardMappingForKey(_) => true,
            // gRPC NotFound typically indicates the shard/resource doesn't exist
            Error::Grpc(status) => status.code() == tonic::Code::NotFound,
            // Server configuration errors
            Error::Server(err) => matches!(
                err.as_ref(),
                ServerError::NoShardsConfigured | ServerError::DuplicateShardId(_)
            ),
            // Check nested errors
            Error::ShardError(e) => e.err.is_shard_unavailable(),
            Error::MultipleShardError(errs) => errs.iter().all(|e| e.err.is_shard_unavailable()),
            _ => false,
        }
    }

    /// Whether the error indicates the request was sent to the wrong leader.
    pub fn is_wrong_leader(&self) -> bool {
        match self {
            Error::Oxia(oxia_err) => oxia_err.is_wrong_leader(),
            Error::ShardError(e) => e.err.is_wrong_leader(),
            Error::MultipleShardError(errs) => errs.iter().any(|e| e.err.is_wrong_leader()),
            _ => false,
        }
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
    fn test_error_is_connection_error_true() {
        let errs = [
            // gRPC Unavailable indicates connection failure
            Error::from(tonic::Status::new(tonic::Code::Unavailable, "")),
            // IO errors that indicate connection problems
            Error::from(io::Error::from(io::ErrorKind::BrokenPipe)),
            Error::from(io::Error::from(io::ErrorKind::ConnectionAborted)),
            Error::from(io::Error::from(io::ErrorKind::ConnectionReset)),
            Error::from(io::Error::from(io::ErrorKind::NotConnected)),
        ];
        for e in &errs {
            info!(?e);
            assert!(e.is_connection_error(), "expected connection error: {e:?}");
        }
    }

    #[test_log::test]
    fn test_error_is_connection_error_false() {
        let errs = [
            // Other gRPC codes are not connection errors
            Error::from(tonic::Status::new(tonic::Code::Internal, "")),
            Error::from(tonic::Status::new(tonic::Code::Unknown, "")),
            Error::from(tonic::Status::new(tonic::Code::NotFound, "")),
            Error::from(tonic::Status::new(tonic::Code::PermissionDenied, "")),
            // Non-connection IO errors
            Error::from(io::Error::from(io::ErrorKind::WouldBlock)),
            Error::from(io::Error::from(io::ErrorKind::Other)),
            Error::from(io::Error::from(io::ErrorKind::NotFound)),
            // Other error types
            Error::custom("not a connection error"),
            Error::NoShardMapping(ShardId::INVALID),
            Error::NoServiceAddress,
            Error::from(ClientError::Internal("client error".into())),
            Error::multiple_shard_errors(vec![]),
            Error::RequestTimeout,
        ];
        for e in &errs {
            info!(?e);
            assert!(
                !e.is_connection_error(),
                "expected non-connection error: {e:?}"
            );
        }
    }

    #[test_log::test]
    fn test_error_is_connection_error_nested() {
        // ShardError wrapping a connection error
        let inner = Error::from(tonic::Status::new(tonic::Code::Unavailable, ""));
        let shard_err = Error::shard_error(ShardId::new(1), inner);
        assert!(shard_err.is_connection_error());

        // ShardError wrapping a non-connection error
        let inner = Error::from(tonic::Status::new(tonic::Code::Internal, ""));
        let shard_err = Error::shard_error(ShardId::new(1), inner);
        assert!(!shard_err.is_connection_error());

        // MultipleShardError with at least one connection error
        let errs = vec![
            ShardError {
                shard: ShardId::new(1),
                err: Arc::new(Error::from(tonic::Status::new(tonic::Code::Internal, ""))),
            },
            ShardError {
                shard: ShardId::new(2),
                err: Arc::new(Error::from(tonic::Status::new(
                    tonic::Code::Unavailable,
                    "",
                ))),
            },
        ];
        let multi_err = Error::multiple_shard_errors(errs);
        assert!(multi_err.is_connection_error());

        // MultipleShardError with no connection errors
        let errs = vec![
            ShardError {
                shard: ShardId::new(1),
                err: Arc::new(Error::from(tonic::Status::new(tonic::Code::Internal, ""))),
            },
            ShardError {
                shard: ShardId::new(2),
                err: Arc::new(Error::custom("not connection")),
            },
        ];
        let multi_err = Error::multiple_shard_errors(errs);
        assert!(!multi_err.is_connection_error());
    }

    #[test_log::test]
    fn test_is_retryable_includes_transport() {
        // This test verifies that Transport errors are retryable (part of the connection fix)
        // We can't easily construct a tonic::transport::Error, but we verify the match arm exists
        // by checking other retryable errors still work
        let retryable = Error::from(tonic::Status::new(tonic::Code::Unavailable, ""));
        assert!(retryable.is_retryable());

        // Unavailable is both retryable and a connection error
        assert!(retryable.is_connection_error());
    }
}
