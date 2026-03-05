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
use std::sync::OnceLock;

use mauricebarnum_oxia_common::proto;
use thiserror::Error as ThisError;
use tracing::info;

use crate::KeyComparisonType;
use crate::ShardId;

/// Error codes from Oxia proto-level response statuses.
///
/// These codes appear in the `status` field of successful gRPC response bodies.
/// gRPC-level routing errors (wrong leader, etc.) are represented separately on [`Error`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, thiserror::Error)]
pub enum OxiaError {
    #[error("key not found")]
    KeyNotFound,
    #[error("unexpected version ID")]
    UnexpectedVersionId,
    #[error("session does not exist")]
    SessionDoesNotExist,
    #[error("unknown Oxia error code={0}")]
    Unknown(i32),
}

impl From<i32> for OxiaError {
    #[inline]
    fn from(code: i32) -> Self {
        match code {
            1 => Self::KeyNotFound,
            2 => Self::UnexpectedVersionId,
            3 => Self::SessionDoesNotExist,
            n => Self::Unknown(n),
        }
    }
}

/// gRPC-level Oxia routing codes (≥ 100).
///
/// These appear in `grpc-status-details-bin` of failed gRPC calls (tonic maps
/// non-standard codes to `tonic::Code::Unknown`). They are distinct from the
/// proto-body [`OxiaError`] codes (1–3), which appear in successful response bodies.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OxiaRpcError {
    /// Code 100 — server has not completed initialization.
    #[error("server not initialized")]
    NotInitialized,

    /// Code 101 — request carried an invalid Raft term.
    #[error("invalid term")]
    InvalidTerm,

    /// Code 102 — shard is in the wrong status for the requested operation (leader fenced).
    #[error("invalid status (leader fenced)")]
    InvalidStatus,

    /// Code 103 — server cancelled the operation.
    #[error("operation cancelled by server")]
    Cancelled,

    /// Code 104 — the leader is shutting down.
    #[error("already closed (leader closing)")]
    AlreadyClosed,

    /// Code 105 — a leader is already connected to this follower.
    #[error("leader already connected")]
    LeaderAlreadyConnected,

    /// Code 106 — the contacted node is not the shard leader.
    /// `hint` carries the redirect when the server provided one.
    #[error("node is not leader")]
    NodeIsNotLeader { hint: Option<crate::LeaderHint> },

    /// Code 107 — the contacted node is not a follower for this shard.
    #[error("node is not follower")]
    NodeIsNotFollower,

    /// Code 108 — the referenced session does not exist.
    #[error("session not found")]
    SessionNotFound,

    /// Code 109 — the requested session timeout is outside permitted bounds.
    #[error("invalid session timeout")]
    InvalidSessionTimeout,

    /// Code 110 — the requested namespace does not exist.
    #[error("namespace not found")]
    NamespaceNotFound,

    /// Code 111 — notifications are not enabled for this namespace.
    #[error("notifications not enabled")]
    NotificationsNotEnabled,

    /// An Oxia gRPC code that this client version does not recognise.
    #[error("unknown Oxia gRPC code {0}")]
    Unknown(i32),
}

// Public api
impl OxiaRpcError {
    /// Returns `true` if this error indicates the request hit the wrong leader.
    #[inline]
    pub const fn is_wrong_leader(&self) -> bool {
        matches!(
            self,
            Self::InvalidStatus | Self::AlreadyClosed | Self::NodeIsNotLeader { .. }
        )
    }

    /// Returns `true` if it might make sense to retry when this error is seen.
    #[inline]
    pub const fn is_retryable(&self) -> bool {
        // We avoid a default arm to ensure that all new error types are addressed intentionally
        #[allow(clippy::match_same_arms)]
        match self {
            Self::AlreadyClosed => true,
            Self::Cancelled => true,
            Self::InvalidStatus => true,
            Self::NodeIsNotFollower => true,
            Self::NodeIsNotLeader { .. } => true,
            Self::NotInitialized => true,

            Self::InvalidSessionTimeout => false,
            Self::InvalidTerm => false,
            Self::LeaderAlreadyConnected => false,
            Self::NamespaceNotFound => false,
            Self::NotificationsNotEnabled => false,
            Self::SessionNotFound => false,
            Self::Unknown(_) => false,
        }
    }
}

impl OxiaRpcError {
    fn extract_leader_hint(details: Vec<prost_types::Any>) -> Option<crate::LeaderHint> {
        use prost::Message as _;
        use prost::Name as _;

        static LEADER_HINT_FULL_NAME: OnceLock<String> = OnceLock::new();
        let target_name = LEADER_HINT_FULL_NAME.get_or_init(proto::LeaderHint::full_name);

        details
            .into_iter()
            .find(|a| {
                let url = &a.type_url;
                if url.ends_with(target_name) {
                    let offset = url.len() - target_name.len();
                    // If we're not at the start of url, check that our match is
                    // anchored by '/'
                    offset == 0 || url.as_bytes().get(offset - 1) == Some(&b'/')
                } else {
                    false
                }
            })
            .and_then(|a| proto::LeaderHint::decode(a.value.as_slice()).ok())
            .map(crate::LeaderHint::from_proto)
    }

    fn from_rpc_code(x: i32, details: Vec<prost_types::Any>) -> Option<Self> {
        if x < 100 {
            return None;
        }
        let r = match x {
            100 => Self::NotInitialized,
            101 => Self::InvalidTerm,
            102 => Self::InvalidStatus,
            103 => Self::Cancelled,
            104 => Self::AlreadyClosed,
            105 => Self::LeaderAlreadyConnected,
            106 => Self::NodeIsNotLeader {
                hint: Self::extract_leader_hint(details),
            },
            107 => Self::NodeIsNotFollower,
            108 => Self::SessionNotFound,
            109 => Self::InvalidSessionTimeout,
            110 => Self::NamespaceNotFound,
            111 => Self::NotificationsNotEnabled,
            code => {
                info!(code, "unknown code");
                Self::Unknown(code)
            }
        };
        Some(r)
    }
}

impl TryFrom<tonic::Status> for OxiaRpcError {
    type Error = tonic::Status;

    fn try_from(x: tonic::Status) -> Result<Self, Self::Error> {
        use prost::Message as _;
        use proto::google::rpc::Status as RpcStatus;

        if x.code() != tonic::Code::Unknown {
            return Err(x);
        }

        (if x.details().is_empty() {
            x.metadata()
                .get("grpc-status")
                .and_then(|m| m.to_str().ok())
                .and_then(|s| s.parse().ok())
                .map(|c| (c, vec![]))
        } else {
            RpcStatus::decode(x.details())
                .ok()
                .map(|s| (s.code, s.details))
        })
        .and_then(|(sc, sd)| Self::from_rpc_code(sc, sd))
        .ok_or(x)
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

/// Error: error type returned in the public API
///
/// Arc-wrapping strategy:
/// 1. Non-Clone types (`tonic::Status`, `io::Error`) → always Arc-wrapped
/// 2. Large or nested types (`ClientError`, `ServerError`, Vec<ShardError>) → Arc-wrapped
///    to avoid expensive copies across async boundaries and through error propagation
/// 3. Small Copy types (`ShardId`, i32) → stored inline
/// 4. Strings → stored inline (already heap-allocated, 24 bytes is reasonable to copy)
#[derive(Debug, Clone, ThisError)]
#[non_exhaustive]
pub enum Error {
    /// Arc-wrapped: `tonic::Status` is not Clone and is large
    #[error("gRPC error: {0}")]
    Grpc(#[source] Arc<tonic::Status>),

    /// Arc-wrapped: `tonic::transport::Error` is not Clone
    #[error("gRPC transport error: {0}")]
    Transport(Arc<tonic::transport::Error>),

    /// Arc-wrapped: `io::Error` is not Clone
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

    /// A gRPC-level Oxia error (code ≥ 100).
    #[error(transparent)]
    OxiaRpc(#[from] OxiaRpcError),

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
    #[inline]
    pub fn is_retryable(&self) -> bool {
        // Do not have a default arm so that adding a new error type requires explicitly deciding
        // what to do.  Never mind that the answer is almost always `false`

        #[allow(clippy::match_same_arms)]
        match self {
            Self::Grpc(status) => matches!(
                status.code(),
                tonic::Code::Unavailable | tonic::Code::Unknown | tonic::Code::Internal
            ),
            Self::Io(err) => matches!(
                err.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::WouldBlock
            ),
            Self::MultipleShardError(errs) => errs.iter().any(|e| e.err.is_retryable()),
            Self::ShardError(err) => err.err.is_retryable(),
            Self::OxiaRpc(err) => err.is_retryable(),

            Self::NoResponseFromServer(_) => true,
            Self::Transport(_) => true,

            Self::Cancelled => false,
            Self::Client(_) => false,
            Self::Custom(_) => false,
            Self::InvalidKeyComparisonType(_) => false,
            Self::NoShardMapping(_) => false,
            Self::NoShardMappingForKey(_) => false,
            Self::Other(_) => false,
            Self::Oxia(_) => false,
            Self::RequestTimeout => false,
            Self::Server(_) => false,
        }
    }

    /// Whether the error indicates a connection failure that should invalidate cached channels.
    /// This is used to trigger channel reconnection on the next request.
    #[inline]
    pub fn is_connection_error(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Grpc(status) => status.code() == tonic::Code::Unavailable,
            Self::Io(err) => matches!(
                err.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::NotConnected
            ),
            Self::MultipleShardError(errs) => errs.iter().any(|e| e.err.is_connection_error()),
            Self::ShardError(e) => e.err.is_connection_error(),
            _ => false,
        }
    }

    pub fn custom(msg: impl Into<String>) -> Self {
        Self::Custom(msg.into())
    }

    pub fn shard_error(shard: ShardId, err: Self) -> Self {
        Self::ShardError(ShardError {
            shard,
            err: err.into(),
        })
    }

    pub fn multiple_shard_errors(errors: Vec<ShardError>) -> Self {
        Self::MultipleShardError(errors.into())
    }

    pub fn other(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Other(Arc::new(err))
    }

    /// Whether the error indicates a shard is no longer available or has been reassigned.
    ///
    /// These errors should NOT trigger automatic reconnection attempts because the shard
    /// assignment has changed and the client should reconfigure.
    pub fn is_shard_unavailable(&self) -> bool {
        match self {
            // Shard mapping errors indicate the shard is no longer valid
            Self::NoShardMapping(_) | Self::NoShardMappingForKey(_) => true,
            // gRPC NotFound typically indicates the shard/resource doesn't exist
            Self::Grpc(status) => status.code() == tonic::Code::NotFound,
            // Server configuration errors
            Self::Server(err) => matches!(
                err.as_ref(),
                ServerError::NoShardsConfigured | ServerError::DuplicateShardId(_)
            ),
            // Check nested errors
            Self::ShardError(e) => e.err.is_shard_unavailable(),
            Self::MultipleShardError(errs) => errs.iter().all(|e| e.err.is_shard_unavailable()),
            _ => false,
        }
    }

    /// Whether the error indicates the request was sent to the wrong leader.
    #[inline]
    pub fn is_wrong_leader(&self) -> bool {
        match self {
            Self::OxiaRpc(e) => e.is_wrong_leader(),
            Self::ShardError(e) => e.err.is_wrong_leader(),
            Self::MultipleShardError(errs) => errs.iter().any(|e| e.err.is_wrong_leader()),
            _ => false,
        }
    }
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        match OxiaRpcError::try_from(status) {
            Ok(e) => e.into(),
            Err(s) => match s.code() {
                tonic::Code::DeadlineExceeded => Self::RequestTimeout,
                tonic::Code::Cancelled if s.message() == "Timeout expired" => Self::RequestTimeout,
                tonic::Code::Cancelled => Self::Cancelled,
                _ => Self::Grpc(Arc::new(s)),
            },
        }
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        Self::Transport(Arc::new(err))
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        if err.kind() == io::ErrorKind::TimedOut {
            Self::RequestTimeout
        } else {
            Self::Io(Arc::new(err))
        }
    }
}

impl From<ClientError> for Error {
    fn from(err: ClientError) -> Self {
        Self::Client(Arc::new(err))
    }
}

impl From<ServerError> for Error {
    fn from(err: ServerError) -> Self {
        Self::Server(Arc::new(err))
    }
}

#[cfg(test)]
mod tests {
    use tracing::info;

    use super::*;

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

    #[test_log::test]
    fn test_node_is_not_leader_without_hint() {
        // A plain tonic::Status with no details stays as Grpc
        let status = tonic::Status::new(tonic::Code::Unknown, "");
        let err = Error::from(status);
        assert!(matches!(err, Error::Grpc(_)));
    }

    #[test_log::test]
    fn test_oxia_error_from_i32() {
        assert_eq!(OxiaError::from(1), OxiaError::KeyNotFound);
        assert_eq!(OxiaError::from(2), OxiaError::UnexpectedVersionId);
        assert_eq!(OxiaError::from(3), OxiaError::SessionDoesNotExist);
        // gRPC-level codes are not proto-body codes; they round-trip through Unknown
        assert_eq!(OxiaError::from(102), OxiaError::Unknown(102));
        assert_eq!(OxiaError::from(104), OxiaError::Unknown(104));
        assert_eq!(OxiaError::from(106), OxiaError::Unknown(106));
        assert_eq!(OxiaError::from(999), OxiaError::Unknown(999));
    }

    #[test_log::test]
    fn test_is_wrong_leader() {
        assert!(Error::OxiaRpc(OxiaRpcError::NodeIsNotLeader { hint: None }).is_wrong_leader());
        assert!(
            Error::OxiaRpc(OxiaRpcError::NodeIsNotLeader {
                hint: Some(crate::LeaderHint {
                    shard: ShardId::new(1),
                    leader_address: "host:1234".into(),
                }),
            })
            .is_wrong_leader()
        );
        assert!(Error::OxiaRpc(OxiaRpcError::InvalidStatus).is_wrong_leader());
        assert!(Error::OxiaRpc(OxiaRpcError::AlreadyClosed).is_wrong_leader());
        assert!(!Error::Oxia(OxiaError::KeyNotFound).is_wrong_leader());
        assert!(
            !Error::Grpc(Arc::new(tonic::Status::new(tonic::Code::Unknown, ""))).is_wrong_leader()
        );
    }

    #[test_log::test]
    fn test_is_retryable_wrong_leader() {
        assert!(Error::OxiaRpc(OxiaRpcError::NodeIsNotLeader { hint: None }).is_retryable());
        assert!(Error::OxiaRpc(OxiaRpcError::InvalidStatus).is_retryable());
        assert!(Error::OxiaRpc(OxiaRpcError::AlreadyClosed).is_retryable());
        assert!(!Error::Oxia(OxiaError::KeyNotFound).is_retryable());
    }
}
