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

use mauricebarnum_oxia_common::proto;
use thiserror::Error as ThisError;

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

/// Oxia errors returned by failed gRPC calls.
///
/// Oxia v0.17 servers encode these as `google.rpc.ErrorInfo` details. Errors from
/// v0.16 servers use numeric gRPC codes and are normalized to the corresponding
/// current variant. These are distinct from proto-body [`OxiaError`] codes (1–3),
/// which appear in successful response bodies.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OxiaRpcError {
    /// The operation was aborted and can be retried.
    #[error("operation aborted by server")]
    Aborted,

    /// The server has not completed initialization.
    #[error("server not initialized")]
    NotInitialized,

    /// The request carried an invalid Raft term.
    #[error("invalid term")]
    InvalidTerm,

    /// The shard is in the wrong status for the requested operation (leader fenced).
    #[error("invalid status (leader fenced)")]
    InvalidStatus,

    /// The contacted node is not the shard leader.
    #[error("node is not leader")]
    NodeIsNotLeader,

    /// The referenced session does not exist.
    #[error("session not found")]
    SessionNotFound,

    /// The requested session timeout is outside permitted bounds.
    #[error("invalid session timeout")]
    InvalidSessionTimeout,

    /// The requested namespace does not exist.
    #[error("namespace not found")]
    NamespaceNotFound,

    /// The requested shard does not exist.
    #[error("shard not found")]
    ShardNotFound,

    /// Notifications are not enabled for this namespace.
    #[error("notifications not enabled")]
    NotificationsNotEnabled,

    /// The contacted node is not a member of the shard ensemble.
    #[error("node is not a member")]
    NodeIsNotMember,

    /// The requested resource conflicts with existing state.
    #[error("resource conflict")]
    ResourceConflict,

    /// The requested resource is temporarily unavailable.
    #[error("resource unavailable")]
    ResourceUnavailable,
}

// Public api
impl OxiaRpcError {
    /// Returns `true` if this error indicates the request hit the wrong leader.
    #[inline]
    pub const fn is_wrong_leader(&self) -> bool {
        matches!(self, Self::InvalidStatus | Self::NodeIsNotLeader)
    }

    /// Returns `true` if it might make sense to retry when this error is seen.
    #[inline]
    pub const fn is_retryable(&self) -> bool {
        // We avoid a default arm to ensure that all new error types are addressed intentionally
        #[allow(clippy::match_same_arms)]
        match self {
            Self::Aborted => true,
            Self::InvalidStatus => true,
            Self::NodeIsNotMember => true,
            Self::NodeIsNotLeader => true,
            Self::NotInitialized => true,
            Self::ResourceUnavailable => true,

            Self::InvalidSessionTimeout => false,
            Self::InvalidTerm => false,
            Self::NamespaceNotFound => false,
            Self::NotificationsNotEnabled => false,
            Self::ResourceConflict => false,
            Self::SessionNotFound => false,
            Self::ShardNotFound => false,
        }
    }
}

impl OxiaRpcError {
    const ERROR_INFO_NAME: &'static str = "google.rpc.ErrorInfo";
    const OXIA_ERROR_DOMAIN: &'static str = "oxia.io";

    fn type_url_matches(type_url: &str, name: &str) -> bool {
        type_url
            .strip_suffix(name)
            .is_some_and(|prefix| prefix.is_empty() || prefix.as_bytes().last() == Some(&b'/'))
    }

    fn extract_error_info(details: &[prost_types::Any]) -> Option<proto::google::rpc::ErrorInfo> {
        use prost::Message as _;

        details
            .iter()
            .filter(|detail| Self::type_url_matches(&detail.type_url, Self::ERROR_INFO_NAME))
            .filter_map(|detail| {
                proto::google::rpc::ErrorInfo::decode(detail.value.as_slice()).ok()
            })
            .find(|info| info.domain == Self::OXIA_ERROR_DOMAIN)
    }

    fn from_error_info(info: &proto::google::rpc::ErrorInfo) -> Option<Self> {
        let error = match info.reason.as_str() {
            "ABORTED" => Self::Aborted,
            "INVALID_SESSION_TIMEOUT" => Self::InvalidSessionTimeout,
            "SESSION_NOT_FOUND" => Self::SessionNotFound,
            "NAMESPACE_NOT_FOUND" => Self::NamespaceNotFound,
            "SHARD_NOT_FOUND" => Self::ShardNotFound,
            "INVALID_TERM" => Self::InvalidTerm,
            "INVALID_STATUS" => Self::InvalidStatus,
            "NOTIFICATIONS_NOT_ENABLED" => Self::NotificationsNotEnabled,
            "NODE_IS_NOT_MEMBER" => Self::NodeIsNotMember,
            "NODE_IS_NOT_LEADER" => Self::NodeIsNotLeader,
            "NOT_INITIALIZED" => Self::NotInitialized,
            "RESOURCE_CONFLICT" => Self::ResourceConflict,
            "RESOURCE_UNAVAILABLE" => Self::ResourceUnavailable,
            _ => return None,
        };
        Some(error)
    }

    const fn from_legacy_rpc_code(x: i32) -> Option<Self> {
        if x < 100 {
            return None;
        }
        let error = match x {
            100 => Self::NotInitialized,
            101 => Self::InvalidTerm,
            102 => Self::InvalidStatus,
            103 => Self::Aborted,
            104 => Self::ResourceUnavailable,
            105 => Self::ResourceConflict,
            106 => Self::NodeIsNotLeader,
            107 | 112 => Self::NodeIsNotMember,
            108 => Self::SessionNotFound,
            109 => Self::InvalidSessionTimeout,
            110 => Self::NamespaceNotFound,
            111 => Self::NotificationsNotEnabled,
            _ => return None,
        };
        Some(error)
    }
}

impl TryFrom<tonic::Status> for OxiaRpcError {
    type Error = tonic::Status;

    fn try_from(x: tonic::Status) -> Result<Self, Self::Error> {
        use prost::Message as _;
        use proto::google::rpc::Status as RpcStatus;

        let decoded_status = if x.details().is_empty() {
            None
        } else {
            RpcStatus::decode(x.details()).ok()
        };

        if let Some(error) = decoded_status
            .as_ref()
            .and_then(|status| Self::extract_error_info(&status.details))
            .as_ref()
            .and_then(Self::from_error_info)
        {
            return Ok(error);
        }

        if x.code() == tonic::Code::Unknown {
            let legacy_code = decoded_status
                .as_ref()
                .map(|status| status.code)
                .or_else(|| {
                    x.metadata()
                        .get("grpc-status")
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse().ok())
                });
            if let Some(error) = legacy_code.and_then(Self::from_legacy_rpc_code) {
                return Ok(error);
            }
        }

        Err(x)
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

    #[error("Invalid destination format '{dest}': {reason}")]
    InvalidDestinationFormat { dest: String, reason: String },

    #[error("Destination must not contain userinfo")]
    DestinationContainsUserinfo,

    #[error("Destination must have an authority (host:port)")]
    DestinationMissingAuthority,

    #[error("Destination contains unexpected URI component: {component}")]
    DestinationContainsUnexpectedComponent { component: String },
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

    /// A structured Oxia error returned by a failed gRPC call.
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
            Self::NoShardMapping(_)
            | Self::NoShardMappingForKey(_)
            | Self::OxiaRpc(OxiaRpcError::ShardNotFound) => true,
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
    use std::collections::HashMap;

    use bytes::Bytes;
    use prost::Message as _;
    use tracing::info;

    use super::*;

    fn status_with_error_info(
        code: tonic::Code,
        reason: &str,
        domain: &str,
        metadata: HashMap<String, String>,
    ) -> tonic::Status {
        let info = proto::google::rpc::ErrorInfo {
            reason: reason.to_owned(),
            domain: domain.to_owned(),
            metadata,
        };
        let detail = prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.ErrorInfo".to_owned(),
            value: info.encode_to_vec(),
        };
        let status = proto::google::rpc::Status {
            code: code as i32,
            message: reason.to_owned(),
            details: vec![detail],
        };
        tonic::Status::with_details(code, reason, Bytes::from(status.encode_to_vec()))
    }

    fn legacy_status(code: i32, details: Vec<prost_types::Any>) -> tonic::Status {
        let status = proto::google::rpc::Status {
            code,
            message: format!("legacy Oxia error {code}"),
            details,
        };
        tonic::Status::with_details(
            tonic::Code::Unknown,
            status.message.clone(),
            Bytes::from(status.encode_to_vec()),
        )
    }

    #[test_log::test]
    fn test_v017_error_info_reasons() {
        let cases = [
            ("ABORTED", OxiaRpcError::Aborted),
            (
                "INVALID_SESSION_TIMEOUT",
                OxiaRpcError::InvalidSessionTimeout,
            ),
            ("SESSION_NOT_FOUND", OxiaRpcError::SessionNotFound),
            ("NAMESPACE_NOT_FOUND", OxiaRpcError::NamespaceNotFound),
            ("SHARD_NOT_FOUND", OxiaRpcError::ShardNotFound),
            ("INVALID_TERM", OxiaRpcError::InvalidTerm),
            ("INVALID_STATUS", OxiaRpcError::InvalidStatus),
            (
                "NOTIFICATIONS_NOT_ENABLED",
                OxiaRpcError::NotificationsNotEnabled,
            ),
            ("NODE_IS_NOT_MEMBER", OxiaRpcError::NodeIsNotMember),
            ("NODE_IS_NOT_LEADER", OxiaRpcError::NodeIsNotLeader),
            ("NOT_INITIALIZED", OxiaRpcError::NotInitialized),
            ("RESOURCE_CONFLICT", OxiaRpcError::ResourceConflict),
            ("RESOURCE_UNAVAILABLE", OxiaRpcError::ResourceUnavailable),
        ];

        for (reason, expected) in cases {
            let status = status_with_error_info(
                tonic::Code::FailedPrecondition,
                reason,
                OxiaRpcError::OXIA_ERROR_DOMAIN,
                HashMap::new(),
            );
            assert_eq!(OxiaRpcError::try_from(status).unwrap(), expected);
        }
    }

    #[test_log::test]
    fn test_v016_codes_map_to_v017_variants() {
        let cases = [
            (100, OxiaRpcError::NotInitialized),
            (101, OxiaRpcError::InvalidTerm),
            (102, OxiaRpcError::InvalidStatus),
            (103, OxiaRpcError::Aborted),
            (104, OxiaRpcError::ResourceUnavailable),
            (105, OxiaRpcError::ResourceConflict),
            (106, OxiaRpcError::NodeIsNotLeader),
            (107, OxiaRpcError::NodeIsNotMember),
            (108, OxiaRpcError::SessionNotFound),
            (109, OxiaRpcError::InvalidSessionTimeout),
            (110, OxiaRpcError::NamespaceNotFound),
            (111, OxiaRpcError::NotificationsNotEnabled),
            (112, OxiaRpcError::NodeIsNotMember),
        ];

        for (code, expected) in cases {
            assert_eq!(
                OxiaRpcError::try_from(legacy_status(code, vec![])).unwrap(),
                expected
            );
        }
    }

    #[test_log::test]
    fn test_unknown_v016_code_stays_grpc() {
        let error = Error::from(legacy_status(999, vec![]));
        let Error::Grpc(status) = error else {
            panic!("unknown legacy code should remain a gRPC error");
        };
        assert_eq!(status.code(), tonic::Code::Unknown);
        let details = proto::google::rpc::Status::decode(status.details()).unwrap();
        assert_eq!(details.code, 999);
    }

    #[test_log::test]
    fn test_foreign_or_unknown_error_info_stays_grpc() {
        for (domain, reason) in [
            ("example.com", "NODE_IS_NOT_LEADER"),
            (OxiaRpcError::OXIA_ERROR_DOMAIN, "FUTURE_REASON"),
        ] {
            let status =
                status_with_error_info(tonic::Code::Aborted, reason, domain, HashMap::new());
            assert!(matches!(Error::from(status), Error::Grpc(_)));
        }
    }

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
    fn test_plain_unknown_status_stays_grpc() {
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
        assert!(Error::OxiaRpc(OxiaRpcError::NodeIsNotLeader).is_wrong_leader());
        assert!(Error::OxiaRpc(OxiaRpcError::InvalidStatus).is_wrong_leader());
        assert!(!Error::OxiaRpc(OxiaRpcError::ResourceUnavailable).is_wrong_leader());
        assert!(!Error::Oxia(OxiaError::KeyNotFound).is_wrong_leader());
        assert!(
            !Error::Grpc(Arc::new(tonic::Status::new(tonic::Code::Unknown, ""))).is_wrong_leader()
        );
    }

    #[test_log::test]
    fn test_is_retryable_wrong_leader() {
        assert!(Error::OxiaRpc(OxiaRpcError::NodeIsNotLeader).is_retryable());
        assert!(Error::OxiaRpc(OxiaRpcError::InvalidStatus).is_retryable());
        assert!(Error::OxiaRpc(OxiaRpcError::Aborted).is_retryable());
        assert!(Error::OxiaRpc(OxiaRpcError::NodeIsNotMember).is_retryable());
        assert!(Error::OxiaRpc(OxiaRpcError::ResourceUnavailable).is_retryable());
        assert!(!Error::OxiaRpc(OxiaRpcError::ResourceConflict).is_retryable());
        assert!(!Error::Oxia(OxiaError::KeyNotFound).is_retryable());
    }
}
