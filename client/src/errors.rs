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
    #[error("OK")]
    Ok, // not an error
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
            0 => Self::Ok,
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
    ///
    /// Retry policy intentionally differs from the Go client's `IsRetriable`
    /// (`ext/oxia/oxia/internal/batch/rpc_errors.go`): Rust retries `Aborted`
    /// and `ResourceUnavailable`.
    #[inline]
    pub const fn is_retryable(&self) -> bool {
        match self {
            Self::Aborted
            | Self::InvalidStatus
            | Self::NodeIsNotLeader
            | Self::NodeIsNotMember
            | Self::NotInitialized
            | Self::ResourceUnavailable => true,

            Self::InvalidSessionTimeout
            | Self::InvalidTerm
            | Self::NamespaceNotFound
            | Self::NotificationsNotEnabled
            | Self::ResourceConflict
            | Self::SessionNotFound
            | Self::ShardNotFound => false,
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
        match GrpcStatus::from_tonic_status(&x) {
            GrpcStatus::Oxia(err) => Ok(err),
            _ => Err(x),
        }
    }
}

/// Standard gRPC status codes (1–16).
///
/// Mirrors the integer `grpc-status` header value space independently of tonic.
/// `Ok` (0) is not an error
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, thiserror::Error)]
pub enum GrpcCode {
    #[error("OK")]
    Ok,
    #[error("cancelled")]
    Cancelled,
    #[error("unknown")]
    Unknown,
    #[error("invalid argument")]
    InvalidArgument,
    #[error("deadline exceeded")]
    DeadlineExceeded,
    #[error("not found")]
    NotFound,
    #[error("already exists")]
    AlreadyExists,
    #[error("permission denied")]
    PermissionDenied,
    #[error("resource exhausted")]
    ResourceExhausted,
    #[error("failed precondition")]
    FailedPrecondition,
    #[error("aborted")]
    Aborted,
    #[error("out of range")]
    OutOfRange,
    #[error("unimplemented")]
    Unimplemented,
    #[error("internal")]
    Internal,
    #[error("unavailable")]
    Unavailable,
    #[error("data loss")]
    DataLoss,
    #[error("unauthenticated")]
    Unauthenticated,
    #[error("unknown gRPC code {0}")]
    UnknownCode(i32),
}

impl From<i32> for GrpcCode {
    #[inline]
    fn from(code: i32) -> Self {
        match code {
            1 => Self::Cancelled,
            2 => Self::Unknown,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            n => Self::UnknownCode(n),
        }
    }
}

impl GrpcCode {
    /// Convert from tonic's `Code` enum.
    ///
    /// `tonic::Code::Ok` is not an error; it maps to `UnknownCode(0)`.
    #[inline]
    const fn from_tonic(code: tonic::Code) -> Self {
        match code {
            tonic::Code::Ok => Self::Ok,
            tonic::Code::Cancelled => Self::Cancelled,
            tonic::Code::Unknown => Self::Unknown,
            tonic::Code::InvalidArgument => Self::InvalidArgument,
            tonic::Code::DeadlineExceeded => Self::DeadlineExceeded,
            tonic::Code::NotFound => Self::NotFound,
            tonic::Code::AlreadyExists => Self::AlreadyExists,
            tonic::Code::PermissionDenied => Self::PermissionDenied,
            tonic::Code::ResourceExhausted => Self::ResourceExhausted,
            tonic::Code::FailedPrecondition => Self::FailedPrecondition,
            tonic::Code::Aborted => Self::Aborted,
            tonic::Code::OutOfRange => Self::OutOfRange,
            tonic::Code::Unimplemented => Self::Unimplemented,
            tonic::Code::Internal => Self::Internal,
            tonic::Code::Unavailable => Self::Unavailable,
            tonic::Code::DataLoss => Self::DataLoss,
            tonic::Code::Unauthenticated => Self::Unauthenticated,
        }
    }

    /// Returns `true` if this standard gRPC code is worth retrying.
    ///
    /// `Unknown` (2) is **not** retryable. Only transport-level
    /// `Unavailable` and opaque `Internal` errors are retried at this layer.
    #[inline]
    pub const fn is_retryable(&self) -> bool {
        match self {
            Self::Internal | Self::Unavailable => true,

            Self::Aborted
            | Self::AlreadyExists
            | Self::Cancelled
            | Self::DataLoss
            | Self::DeadlineExceeded
            | Self::FailedPrecondition
            | Self::InvalidArgument
            | Self::NotFound
            | Self::Ok
            | Self::OutOfRange
            | Self::PermissionDenied
            | Self::ResourceExhausted
            | Self::Unauthenticated
            | Self::Unimplemented
            | Self::Unknown
            | Self::UnknownCode(_) => false,
        }
    }

    /// Returns `true` if this code indicates a connection-level failure.
    #[inline]
    pub const fn is_connection_error(&self) -> bool {
        matches!(self, Self::Unavailable)
    }

    /// Returns `true` if this code indicates the shard/resource is unavailable.
    #[inline]
    pub const fn is_shard_unavailable(&self) -> bool {
        matches!(self, Self::NotFound)
    }

    /// Returns `true` if this code indicates the request went to the wrong leader.
    #[inline]
    pub const fn is_wrong_leader(&self) -> bool {
        false
    }
}

/// Unified representation of the integer `grpc-status` code space.
///
/// Covers standard gRPC codes (1–16) via [`GrpcCode`] and Oxia-specific codes
/// (≥ 100) via [`OxiaRpcError`]. This is the single type the classifiers will
/// match on; it is also the entry point for the future tonic-less client.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum GrpcStatus {
    /// A standard gRPC status code (1–16).
    #[error(transparent)]
    Standard(#[from] GrpcCode),

    /// An Oxia-specific gRPC status code (≥ 100).
    #[error(transparent)]
    Oxia(#[from] OxiaRpcError),

    /// An unrecognized status code.
    #[error("unknown grpc-status code {0}")]
    Unknown(i32),
}

impl From<i32> for GrpcStatus {
    #[inline]
    fn from(code: i32) -> Self {
        Self::from_grpc_status_header(code)
    }
}

impl GrpcStatus {
    /// Convert from a raw `grpc-status` header integer.
    ///
    /// This is the entry point the tonic-less client will use once
    /// `channel_pool.rs` replaces the tonic-based `pool.rs`.
    #[inline]
    pub const fn from_grpc_status_header(code: i32) -> Self {
        match code {
            1 => Self::Standard(GrpcCode::Cancelled),
            2 => Self::Standard(GrpcCode::Unknown),
            3 => Self::Standard(GrpcCode::InvalidArgument),
            4 => Self::Standard(GrpcCode::DeadlineExceeded),
            5 => Self::Standard(GrpcCode::NotFound),
            6 => Self::Standard(GrpcCode::AlreadyExists),
            7 => Self::Standard(GrpcCode::PermissionDenied),
            8 => Self::Standard(GrpcCode::ResourceExhausted),
            9 => Self::Standard(GrpcCode::FailedPrecondition),
            10 => Self::Standard(GrpcCode::Aborted),
            11 => Self::Standard(GrpcCode::OutOfRange),
            12 => Self::Standard(GrpcCode::Unimplemented),
            13 => Self::Standard(GrpcCode::Internal),
            14 => Self::Standard(GrpcCode::Unavailable),
            15 => Self::Standard(GrpcCode::DataLoss),
            16 => Self::Standard(GrpcCode::Unauthenticated),
            100 => Self::Oxia(OxiaRpcError::NotInitialized),
            101 => Self::Oxia(OxiaRpcError::InvalidTerm),
            102 => Self::Oxia(OxiaRpcError::InvalidStatus),
            103 => Self::Oxia(OxiaRpcError::Aborted),
            104 => Self::Oxia(OxiaRpcError::ResourceUnavailable),
            105 => Self::Oxia(OxiaRpcError::ResourceConflict),
            106 => Self::Oxia(OxiaRpcError::NodeIsNotLeader),
            107 | 112 => Self::Oxia(OxiaRpcError::NodeIsNotMember),
            108 => Self::Oxia(OxiaRpcError::SessionNotFound),
            109 => Self::Oxia(OxiaRpcError::InvalidSessionTimeout),
            110 => Self::Oxia(OxiaRpcError::NamespaceNotFound),
            111 => Self::Oxia(OxiaRpcError::NotificationsNotEnabled),
            n if n < 100 => Self::Standard(GrpcCode::UnknownCode(n)),
            n => Self::Unknown(n),
        }
    }

    /// Convert from a `grpc-rust` trailers object.
    ///
    /// `grpc-rust`'s `StatusError` only carries a standard 1-16 code and a
    /// message, so the Oxia-specific code and `ErrorInfo` details are
    /// recovered from the trailing metadata (`grpc-status-details-bin`, then
    /// the `grpc-status` header), mirroring the tonic path. Otherwise the
    /// status maps to a standard [`GrpcCode`].
    ///
    /// A successful (Ok) status is not an error; it maps to
    /// `Standard(GrpcCode::UnknownCode(0))`. Callers should not normally pass
    /// an Ok status.
    pub fn from_grpc_rust_trailers(trailers: &grpc::core::Trailers) -> Self {
        use prost::Message as _;
        use proto::google::rpc::Status as RpcStatus;

        let status_error = match trailers.status() {
            Ok(()) => return Self::Standard(GrpcCode::UnknownCode(0)),
            Err(status_error) => status_error,
        };

        let decoded_status = trailers
            .metadata()
            .get_bin("grpc-status-details-bin")
            .and_then(|value| {
                let bytes: bytes::Bytes = value.clone().into();
                RpcStatus::decode(bytes).ok()
            });

        if let Some(error) = decoded_status
            .as_ref()
            .and_then(|s| OxiaRpcError::extract_error_info(&s.details))
            .as_ref()
            .and_then(OxiaRpcError::from_error_info)
        {
            return Self::Oxia(error);
        }

        if let Some(code) = trailers
            .metadata()
            .get("grpc-status")
            .and_then(|value| value.to_str().parse::<i32>().ok())
        {
            return Self::from_grpc_status_header(code);
        }

        Self::Standard(GrpcCode::from(status_error.code() as i32))
    }

    /// Convert from a tonic status.
    ///
    /// When tonic reports `Code::Unknown` and the status carries an Oxia
    /// `grpc-status` value (via metadata header or `grpc-status-details-bin`),
    /// the decoded Oxia error is returned. Transport-wrapped Unknown statuses
    /// (client-side transport failures) are mapped to `Standard(Unavailable)`
    /// so they are retried as connection errors. Otherwise the status maps to
    /// a standard [`GrpcCode`].
    ///
    fn from_tonic_status(status: &tonic::Status) -> Self {
        use prost::Message as _;
        use proto::google::rpc::Status as RpcStatus;

        let decoded_status = if status.details().is_empty() {
            None
        } else {
            RpcStatus::decode(status.details()).ok()
        };

        if let Some(error) = decoded_status
            .as_ref()
            .and_then(|s| OxiaRpcError::extract_error_info(&s.details))
            .as_ref()
            .and_then(OxiaRpcError::from_error_info)
        {
            return Self::Oxia(error);
        }

        if status.code() == tonic::Code::Unknown {
            use std::error::Error as _;

            if status.message() == "transport error"
                || status
                    .source()
                    .and_then(|s| s.downcast_ref::<tonic::transport::Error>())
                    .is_some()
            {
                return Self::Standard(GrpcCode::Unavailable);
            }

            let legacy_code = decoded_status.as_ref().map(|s| s.code).or_else(|| {
                status
                    .metadata()
                    .get("grpc-status")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
            });
            if let Some(error) = legacy_code.and_then(OxiaRpcError::from_legacy_rpc_code) {
                return Self::Oxia(error);
            }
        }

        Self::Standard(GrpcCode::from_tonic(status.code()))
    }

    /// Returns `true` if this gRPC status is worth retrying.
    #[inline]
    pub const fn is_retryable(&self) -> bool {
        match self {
            Self::Standard(code) => code.is_retryable(),
            Self::Oxia(err) => err.is_retryable(),
            Self::Unknown(_) => false,
        }
    }

    /// Returns `true` if this status indicates a connection-level failure.
    #[inline]
    pub const fn is_connection_error(&self) -> bool {
        matches!(self, Self::Standard(GrpcCode::Unavailable))
    }

    /// Returns `true` if this status indicates the shard/resource is unavailable.
    ///
    /// `Oxia::ShardNotFound` (111) means the shard does not exist
    /// `Oxia::NodeIsNotMember` (112) means the contacted node is not a valid
    /// cluster member; both indicate the client might want to reconfigure rather than
    /// retry-in-place.  However, these errors could also indicate a race as
    /// cluster configuration changes propagate.
    #[inline]
    pub const fn is_shard_unavailable(&self) -> bool {
        match self {
            Self::Standard(code) => code.is_shard_unavailable(),
            Self::Oxia(OxiaRpcError::ShardNotFound) => true,
            Self::Oxia(_) | Self::Unknown(_) => false,
        }
    }

    /// Returns `true` if this status indicates the request went to the wrong leader.
    #[inline]
    pub const fn is_wrong_leader(&self) -> bool {
        match self {
            Self::Oxia(err) => err.is_wrong_leader(),
            Self::Standard(_) | Self::Unknown(_) => false,
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

/// A gRPC error returned by the grpc-rust client, retaining the trailing
/// metadata needed to decode Oxia-specific statuses.
///
/// grpc-rust's [`grpc::StatusError`] only carries a standard 1-16 code and a
/// message; the trailing metadata holds the `grpc-status` /
/// `grpc-status-details-bin` values that [`GrpcStatus::from_grpc_rust_trailers`]
/// decodes. The recv loop at each grpc-rust call site must capture the
/// trailers' metadata before the high-level builders drop it.
#[derive(Debug, Clone)]
pub struct GrpcCallError {
    status: grpc::StatusError,
    metadata: grpc::metadata::MetadataMap,
}

impl GrpcCallError {
    /// Construct a carrier from the status and trailing metadata of a failed
    /// grpc-rust call.
    #[inline]
    pub const fn new(status: grpc::StatusError, metadata: grpc::metadata::MetadataMap) -> Self {
        Self { status, metadata }
    }

    fn to_trailers(&self) -> grpc::core::Trailers {
        grpc::core::Trailers::new(Err(self.status.clone())).with_metadata(self.metadata.clone())
    }
}

impl std::fmt::Display for GrpcCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:?})", self.status.message(), self.status.code())
    }
}

impl std::error::Error for GrpcCallError {}

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

    /// Arc-wrapped: the grpc-rust status plus trailing metadata
    #[error("gRPC error: {0}")]
    GrpcRust(#[source] Arc<GrpcCallError>),

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
    /// Returns the [`GrpcStatus`] view of this error, if any.
    ///
    /// `Error::Grpc`, `Error::GrpcRust`, and `Error::OxiaRpc` all map into
    /// the unified `grpc-status` code space; all other variants return `None`.
    #[inline]
    pub fn grpc_status(&self) -> Option<GrpcStatus> {
        match self {
            Self::Grpc(status) => Some(GrpcStatus::from_tonic_status(status)),
            Self::GrpcRust(err) => Some(GrpcStatus::from_grpc_rust_trailers(&err.to_trailers())),
            Self::OxiaRpc(err) => Some(GrpcStatus::Oxia(err.clone())),
            _ => None,
        }
    }

    /// Whether the error is likely transient and worth retrying
    #[inline]
    pub fn is_retryable(&self) -> bool {
        // Do not have a default arm so that adding a new error type requires explicitly deciding
        // what to do.  Never mind that the answer is almost always `false`

        match self {
            Self::Grpc(_) | Self::GrpcRust(_) | Self::OxiaRpc(_) => {
                self.grpc_status().is_some_and(|s| s.is_retryable())
            }
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

            Self::NoResponseFromServer(_) | Self::Transport(_) => true,

            Self::Cancelled
            | Self::Client(_)
            | Self::Custom(_)
            | Self::InvalidKeyComparisonType(_)
            | Self::NoShardMapping(_)
            | Self::NoShardMappingForKey(_)
            | Self::Other(_)
            | Self::Oxia(_)
            | Self::RequestTimeout
            | Self::Server(_) => false,
        }
    }

    /// Whether the error indicates a connection failure that should invalidate cached channels.
    /// This is used to trigger channel reconnection on the next request.
    #[inline]
    pub fn is_connection_error(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Grpc(_) | Self::GrpcRust(_) | Self::OxiaRpc(_) => {
                self.grpc_status().is_some_and(|s| s.is_connection_error())
            }
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
            // gRPC/Oxia status classification (NotFound, NodeIsNotMember, ShardNotFound)
            Self::Grpc(_) | Self::GrpcRust(_) | Self::OxiaRpc(_) => {
                self.grpc_status().is_some_and(|s| s.is_shard_unavailable())
            }
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
            Self::Grpc(_) | Self::GrpcRust(_) | Self::OxiaRpc(_) => {
                self.grpc_status().is_some_and(|s| s.is_wrong_leader())
            }
            Self::ShardError(e) => e.err.is_wrong_leader(),
            Self::MultipleShardError(errs) => errs.iter().any(|e| e.err.is_wrong_leader()),
            _ => false,
        }
    }
}

impl From<GrpcCallError> for Error {
    fn from(call_error: GrpcCallError) -> Self {
        let trailers = call_error.to_trailers();
        match GrpcStatus::from_grpc_rust_trailers(&trailers) {
            GrpcStatus::Oxia(err) => err.into(),
            GrpcStatus::Standard(GrpcCode::DeadlineExceeded) => Self::RequestTimeout,
            GrpcStatus::Standard(GrpcCode::Cancelled)
                if call_error.status.message() == "Timeout expired" =>
            {
                Self::RequestTimeout
            }
            GrpcStatus::Standard(GrpcCode::Cancelled) => Self::Cancelled,
            _ => Self::GrpcRust(Arc::new(call_error)),
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
            // Unknown standard gRPC code is no longer retryable.
            Error::from(tonic::Status::new(tonic::Code::Unknown, "")),
            // Aborted requires higher-level retry, not a simple re-send.
            Error::from(tonic::Status::new(tonic::Code::Aborted, "")),
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
            // Oxia codes are decoded statuses, not transport failures
            Error::OxiaRpc(OxiaRpcError::NodeIsNotMember),
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
        assert_eq!(OxiaError::from(0), OxiaError::Ok);
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
    fn test_grpc_code_from_i32() {
        assert_eq!(GrpcCode::from(1), GrpcCode::Cancelled);
        assert_eq!(GrpcCode::from(2), GrpcCode::Unknown);
        assert_eq!(GrpcCode::from(3), GrpcCode::InvalidArgument);
        assert_eq!(GrpcCode::from(4), GrpcCode::DeadlineExceeded);
        assert_eq!(GrpcCode::from(5), GrpcCode::NotFound);
        assert_eq!(GrpcCode::from(6), GrpcCode::AlreadyExists);
        assert_eq!(GrpcCode::from(7), GrpcCode::PermissionDenied);
        assert_eq!(GrpcCode::from(8), GrpcCode::ResourceExhausted);
        assert_eq!(GrpcCode::from(9), GrpcCode::FailedPrecondition);
        assert_eq!(GrpcCode::from(10), GrpcCode::Aborted);
        assert_eq!(GrpcCode::from(11), GrpcCode::OutOfRange);
        assert_eq!(GrpcCode::from(12), GrpcCode::Unimplemented);
        assert_eq!(GrpcCode::from(13), GrpcCode::Internal);
        assert_eq!(GrpcCode::from(14), GrpcCode::Unavailable);
        assert_eq!(GrpcCode::from(15), GrpcCode::DataLoss);
        assert_eq!(GrpcCode::from(16), GrpcCode::Unauthenticated);
        assert_eq!(GrpcCode::from(0), GrpcCode::UnknownCode(0));
        assert_eq!(GrpcCode::from(17), GrpcCode::UnknownCode(17));
        assert_eq!(GrpcCode::from(-1), GrpcCode::UnknownCode(-1));
        assert_eq!(GrpcCode::from(99), GrpcCode::UnknownCode(99));
    }

    #[test_log::test]
    fn test_grpc_code_from_tonic() {
        assert_eq!(GrpcCode::from_tonic(tonic::Code::Ok), GrpcCode::Ok);
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::Cancelled),
            GrpcCode::Cancelled
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::Unknown),
            GrpcCode::Unknown
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::InvalidArgument),
            GrpcCode::InvalidArgument
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::DeadlineExceeded),
            GrpcCode::DeadlineExceeded
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::NotFound),
            GrpcCode::NotFound
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::AlreadyExists),
            GrpcCode::AlreadyExists
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::PermissionDenied),
            GrpcCode::PermissionDenied
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::ResourceExhausted),
            GrpcCode::ResourceExhausted
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::FailedPrecondition),
            GrpcCode::FailedPrecondition
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::Aborted),
            GrpcCode::Aborted
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::OutOfRange),
            GrpcCode::OutOfRange
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::Unimplemented),
            GrpcCode::Unimplemented
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::Internal),
            GrpcCode::Internal
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::Unavailable),
            GrpcCode::Unavailable
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::DataLoss),
            GrpcCode::DataLoss
        );
        assert_eq!(
            GrpcCode::from_tonic(tonic::Code::Unauthenticated),
            GrpcCode::Unauthenticated
        );
    }

    #[test_log::test]
    fn test_grpc_status_from_i32() {
        assert_eq!(
            GrpcStatus::from(0),
            GrpcStatus::Standard(GrpcCode::UnknownCode(0))
        );
        assert_eq!(
            GrpcStatus::from(1),
            GrpcStatus::Standard(GrpcCode::Cancelled)
        );
        assert_eq!(
            GrpcStatus::from(16),
            GrpcStatus::Standard(GrpcCode::Unauthenticated)
        );
        assert_eq!(
            GrpcStatus::from(17),
            GrpcStatus::Standard(GrpcCode::UnknownCode(17))
        );
        assert_eq!(
            GrpcStatus::from(99),
            GrpcStatus::Standard(GrpcCode::UnknownCode(99))
        );
        assert_eq!(
            GrpcStatus::from(100),
            GrpcStatus::Oxia(OxiaRpcError::NotInitialized)
        );
        assert_eq!(
            GrpcStatus::from(112),
            GrpcStatus::Oxia(OxiaRpcError::NodeIsNotMember)
        );
        assert_eq!(GrpcStatus::from(113), GrpcStatus::Unknown(113));
        assert_eq!(GrpcStatus::from(999), GrpcStatus::Unknown(999));
    }

    #[test_log::test]
    fn test_grpc_status_from_i32_matches_header() {
        // from_grpc_status_header is the single source of truth for the
        // integer grpc-status map; From<i32> must agree with it.
        for code in [-1, 0, 1, 2, 16, 17, 99, 100, 106, 111, 112, 113, 999] {
            assert_eq!(
                GrpcStatus::from(code),
                GrpcStatus::from_grpc_status_header(code),
                "code {code}"
            );
        }
    }

    #[test_log::test]
    fn test_grpc_status_from_tonic_status_standard() {
        assert_eq!(
            GrpcStatus::from_tonic_status(&tonic::Status::new(tonic::Code::Unavailable, "")),
            GrpcStatus::Standard(GrpcCode::Unavailable)
        );
        assert_eq!(
            GrpcStatus::from_tonic_status(&tonic::Status::new(tonic::Code::Internal, "")),
            GrpcStatus::Standard(GrpcCode::Internal)
        );
        assert_eq!(
            GrpcStatus::from_tonic_status(&tonic::Status::new(tonic::Code::NotFound, "")),
            GrpcStatus::Standard(GrpcCode::NotFound)
        );
    }

    #[test_log::test]
    fn test_grpc_status_from_tonic_status_unknown_with_legacy_code() {
        let status_proto = proto::google::rpc::Status {
            code: 106,
            message: "not leader".into(),
            details: vec![],
        };
        let tonic_status = tonic::Status::with_details(
            tonic::Code::Unknown,
            "",
            status_proto.encode_to_vec().into(),
        );

        let grpc_status = GrpcStatus::from_tonic_status(&tonic_status);
        assert_eq!(grpc_status, GrpcStatus::Oxia(OxiaRpcError::NodeIsNotLeader));
    }

    fn grpc_trailers_with_error_info(reason: &str, domain: &str) -> grpc::core::Trailers {
        let info = proto::google::rpc::ErrorInfo {
            reason: reason.to_owned(),
            domain: domain.to_owned(),
            metadata: HashMap::new(),
        };
        let detail = prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.ErrorInfo".to_owned(),
            value: info.encode_to_vec(),
        };
        let status = proto::google::rpc::Status {
            code: tonic::Code::Unknown as i32,
            message: reason.to_owned(),
            details: vec![detail],
        };
        let mut metadata = grpc::metadata::MetadataMap::new();
        metadata.insert_bin(
            "grpc-status-details-bin",
            grpc::metadata::BinaryMetadataValue::from_bytes(&status.encode_to_vec()),
        );
        grpc::core::Trailers::new(Err(grpc::StatusError::new(
            grpc::StatusCodeError::Unknown,
            reason,
        )))
        .with_metadata(metadata)
    }

    fn grpc_legacy_trailers(code: i32) -> grpc::core::Trailers {
        let mut metadata = grpc::metadata::MetadataMap::new();
        metadata.insert("grpc-status", code.to_string().parse().unwrap());
        grpc::core::Trailers::new(Err(grpc::StatusError::new(
            grpc::StatusCodeError::Unknown,
            format!("legacy Oxia error {code}"),
        )))
        .with_metadata(metadata)
    }

    #[test_log::test]
    fn test_grpc_status_from_grpc_rust_trailers_error_info() {
        let trailers =
            grpc_trailers_with_error_info("NODE_IS_NOT_LEADER", OxiaRpcError::OXIA_ERROR_DOMAIN);
        assert_eq!(
            GrpcStatus::from_grpc_rust_trailers(&trailers),
            GrpcStatus::Oxia(OxiaRpcError::NodeIsNotLeader)
        );
    }

    #[test_log::test]
    fn test_grpc_status_from_grpc_rust_trailers_legacy_header() {
        let trailers = grpc_legacy_trailers(106);
        assert_eq!(
            GrpcStatus::from_grpc_rust_trailers(&trailers),
            GrpcStatus::Oxia(OxiaRpcError::NodeIsNotLeader)
        );
    }

    #[test_log::test]
    fn test_grpc_status_from_grpc_rust_trailers_standard() {
        let trailers = grpc::core::Trailers::new(Err(grpc::StatusError::new(
            grpc::StatusCodeError::Unavailable,
            "unavailable",
        )));
        assert_eq!(
            GrpcStatus::from_grpc_rust_trailers(&trailers),
            GrpcStatus::Standard(GrpcCode::Unavailable)
        );
    }

    fn grpc_call_error_from_trailers(trailers: &grpc::core::Trailers) -> GrpcCallError {
        let status = match trailers.status() {
            Ok(()) => panic!("expected an error status"),
            Err(status) => status.clone(),
        };
        GrpcCallError::new(status, trailers.metadata().clone())
    }

    #[test_log::test]
    fn test_error_from_grpc_call_error_oxia() {
        let trailers =
            grpc_trailers_with_error_info("NODE_IS_NOT_LEADER", OxiaRpcError::OXIA_ERROR_DOMAIN);
        assert!(matches!(
            Error::from(grpc_call_error_from_trailers(&trailers)),
            Error::OxiaRpc(OxiaRpcError::NodeIsNotLeader)
        ));
    }

    #[test_log::test]
    fn test_error_from_grpc_call_error_legacy_header() {
        let trailers = grpc_legacy_trailers(106);
        assert!(matches!(
            Error::from(grpc_call_error_from_trailers(&trailers)),
            Error::OxiaRpc(OxiaRpcError::NodeIsNotLeader)
        ));
    }

    #[test_log::test]
    fn test_error_from_grpc_call_error_deadline() {
        let call_error = GrpcCallError::new(
            grpc::StatusError::new(grpc::StatusCodeError::DeadlineExceeded, "deadline exceeded"),
            grpc::metadata::MetadataMap::new(),
        );
        assert!(matches!(Error::from(call_error), Error::RequestTimeout));
    }

    #[test_log::test]
    fn test_error_from_grpc_call_error_cancelled() {
        let call_error = GrpcCallError::new(
            grpc::StatusError::new(grpc::StatusCodeError::Cancelled, "cancelled"),
            grpc::metadata::MetadataMap::new(),
        );
        assert!(matches!(Error::from(call_error), Error::Cancelled));
    }

    #[test_log::test]
    fn test_error_from_grpc_call_error_generic_classifies() {
        let call_error = GrpcCallError::new(
            grpc::StatusError::new(grpc::StatusCodeError::Unavailable, "unavailable"),
            grpc::metadata::MetadataMap::new(),
        );
        let err = Error::from(call_error);
        assert!(matches!(err, Error::GrpcRust(_)));
        assert_eq!(
            err.grpc_status(),
            Some(GrpcStatus::Standard(GrpcCode::Unavailable))
        );
        assert!(err.is_retryable());
        assert!(err.is_connection_error());
        assert!(!err.is_wrong_leader());
        assert!(!err.is_shard_unavailable());
    }

    #[test_log::test]
    fn test_grpc_code_classification() {
        assert!(GrpcCode::Unavailable.is_retryable());
        assert!(GrpcCode::Internal.is_retryable());
        assert!(!GrpcCode::Unknown.is_retryable());
        assert!(!GrpcCode::Aborted.is_retryable());
        assert!(!GrpcCode::ResourceExhausted.is_retryable());
        assert!(!GrpcCode::UnknownCode(99).is_retryable());

        assert!(GrpcCode::Unavailable.is_connection_error());
        assert!(!GrpcCode::Internal.is_connection_error());

        assert!(GrpcCode::NotFound.is_shard_unavailable());
        assert!(!GrpcCode::Unavailable.is_shard_unavailable());

        assert!(!GrpcCode::Unavailable.is_wrong_leader());
    }

    #[test_log::test]
    fn test_grpc_status_classification() {
        assert!(GrpcStatus::Standard(GrpcCode::Unavailable).is_retryable());
        assert!(GrpcStatus::Standard(GrpcCode::Internal).is_retryable());
        assert!(!GrpcStatus::Standard(GrpcCode::Unknown).is_retryable());
        assert!(!GrpcStatus::Unknown(999).is_retryable());

        assert!(GrpcStatus::Oxia(OxiaRpcError::NodeIsNotLeader).is_wrong_leader());
        assert!(GrpcStatus::Oxia(OxiaRpcError::InvalidStatus).is_wrong_leader());
        assert!(!GrpcStatus::Oxia(OxiaRpcError::NodeIsNotMember).is_wrong_leader());
        assert!(!GrpcStatus::Standard(GrpcCode::Unavailable).is_wrong_leader());

        assert!(!GrpcStatus::Oxia(OxiaRpcError::NodeIsNotMember).is_shard_unavailable());
        assert!(!GrpcStatus::Oxia(OxiaRpcError::NodeIsNotLeader).is_shard_unavailable());
        assert!(GrpcStatus::Standard(GrpcCode::NotFound).is_shard_unavailable());

        assert!(GrpcStatus::Standard(GrpcCode::Unavailable).is_connection_error());
        assert!(!GrpcStatus::Oxia(OxiaRpcError::NodeIsNotMember).is_connection_error());
    }

    #[test_log::test]
    fn test_error_grpc_status_accessor() {
        assert_eq!(
            Error::from(tonic::Status::new(tonic::Code::Unavailable, "")).grpc_status(),
            Some(GrpcStatus::Standard(GrpcCode::Unavailable))
        );
        assert_eq!(
            Error::OxiaRpc(OxiaRpcError::NodeIsNotMember).grpc_status(),
            Some(GrpcStatus::Oxia(OxiaRpcError::NodeIsNotMember))
        );
        assert!(Error::custom("x").grpc_status().is_none());
        assert!(Error::RequestTimeout.grpc_status().is_none());
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
        assert!(Error::OxiaRpc(OxiaRpcError::ResourceUnavailable).is_retryable());
        assert!(!Error::OxiaRpc(OxiaRpcError::ResourceConflict).is_retryable());
        assert!(!Error::Oxia(OxiaError::KeyNotFound).is_retryable());
    }

    #[test_log::test]
    fn test_is_shard_unavailable_node_is_not_member() {
        assert!(
            !Error::from(tonic::Status::new(tonic::Code::Unavailable, "")).is_shard_unavailable()
        );
        assert!(!Error::OxiaRpc(OxiaRpcError::NodeIsNotLeader).is_shard_unavailable());
    }

    #[test_log::test]
    fn test_is_shard_unavailable_shard_not_found() {
        // ShardNotFound means the shard does not exist, so the client should
        // reconfigure rather than retry-in-place.
        assert!(Error::OxiaRpc(OxiaRpcError::ShardNotFound).is_shard_unavailable());

        // ShardNotFound is not retryable (terminal).
        assert!(!OxiaRpcError::ShardNotFound.is_retryable());
    }
}
