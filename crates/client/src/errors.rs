use crate::KeyComparisonType;
use std::{fmt::Display, time::Duration};
use thiserror::Error as ThisError;

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

#[derive(Debug, ThisError)]
#[non_exhaustive]
pub enum Error {
    #[error("gRPC error: {0}")]
    TonicStatus(tonic::Status),

    #[error("gRPC transport error: {0}")]
    TonicTransport(#[from] tonic::transport::Error),

    #[error("I/O error: {0}")]
    Io(std::io::Error),

    #[error("Oxia protocol error: {0}")]
    OxiaError(#[from] OxiaError),

    #[error("Client error: {0}")]
    Client(#[from] ClientError),

    #[error("No service addresss")]
    NoServiceAddress,

    #[error("Unexpected server response: {0}")]
    UnexpectedServerResponse(#[from] UnexpectedServerResponse),

    #[error("No shard mapping for {0}")]
    NoShardMapping(i64),

    #[error("Custom error: {0}")]
    Custom(String),

    #[error("Unknown boxed error: {0}")]
    Boxed(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Multiple errors")]
    Multiple(Vec<Box<Error>>),
    // #[error("Unknown error")]
    // Unknown,
    #[error("Request time out")]
    RequestTimeout{
        source : Box<dyn std::error::Error + Send + Sync>,
    },
}

impl Error {
    /// Whether the error is likely transient and worth retrying
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::TonicTransport(_) | Error::Io(_))
    }

    pub fn as_io_error<'a, T>(err: &'a T) -> Option<&'a std::io::Error>
    where T: std::error::Error + 'a {
        let mut source = Some(err as &dyn Error);
        while let Some(err) = source {
            if let Some(io_err) = err.downcast_ref::<io::Error>() {
                return Some(io_err);
            }
            source = err.source();
        }
        None
}

impl From<tonic::Status> for Error {
    fn from(value: tonic::Status) -> Self {
        if value.code() == tonic::Code::DeadlineExceeded {
            Error::RequestTimeout{source: Box::new(value)}
        } else {
            Error::TonicStatus(value)
        }
    }
}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(value: tokio::time::error::Elapsed) -> Self {
        Error::RequestTimeout{source: Box::new(value)}
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        if value.kind() == std::io::ErrorKind::TimedOut {
            Error::RequestTimeout{source: Box::new(value)}
        } else {
            Error::Io(value)
        }
    }
}