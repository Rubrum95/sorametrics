//! Unified API error type with `IntoResponse` impl.
//!
//! Strategy: every handler returns `Result<T, ApiError>`. `ApiError`
//! converts to a JSON-shaped HTTP response with a stable error code +
//! human message, never leaking internal details (stack traces, raw
//! SQL errors). Internals are logged at `error` level so operators
//! still see them.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;
use tracing::error;

/// Stable, public error codes. Frontend can branch on `code` without
/// parsing the message.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Database access failed (connection, query, decode).
    Database,
    /// Caller-supplied query parameter was invalid.
    BadRequest,
    /// Resource not found (e.g., wallet has no rows).
    NotFound,
    /// Unhandled internal failure.
    Internal,
    /// A dependency (chain RPC) is not configured or not reachable.
    Unavailable,
}

impl ErrorCode {
    fn http_status(self) -> StatusCode {
        match self {
            Self::Database | Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// Public-facing API error.
#[derive(Debug, Error)]
pub enum ApiError {
    /// Database / sqlx error. Internal details stay in logs.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Error from the typed `sorametrics-db` layer (same policy).
    #[error("database error: {0}")]
    DbLayer(#[from] sorametrics_db::DbError),

    /// Invalid query / path parameter.
    #[error("bad request: {0}")]
    BadRequest(String),

    /// Resource not found (e.g., unknown asset id).
    #[error("not found: {0}")]
    NotFound(String),

    /// Server-side invariant violation (a bug, not a client error).
    /// Details stay in logs.
    #[error("internal error: {0}")]
    Internal(String),

    /// Chain RPC not configured / unreachable / failing.
    #[error("chain unavailable: {0}")]
    Chain(#[from] crate::chain::ChainError),

    /// Chain routes without a configured client.
    #[error("chain client not configured")]
    NoChain,

    /// A full-storage scan is still running; retry shortly.
    #[error("scan in progress")]
    ScanPending,
}

impl ApiError {
    fn code(&self) -> ErrorCode {
        match self {
            Self::Database(_) | Self::DbLayer(_) => ErrorCode::Database,
            Self::BadRequest(_) => ErrorCode::BadRequest,
            Self::NotFound(_) => ErrorCode::NotFound,
            Self::Internal(_) => ErrorCode::Internal,
            Self::Chain(_) | Self::NoChain | Self::ScanPending => ErrorCode::Unavailable,
        }
    }

    fn public_message(&self) -> String {
        match self {
            // Never leak the raw sqlx error to clients.
            Self::Database(_) | Self::DbLayer(_) => "database error".to_string(),
            Self::BadRequest(msg) => msg.clone(),
            Self::NotFound(msg) => msg.clone(),
            // Same policy as Database: internals stay in logs.
            Self::Internal(_) => "internal error".to_string(),
            Self::Chain(_) => "chain rpc unavailable".to_string(),
            Self::NoChain => "chain client not configured".to_string(),
            Self::ScanPending => "scan in progress, retry shortly".to_string(),
        }
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: ErrorCode,
    message: &'a str,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = self.code();
        // Log the FULL error (with chain) at server side. Keep client
        // payload minimal.
        error!(error = %self, code = ?code, "api error");

        let body = ErrorBody {
            code,
            message: &self.public_message(),
        };
        let mut resp = (code.http_status(), Json(body)).into_response();
        if matches!(self, Self::ScanPending) {
            resp.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("15"),
            );
        }
        resp
    }
}
