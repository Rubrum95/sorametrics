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
}

impl ErrorCode {
    fn http_status(self) -> StatusCode {
        match self {
            Self::Database | Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::NotFound => StatusCode::NOT_FOUND,
        }
    }
}

/// Public-facing API error.
#[derive(Debug, Error)]
pub enum ApiError {
    /// Database / sqlx error. Internal details stay in logs.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

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
}

impl ApiError {
    fn code(&self) -> ErrorCode {
        match self {
            Self::Database(_) => ErrorCode::Database,
            Self::BadRequest(_) => ErrorCode::BadRequest,
            Self::NotFound(_) => ErrorCode::NotFound,
            Self::Internal(_) => ErrorCode::Internal,
        }
    }

    fn public_message(&self) -> String {
        match self {
            // Never leak the raw sqlx error to clients.
            Self::Database(_) => "database error".to_string(),
            Self::BadRequest(msg) => msg.clone(),
            Self::NotFound(msg) => msg.clone(),
            // Same policy as Database: internals stay in logs.
            Self::Internal(_) => "internal error".to_string(),
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
        (code.http_status(), Json(body)).into_response()
    }
}
