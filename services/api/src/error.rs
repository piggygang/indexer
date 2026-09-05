//! The contract's error shape (ALG-625).
//!
//! `openapi/v1.yaml`'s `Error` schema is closed and its `error` field is a
//! six-member enum, so this is the only way a `/v1` failure may be serialized:
//! `{"error": "...", "message": "...", "details": ... | null}`. `details` is
//! the one property in the whole document that is not `required`, and every
//! example still sends it — so it is always serialized here too.

use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use serde_json::{json, Value};

/// The `Error.error` enum, verbatim from the contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    InvalidParameter,
    InvalidCursor,
    UnsupportedSort,
    NotFound,
    Internal,
}

impl Code {
    const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidParameter => "invalid_parameter",
            Self::InvalidCursor => "invalid_cursor",
            Self::UnsupportedSort => "unsupported_sort",
            Self::NotFound => "not_found",
            Self::Internal => "internal",
        }
    }

    const fn status(self) -> StatusCode {
        match self {
            // Both are 400 by the contract: a malformed parameter and a cursor
            // that no longer applies are recoverable client-side conditions.
            Self::InvalidParameter | Self::InvalidCursor => StatusCode::BAD_REQUEST,
            // 422 lives only on browse, and only for a sort the contract
            // reserves but cannot serve yet.
            Self::UnsupportedSort => StatusCode::UNPROCESSABLE_ENTITY,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct ApiError {
    pub code: Code,
    pub message: String,
    pub details: Value,
    /// Overrides the code's default status.
    ///
    /// One case needs it: the contract says an over-large `limit` is `422`
    /// "never a silent clamp", but `422` is declared only on browse and the
    /// honest code for it is `invalid_parameter`, not `unsupported_sort`.
    /// Code and status are therefore separate knobs.
    pub status: Option<StatusCode>,
}

impl ApiError {
    pub fn new(code: Code, message: impl Into<String>, details: Value) -> Self {
        Self {
            code,
            message: message.into(),
            details,
            status: None,
        }
    }

    /// Same code, a different status — see [`ApiError::status`].
    pub fn with_status(mut self, status: StatusCode) -> Self {
        self.status = Some(status);
        self
    }

    fn status(&self) -> StatusCode {
        self.status.unwrap_or_else(|| self.code.status())
    }

    pub fn invalid(parameter: &str, message: impl Into<String>) -> Self {
        Self::new(
            Code::InvalidParameter,
            message,
            json!({ "parameter": parameter }),
        )
    }

    pub fn not_found(message: impl Into<String>, details: Value) -> Self {
        Self::new(Code::NotFound, message, details)
    }

    pub fn cursor(message: impl Into<String>) -> Self {
        Self::new(Code::InvalidCursor, message, json!({"parameter": "cursor"}))
    }
}

/// A database failure is never surfaced to the client: the contract says
/// `message` is generic on purpose and the operator correlates by timestamp.
impl From<sqlx::Error> for ApiError {
    fn from(error: sqlx::Error) -> Self {
        log::error!("query failed: {error}");
        Self::new(Code::Internal, "internal server error", Value::Null)
    }
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        self.status()
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status()).json(json!({
            "error": self.code.as_str(),
            "message": self.message,
            "details": self.details,
        }))
    }
}
