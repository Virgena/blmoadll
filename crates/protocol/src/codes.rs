//! JSON-RPC error codes: the standard five plus the eggshellmod block.

// JSON-RPC 2.0 standard codes, reused as-is.
pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;

// eggshellmod codes.
pub const UNKNOWN_CAPABILITY: i64 = -32010;
/// Provider unavailable / already exited. In `--check` this means the plugin
/// exited unexpectedly after answering `initialize`.
pub const PROVIDER_UNAVAILABLE: i64 = -32011;
pub const REQUEST_TIMEOUT: i64 = -32012;
pub const CANCELLED: i64 = -32013;
pub const NOT_STARTED: i64 = -32014;
pub const PROTOCOL_VERSION_MISMATCH: i64 = -32015;
/// Frame exceeded `max_frame_bytes`. The pipe is already broken, so this is only
/// reported through logs, events and `--check`, never as a response.
pub const FRAME_TOO_LARGE: i64 = -32016;
pub const UNKNOWN_STREAM: i64 = -32017;
pub const INVALID_CONFIG: i64 = -32018;
pub const OVERLOADED: i64 = -32019;
pub const PAYLOAD_TOO_LARGE: i64 = -32020;
pub const UNKNOWN_SUBSCRIPTION: i64 = -32021;

/// Stable short name, for logs and `--check` output.
pub fn name(code: i64) -> &'static str {
    match code {
        PARSE_ERROR => "parse_error",
        INVALID_REQUEST => "invalid_request",
        METHOD_NOT_FOUND => "method_not_found",
        INVALID_PARAMS => "invalid_params",
        INTERNAL_ERROR => "internal_error",
        UNKNOWN_CAPABILITY => "unknown_capability",
        PROVIDER_UNAVAILABLE => "provider_unavailable",
        REQUEST_TIMEOUT => "request_timeout",
        CANCELLED => "cancelled",
        NOT_STARTED => "not_started",
        PROTOCOL_VERSION_MISMATCH => "protocol_version_mismatch",
        FRAME_TOO_LARGE => "frame_too_large",
        UNKNOWN_STREAM => "unknown_stream",
        INVALID_CONFIG => "invalid_config",
        OVERLOADED => "overloaded",
        PAYLOAD_TOO_LARGE => "payload_too_large",
        UNKNOWN_SUBSCRIPTION => "unknown_subscription",
        _ => "unknown_code",
    }
}
