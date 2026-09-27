//! JSON-RPC 2.0 envelopes. Params and results stay opaque `Value`s: the kernel
//! has no idea what is inside them.

use serde_json::{Value, json};

use crate::codes;

#[derive(Debug, Clone, PartialEq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        RpcError {
            code,
            message: message.into(),
            data: None,
        }
    }

    pub fn with_data(code: i64, message: impl Into<String>, data: Value) -> Self {
        RpcError {
            code,
            message: message.into(),
            data: Some(data),
        }
    }

    pub fn to_value(&self) -> Value {
        let mut obj = json!({ "code": self.code, "message": self.message });
        if let Some(data) = &self.data {
            obj["data"] = data.clone();
        }
        obj
    }

    pub fn from_value(v: &Value) -> RpcError {
        RpcError {
            code: v
                .get("code")
                .and_then(Value::as_i64)
                .unwrap_or(codes::INTERNAL_ERROR),
            message: v
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("(no message)")
                .to_string(),
            data: v.get("data").cloned(),
        }
    }
}

#[derive(Debug)]
pub enum Incoming {
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    Notification {
        method: String,
        params: Value,
    },
    Response {
        id: Value,
        result: Value,
    },
    ErrorResponse {
        id: Value,
        error: RpcError,
    },
}

impl Incoming {
    pub fn method(&self) -> Option<&str> {
        match self {
            Incoming::Request { method, .. } | Incoming::Notification { method, .. } => {
                Some(method)
            }
            _ => None,
        }
    }

    pub fn id(&self) -> Option<&Value> {
        match self {
            Incoming::Request { id, .. }
            | Incoming::Response { id, .. }
            | Incoming::ErrorResponse { id, .. } => Some(id),
            Incoming::Notification { .. } => None,
        }
    }
}

/// Parses one frame body. Trailing whitespace is tolerated.
pub fn parse_frame(payload: &[u8]) -> Result<Incoming, RpcError> {
    let value: Value = serde_json::from_slice(payload)
        .map_err(|e| RpcError::new(codes::PARSE_ERROR, format!("invalid JSON: {e}")))?;

    let obj = value
        .as_object()
        .ok_or_else(|| RpcError::new(codes::INVALID_REQUEST, "frame must be a JSON object"))?;

    if let Some(err) = obj.get("error") {
        let id = obj.get("id").cloned().unwrap_or(Value::Null);
        return Ok(Incoming::ErrorResponse {
            id,
            error: RpcError::from_value(err),
        });
    }

    if let Some(method) = obj.get("method").and_then(Value::as_str) {
        let params = obj.get("params").cloned().unwrap_or(Value::Null);
        return Ok(match obj.get("id") {
            Some(id) if !id.is_null() => Incoming::Request {
                id: id.clone(),
                method: method.to_string(),
                params,
            },
            _ => Incoming::Notification {
                method: method.to_string(),
                params,
            },
        });
    }

    match obj.get("id") {
        Some(id) => Ok(Incoming::Response {
            id: id.clone(),
            result: obj.get("result").cloned().unwrap_or(Value::Null),
        }),
        None => Err(RpcError::new(
            codes::INVALID_REQUEST,
            "neither method nor id",
        )),
    }
}

pub fn request(id: u64, method: &str, params: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
}

pub fn notify(method: &str, params: Value) -> Value {
    json!({ "jsonrpc": "2.0", "method": method, "params": params })
}

pub fn success(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

pub fn failure(id: Value, error: &RpcError) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": error.to_value() })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_request_notification_and_response() {
        match parse_frame(br#"{"jsonrpc":"2.0","id":7,"method":"invoke","params":{"a":1}}"#)
            .unwrap()
        {
            Incoming::Request { id, method, params } => {
                assert_eq!(id, json!(7));
                assert_eq!(method, "invoke");
                assert_eq!(params["a"], json!(1));
            }
            other => panic!("expected Request, got {other:?}"),
        }

        match parse_frame(br#"{"jsonrpc":"2.0","method":"$/event","params":{}}"#).unwrap() {
            Incoming::Notification { method, .. } => assert_eq!(method, "$/event"),
            other => panic!("expected Notification, got {other:?}"),
        }

        match parse_frame(br#"{"jsonrpc":"2.0","id":"r-1","result":{"ok":true}}"#).unwrap() {
            Incoming::Response { id, result } => {
                assert_eq!(id, json!("r-1"));
                assert_eq!(result["ok"], json!(true));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[test]
    fn parses_error_response() {
        let incoming = parse_frame(
            br#"{"jsonrpc":"2.0","id":3,"error":{"code":-32011,"message":"gone","data":{"plugin":"x"}}}"#,
        )
        .unwrap();
        match incoming {
            Incoming::ErrorResponse { error, .. } => {
                assert_eq!(error.code, codes::PROVIDER_UNAVAILABLE);
                assert_eq!(error.data.unwrap()["plugin"], json!("x"));
            }
            other => panic!("expected ErrorResponse, got {other:?}"),
        }
    }

    #[test]
    fn null_id_with_method_is_a_notification() {
        match parse_frame(br#"{"jsonrpc":"2.0","id":null,"method":"kernel.log"}"#).unwrap() {
            Incoming::Notification { method, .. } => assert_eq!(method, "kernel.log"),
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    #[test]
    fn rejects_garbage_and_non_objects() {
        assert_eq!(
            parse_frame(b"not json").unwrap_err().code,
            codes::PARSE_ERROR
        );
        assert_eq!(
            parse_frame(b"[1,2]").unwrap_err().code,
            codes::INVALID_REQUEST
        );
    }

    #[test]
    fn builds_envelopes() {
        let req = request(1, "invoke", json!({"capability":"demo.text"}));
        assert_eq!(req["id"], json!(1));
        assert_eq!(req["method"], json!("invoke"));

        let note = notify("$/cancel", json!({"request_id":"r-1"}));
        assert!(note.get("id").is_none());

        let ok = success(json!("r-1"), json!({"x":1}));
        assert_eq!(ok["result"]["x"], json!(1));

        let err = failure(json!(2), &RpcError::new(codes::OVERLOADED, "slow down"));
        assert_eq!(err["error"]["code"], json!(codes::OVERLOADED));
    }
}
