use std::fmt::Display;

use crate::prelude::axum::{AxumJson, AxumResponse, IntoAxumResponse, StatusCode};
use crate::prelude::{chrono::*, log::error, serde::*, strum::EnumString};
use crate::{APP, AppError};
use reqwest::IntoUrl;
use thiserror::Error;

const ERR_ANSWER: AxumJson<&str> = AxumJson("{}");

/// Request body a client sends to a gate: `{"Ts", "Method", "Params"}`. For signed requests these
/// exact bytes are what the signature covers, `Ts` included (see `skw_is_outdated`).
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct JsonRpcGateHttpRequest {
    pub ts: DateTime<Utc>,
    #[serde(flatten)]
    pub method: Value,
}

#[derive(Serialize, Error, Debug, EnumString, Clone)]
pub enum SkwServiceHttpError {
    #[error("NotFound")]
    NotFound,
    #[error("Forbidden")]
    Forbidden,
    #[error("BadRequest")]
    BadRequest,
    #[error("UnprocessableEntity")]
    UnprocessableEntity,
    #[error("InternalServerError")]
    InternalServerError,
}

impl SkwServiceHttpError {
    /// HTTP status of this error, shared by every gate/service reporting it to clients
    pub fn status_code(&self) -> StatusCode {
        use SkwServiceHttpError::*;
        match self {
            NotFound => StatusCode::NOT_FOUND,
            Forbidden => StatusCode::FORBIDDEN,
            BadRequest => StatusCode::BAD_REQUEST,
            UnprocessableEntity => StatusCode::UNPROCESSABLE_ENTITY,
            InternalServerError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// Status of a provisional reply: the request is taken, its final reply comes later under the
/// same signature. `Display` is the Iggy `response-status` header value.
#[derive(Serialize, Debug, strum::Display, EnumString, Clone, Copy, PartialEq, Eq)]
pub enum SkwServiceStatus {
    Created,
    Accepted,
}

impl SkwServiceStatus {
    pub fn status_code(&self) -> StatusCode {
        match self {
            SkwServiceStatus::Created => StatusCode::CREATED,
            SkwServiceStatus::Accepted => StatusCode::ACCEPTED,
        }
    }
}

#[derive(Debug)]
pub struct JsonRpcResponse<REQ: Deserialize<'static>, RESP: Serialize> {
    _request: REQ,
    response: RESP,
}

impl<REQ: Deserialize<'static>, RESP: Serialize> JsonRpcResponse<REQ, RESP> {
    pub fn new(_request: REQ, response: RESP) -> Self {
        Self { _request, response }
    }

    pub fn into_response(self) -> AxumResponse {
        (StatusCode::OK, AxumJson(self.response)).into_response()
    }
}

#[derive(Debug)]
pub struct JsonRpcErrorResponse<REQ: Deserialize<'static>> {
    _request: REQ, //@todo save _request for tracing/analitics
    err: AppError,
}

impl<REQ: Deserialize<'static>> JsonRpcErrorResponse<REQ> {
    pub fn new(_request: REQ, err: AppError) -> Self {
        Self { _request, err } //@todo save _request for tracing/analitics
    }

    pub fn into_response(self) -> AxumResponse {
        (self.err.service_error().status_code(), ERR_ANSWER).into_response()
    }
}

pub async fn internal_http_request<S: AsRef<str> + IntoUrl + Display, T: Serialize + for<'a> Deserialize<'a> + Into<&'static str>>(
    url: S,
    req: T,
) -> Result<String, AppError> {
    use SkwServiceHttpError::*;

    let req_value: Value = serde_json::to_value(&req)?;
    let method: &'static str = req.into(); // strum return method name

    let body = json!({
        "Method": method,
        "Params": req_value["Params"]
    });

    // debug!("internal request. url: {}; body: {}", url, body);

    let resp = APP
        .reqwest
        .post(url)
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await?;

    // debug!("{:?}", &resp);

    // any non-2xx is an error: a 502/503 page must not reach the caller as a payload
    let status = resp.status();
    if !status.is_success() {
        let err = match status {
            StatusCode::FORBIDDEN => Forbidden,
            StatusCode::NOT_FOUND => NotFound,
            StatusCode::BAD_REQUEST => BadRequest,
            StatusCode::UNPROCESSABLE_ENTITY => UnprocessableEntity,
            // 401, 429, 5xx, ...: the callee failed or is unreachable; to the caller it's internal
            _ => {
                error!(
                    "internal request {} -> unexpected status {}",
                    resp.url(),
                    status
                );
                InternalServerError
            }
        };
        return Err(err.into());
    }

    let resp_text = resp.text().await?;

    // debug!("response: {}", resp_text);

    Ok(resp_text)
}

#[cfg(test)]
mod tests {
    use super::*;

    // `Display` is the Iggy header value services send and gates parse back
    #[test]
    fn service_error_header_round_trips() {
        use SkwServiceHttpError::*;
        for (e, code) in [
            (NotFound, 404),
            (Forbidden, 403),
            (BadRequest, 400),
            (UnprocessableEntity, 422),
            (InternalServerError, 500),
        ] {
            let parsed: SkwServiceHttpError = e.to_string().parse().unwrap();
            assert_eq!(parsed.status_code().as_u16(), code, "{e}");
        }
        assert!("Teapot".parse::<SkwServiceHttpError>().is_err());
    }

    #[test]
    fn service_status_header_round_trips() {
        for (s, code) in [
            (SkwServiceStatus::Created, 201),
            (SkwServiceStatus::Accepted, 202),
        ] {
            let parsed: SkwServiceStatus = s.to_string().parse().unwrap();
            assert_eq!(parsed, s);
            assert_eq!(parsed.status_code().as_u16(), code);
        }
    }

    #[test]
    fn gate_request_keeps_method_and_params() {
        let req: JsonRpcGateHttpRequest = serde_json::from_str(r#"{"Ts":"2026-01-01T00:00:00Z","Method":"Ping","Params":{"A":1}}"#).unwrap();
        assert_eq!(req.method["Method"], "Ping");
        assert_eq!(req.method["Params"]["A"], 1);
        assert!(
            req.method.get("Ts").is_none(),
            "Ts is a field, not part of the method"
        );
    }
}
