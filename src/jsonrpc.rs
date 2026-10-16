use std::fmt::Display;

use crate::prelude::axum::{AxumJson, AxumResponse, IntoAxumResponse, StatusCode};
use crate::prelude::{chrono::*, serde::*, strum::EnumString};
use crate::{APP, AppError};
use reqwest::IntoUrl;
use thiserror::Error;

const ERR_ANSWER: AxumJson<&str> = AxumJson("{}");

/// запрос который приходит на gate в http
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

    match resp.status() {
        StatusCode::FORBIDDEN => return Err(Forbidden.into()),
        StatusCode::NOT_FOUND => return Err(NotFound.into()),
        StatusCode::BAD_REQUEST => return Err(BadRequest.into()),
        StatusCode::UNPROCESSABLE_ENTITY => return Err(UnprocessableEntity.into()),
        StatusCode::INTERNAL_SERVER_ERROR => return Err(InternalServerError.into()),
        _ => {}
    }

    let resp_text = resp.text().await?;

    // debug!("response: {}", resp_text);

    Ok(resp_text)
}
