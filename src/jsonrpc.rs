use std::fmt::Display;

use crate::prelude::axum::{AxumJson, AxumResponse, IntoAxumResponse, StatusCode};
use crate::prelude::{chrono::*, log::debug, serde::*, strum::EnumString};
use crate::{APP, AppError};
use reqwest::IntoUrl;
use thiserror::Error;

const ERR_ANSWER: AxumJson<&str> = AxumJson("{}");

/// запрос который приходит на gate в http
// @todo попробовать добавитиь сюда дополнительное обязательно поле ts: DateTime<Utc> c милисекундами или нано секундами
// что бы любой сформированный пользовательский запрос имел уникальную signature
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct JsonRpcGateHttpRequest {
    pub ts: DateTime<Utc>,
    #[serde(flatten)]
    pub method: Value,
}

#[derive(Serialize, Error, Debug, EnumString, Clone)]
pub enum ServiceHttpError {
    #[error("NotFound")]
    NotFound,
    #[error("Forbidden")]
    Forbidden,
    #[error("BadRequest")]
    BadRequest,
    #[error("UnprocessableEntity")]
    UnprocessableEntity, //@todo represent all passible http service errors (forbidden/not found/unprocessable entity/... )
}

#[derive(Debug)]
pub struct JsonRpcResponse<REQ: Deserialize<'static>, RESP: Serialize> {
    request: REQ,
    response: RESP,
}

impl<REQ: Deserialize<'static>, RESP: Serialize> JsonRpcResponse<REQ, RESP> {
    pub fn new(request: REQ, response: RESP) -> Self {
        Self { request, response }
    }

    pub fn into_response(self) -> AxumResponse {
        (StatusCode::OK, AxumJson(self.response)).into_response()
    }
}

#[derive(Debug)]
pub struct JsonRpcErrorResponse<REQ: Deserialize<'static>> {
    request: REQ,
    err: AppError,
}

impl<REQ: Deserialize<'static>> JsonRpcErrorResponse<REQ> {
    pub fn new(request: REQ, err: AppError) -> Self {
        Self { request, err }
    }

    pub fn into_response(self) -> AxumResponse {
        use ServiceHttpError::*;
        match self.err {
            AppError::Service(serr) => match serr {
                NotFound => (StatusCode::NOT_FOUND, ERR_ANSWER).into_response(),
                Forbidden => (StatusCode::FORBIDDEN, ERR_ANSWER).into_response(),
                BadRequest => (StatusCode::BAD_REQUEST, ERR_ANSWER).into_response(),
                UnprocessableEntity => (StatusCode::UNPROCESSABLE_ENTITY, ERR_ANSWER).into_response(),
            },
            _ => (StatusCode::INTERNAL_SERVER_ERROR, ERR_ANSWER).into_response(),
        }
    }
}

pub async fn internal_http_request<S: AsRef<str> + IntoUrl + Display, T: Serialize + for<'a> Deserialize<'a> + Into<&'static str>>(
    url: S,
    req: T,
) -> Result<String, AppError> {
    use ServiceHttpError::*;

    let req_value: Value = serde_json::to_value(&req).unwrap(); // unwrap is safe
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
        _ => {}
    }

    let resp_text = resp.text().await?;
    debug!("response: {}", resp_text);
    Ok(resp_text)
}
