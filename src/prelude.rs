pub use super::{APP, AppContext, AppError};
pub use crate::RequestId;

pub mod tools {
    pub use crate::{
        skw_get_back_topic_name, skw_get_consumer_name, skw_get_queue_name, skw_get_stream_name, skw_get_stream_topic_name, skw_is_valid_name,
    };
    pub use bytes::Bytes;
}

pub mod chrono {
    pub use chrono::{DateTime, Utc};
}

pub mod consts {
    pub const DEFAULT_ENVIRONMENT: &str = "dev";
    pub const VERSION: &str = env!("CARGO_PKG_VERSION");

    pub const IGGY_STREAM_PREFIX: &str = "/iggy/streamPrefix";
    pub const IGGY_URL_PATH: &str = "/iggy/url";

    pub const RABBITMQ_URL_PATH: &str = "/rabbitmq/url";

    pub const REDIS_URL_PATH: &str = "/redis/url";

    pub const IGGY_HEADER_SIGNATURE: &str = "signature";
    pub const IGGY_HEADER_HTTP_METHOD: &str = "http-method";
    pub const IGGY_HEADER_HTTP_PATH: &str = "http-path";

    pub const IGGY_HEADER_API_KEY: &str = "api-key";

    // used when service had signature and response with error (BadRequest, NotFound, Forbidden, ...)
    pub const IGGY_HEADER_SERVICE_ERROR: &str = "err-response-code";

    // request-timing trace headers, stamped by the service on the back-topic response message
    pub const IGGY_HEADER_TS_SERVICE_RECEIVED: &str = "ts-service-received";
    pub const IGGY_HEADER_TS_SERVICE_SENT: &str = "ts-service-sent";

    pub const JSON_ENV_OVERRIDES_PATH: &str = "/envOverrides";

    pub const SERVICE_BIND_ADDRESS: &str = "/service/bindAddress";
    pub const SERVICE_NAME_PATH: &str = "/service/name";

    pub const GATE_TOPICS_MAP: &str = "/topicsMap";

    pub const TLS_CERT_PATH: &str = "/service/tls/certPath";
    pub const TLS_KEY_PATH: &str = "/service/tls/keyPath";

    pub const INTERNAL_URL_PREFIX: &str = "/internal";
}

pub mod futures {
    pub use futures_util::stream::StreamExt;
}

pub mod base64 {
    pub use base64::prelude::*;
}

pub mod strum {
    pub use strum::Display;
    pub use strum_macros::{EnumString, IntoStaticStr};
}
pub mod uuid {
    pub use uuid::Uuid;
}

pub mod serde {
    pub use serde::{Deserialize, Serialize};
    pub use serde_json::{Value, from_str, json};
}

pub mod axum {
    pub use axum::{
        Json as AxumJson, Router,
        body::Bytes,
        extract::State,
        http::HeaderMap,
        http::StatusCode,
        response::{IntoResponse as IntoAxumResponse, Response as AxumResponse},
        routing::{get, post},
    };
}

pub mod jsonrpc {
    pub use crate::jsonrpc::{JsonRpcErrorResponse, JsonRpcGateHttpRequest, JsonRpcResponse, ServiceHttpError, internal_http_request};
}

pub mod log {
    pub use log::{debug, error, info, trace, warn};
}

pub mod iggy {
    pub use crate::iggy::TopicConfig;
    pub use crate::iggy::rpc::{RpcMessageTrait, RpcRequestMeta, run_service_consumer};
    pub use iggy::prelude::*;
}

pub mod postgres {
    pub use crate::postgres::{Connected as PostgresConnected, PostgresError, PostgresPools};
}

pub mod rabbitmq {
    pub use crate::rabbitmq::{QueueConfig, RabbitMqError, RabbitMqPublisher, TaskOutcome, run_task_consumer};
}

pub mod redis {
    pub use crate::redis::{Redis, RedisError};
    pub use redis::aio::ConnectionManager;
    pub use redis::{AsyncCommands, RedisResult};
}

pub mod sqlx {}

pub mod migrations {
    pub use crate::migrations::run_migrations;
}
