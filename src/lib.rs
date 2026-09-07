#[allow(unused_imports)]
use env_logger::{Builder, Target};

use config::{Config, ConfigReady};
use postgres::{Loaded as PostgresLoaded, PostgresPools};
use reqwest::{Client as ReqwestClient, Error as ReqwestError};
use sqlx::migrate::MigrateError;
use std::sync::LazyLock;
use std::time::Duration;
use thiserror::Error;

mod config;
mod iggy;
mod jsonrpc;
mod migrations;
mod postgres;
mod rabbitmq;
mod redis;

pub mod prelude;

use crate::iggy::{Iggy, Loaded as IggyLoaded};
use crate::rabbitmq::{Loaded as RabbitMqLoaded, RabbitMq};
use crate::redis::{Loaded as RedisLoaded, Redis};
use prelude::log::info;

use crate::jsonrpc::ServiceHttpError;
use crate::prelude::consts::{DEFAULT_ENVIRONMENT, IGGY_STREAM_PREFIX, SERVICE_NAME_PATH, VERSION};

pub type RequestId = String;

pub static APP: LazyLock<AppContext> = LazyLock::new(init_by_env);

#[derive(Clone)]
pub struct AppContext {
    env: String,
    pub service_name: String,
    pub config: Config<ConfigReady>,
    pub postgres: PostgresPools<PostgresLoaded>,
    pub iggy: Iggy<IggyLoaded>,
    pub rabbitmq: RabbitMq<RabbitMqLoaded>,
    pub redis: Redis<RedisLoaded>,
    pub reqwest: ReqwestClient,
}

#[derive(Error, Debug)]
pub enum AppError {
    #[error("General error: {0}")]
    Custom(String),
    #[error("Migration error: {0}")]
    Migration(#[from] MigrateError),
    #[error("Postgres error: {0}")]
    Postgres(#[from] postgres::PostgresError),
    #[error("Iggy error: {0}")]
    Iggy(#[from] iggy::IggyError),
    #[error("RabbitMq error: {0}")]
    RabbitMq(#[from] rabbitmq::RabbitMqError),
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] ReqwestError),
    #[error("Service error: {0}")]
    Service(#[from] ServiceHttpError),
    #[error("Deserialize error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub fn skw_get_stream_name() -> String {
    format!("{}-{}", APP.config.expect_str(IGGY_STREAM_PREFIX), VERSION)
}

pub fn skw_get_back_topic_name() -> String {
    format!("gate-back-{}", VERSION)
}

pub fn skw_get_consumer_name(topic: &str) -> String {
    format!("consumer-{}", topic)
}

pub fn skw_get_queue_name(base: &str) -> String {
    format!("{}-{}", base, VERSION)
}

pub fn skw_get_stream_topic_name() -> String {
    let srv = APP
        .config
        .expect_str(SERVICE_NAME_PATH)
        .to_ascii_lowercase();

    let srv = srv.strip_prefix("skw-").unwrap_or(&srv);
    let srv = srv.strip_suffix("-service").unwrap_or(srv);

    format!("{}-{}", srv, VERSION)
}

pub fn skw_is_valid_name(name: &str) -> Option<String> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphabetic() || c.eq(&'_') || c.is_ascii_digit() || c.eq(&'-') || c.eq(&'.'))
    {
        return None;
    }

    Some(name.into())
}

fn init_by_env() -> AppContext {
    init_logger();

    let env = std::env::var("ENV").unwrap_or(DEFAULT_ENVIRONMENT.to_string());
    info!("Loading configuration for: '{}' environment", env);
    let cfg = Config::new(&env).load().override_values_from_env();

    let service_name = skw_is_valid_name(cfg.expect_str(SERVICE_NAME_PATH)).unwrap_or_else(|| {
        panic!(
            "invalid service name at {}; allowed characters is [A-z0-9_-.]",
            SERVICE_NAME_PATH
        )
    });

    let db = PostgresPools::<PostgresLoaded>::new(&cfg);
    let iggy = Iggy::<IggyLoaded>::new(&cfg);
    let rabbitmq = RabbitMq::<RabbitMqLoaded>::new(&cfg);
    let redis = Redis::<RedisLoaded>::new(&cfg);

    let app_context = AppContext {
        env,
        service_name,
        config: cfg,
        postgres: db,
        iggy,
        rabbitmq,
        redis,
        reqwest: ReqwestClient::builder()
            .timeout(Duration::from_secs(5)) // @todo move to config
            .build()
            .expect("cant build reqwest"),
    };

    info!(
        "service started: {} ({})",
        app_context.service_name, app_context.env
    );

    app_context
}

fn init_logger() {
    let mut builder = Builder::from_default_env();
    builder.target(Target::Stdout).init();
}
