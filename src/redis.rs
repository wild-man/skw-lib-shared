use std::marker::PhantomData;

use redis::Client;
use redis::aio::ConnectionManager;
use thiserror::Error;

use crate::config::{Config, ConfigReady};
use crate::{APP, prelude::consts::REDIS_URL_PATH};

#[derive(Error, Debug)]
pub enum SkwRedisError {
    #[error("Other error: {0}")]
    Custom(String),
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),
}

pub trait RedisStates {}

#[derive(Clone, Debug)]
pub struct Loaded;
#[derive(Clone, Debug)]
pub struct Connected;

impl RedisStates for Loaded {}
impl RedisStates for Connected {}

#[derive(Clone, Debug)]
pub struct Redis<S: RedisStates> {
    connection: Option<ConnectionManager>,
    _state: PhantomData<S>,
}

impl<S: RedisStates> Redis<S> {
    pub fn new(_cfg: &Config<ConfigReady>) -> Redis<Loaded> {
        Redis::<Loaded> {
            connection: None,
            _state: PhantomData,
        }
    }
}

impl Redis<Loaded> {
    pub async fn connect(self) -> Result<Redis<Connected>, SkwRedisError> {
        let url = APP.config.expect_str(REDIS_URL_PATH);
        let client = Client::open(url)?;
        let manager = ConnectionManager::new(client).await?;

        Ok(Redis::<Connected> {
            connection: Some(manager),
            _state: PhantomData,
        })
    }
}

impl Redis<Connected> {
    pub fn inner(&self) -> ConnectionManager {
        // `Connected` is only built by `connect()`, which always sets the connection
        self.connection
            .clone()
            .expect("Redis<Connected> always holds a connection")
    }
}
