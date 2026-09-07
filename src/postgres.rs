use std::marker::PhantomData;

use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use thiserror::Error;

#[allow(unused_imports)]
use crate::prelude::log::{error, info, warn};

use crate::{
    APP,
    config::{Config, ConfigReady},
};

#[derive(Error, Debug)]
pub enum PostgresError {
    #[error("Other error: {0}")]
    Custom(String),
    #[error("sqlx::Error: {0}")]
    Sqlx(#[from] sqlx::Error),
}
pub trait PostgresStates {}

#[derive(Clone, Debug)]
pub struct Loaded;

#[derive(Clone, Debug)]
pub struct Connected;

impl PostgresStates for Loaded {}
impl PostgresStates for Connected {}

const MASTER_CONFIG_PATH: &str = "/postgres/master";
const SLAVE_CONFIG_PATH: &str = "/postgres/slave";

#[derive(Debug, Clone)]
pub struct PostgresPools<S: PostgresStates> {
    master_pool: Option<PgPool>,
    master_url: Option<String>,
    slave_pool: Option<PgPool>,
    slave_url: Option<String>,
    _state: PhantomData<S>,
}

impl<S: PostgresStates> PostgresPools<S> {
    pub fn new(cfg: &Config<ConfigReady>) -> PostgresPools<Loaded> {
        let (mut master_url, mut slave_url) = (None, None);

        if cfg.get(MASTER_CONFIG_PATH).is_some() {
            master_url = Some(cfg.expect_string(&format!("{}/url", MASTER_CONFIG_PATH)));
        }

        if cfg.get(SLAVE_CONFIG_PATH).is_some() {
            slave_url = Some(cfg.expect_string(&format!("{}/url", SLAVE_CONFIG_PATH)));
        }

        PostgresPools::<Loaded> {
            master_pool: None,
            master_url,
            slave_pool: None,
            slave_url,
            _state: PhantomData,
        }
    }
}

fn configure_pool(mut options: PgPoolOptions, cfg_path: &str) -> PgPoolOptions {
    options = options.test_before_acquire(true);
    // @IMPORTANT: do not add "public" scheme in search_path it's break migrations

    if APP.config.get(cfg_path).is_some() {
        let create_schema = APP
            .config
            .expect_bool(&format!("{}/createServiceSchema", cfg_path));
        let set_search_path = APP
            .config
            .expect_bool(&format!("{}/setSearchPathByService", cfg_path));

        options = options.after_connect(move |conn, _meta| {
            let service_name = APP.service_name.clone();

            Box::pin(async move {
                if create_schema {
                    info!("Creating schema {}", service_name);
                    conn.execute(
                        format!(
                            "CREATE SCHEMA IF NOT EXISTS \"{0}\"; SET search_path = '{0}';",
                            service_name
                        )
                        .as_str(),
                    )
                    .await?;
                    return Ok(()); // if we create schema then we need to set search_path to so skip next step that sets this
                }

                if set_search_path {
                    info!("Setting search path {}", service_name);
                    conn.execute(format!("SET search_path = '{}'", service_name).as_str())
                        .await?;
                }

                Ok(())
            })
        });
    }

    options
}

impl PostgresPools<Loaded> {
    pub async fn connect(&self) -> Result<PostgresPools<Connected>, PostgresError> {
        let master_pool = match self.master_url {
            Some(ref url) => {
                info!("Connecting postgres master");
                let opts = configure_pool(PgPoolOptions::new(), MASTER_CONFIG_PATH);
                Some(opts.connect(&url.clone()).await?)
            }
            None => None,
        };

        let slave_pool = match self.slave_url {
            Some(ref url) => {
                info!("Connecting postgres slave");
                let opts = configure_pool(PgPoolOptions::new(), SLAVE_CONFIG_PATH);
                Some(opts.connect(&url.clone()).await?)
            }
            None => None,
        };

        Ok(PostgresPools::<Connected> {
            master_pool,
            master_url: self.master_url.clone(),
            slave_pool,
            slave_url: self.slave_url.clone(),
            _state: PhantomData,
        })
    }
}

impl PostgresPools<Connected> {
    pub async fn master(&self) -> Result<&PgPool, PostgresError> {
        self.master_pool.as_ref().ok_or(PostgresError::Custom(
            "Master pool not connected".to_string(),
        ))
    }

    pub async fn slave(&self) -> Result<&PgPool, PostgresError> {
        self.slave_pool.as_ref().ok_or(PostgresError::Custom(
            "Slave pool not connected".to_string(),
        ))
    }
}
