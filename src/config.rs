use crate::prelude::consts::JSON_ENV_OVERRIDES_PATH;
use crate::prelude::log::warn;
use log::info;
use serde_json::Value;
use std::env;
use std::marker::PhantomData;
use std::path::PathBuf;
pub trait ConfigStates {}

#[derive(Debug, Clone)]
pub struct ConfigNew;

#[derive(Debug, Clone)]
pub struct ConfigReady;

impl ConfigStates for ConfigNew {}
impl ConfigStates for ConfigReady {}

#[derive(Debug, Clone)]
pub struct Config<S: ConfigStates> {
    pub config: Option<Value>,
    env: String,
    pub real_path: Option<PathBuf>,
    _state: PhantomData<S>,
}

impl Config<ConfigNew> {
    pub fn new(env: &str) -> Config<ConfigNew> {
        Config {
            real_path: None,
            env: env.to_string(),
            config: None,
            _state: PhantomData,
        }
    }
}

impl Config<ConfigNew> {
    pub fn load(self) -> Config<ConfigReady> {
        let cdir = env::current_dir()
            .expect("invalid current directory")
            .canonicalize()
            .expect("invalid working directory");

        let config_path = format!("{}/{}.json", cdir.display(), self.env);
        info!("Loading configuration file: {}", config_path);

        let cfg = std::fs::read_to_string(config_path)
            .map(|json| serde_json::from_str(&json).expect("invalid config json"))
            .expect("read config file error");

        Config {
            config: Some(cfg),
            env: self.env,
            real_path: Some(cdir),
            _state: PhantomData,
        }
    }
}

fn convert_env_to_value(orig_value: &mut Value, new_value: String) -> Value {
    if orig_value.is_string() {
        return Value::String(new_value.parse().expect("invalid string in env"));
    }

    if orig_value.is_number() {
        return Value::Number(new_value.parse().expect("invalid number in env"));
    }

    if orig_value.is_boolean() {
        return Value::Bool(new_value.parse().expect("invalid boolean in env"));
    }

    if orig_value.is_array() {
        let v: Vec<Value> = serde_json::from_str(&new_value).expect("invalid array in env");
        return Value::Array(v);
    }

    panic!("Unsupported config type");
}

impl Config<ConfigReady> {
    pub fn override_values_from_env(mut self) -> Config<ConfigReady> {
        let cfg = self.config.as_mut().unwrap();

        if let Some(overrides) = cfg
            .pointer(JSON_ENV_OVERRIDES_PATH)
            .and_then(|v| v.as_object().cloned())
        {
            for (key, env_name) in overrides {
                match cfg.pointer_mut(&key) {
                    Some(v) => {
                        let env_value = std::env::var(env_name.as_str().unwrap());

                        match env_value {
                            Ok(ev) => {
                                warn!("Override value for {} from env var {}", key, env_name);
                                *v = convert_env_to_value(v, ev);
                            }
                            Err(e) => match e {
                                std::env::VarError::NotPresent => {
                                    warn!("Environment variable {} not found", env_name);
                                }
                                _ => {
                                    warn!(
                                        "Cannot override value for {} from env var {}: {}",
                                        key, env_name, e
                                    );
                                    panic!()
                                }
                            },
                        }
                    }
                    None => {
                        warn!("Cannot override value for '{}' path does not exist", key);
                    }
                }
            }
        }
        // dbg!(&self);
        self
    }
}

impl Config<ConfigReady> {
    pub fn get(&self, path: &str) -> Option<&Value> {
        self.config.as_ref().and_then(|cfg| cfg.pointer(path))
    }

    pub fn expect_str(&self, path: &str) -> &str {
        self.config
            .as_ref()
            .and_then(|cfg| cfg.pointer(path))
            .and_then(|v| v.as_str())
            .expect(&format!("invalid string config value: {}", path))
    }

    pub fn expect_string(&self, path: &str) -> String {
        self.config
            .as_ref()
            .and_then(|cfg| cfg.pointer(path))
            .and_then(|v| v.as_str())
            .expect(&format!("invalid string config value: {}", path))
            .to_string()
    }

    pub fn expect_bool(&self, path: &str) -> bool {
        self.config
            .as_ref()
            .and_then(|cfg| cfg.pointer(path))
            .and_then(|v| v.as_bool())
            .expect(&format!("invalid bool config value: {}", path))
    }

    pub fn expect_value_cloned(&self, path: &str) -> Value {
        self.config
            .as_ref()
            .and_then(|cfg| cfg.pointer(path))
            .expect(&format!("invalid config value: {}", path))
            .clone()
    }

    pub fn expect_u64(&self, path: &str) -> u64 {
        self.config
            .as_ref()
            .and_then(|cfg| cfg.pointer(path))
            .and_then(|v| v.as_u64())
            .expect(&format!("invalid u64 config value: {}", path))
    }

    pub fn expect_u32(&self, path: &str) -> u32 {
        self.config
            .as_ref()
            .and_then(|cfg| cfg.pointer(path))
            .and_then(|v| v.as_u64().map(|x| x as u32))
            .expect(&format!("invalid u32 config value: {}", path))
    }
}
