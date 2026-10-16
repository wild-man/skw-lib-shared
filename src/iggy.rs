use bytes::Bytes;
use futures_util::StreamExt;
use iggy::prelude::*;
use itertools::Itertools;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{collections::BTreeMap, fmt::Display, future::Future, str::FromStr};
use strum::{Display, EnumProperty, EnumString, IntoEnumIterator};

#[allow(unused_imports)]
use crate::prelude::log::{error, info, warn};
use crate::{
    APP, AppError,
    prelude::consts::{IGGY_HEADER_SERVICE_ERROR, IGGY_HEADER_SIGNATURE, IGGY_URL_PATH},
    skw_get_back_topic_name, skw_get_consumer_name, skw_get_stream_name, skw_get_stream_topic_name,
};

use iggy::{
    clients::client::IggyClient,
    prelude::{Client, CompressionAlgorithm, IggyDuration, IggyExpiry, MaxTopicSize},
};
use serde_json::Value;

use crate::config::{Config, ConfigReady};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SkwIggyError {
    #[error("Other error: {0}")]
    Custom(String),
    #[error("iggy::Error: {0}")]
    Iggy(#[from] iggy::prelude::IggyError),
}

#[derive(Clone, Debug, Display, EnumString, PartialEq, Eq)]
#[strum(ascii_case_insensitive)]
pub enum SkwTopicType {
    #[strum(serialize = "public", to_string = "public")]
    Public,
    #[strum(serialize = "open", to_string = "open")]
    Open,
    #[strum(serialize = "signed", to_string = "signed")]
    Signed,
}

impl TryFrom<&Value> for SkwTopicType {
    type Error = AppError;
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let s = value
            .as_str()
            .ok_or_else(|| format!("topic type must be a string, got: {value}"))?;
        s.parse()
            .map_err(|_| format!("unknown topic type: {s}").into())
    }
}

pub trait IggyStates {}

#[derive(Clone, Debug)]
pub struct Loaded;
pub struct Connected;

impl IggyStates for Loaded {}
impl IggyStates for Connected {}

#[derive(Clone, Debug)]
pub struct Iggy<S: IggyStates> {
    client: Option<Arc<IggyClient>>,
    _state: PhantomData<S>,
}

impl<S: IggyStates> Iggy<S> {
    pub fn new(_cfg: &Config<ConfigReady>) -> Iggy<Loaded> {
        Iggy::<Loaded> {
            client: None,
            _state: PhantomData,
        }
    }
}

impl Iggy<Loaded> {
    pub async fn connect(self) -> Iggy<Connected> {
        let url = APP.config.expect_str(IGGY_URL_PATH); // only check that url exists
        let iggy = IggyClient::from_connection_string(url).expect("Failed to create iggy client from url");

        iggy.connect().await.expect("cant connect to iggy");

        Iggy::<Connected> {
            client: Some(Arc::new(iggy)),
            _state: PhantomData,
        }
    }
}

impl Iggy<Connected> {
    pub fn inner(&self) -> Option<Arc<IggyClient>> {
        self.client.clone()
    }
}

pub struct TopicConfig {
    pub topic_types: Vec<SkwTopicType>,
    pub partitions_count: u32,
    pub compression_algorithm: CompressionAlgorithm,
    pub message_expiry: IggyExpiry,
    pub max_topic_size: MaxTopicSize,
}

impl TryInto<TopicConfig> for &Value {
    type Error = AppError;

    fn try_into(self) -> Result<TopicConfig, Self::Error> {
        let obj = self
            .as_object()
            .ok_or_else(|| AppError::Custom("invalid topic config".into()))?;

        // let custom_name = obj
        //     .get("customName")
        //     .and_then(Value::as_str)
        //     .filter(|s| !s.is_empty())
        //     .map(|s| s.to_string());

        let partitions_count = obj
            .get("partitionsCount")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32;

        let compression_algorithm = obj
            .get("compressionAlgorithm")
            .and_then(Value::as_str)
            .and_then(|s| s.parse().ok())
            .unwrap_or_default();

        let message_expiry = IggyExpiry::ExpireDuration(IggyDuration::new_from_secs(
            obj.get("messageExpiry")
                .and_then(Value::as_u64)
                .expect("invalid duration"),
        ));

        let max_topic_size: MaxTopicSize = obj
            .get("maxTopicSize")
            .and_then(Value::as_u64)
            .unwrap_or_default()
            .into();

        let mut topic_types = vec![];
        for t in obj
            .get("topicTypes")
            .and_then(Value::as_array)
            .expect("invalid topic types")
        {
            let topic_type: SkwTopicType = t.try_into()?;
            topic_types.push(topic_type);
        }

        if topic_types.is_empty() {
            return Err(AppError::Custom("empty topics config".into()));
        }

        Ok(TopicConfig {
            topic_types,
            partitions_count,
            compression_algorithm,
            message_expiry,
            max_topic_size,
        })
    }
}

pub mod rpc {
    use super::*;
    use crate::prelude::consts::SERVICE_TOPIC_TYPES_PATH;
    use crate::prelude::consts::{BACK_MESSAGE_TIMEOUT, IGGY_HEADER_TS_SERVICE_RECEIVED, IGGY_HEADER_TS_SERVICE_SENT};
    use chrono::{SecondsFormat, Utc};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::task::JoinSet;

    #[derive(Debug, Clone)]
    pub struct RpcRequestMeta {
        pub http_method: String,
        pub http_path: String,
        pub signature: String,
        pub api_key: Option<String>,
    }

    pub trait RpcMessageTrait: Sized {
        type RpcMethod;
        fn signature(&self) -> &str;
        fn http_method(&self) -> &str;
        fn http_path(&self) -> &str;
        fn api_key(&self) -> Option<&str>;
        fn into_rpc_method(self) -> Result<Self::RpcMethod, AppError>;

        fn meta(&self) -> RpcRequestMeta {
            RpcRequestMeta {
                http_method: self.http_method().to_string(),
                http_path: self.http_path().to_string(),
                signature: self.signature().to_string(),
                api_key: self.api_key().map(|s| s.to_string()),
            }
        }
    }

    pub async fn run_service_consumer<Req, H, Fut, Resp>(
        handler: H,
        pool_interval: Option<&str>,
        batch_size: Option<u32>,
        topic_type: SkwTopicType,
    ) -> Result<(), AppError>
    where
        ReceivedMessage: TryInto<Req>,
        <ReceivedMessage as TryInto<Req>>::Error: Display,
        Req: RpcMessageTrait,
        H: Fn(Req::RpcMethod, RpcRequestMeta) -> Fut,
        Fut: Future<Output = Result<Resp, AppError>>,
        Resp: Serialize,
    {
        let stream = skw_get_stream_name();
        let topic = skw_get_stream_topic_name(topic_type);
        let back_topic = skw_get_back_topic_name();
        let consumer = skw_get_consumer_name(&topic);

        let iggy = APP.iggy.clone().connect().await;
        let iggy_client = iggy.inner().expect("bad iggy client");

        info!(
            "connecting to iggy. stream: {}; topic: {}; back topic: {}",
            stream, topic, back_topic
        );

        let pool_interval = match pool_interval {
            Some(i) => IggyDuration::from_str(i).map_err(|e| AppError::Custom(format!("invalid pool interval: {}; error:{}", i, e)))?,
            None => IggyDuration::from_str("1ms").map_err(|e| AppError::Custom(e.to_string()))?,
        };

        let batch_size = batch_size.unwrap_or(1);

        let mut iggy_consumer = iggy_client
            .consumer_group(&consumer, &stream, &topic)?
            .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingEachMessage))
            .create_consumer_group_if_not_exists()
            .auto_join_consumer_group()
            .polling_strategy(PollingStrategy::next())
            .poll_interval(pool_interval)
            .batch_length(batch_size)
            .build();

        iggy_consumer.init().await?;

        let back_topic_producer = iggy_client
            .producer(&stream, &back_topic)
            .expect("producer create error")
            .partitioning(Partitioning::balanced())
            .build();

        while let Some(message) = iggy_consumer.next().await {
            let ts_service_received = Utc::now();

            let received: ReceivedMessage = match message {
                Ok(received) => received,
                Err(error) => {
                    error!("Error while receiving message: {error}");
                    continue;
                }
            };

            // messages older than BACK_MESSAGE_TIMEOUT have already caused the
            // originating gate to give up waiting on this signature (see
            // gates/api's await_service_response / gates/ws's timeout
            // watchdog) — skip the handler entirely rather than doing work
            // (and publishing a reply) that nobody is listening for anymore.
            let message_sent_at = UNIX_EPOCH + std::time::Duration::from_micros(received.message.header.timestamp);
            let message_age = SystemTime::now()
                .duration_since(message_sent_at)
                .unwrap_or_default();

            if message_age > BACK_MESSAGE_TIMEOUT {
                warn!(
                    "skipping stale message (age: {:?} > {:?}); offset: {}, partition: {}",
                    message_age, BACK_MESSAGE_TIMEOUT, received.current_offset, received.partition_id
                );
                continue;
            }

            let req: Req = match received.try_into() {
                Ok(req) => req,
                Err(e) => {
                    error!("error: {}", e); // нет сигнатуры, отвечать некому
                    continue;
                }
            };

            let signature = req.signature().to_string();
            let mut headers = BTreeMap::new();
            headers.insert(
                HeaderKey::from_str(IGGY_HEADER_SIGNATURE)?,
                HeaderValue::from_str(&signature)?,
            );

            let meta = req.meta();
            let rpc_payload = match req.into_rpc_method() {
                Ok(rpc_method) => handler(rpc_method, meta).await,
                Err(svc_err) => Err(svc_err),
            };

            let ts_service_sent = Utc::now();
            headers.insert(
                HeaderKey::from_str(IGGY_HEADER_TS_SERVICE_RECEIVED)?,
                HeaderValue::from_str(&ts_service_received.to_rfc3339_opts(SecondsFormat::Nanos, true))?,
            );
            headers.insert(
                HeaderKey::from_str(IGGY_HEADER_TS_SERVICE_SENT)?,
                HeaderValue::from_str(&ts_service_sent.to_rfc3339_opts(SecondsFormat::Nanos, true))?,
            );

            let payload_bytes = match rpc_payload {
                Ok(resp) => Bytes::copy_from_slice(&serde_json::to_vec(&resp)?),
                Err(svc_err) => {
                    // full error stays in the service log; only the client-facing code goes on the bus
                    error!("{}", svc_err);
                    headers.insert(
                        HeaderKey::from_str(IGGY_HEADER_SERVICE_ERROR)?,
                        HeaderValue::from_str(&svc_err.service_error().to_string())?,
                    );
                    Bytes::copy_from_slice(b"{}")
                }
            };

            let message = IggyMessage::builder()
                .payload(payload_bytes)
                .user_headers(headers)
                .build()?;

            if let Err(e) = back_topic_producer.send_one(message).await {
                error!("error sending message to iggy: {}", e);
            }
        }

        Ok(())
    }

    pub fn get_config_topic_types() -> Result<Vec<SkwTopicType>, AppError> {
        APP.config
            .get(SERVICE_TOPIC_TYPES_PATH)
            .and_then(Value::as_array)
            .ok_or_else(|| {
                AppError::Custom(format!(
                    "invalid config array value: {}",
                    SERVICE_TOPIC_TYPES_PATH
                ))
            })?
            .iter()
            .unique()
            .map(SkwTopicType::try_from)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Runs one `run_service_consumer` task per topic type listed in `/service/topicTypes` config,
    /// all sharing the same handler. Keeps running while at least one consumer is alive and
    /// returns an error only after every consumer has stopped.
    pub async fn run_service_consumers_all_topic_types<Req, H, Fut, Resp>(
        handler: H,
        pool_interval: Option<&str>,
        batch_size: Option<u32>,
    ) -> Result<(), AppError>
    where
        ReceivedMessage: TryInto<Req>,
        <ReceivedMessage as TryInto<Req>>::Error: Display,
        Req: RpcMessageTrait + Send + 'static,
        Req::RpcMethod: Send + EnumProperty + IntoEnumIterator,
        for<'a> &'a Req::RpcMethod: Into<&'static str>,
        H: Fn(Req::RpcMethod, RpcRequestMeta) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<Resp, AppError>> + Send,
        Resp: Serialize + Send,
    {
        let topic_types = get_config_topic_types()?;

        validate_rpc_methods_by_topics::<Req::RpcMethod>(&topic_types);

        let pool_interval = pool_interval.map(str::to_string);

        let mut consumers = JoinSet::new();
        for topic_type in topic_types {
            let handler = handler.clone();
            let pool_interval = pool_interval.clone();

            consumers.spawn(async move {
                let result = run_service_consumer::<Req, _, _, _>(
                    handler,
                    pool_interval.as_deref(),
                    batch_size,
                    topic_type.clone(),
                )
                .await;
                (topic_type, result)
            });
        }

        while let Some(joined) = consumers.join_next().await {
            match joined {
                Ok((topic_type, Ok(()))) => warn!(
                    "consumer for topic type {topic_type} stopped; {} still running",
                    consumers.len()
                ),
                Ok((topic_type, Err(e))) => error!(
                    "consumer for topic type {topic_type} failed: {e}; {} still running",
                    consumers.len()
                ),
                Err(e) => error!(
                    "consumer task panicked or was cancelled: {e}; {} still running",
                    consumers.len()
                ),
            }
        }

        Err(AppError::Custom("all topic consumers stopped".into()))
    }
}

fn validate_rpc_methods_by_topics<M>(topic_types: &[SkwTopicType])
where
    M: IntoEnumIterator + EnumProperty,
    for<'a> &'a M: Into<&'static str>,
{
    if topic_types.is_empty() {
        panic!("Error: empty topic types");
    }

    for m in M::iter() {
        let name: &'static str = (&m).into();
        let signed: Option<SkwTopicType> = m.get_str("topic").map(|tt| {
            tt.parse()
                .unwrap_or_else(|_| panic!("Invalid topic type: {tt}"))
        });

        if let Some(SkwTopicType::Signed) = signed
            && !topic_types.iter().all(|t| t == &SkwTopicType::Signed)
        {
            panic!("Error: signed method: {name} can be used in unsigned topics");
        }
    }
}
