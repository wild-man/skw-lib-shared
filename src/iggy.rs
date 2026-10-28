use bytes::Bytes;
use futures_util::StreamExt;
use iggy::prelude::*;
use itertools::Itertools;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{collections::BTreeMap, fmt::Display, future::Future, str::FromStr};
use strum::{Display, EnumProperty, EnumString, IntoEnumIterator};

use crate::prelude::log::{error, info, warn};
use crate::{
    APP, AppError, prelude::consts::IGGY_URL_PATH, skw_get_back_topic_name, skw_get_consumer_name, skw_get_stream_name, skw_get_stream_topic_name,
};

use iggy::{
    clients::client::IggyClient,
    prelude::{Client, CompressionAlgorithm, IggyDuration, IggyExpiry, MaxTopicSize},
};
use serde_json::Value;

use crate::config::{Config, ConfigReady};
use thiserror::Error;

/// Iggy header keys, built once from the `IGGY_HEADER_*` names in `prelude::consts`
/// (valid constant names, so building them can't fail; `header_keys_build` checks every one).
pub mod header_keys {
    use std::sync::LazyLock;

    use iggy::prelude::HeaderKey;

    use crate::prelude::consts::*;

    fn key(name: &str) -> HeaderKey {
        name.parse()
            .unwrap_or_else(|e| panic!("invalid iggy header name {name}: {e}"))
    }

    pub static SIGNATURE: LazyLock<HeaderKey> = LazyLock::new(|| key(IGGY_HEADER_SIGNATURE));
    pub static API_KEY: LazyLock<HeaderKey> = LazyLock::new(|| key(IGGY_HEADER_API_KEY));
    pub static HTTP_METHOD: LazyLock<HeaderKey> = LazyLock::new(|| key(IGGY_HEADER_HTTP_METHOD));
    pub static HTTP_PATH: LazyLock<HeaderKey> = LazyLock::new(|| key(IGGY_HEADER_HTTP_PATH));
    pub static SERVICE_ERROR: LazyLock<HeaderKey> = LazyLock::new(|| key(IGGY_HEADER_SERVICE_ERROR));
    pub static SERVICE_STATUS: LazyLock<HeaderKey> = LazyLock::new(|| key(IGGY_HEADER_SERVICE_STATUS));
    pub static TS_SERVICE_RECEIVED: LazyLock<HeaderKey> = LazyLock::new(|| key(IGGY_HEADER_TS_SERVICE_RECEIVED));
    pub static TS_SERVICE_SENT: LazyLock<HeaderKey> = LazyLock::new(|| key(IGGY_HEADER_TS_SERVICE_SENT));
    pub static TARGET_WS_CONNECTION_ID: LazyLock<HeaderKey> = LazyLock::new(|| key(IGGY_HEADER_TARGET_WS_CONNECTION_ID));
}

/// A header's value as a string, looked up in a header map decoded once per message
/// (`IggyMessage::user_headers_map`; `get_user_header` decodes the whole map on every call).
pub fn header_str(headers: Option<&BTreeMap<HeaderKey, HeaderValue>>, key: &HeaderKey) -> Option<String> {
    headers?.get(key).map(|v| v.to_string_value())
}

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
    pub async fn connect(self) -> Result<Iggy<Connected>, SkwIggyError> {
        let url = APP.config.expect_str(IGGY_URL_PATH); // config is fail-fast by design
        let iggy = IggyClient::from_connection_string(url)?;

        iggy.connect().await?;

        Ok(Iggy::<Connected> {
            client: Some(Arc::new(iggy)),
            _state: PhantomData,
        })
    }
}

impl Iggy<Connected> {
    pub fn inner(&self) -> Arc<IggyClient> {
        // `Connected` is only built by `connect()`, which always sets the client
        self.client
            .clone()
            .expect("Iggy<Connected> always holds a client")
    }
}

pub struct TopicConfig {
    pub topic_types: Vec<SkwTopicType>,
    pub partitions_count: u32,
    pub compression_algorithm: CompressionAlgorithm,
    pub message_expiry: IggyExpiry,
    pub max_topic_size: MaxTopicSize,
}

impl TryFrom<&Value> for TopicConfig {
    type Error = AppError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let obj = value.as_object().ok_or("topic config must be an object")?;

        // required, 1..=u32::MAX: a topic with no partitions accepts no messages
        let partitions_count = obj
            .get("partitionsCount")
            .and_then(Value::as_u64)
            .ok_or("partitionsCount must be a positive integer")?;

        let partitions_count = u32::try_from(partitions_count)
            .ok()
            .filter(|&n| n > 0)
            .ok_or_else(|| format!("partitionsCount out of range: {partitions_count}"))?;

        // optional; present but unknown is an error, not a silent default
        let compression_algorithm = match obj.get("compressionAlgorithm") {
            None => CompressionAlgorithm::default(),
            Some(v) => v
                .as_str()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| format!("unknown compressionAlgorithm: {v}"))?,
        };

        let expiry_secs = obj
            .get("messageExpiry")
            .and_then(Value::as_u64)
            .ok_or("messageExpiry (seconds) is required")?;
        let message_expiry = IggyExpiry::ExpireDuration(IggyDuration::new_from_secs(expiry_secs));

        // optional; missing or 0 = server default
        let max_topic_size: MaxTopicSize = obj
            .get("maxTopicSize")
            .and_then(Value::as_u64)
            .unwrap_or_default()
            .into();

        let topic_types = obj
            .get("topicTypes")
            .and_then(Value::as_array)
            .ok_or("topicTypes must be an array")?
            .iter()
            .map(SkwTopicType::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        if topic_types.is_empty() {
            return Err("topicTypes must not be empty".into());
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
    use crate::prelude::consts::BACK_MESSAGE_TIMEOUT;
    use crate::prelude::consts::SERVICE_TOPIC_TYPES_PATH;
    use crate::prelude::jsonrpc::SkwServiceStatus;
    use chrono::{DateTime, SecondsFormat, Utc};
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

    /// Reply payload of an RPC handler. `status()` = Some → provisional reply (Created/Accepted):
    /// the request is taken (e.g. a task queued). Whoever finishes the work sends the final reply
    /// later under the same signature (see [`task_done::TaskDonePublisher`]): the ws gate delivers
    /// both, the api gate answers HTTP with the provisional one and skips the final one.
    pub trait RpcReply: Serialize {
        fn status(&self) -> Option<SkwServiceStatus> {
            None
        }
    }

    /// Back-topic message answering `signature`: the payload (+ provisional status header), or the
    /// error header + `{}`.
    fn build_back_message<Resp: RpcReply>(
        signature: &str,
        result: Result<Resp, AppError>,
        ts_service_received: Option<DateTime<Utc>>,
    ) -> Result<IggyMessage, AppError> {
        let mut headers = BTreeMap::new();
        headers.insert(
            header_keys::SIGNATURE.clone(),
            HeaderValue::from_str(signature)?,
        );

        if let Some(ts) = ts_service_received {
            headers.insert(
                header_keys::TS_SERVICE_RECEIVED.clone(),
                HeaderValue::from_str(&ts.to_rfc3339_opts(SecondsFormat::Nanos, true))?,
            );
        }
        headers.insert(
            header_keys::TS_SERVICE_SENT.clone(),
            HeaderValue::from_str(&Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true))?,
        );

        let payload_bytes = match result {
            Ok(resp) => {
                if let Some(status) = resp.status() {
                    headers.insert(
                        header_keys::SERVICE_STATUS.clone(),
                        HeaderValue::from_str(&status.to_string())?,
                    );
                }
                Bytes::copy_from_slice(&serde_json::to_vec(&resp)?)
            }
            Err(svc_err) => {
                // full error stays in the service log; only the client-facing code goes on the bus
                error!("{}", svc_err);
                headers.insert(
                    header_keys::SERVICE_ERROR.clone(),
                    HeaderValue::from_str(&svc_err.service_error().to_string())?,
                );
                Bytes::copy_from_slice(b"{}")
            }
        };

        Ok(IggyMessage::builder()
            .payload(payload_bytes)
            .user_headers(headers)
            .build()?)
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
        Resp: RpcReply,
    {
        let stream = skw_get_stream_name();
        let topic = skw_get_stream_topic_name(topic_type);
        let back_topic = skw_get_back_topic_name();
        let consumer = skw_get_consumer_name(&topic);

        let iggy_client = APP.iggy.clone().connect().await?.inner();

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

        // up to `batch_size` messages handled at once (in this task, so `handler` needs no `Send`):
        // a slow handler delays only its own reply, not every request behind it on this topic
        let concurrency = usize::try_from(batch_size).unwrap_or(usize::MAX).max(1);
        let handler = &handler;
        let back_topic_producer = &back_topic_producer;

        iggy_consumer
            .for_each_concurrent(concurrency, |message| async move {
                let ts_service_received = Utc::now();

                let received: ReceivedMessage = match message {
                    Ok(received) => received,
                    Err(error) => {
                        error!("Error while receiving message: {error}");
                        return;
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
                    return;
                }

                let req: Req = match received.try_into() {
                    Ok(req) => req,
                    Err(e) => {
                        error!("error: {}", e); // no signature: nobody to answer
                        return;
                    }
                };

                let signature = req.signature().to_string();

                let meta = req.meta();
                let rpc_payload = match req.into_rpc_method() {
                    Ok(rpc_method) => handler(rpc_method, meta).await,
                    Err(svc_err) => Err(svc_err),
                };

                match build_back_message(&signature, rpc_payload, Some(ts_service_received)) {
                    Err(e) => error!(
                        "error built back message signature: {}; error {}",
                        signature, e
                    ),
                    Ok(message) => {
                        if let Err(e) = back_topic_producer.send_one(message).await {
                            error!(
                                "error sending message to iggy. signature: {}; {}",
                                signature, e
                            );
                        }
                    }
                }
            })
            .await;

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
        // Sync: one consumer runs several handler calls at once (see run_service_consumer)
        H: Fn(Req::RpcMethod, RpcRequestMeta) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<Resp, AppError>> + Send,
        Resp: RpcReply + Send,
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

/// "Task done" messages from RabbitMQ workers, published to any Iggy topic of the service stream:
/// - the gates' back topic (`skw_get_back_topic_name`) with the request's `signature`: the final
///   reply to a request that got a provisional (Created/Accepted) reply, delivered by the gate
///   that still waits for it (ws); used by ping-mq-service
/// - a per-task topic (`skw_get_task_done_notify_topic_name`): a notification for other services;
///   nothing consumes these yet
pub mod task_done {
    use super::*;
    use crate::{jsonrpc::SkwServiceStatus, skw_is_valid_name};

    pub struct TaskDonePublisher {
        producer: IggyProducer,
        topic: String,
        _iggy_client: Arc<IggyClient>,
    }

    impl TaskDonePublisher {
        /// Producer on `topic` in the service stream; the topic is created if missing,
        /// as nothing else creates it while it has no consumer.
        pub async fn new(topic: &str) -> Result<Self, AppError> {
            skw_is_valid_name(topic).ok_or_else(|| AppError::Custom(format!("invalid topic name: {topic}")))?;

            let iggy_client = APP.iggy.clone().connect().await?.inner();

            let producer = iggy_client
                .producer(&skw_get_stream_name(), topic)?
                .partitioning(Partitioning::balanced())
                .create_topic_if_not_exists(
                    1,
                    None,
                    IggyExpiry::ServerDefault,
                    MaxTopicSize::ServerDefault,
                )
                .build();
            producer.init().await?;

            info!("task done notifications go to topic: {}", topic);

            Ok(Self {
                producer,
                topic: topic.to_string(),
                _iggy_client: iggy_client,
            })
        }

        pub fn topic(&self) -> &str {
            &self.topic
        }

        /// Publishes `payload` (JSON) as one notification.
        pub async fn publish<T: Serialize>(&self, signature: &str, payload: &T, status: Option<SkwServiceStatus>) -> Result<(), AppError> {
            let mut headers = BTreeMap::new();

            headers.insert(
                header_keys::SIGNATURE.clone(),
                HeaderValue::from_str(signature)?,
            );

            if let Some(status) = status {
                headers.insert(
                    header_keys::SERVICE_STATUS.clone(),
                    HeaderValue::from_str(&status.to_string())?,
                );
            }

            let message = IggyMessage::builder()
                .payload(Bytes::from(serde_json::to_vec(payload)?))
                .user_headers(headers)
                .build()?;

            self.producer.send_one(message).await?;

            Ok(())
        }
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
            panic!("Error: signed method: {name} cannot be used in unsigned topics");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::consts::*;
    use serde_json::json;

    #[test]
    fn topic_config_valid() {
        let cfg = TopicConfig::try_from(&json!({
            "partitionsCount": 3,
            "messageExpiry": 31,
            "topicTypes": ["open", "signed"]
        }))
        .unwrap();

        assert_eq!(cfg.partitions_count, 3);
        assert_eq!(
            cfg.topic_types,
            vec![SkwTopicType::Open, SkwTopicType::Signed]
        );
        assert_eq!(cfg.compression_algorithm, CompressionAlgorithm::default());
        assert_eq!(cfg.max_topic_size, MaxTopicSize::ServerDefault);
    }

    #[test]
    fn topic_config_rejects() {
        for bad in [
            json!({"messageExpiry": 31, "topicTypes": ["open"]}),                       // no partitionsCount
            json!({"partitionsCount": 0, "messageExpiry": 31, "topicTypes": ["open"]}), // zero partitions
            json!({"partitionsCount": 5_000_000_000u64, "messageExpiry": 31, "topicTypes": ["open"]}), // > u32::MAX
            json!({"partitionsCount": "3", "messageExpiry": 31, "topicTypes": ["open"]}), // not a number
            json!({"partitionsCount": 1, "messageExpiry": 31, "topicTypes": ["open"], "compressionAlgorithm": "gizp"}),
            json!({"partitionsCount": 1, "topicTypes": ["open"]}), // no messageExpiry
            json!({"partitionsCount": 1, "messageExpiry": 31}),    // no topicTypes
            json!({"partitionsCount": 1, "messageExpiry": 31, "topicTypes": []}),
            json!({"partitionsCount": 1, "messageExpiry": 31, "topicTypes": ["bogus"]}),
            json!({"partitionsCount": 1, "messageExpiry": 31, "topicTypes": [1]}),
            json!([]),
        ] {
            assert!(TopicConfig::try_from(&bad).is_err(), "accepted: {bad}");
        }
    }

    #[test]
    fn header_keys_build() {
        use header_keys::*;
        // forcing every LazyLock: an invalid name panics here, not on the first message
        for (key, name) in [
            (&*SIGNATURE, IGGY_HEADER_SIGNATURE),
            (&*API_KEY, IGGY_HEADER_API_KEY),
            (&*HTTP_METHOD, IGGY_HEADER_HTTP_METHOD),
            (&*HTTP_PATH, IGGY_HEADER_HTTP_PATH),
            (&*SERVICE_ERROR, IGGY_HEADER_SERVICE_ERROR),
            (&*SERVICE_STATUS, IGGY_HEADER_SERVICE_STATUS),
            (&*TS_SERVICE_RECEIVED, IGGY_HEADER_TS_SERVICE_RECEIVED),
            (&*TS_SERVICE_SENT, IGGY_HEADER_TS_SERVICE_SENT),
            (
                &*TARGET_WS_CONNECTION_ID,
                IGGY_HEADER_TARGET_WS_CONNECTION_ID,
            ),
        ] {
            assert_eq!(key.as_str().unwrap(), name);
        }
    }

    #[test]
    fn header_str_reads_decoded_map() {
        let headers = BTreeMap::from([(
            header_keys::SIGNATURE.clone(),
            HeaderValue::from_str("sig").unwrap(),
        )]);
        assert_eq!(
            header_str(Some(&headers), &header_keys::SIGNATURE).as_deref(),
            Some("sig")
        );
        assert_eq!(header_str(Some(&headers), &header_keys::API_KEY), None);
        assert_eq!(header_str(None, &header_keys::SIGNATURE), None);
    }

    #[test]
    fn topic_type_names() {
        for (s, t) in [
            ("public", SkwTopicType::Public),
            ("open", SkwTopicType::Open),
            ("signed", SkwTopicType::Signed),
        ] {
            assert_eq!(s.parse::<SkwTopicType>().unwrap(), t);
            assert_eq!(t.to_string(), s); // used in topic names: `{service}-{topic_type}-{VERSION}`
        }
    }
}
