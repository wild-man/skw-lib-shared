use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use futures_util::StreamExt;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, BasicQosOptions, ConfirmSelectOptions, QueueDeclareOptions,
    },
    types::FieldTable,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

#[allow(unused_imports)]
use crate::prelude::log::{error, info, warn};
use crate::{APP, AppError, prelude::consts::RABBITMQ_URL_PATH, skw_get_consumer_name, skw_get_queue_name};

use crate::config::{Config, ConfigReady};

#[derive(Error, Debug)]
pub enum RabbitMqError {
    #[error("Other error: {0}")]
    Custom(String),
    #[error("lapin error: {0}")]
    Lapin(#[from] lapin::Error),
}

pub trait RabbitMqStates {}

#[derive(Clone, Debug)]
pub struct Loaded;
#[derive(Clone, Debug)]
pub struct Connected;

impl RabbitMqStates for Loaded {}
impl RabbitMqStates for Connected {}

#[derive(Clone, Debug)]
pub struct RabbitMq<S: RabbitMqStates> {
    connection: Option<Arc<Connection>>,
    _state: PhantomData<S>,
}

impl<S: RabbitMqStates> RabbitMq<S> {
    pub fn new(_cfg: &Config<ConfigReady>) -> RabbitMq<Loaded> {
        RabbitMq::<Loaded> {
            connection: None,
            _state: PhantomData,
        }
    }
}

impl RabbitMq<Loaded> {
    pub async fn connect(self) -> RabbitMq<Connected> {
        let url = APP.config.expect_str(RABBITMQ_URL_PATH);

        // auto_recover: transparently reconnects and replays queue/consumer topology
        // on network blips, so a long-lived worker doesn't need to be restarted by
        // the orchestrator for every transient connection drop.
        let props = ConnectionProperties::default().enable_auto_recover();

        let conn = Connection::connect(url, props)
            .await
            .expect("cant connect to rabbitmq");

        RabbitMq::<Connected> {
            connection: Some(Arc::new(conn)),
            _state: PhantomData,
        }
    }
}

impl RabbitMq<Connected> {
    pub fn inner(&self) -> Option<Arc<Connection>> {
        self.connection.clone()
    }
}

#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub name: String,
    pub durable: bool,
    pub prefetch_count: u16,
}

impl QueueConfig {
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            name: skw_get_queue_name(&base.into()),
            durable: true,
            prefetch_count: 10,
        }
    }

    pub fn prefetch(mut self, prefetch_count: u16) -> Self {
        self.prefetch_count = prefetch_count;
        self
    }
}

/// Outcome of processing one task; drives whether the delivery is ack'd or nack'd.
pub enum TaskOutcome {
    Ack,
    /// `requeue: true` asks the broker to redeliver (retry); `false` drops the
    /// message (or routes it to a dead-letter exchange if one is configured).
    Nack {
        requeue: bool,
    },
}

async fn declare_queue(channel: &Channel, queue: &QueueConfig) -> Result<(), RabbitMqError> {
    channel
        .queue_declare(
            queue.name.as_str().into(),
            QueueDeclareOptions {
                durable: queue.durable,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    Ok(())
}

/// Runs a manual-ack consumer loop against `queue`, blocking the caller for the
/// A message that fails to deserialize is nack'd with `requeue`; otherwise the
/// delivery is ack'd/nack'd per `handler`'s returned `TaskOutcome`.
pub async fn run_task_consumer<Task, H, Fut>(queue: QueueConfig, handler: H) -> Result<(), AppError>
where
    Task: DeserializeOwned,
    H: Fn(Task) -> Fut,
    Fut: Future<Output = TaskOutcome>,
{
    let rabbitmq = APP.rabbitmq.clone().connect().await;
    let conn = rabbitmq.inner().expect("bad rabbitmq connection");
    let channel = conn.create_channel().await.map_err(RabbitMqError::from)?;

    info!("connecting to rabbitmq. queue: {}", queue.name);

    channel
        .basic_qos(queue.prefetch_count, BasicQosOptions::default())
        .await
        .map_err(RabbitMqError::from)?;

    declare_queue(&channel, &queue).await?;

    let consumer_tag = skw_get_consumer_name(&queue.name);
    let mut consumer = channel
        .basic_consume(
            queue.name.as_str().into(),
            consumer_tag.as_str().into(),
            BasicConsumeOptions::default(), // no_ack: false — manual ack required
            FieldTable::default(),
        )
        .await
        .map_err(RabbitMqError::from)?;

    while let Some(delivery) = consumer.next().await {
        let delivery = match delivery {
            Ok(delivery) => delivery,
            Err(e) => {
                error!("error receiving rabbitmq delivery: {e}");
                continue;
            }
        };

        let task: Task = match serde_json::from_slice(&delivery.data) {
            Ok(task) => task,
            Err(e) => {
                error!("bad task payload: {e}");
                if let Err(e) = delivery
                    .acker
                    .nack(BasicNackOptions {
                        requeue: false, // if message can't be deserialized force to not requeue
                        ..Default::default()
                    })
                    .await
                {
                    error!("nack failed: {e}");
                }
                continue;
            }
        };

        match handler(task).await {
            TaskOutcome::Ack => {
                if let Err(e) = delivery.acker.ack(BasicAckOptions::default()).await {
                    error!("ack failed: {e}");
                }
            }
            TaskOutcome::Nack { requeue } => {
                if let Err(e) = delivery
                    .acker
                    .nack(BasicNackOptions {
                        requeue: requeue,
                        ..Default::default()
                    })
                    .await
                {
                    error!("nack failed: {e}");
                }
            }
        }
    }

    Ok(())
}

/// Reusable publisher handle: connects and declares its queue once, then can be
/// cloned/shared across concurrent callers to enqueue tasks.
#[derive(Clone)]
pub struct RabbitMqPublisher {
    channel: Arc<Channel>,
    queue: QueueConfig,
}

impl RabbitMqPublisher {
    pub async fn new(queue: QueueConfig) -> Result<Self, RabbitMqError> {
        let rabbitmq = APP.rabbitmq.clone().connect().await;
        let conn = rabbitmq.inner().expect("bad rabbitmq connection");
        let channel = conn.create_channel().await?;

        // required for publish() to be able to await a real broker confirmation
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await?;

        declare_queue(&channel, &queue).await?;

        Ok(Self {
            channel: Arc::new(channel),
            queue,
        })
    }

    pub async fn publish<T: Serialize>(&self, task: &T) -> Result<(), RabbitMqError> {
        let payload = serde_json::to_vec(task).map_err(|e| RabbitMqError::Custom(e.to_string()))?;

        let confirm = self
            .channel
            .basic_publish(
                "".into(), // default exchange: routes directly to the queue named by routing_key
                self.queue.name.as_str().into(),
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default().with_delivery_mode(2), // persistent
            )
            .await?;

        let confirmation = confirm.await?;
        if !confirmation.is_ack() {
            return Err(RabbitMqError::Custom(format!(
                "broker did not confirm publish to {}: {:?}",
                self.queue.name, confirmation
            )));
        }

        Ok(())
    }
}
