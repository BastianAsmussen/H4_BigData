use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use rand::{prelude::ThreadRng, Rng};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Wrapper type for `f32` when used as mWh.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MilliwattHours(pub f32);

/// A message from or to a Kafka cluster.
///
/// # Fields
///
/// * `customer_id` - The ID of the customer.
/// * `consumption` - The mWh of the customer's electrical consumption.
/// * `timestamp` - The time, in milliseconds since the [Unix Epoch](https://en.wikipedia.org/wiki/Unix_time).
#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    customer_id: u32,
    consumption: MilliwattHours,
    timestamp: u128,
}

impl Message {
    /// Construct a new `Message` instance.
    ///
    /// # Arguments
    ///
    /// * `customer_id` - The ID of the customer.
    /// * `consumption` - The mWh of the customer's electrical consumption.
    /// * `timestamp` - The time, in milliseconds since the [Unix Epoch](https://en.wikipedia.org/wiki/Unix_time).
    ///
    /// # Returns
    ///
    /// * A new instance of `Message`.
    #[must_use]
    pub const fn new(customer_id: u32, consumption: MilliwattHours, timestamp: u128) -> Self {
        Self {
            customer_id,
            consumption,
            timestamp,
        }
    }

    /// Generate a new instance of `Message` with randomized values.
    ///
    /// # Arguments
    ///
    /// * `rng` - The randomness seed to use for generation.
    ///
    /// # Returns
    ///
    /// * A new `Message` instance random values.
    ///
    /// # Panics
    ///
    /// * If the system time is less than the [Unix Epoch](https://en.wikipedia.org/wiki/Unix_time).
    pub fn with_rng(rng: &mut ThreadRng) -> Self {
        let customer_id = rng.random_range(1_000..=9_999);
        let consumption = MilliwattHours(rng.random::<f32>() * 10.0);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards!")
            .as_millis();

        Self::new(customer_id, consumption, timestamp)
    }

    /// Get the customer ID of the message.
    ///
    /// # Returns
    ///
    /// * The customer's ID as a `u32`.
    #[must_use]
    pub const fn customer_id(&self) -> u32 {
        self.customer_id
    }

    /// Get the mWh electrical consumption of the customer.
    ///
    /// # Returns
    ///
    /// * The electrical consumption, in mWh.
    #[must_use]
    pub const fn consumption(&self) -> MilliwattHours {
        self.consumption
    }

    /// Get the timestamp of the message.
    ///
    /// # Returns
    ///
    /// * The timestamp, in milliseconds.
    #[must_use]
    pub const fn timestamp(&self) -> u128 {
        self.timestamp
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let brokers: Vec<String> = [
        "172.16.250.32:9092",
        "172.16.250.33:9092",
        "172.16.250.34:9092",
        "172.16.250.35:9092",
        "172.16.250.36:9092",
        "172.16.250.37:9092",
        "172.16.250.38:9092",
        "172.16.250.39:9092",
        "172.16.250.40:9092",
        "172.16.250.41:9092",
        "172.16.250.42:9092",
    ]
    .iter()
    .map(|x| (*x).to_string())
    .collect();

    let topic = "household_consumption2";
    let producer = create_producer(&brokers.join(","))?;

    let mut rng = rand::rng();
    let mut handles = Vec::new();
    loop {
        let message = Message::with_rng(&mut rng);
        let json = serde_json::to_string(&message)?;

        let result = producer
            .send_result(
                FutureRecord::to(topic)
                    .key(&message.customer_id().to_string())
                    .payload(json.as_bytes()),
            )
            .map_err(|(e, _)| e);
        let result = match result {
            Ok(v) => v,
            Err(e) => {
                error!("Kafka Error: {e}");
                continue;
            }
        };

        handles.push(tokio::spawn(async move {
            match result.await {
                Ok(Ok((_, id))) => info!("Produced Message: {id}"),
                Ok(Err((e, _))) => error!("Kafka Error: {e}"),
                Err(e) => warn!("Producer Cancelled: {e}"),
            };
        }));

        drain_threadpool(&mut handles, 1024 * 1024).await;
    }
}

fn create_producer(bootstrap_server: &str) -> Result<FutureProducer> {
    let config = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("queue.buffering.max.messages", "100000000")
        .set("queue.buffering.max.ms", "0")
        .set("batch.num.messages", "100")
        .create()?;

    Ok(config)
}

/// Drain the thread pool if the limit is exceeded.
///
/// # Arguments
///
/// * `handles` - A mutable reference to the `JoinHandle` array.
/// * `limit` - The maximum number of thread handles allowed to exist at once.
async fn drain_threadpool(handles: &mut Vec<JoinHandle<()>>, limit: usize) {
    if handles.len() < limit {
        return;
    }

    info!("Draining thread pool...");
    while let Some(thread) = handles.pop() {
        if let Err(e) = thread.await {
            error!("Failed to join thread: {e}");
            continue;
        }
    }
}
