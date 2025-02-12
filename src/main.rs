use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use rand::Rng;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    customer_id: u32,
    consumption: f32,
    timestamp: u128,
}

impl Message {
    #[must_use]
    pub const fn new(customer_id: u32, consumption: f32, timestamp: u128) -> Self {
        Self {
            customer_id,
            consumption,
            timestamp,
        }
    }
}

impl Default for Message {
    fn default() -> Self {
        let mut rng = rand::rng();

        Self {
            customer_id: rng.random_range(1_000..=9_999),
            consumption: rng.random::<f32>() * 10.0,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards!")
                .as_millis(),
        }
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

    let topic = "household_consumption";

    let producer = create_producer(&brokers.join(","))?;

    let mut handles = Vec::new();
    loop {
        let message = Message::default();
        let json = serde_json::to_string(&message)?;

        let result = producer
            .send_result(
                FutureRecord::to(topic)
                    .key(&message.customer_id.to_string())
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

        if handles.len() < 1024 * 1024 {
            continue;
        }

        // Drain the pool.
        info!("Draining thread pool...");
        while let Some(thread) = handles.pop() {
            if let Err(e) = thread.await {
                error!("Failed to join thread: {e}");
                continue;
            }
        }
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
