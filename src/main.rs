use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use rand::Rng;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

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
    loop {
        let message = Message::default();
        let json = serde_json::to_string(&message)?;

        let result = producer
            .send_result(
                FutureRecord::to(topic)
                    .key(&message.customer_id.to_string())
                    .payload(json.as_bytes()),
            )
            .map_err(|(e, _)| e.to_string());

        tokio::spawn(async move {
            match result {
                Ok(v) => match v.await {
                    Ok(Ok((_, id))) => info!("Produced message: {id}"),
                    Ok(Err((e, _))) => error!("Kafka Error: {e}"),
                    Err(e) => error!("Producer Cancelled: {e}"),
                },
                Err(e) => error!("{e}"),
            }
        });
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
