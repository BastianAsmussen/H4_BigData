use anyhow::Result;
use h4_bigdata::Message;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

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
