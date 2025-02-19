use std::time::{SystemTime, UNIX_EPOCH};

use rand::{prelude::ThreadRng, Rng};
use serde::{Deserialize, Serialize};

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
