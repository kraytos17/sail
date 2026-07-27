use std::time::Duration;

use rand::RngExt;
use tokio::time::sleep;

pub async fn sleep_with_jitter(base_ms: u64, attempt: usize) {
    let max_ms = base_ms * (1u64 << attempt);
    let jittered = rand::rng().random_range(0..=max_ms);
    sleep(Duration::from_millis(jittered)).await;
}
