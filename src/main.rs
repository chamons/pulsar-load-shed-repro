use std::time::Duration;

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use futures::{StreamExt, stream::FuturesUnordered, task::SpawnExt};
use pulsar::{
    Consumer, ConsumerOptions, ProducerOptions, Pulsar, SubType, TokioExecutor,
    compression::CompressionSnappy, consumer::InitialPosition,
};
use redis::AsyncCommands;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    enable_tracing();

    match std::env::var("TEST_CASE").as_ref().map(|v| v.as_str()) {
        Ok("PRODUCER") => run_producer().await,
        Ok("CONSUMER") => run_consumer().await,
        _ => {
            eprintln!("Run executable with TEST_CASE set to PRODUCER or CONSUMER");
            return;
        }
    }
}

async fn run_consumer() {
    let redis = get_redis().await;
    let pulsar = get_pulsar().await;

    let mut consumer: Consumer<String, TokioExecutor> = pulsar
        .consumer()
        .with_topics(&TOPICS)
        .with_consumer_name("delivery-consumer")
        .with_subscription_type(SubType::Shared)
        .with_subscription("delivery-subscription")
        .with_options(ConsumerOptions::default().with_initial_position(InitialPosition::Earliest))
        .with_unacked_message_resend_delay(Some(Duration::from_secs(60 * 30)))
        .build()
        .await
        .unwrap();

    println!("Running Consumer");

    while let Some(Ok(msg)) = consumer.next().await {
        let message = match msg.deserialize() {
            Ok(data) => data,
            Err(e) => {
                println!("could not deserialize message: {e:?}");
                break;
            }
        };

        let app_id = message.split_once(":").unwrap().0.to_string();

        let mut connection = redis.get().await.unwrap();
        let _: () = connection.incr(app_id, 1).await.unwrap();

        consumer.ack(&msg).await.unwrap();
    }
}

const WORKER_COUNT: u64 = 25;
const PAUSE_BETWEEN_REDIS_CHECKS: u64 = 250;
const SEND_COUNT: u64 = 400;

async fn run_producer() {
    let redis = get_redis().await;

    let tasks = FuturesUnordered::new();
    for worker in 0..WORKER_COUNT {
        println!("Spawning Worker: {worker}");
        let redis = redis.clone();
        tasks
            .spawn(async move {
                spam_deliveries(redis).await;
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _: Vec<_> = tasks.collect().await;
}

async fn spam_deliveries(pool: Pool<RedisConnectionManager>) {
    let pulsar = get_pulsar().await;

    let mut producer = pulsar
        .producer()
        .with_name(format!("producer: {}", uuid::Uuid::new_v4()))
        .with_options(ProducerOptions {
            compression: Some(pulsar::compression::Compression::Snappy(
                CompressionSnappy::default(),
            )),
            ..Default::default()
        })
        .build_multi_topic();

    let mut batch_set = 0;
    loop {
        let app_id = uuid::Uuid::new_v4();
        let topic = get_topic(batch_set);

        for _ in 0..SEND_COUNT {
            loop {
                let notification_id = uuid::Uuid::new_v4();
                let request = format!("{app_id}:{notification_id}");

                let send_future = match producer.send_non_blocking(topic, request).await {
                    Ok(send_future) => send_future,
                    Err(e) => {
                        println!("Error sending delivery ({e:?}). Trying again in a minute");
                        continue;
                    }
                };

                match send_future.await {
                    Ok(_) => {
                        break;
                    }
                    Err(e) => {
                        println!("Error sending delivery ({e:?}). Trying again in a minute");
                        continue;
                    }
                }
            }
        }

        let mut connection = pool.get().await.unwrap();
        let mut missed_count = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(PAUSE_BETWEEN_REDIS_CHECKS)).await;
            let count = connection
                .get::<String, u64>(app_id.to_string())
                .await
                .unwrap_or_default();

            if count >= SEND_COUNT {
                break;
            } else {
                if missed_count > 5 {
                    println!(
                        "Expected {SEND_COUNT} found {count} on {app_id} (Count {missed_count})"
                    );
                }
                missed_count += 1;
            }
        }
        batch_set += 1;
    }
}

const TOPICS: [&'static str; 4] = [
    "persistent://example/delivery/notifications",
    "persistent://example/delivery/notifications-retries",
    "persistent://example/delivery/notifications-enterprise",
    "persistent://example/delivery/notifications-enterprise-retries",
];

fn get_topic(batch_set: usize) -> &'static str {
    TOPICS.get(batch_set % 4).unwrap()
}

async fn get_redis() -> Pool<RedisConnectionManager> {
    let manager = bb8_redis::RedisConnectionManager::new("redis://redis:6379/0").unwrap();
    bb8::Pool::builder()
        .max_size(100)
        .connection_timeout(Duration::from_secs(10))
        .build(manager)
        .await
        .unwrap()
}

async fn get_pulsar() -> Pulsar<TokioExecutor> {
    Pulsar::builder("pulsar://broker:6650", TokioExecutor)
        .build()
        .await
        .unwrap()
}

fn enable_tracing() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();
}
