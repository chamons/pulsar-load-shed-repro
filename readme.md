# Pulsar Load Shedding Issue Repro

## Steps to Reproduce

1. docker compose up -d
2. `docker compose exec broker-1 bash`
    a) Copy the six commands in ./script/setup_partitions and run them
    b) Exit prompt
3. Check the logs for the bookies and brokers to make sure they are started up correctly. 
3. `docker compose exec code bash`
    a) `apt-get update && apt install -y protobuf-compiler`
    b) `RUST_LOG=INFO TEST_CASE=CONSUMER cargo run --release`
    c) Wait for `Running Consumer` line
3. In a second terminal `docker compose exec code bash`
    a) `RUST_LOG=INFO TEST_CASE=PRODUCER cargo run --release`

This will get you a steady state producer and consumer. The producer sends 400 messages at a time to one of a few random topics, and then polls for completion on a shared Redis. 

Depending on your machine, you might see lines such as this come up.

`Expected 400 found 398 on e0b874ee-cfe7-49a5-80a1-86f85f7cdb0c (Count 14)`

That means the test tolerances are too tight for your hardware. Increase `PAUSE_BETWEEN_REDIS_CHECKS` and decrease `WORKER_COUNT` in main.rs and try again until things are stable.

Now let's break it to show the bug.

### Bug 1 - Cluster restart 

1. In docker desktop, shut down one of the brokers (pick the highest CPU one), then restart it.

In the consumer you will see spam of:
```
2025-12-11T16:39:38.781142Z ERROR pulsar::consumer::engine: tx returned SendError { kind: Disconnected }
2025-12-11T16:39:38.781268Z ERROR pulsar::consumer::engine: cannot send a message from the consumer engine to the consumer(9), stopping the engine
2025-12-11T16:39:38.782559Z  WARN pulsar::consumer::engine: rx terminated
2025-12-11T16:39:38.800551Z ERROR pulsar::consumer::engine: tx returned SendError { kind: Disconnected }
2025-12-11T16:39:38.800602Z ERROR pulsar::consumer::engine: cannot send a message from the consumer engine to the consumer(6), stopping the engine
2025-12-11T16:39:38.802778Z  WARN pulsar::consumer::engine: rx terminated
2025-12-11T16:39:39.074912Z ERROR pulsar::consumer::engine: Error sending event to channel - send failed because receiver is gone
2025-12-11T16:39:39.074918Z ERROR pulsar::consumer::engine: Error sending event to channel - send failed because receiver is gone
2025-12-11T16:39:39.074961Z  WARN pulsar::consumer::engine: rx terminated
2025-12-11T16:39:39.074974Z  WARN pulsar::consumer::engine: rx terminated

```

In the producer you will see spam of (different uuid):
```
Expected 400 found 267 on c9507e39-4f8f-423e-99a3-e2e67ab10912 (Count 104)
Expected 400 found 266 on 7ef7f991-37c2-4ae4-9b0c-5fa12ec5cfba (Count 104)
Expected 400 found 266 on 5c75b475-265b-4edd-b661-9e507177f65b (Count 104)
Expected 400 found 267 on 7a454976-0536-4465-a025-ef54b4f4ecaf (Count 104)
```

NOTE: If you don't see ^, try repeating shutting down a different broker and restarting it. Depending on topic distribution, you might need a few attempts

Note, we do not make progress in the producer finding messages it sent.

2) ctrl+c the consumer, then restart it
3) Note as soon as it restarts, the producer unjams and the logs shortly stop

### Bug 2 - Overloaded causes topic transfer

I've had a lot more trouble reproducing this one with the simplified repro code, through I can trigger it with a pulsar-admin command often enough. 

Setup the steady state of producer and consumer.

1. `docker compose exec broker-1 bash`
    a) `pulsar-admin topics unload persistent://example/delivery/notifications`

If you trigger it, you should see the same internal logs from your consumer:

```
2025-12-11T16:39:38.781142Z ERROR pulsar::consumer::engine: tx returned SendError { kind: Disconnected }
2025-12-11T16:39:38.781268Z ERROR pulsar::consumer::engine: cannot send a message from the consumer engine to the consumer(9), stopping the engine
2025-12-11T16:39:38.782559Z  WARN pulsar::consumer::engine: rx terminated
```

And things stop working until you restart the consumer.
