# Distributed Order Saga Engine

A TypeScript and KafkaJS prototype for publishing and consuming order events. The current code provides the messaging foundation for an event driven order workflow: configurable Kafka connectivity, topic setup, keyed event publishing, and a notification consumer example.

> **Project status:** early prototype. Despite the repository name, saga orchestration, order state persistence, compensating transactions, retries/dead letter handling, and production deployment are not implemented yet. The current examples demonstrate Kafka messaging rather than a complete distributed saga engine.

## What is included

- Kafka client configuration from environment variables, with optional SASL and TLS.
- Topic creation and metadata inspection through KafkaJS admin APIs.
- A producer that wraps payloads in a versioned event envelope, adds event headers, uses the message key for partition ordering, and requests acknowledgements from all in-sync replicas.
- A consumer service and notification demo that subscribes to an event topic and logs received messages.
- TypeScript configuration with strict type checking.

## Project structure

```text
core-structure/
  admin.ts       Create/check a Kafka topic
  config.ts      Kafka client and environment configuration
  consumer.ts    Consumer service wrapper
  producer.ts    Keyed event producer and envelope creation
  types.ts       Shared MessageEnvelope type
examples/
  demo.ts        Notification consumer and producer demonstration
  test.ts        Producer-only message demonstration
```

## Requirements

- Node.js and npm
- A reachable Kafka broker (local or hosted)

## Setup

```bash
npm install
cp .env.example .env
```

Edit `.env` with the broker and, if needed, authentication settings:

```dotenv
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=order-service
KAFKA_GROUP_ID=order-processing-group
KAFKA_TOPIC=order-events
KAFKA_SSL=false

# For a SASL secured broker, set these and choose a supported mechanism:
KAFKA_SASL_USERNAME=
KAFKA_SASL_PASSWORD=
KAFKA_SASL_MECHANISM=plain
```

`KAFKA_BROKERS` accepts a comma-separated list. For certificate-based TLS, set `KAFKA_SSL=true`; when SASL credentials are present the client enables TLS automatically. The configuration also looks for a local `ca.pem` certificate. Keep broker credentials and private certificates out of source control.

## Run the examples

Create the default topic (`order-events`, three partitions, replication factor one):

```bash
npm run init-topics
```

Start the notification consumer demo, which also publishes one `OrderCreated` event:

```bash
npm run demo
```

Publish one `OrderCreated` event without starting a consumer:

```bash
npm run test
```

The demos connect to Kafka and will not complete successfully unless a broker is available. The package also defines `build`, `produce`, `sms-demo`, and `fraud-demo` scripts; the latter four reference paths that are not present in this checkout and need implementation or correction before use.

## Event format

The producer serializes each event using this envelope:

```json
{
  "eventId": "evt_<timestamp>_<suffix>",
  "eventType": "OrderCreated",
  "version": "1.0",
  "payload": {
    "orderId": "12345",
    "status": "created"
  },
  "metadata": {
    "source": "order-service",
    "timestamp": "<ISO-8601 timestamp>"
  }
}
```

The Kafka record key is supplied by the caller (the examples use an order key). Kafka routes records with the same key to the same partition, preserving their order within that partition. The producer also adds `x-event-type` and `x-sent-at` headers and compresses messages with GZIP.

## Current limitations and next steps

This prototype is a starting point for a saga engine, not a finished implementation. Suitable next steps for a PBC evaluation include:

1. Define an order workflow and explicit saga states/transitions.
2. Add an orchestrator or choreography handlers and idempotent event processing.
3. Persist saga state and implement retry, timeout, and compensation behavior.
4. Complete the consumer run/handling lifecycle and graceful shutdown.
5. Add automated tests, local Kafka setup, and operational documentation.
6. Align the npm scripts with files that exist and validate build/run commands in CI.

## Security

Do not commit `.env`, broker credentials, or private CA material. Use a secret manager for deployed environments and rotate any credentials that may have been exposed.
