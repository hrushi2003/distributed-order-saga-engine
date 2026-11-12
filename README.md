# Distributed Order Saga Engine

## Project explanation

Distributed Order Saga Engine is a TypeScript project exploring event-driven communication for order workflows with Apache Kafka. Services publish order events to Kafka topics, and other services can consume those events to react to changes in an order's lifecycle.

The current implementation provides the messaging foundation:

- **Kafka configuration** is read from environment variables and supports broker lists, SASL authentication, and TLS.
- **Topic administration** checks for a topic and creates it when it is missing.
- **Event publishing** wraps payloads in a versioned envelope containing an event ID, event type, payload, and metadata. A message key keeps related order events on the same Kafka partition.
- **Event consumption** includes a consumer wrapper and a notification example that subscribes to a topic and logs messages.

The code is organized into `core-structure/` for Kafka configuration, administration, producer, consumer, and shared event types, plus `examples/` for producer and notification demonstrations. The current examples demonstrate Kafka event messaging; saga state management and coordination are not implemented yet.

## What's coming next

- Model order workflow states and the transitions between them.
- Add saga coordination to advance a workflow as services publish and consume events.
- Persist workflow state so sagas can recover after service restarts.
- Add idempotent event handling, retries, timeouts, and compensating actions for failed steps.
- Improve consumer lifecycle management, including graceful shutdown and failure handling.
- Add automated tests and a repeatable local Kafka development setup.
