import {Producer, CompressionTypes,RecordMetadata} from "kafkajs";
import {kafka,config} from "./config";
import { MessageEnvelope } from './types';

export class KafkaProducerService {
    private producer: Producer;
    private isConnected: boolean = false;

    constructor() {
        this.producer = kafka.producer({
            allowAutoTopicCreation: true,
            idempotent: true,
            maxInFlightRequests: 5,
        });
    }

    public async connect(): Promise<void> {
        if (!this.isConnected) {
            console.log('[Producer] Connecting to Kafka...');
            await this.producer.connect();
            this.isConnected = true;
            console.log('[Producer] Connected.');
        }
    }

    public async disconnect(): Promise<void> {
        if (this.isConnected) {
            console.log('[Producer] Disconnecting from Kafka...');
            await this.producer.disconnect();
            this.isConnected = false;
            console.log('[Producer] Disconnected.');
        }
    }

    public async sendMessage<T>(
        topic: string,
        key : string,
        payload: T,
        eventType: string,
        headers : Record<string, string> = {}
    ): Promise<RecordMetadata[]> {
        if(!this.isConnected) {
            await this.connect();
        }

        const envelope: MessageEnvelope<T> = {
        eventId: `evt_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
        eventType,
        version: '1.0',
        payload,
        metadata: {
        source: 'order-service',
        timestamp: new Date().toISOString(),
      },
    };
    const record = {
      key, // Messages with the same key go to the SAME partition (guarantees ordering)
      value: JSON.stringify(envelope),
      headers: {
        ...headers,
        'x-event-type': eventType,
        'x-sent-at': new Date().toISOString(),
      },
    };

    console.log(`[Producer] Sending message to topic "${topic}" with Key="${key}"...`);
    const results = await this.producer.send({
      topic,
      messages: [record],
      compression: CompressionTypes.GZIP,
      acks: -1, // -1 or all: wait for all in-sync replicas to acknowledge
    });

    results.forEach((res) => {
      console.log(
        `[Producer] Sent successfully -> Topic: ${res.topicName} | Partition: ${res.partition} | Offset: ${res.baseOffset}`
      );
    });

    return results;
    }
}
