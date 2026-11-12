import {Consumer, EachMessagePayload} from "kafkajs";
import {kafka, config} from "./config";
import { MessageEnvelope } from './types';

export class KafkaConsumerService {
    private consumer: Consumer;
    private isConnected: boolean = false;
    
    constructor(groupId: string = config.groupId) {
        this.consumer = kafka.consumer({ 
            groupId,
            sessionTimeout: 30000, // 30 seconds
            heartbeatInterval: 3000, // 3 seconds
            retry : {
                retries: 5,
                initialRetryTime: 300,
            }
        });
    }

    public async start(topic: string = config.topic): Promise<void> {
        if (!this.isConnected) {
            console.log('[Consumer] Connecting to Kafka...');
            await this.consumer.connect();
            this.isConnected = true;
            console.log('[Consumer] Connected.');
        }
        console.log(`[Consumer] Subscribing to topic "${topic}"...`);
        await this.consumer.subscribe({ topic, fromBeginning: true });
        console.log(`[Consumer] Subscribed to topic "${topic}".`);
    }

}
