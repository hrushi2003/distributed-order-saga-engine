import {kafka } from "../core-structure/config";
import { KafkaProducerService } from "../core-structure/producer";
import { Consumer,EachMessagePayload } from "kafkajs";

export class NotificationService {
    private consumer : Consumer;
    private producer : KafkaProducerService;
    private isRunning : boolean = false;
    private isReady : boolean = false;

    constructor() {
        this.consumer = kafka.consumer({
            groupId: 'notification-service-group',
            sessionTimeout: 30000, // 30 seconds
            heartbeatInterval: 3000, // 3 seconds
        });
        this.producer = new KafkaProducerService();
    }

    public async start(topic: string): Promise<void> {

        await this.consumer.connect();
        await this.consumer.subscribe({ topic, fromBeginning: true });

        this.consumer.on(this.consumer.events.GROUP_JOIN,(e) => {
            console.log(`[NotificationService] Consumer group joined: ${e.payload.groupId}, memberId: ${e.payload.memberId}`);
            this.isReady = true;
        });

        console.log(`[NotificationService] Starting consumer for topic "${topic}"...`);

        await this.consumer.run({
            autoCommit: true,
            eachMessage: async (payload: EachMessagePayload) => {
                await this.handleMessage(payload);
            },
        });
    }

    public async handleMessage(payload: EachMessagePayload): Promise<void> {
        const { topic, partition, message } = payload;
        const key = message.key?.toString() || '';
        console.log(`[NotificationService] Received message on topic "${topic}", partition ${partition}, key: "${key}", value: "${message.value?.toString()}"`);
    }

    public async waitUntilReady(timeout: number = 30000): Promise<void> {
        const startTime = Date.now();
        while (!this.isReady) {
            if (Date.now() - startTime > timeout) {
                throw new Error('NotificationService did not become ready in time.');
            }
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }

    public async stop(): Promise<void> {
        if (this.isRunning) {
            console.log('[NotificationService] Stopping consumer...');
            await this.consumer.disconnect();
            console.log('[NotificationService] Consumer stopped.');
            this.isRunning = false;
        }
    }
}

async function main() {
    const notificationService = new NotificationService();
    const topic = 'order-events';
    await notificationService.start(topic);
    await notificationService.waitUntilReady();
    console.log('[NotificationService] Service is ready and running.');

    const producer = new KafkaProducerService();
    await producer.connect();

    // Send a test message
    const testPayload = { orderId: '12345', status: 'created' };
    await producer.sendMessage(topic, 'order-12345', testPayload, 'OrderCreated');
    console.log('[NotificationService] Test message sent.');
}

main().catch(error => {
    console.error('Error in NotificationService:', error);
    process.exit(1);
});
