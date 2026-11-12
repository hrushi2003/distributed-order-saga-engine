import {kafka } from "../core-structure/config";
import { KafkaProducerService } from "../core-structure/producer";
import { Consumer,EachMessagePayload } from "kafkajs";

async function run(){
     const producer = new KafkaProducerService();
        await producer.connect();
        const topic = 'order-events';
        // Send a test message
        const testPayload = { orderId: '12345', status: 'created' };
        await producer.sendMessage(topic, 'order-12345', testPayload, 'OrderCreated');
        console.log('[NotificationService] Test message sent.');
}
run().catch(error => {
    console.error('Error in NotificationService:', error);
    process.exit(1);
});