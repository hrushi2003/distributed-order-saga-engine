import {kafka,config} from "./config";

export async function createTopicIfNotExists(topic: string = config.topic, numPartitions: number = 3, replicationFactor: number = 1)
{
    const admin = kafka.admin();

    try{
        console.log(`Connecting to Kafka broker(s): ${config.brokers.join(', ')}`);
        await admin.connect();
        console.log(`Connected to Kafka broker(s): ${config.brokers.join(', ')}`);

        const existingTopics = await admin.listTopics();
        if (!existingTopics.includes(topic)) {
            console.log(`Topic "${topic}" does not exist. Creating...`);
            await admin.createTopics({
                topics: [{ topic, numPartitions, replicationFactor }],
            });
            console.log(`Topic "${topic}" created successfully.`);
        } else {
            console.log(`Topic "${topic}" already exists.`);
        }
        try {
            const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
            console.log(`[Admin] Metadata for "${topic}":`, JSON.stringify(metadata.topics[0], null, 2));
        }catch (e) {
            console.log(`[Admin] Could not fetch metadata for "${topic}" yet.`);
        }
    }
    catch (error) {
        console.error(`Error while creating topic "${topic}":`, error);
        throw error;
    }
    finally {
        await admin.disconnect();
        console.log(`Disconnected from Kafka broker(s): ${config.brokers.join(', ')}`);
    }  
}

if (require.main === module) {
  createTopicIfNotExists()
    .then(() => process.exit(0))
    .catch(() => process.exit(1));
}