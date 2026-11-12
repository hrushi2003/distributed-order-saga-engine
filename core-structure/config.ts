import {Kafka, KafkaConfig, logLevel} from "kafkajs";
import dotenv from "dotenv";
dotenv.config();

export const config = {
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  clientId: process.env.KAFKA_CLIENT_ID || 'order-service',
  groupId: process.env.KAFKA_GROUP_ID || 'order-processing-group',
  topic: process.env.KAFKA_TOPIC || 'order-events',
  saslUsername: process.env.KAFKA_SASL_USERNAME,
  saslPassword: process.env.KAFKA_SASL_PASSWORD,
  saslMechanism: (process.env.KAFKA_SASL_MECHANISM || 'plain') as 'plain' | 'scram-sha-256' | 'scram-sha-512',
  ssl: process.env.KAFKA_SSL === 'true',
};

const kafkaConfig: KafkaConfig = {
    clientId: config.clientId,
    brokers: config.brokers,
    logLevel: logLevel.INFO,
    ssl: config.ssl,
    retry: {
        initialRetryTime: 300,
        retries: 10
    },
};

import fs from "fs";
import path from "path";

const caCertPath = path.join(process.cwd(), 'ca.pem');
const caCert = fs.existsSync(caCertPath) ? [fs.readFileSync(caCertPath, 'utf-8')] : undefined;

// If SASL credentials are provided (e.g. Aiven / Confluent Cloud / AWS MSK)
if (config.saslUsername && config.saslPassword) {
  if (config.saslMechanism === 'scram-sha-256') {
    kafkaConfig.sasl = {
      mechanism: 'scram-sha-256',
      username: config.saslUsername,
      password: config.saslPassword,
    };
  } else if (config.saslMechanism === 'scram-sha-512') {
    kafkaConfig.sasl = {
      mechanism: 'scram-sha-512',
      username: config.saslUsername,
      password: config.saslPassword,
    };
  } else {
    kafkaConfig.sasl = {
      mechanism: 'plain',
      username: config.saslUsername,
      password: config.saslPassword,
    };
  }
  kafkaConfig.ssl = caCert ? { ca: caCert, rejectUnauthorized: true } : true;
}

export const kafka = new Kafka(kafkaConfig);
