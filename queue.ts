import { Redis } from 'ioredis';
import { Queue, QueueEvents } from 'bullmq';
import { config } from '../config.js';

export const redis = new Redis(config.redisUrl, { maxRetriesPerRequest: null });

export const diagnosticQueue = new Queue('axo-diagnostic', {
  connection: redis,
  defaultJobOptions: {
    attempts: 2,
    backoff: { type: 'exponential', delay: 2500 },
    removeOnComplete: 500,
    removeOnFail: 500
  }
});

export const queueEvents = new QueueEvents('axo-diagnostic', { connection: redis });
