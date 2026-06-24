import { Redis } from 'ioredis';
import { Queue, QueueEvents } from 'bullmq';
import { config } from './config.js';

// BullMQ connection options — let BullMQ construct its own ioredis client from
// its bundled copy. Passing a connection object (not a constructed Redis
// instance) avoids the ioredis/bullmq duplicate-package type conflict.
const connection = { url: config.redisUrl, maxRetriesPerRequest: null as null };

// Standalone ioredis client for any direct Redis use elsewhere in the app.
export const redis = new Redis(config.redisUrl, { maxRetriesPerRequest: null });

export const diagnosticQueue = new Queue('axo-diagnostic', {
  connection,
  defaultJobOptions: {
    attempts: 2,
    backoff: { type: 'exponential', delay: 2500 },
    removeOnComplete: 500,
    removeOnFail: 500
  }
});

export const queueEvents = new QueueEvents('axo-diagnostic', { connection });
