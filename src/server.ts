import Fastify from 'fastify';
import cors from '@fastify/cors';
import { z } from 'zod';
import { config, requireConfig } from './config.js';
import { createJob, getJobResults, getJobStatus } from './services/jobService.js';

requireConfig();

const app = Fastify({ logger: true });
await app.register(cors, { origin: true });

const createJobSchema = z.object({
  url: z.string().min(3),
  competitors: z.array(z.string()).optional(),
  personas: z.array(z.string()).optional(),
  maxPages: z.number().int().min(25).max(50000).optional(),
  depth: z.number().int().min(1).max(10).optional(),
  includeSubdomains: z.boolean().optional(),
  metadata: z.record(z.unknown()).optional(),
  forceRefresh: z.boolean().optional()
});

app.get('/health', async () => ({ ok: true, service: 'axo-api' }));

app.post('/axo/jobs', async (req, reply) => {
  const parsed = createJobSchema.safeParse(req.body);
  if (!parsed.success) return reply.code(400).send({ success: false, error: parsed.error.flatten() });
  const status = await createJob(parsed.data);
  return reply.code(202).send(status);
});

app.get('/axo/jobs/:jobId/status', async (req, reply) => {
  const { jobId } = req.params as { jobId: string };
  const status = await getJobStatus(jobId);
  if (!status) return reply.code(404).send({ success: false, error: 'Job not found' });
  return status;
});

app.get('/axo/jobs/:jobId/results', async (req, reply) => {
  const { jobId } = req.params as { jobId: string };
  const results = await getJobResults(jobId);
  if (!results) return reply.code(404).send({ success: false, error: 'Job not found' });
  if (!results.readyForResults) return reply.code(202).send({ success: false, error: 'Results not ready', jobId });
  return results;
});

app.post('/axo/jobs/:jobId/n8n-notify', async (req) => {
  // Optional endpoint n8n can call to acknowledge CRM/email handoffs.
  return { ok: true, received: req.body || {} };
});

app.listen({ port: config.port, host: '0.0.0.0' }).catch(err => {
  app.log.error(err);
  process.exit(1);
});
