import 'dotenv/config';

export const config = {
  
  nodeEnv: process.env.NODE_ENV || 'development',
  port: Number(process.env.PORT || 3000),
  databaseUrl: process.env.DATABASE_URL || '',
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6379',
  publicBaseUrl: process.env.PUBLIC_BASE_URL || 'http://localhost:3000',
  n8nWebhookUrl: process.env.N8N_WEBHOOK_URL || '',
  openaiApiKey: process.env.OPENAI_API_KEY || '',
  anthropicApiKey: process.env.ANTHROPIC_API_KEY || '',
  perplexityApiKey: process.env.PERPLEXITY_API_KEY || '',
  geminiApiKey: process.env.GEMINI_API_KEY || '',
  defaultMaxPages: Number(process.env.DEFAULT_MAX_PAGES || 250),
  defaultCompetitorPages: Number(process.env.DEFAULT_COMPETITOR_PAGES || 75),
  jobConcurrency: Number(process.env.JOB_CONCURRENCY || 3),
  llmTimeoutMs: Number(process.env.LLM_TIMEOUT_MS || 25000),
  crawlTimeoutMs: Number(process.env.CRAWL_TIMEOUT_MS || 45000),
  crawlConcurrency: Number(process.env.CRAWL_CONCURRENCY || 25),
  crawlPerHostConcurrency: Number(process.env.CRAWL_PER_HOST_CONCURRENCY || 5),
  resultCacheTtlDays: Number(process.env.RESULT_CACHE_TTL_DAYS || 7),
  enableEmbeddings: String(process.env.ENABLE_EMBEDDINGS || 'false').toLowerCase() === 'true',
  embeddingModel: process.env.EMBEDDING_MODEL || 'text-embedding-3-small',
  embeddingMaxPages: Number(process.env.EMBEDDING_MAX_PAGES || 50),
  changedPageBoost: String(process.env.CHANGED_PAGE_BOOST || 'true').toLowerCase() !== 'false'
};
export { config, requireConfig } from '../config.js';
export function requireConfig() {
  if (!config.databaseUrl) throw new Error('DATABASE_URL is required');
  if (!config.redisUrl) throw new Error('REDIS_URL is required');
}
