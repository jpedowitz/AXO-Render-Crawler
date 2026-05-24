import OpenAI from 'openai';
import { config } from './config.js';
import { query } from './db.js';
import type { CrawledPage } from './crawler.js';

export async function maybeStorePageEmbeddings(jobId: string, pages: CrawledPage[]) {
  if (!config.enableEmbeddings || !config.openaiApiKey) return { enabled: false, stored: 0 };
  const candidates = pages
    .filter(p => p.classification?.action === 'score')
    .sort((a, b) => (b.classification?.priority || 0) - (a.classification?.priority || 0))
    .slice(0, config.embeddingMaxPages);

  if (!candidates.length) return { enabled: true, stored: 0 };

  const client = new OpenAI({ apiKey: config.openaiApiKey });
  const inputs = candidates.map(p => `${p.title}\n${p.excerpt}`.slice(0, 8000));
  const resp = await client.embeddings.create({ model: config.embeddingModel, input: inputs });

  const values: unknown[] = [];
  const placeholders = candidates.map((p, i) => {
    const embedding = resp.data[i]?.embedding || [];
    const base = i * 6;
    values.push(jobId, p.url, p.contentHash || '', config.embeddingModel, embedding.length, JSON.stringify(embedding));
    return `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6})`;
  }).join(',');

  await query(
    `insert into axo_embeddings (job_id, url, content_hash, model, dimensions, embedding)
     values ${placeholders}
     on conflict (job_id, url, model) do update set
       content_hash = excluded.content_hash,
       dimensions = excluded.dimensions,
       embedding = excluded.embedding,
       created_at = now()`,
    values
  );

  return { enabled: true, stored: candidates.length };
}
