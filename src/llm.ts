import OpenAI from 'openai';
import Anthropic from '@anthropic-ai/sdk';
import { config } from './config.js';

export type EngineResult = {
  engine: string;
  ok: boolean;
  score?: number;
  data?: any;
  error?: string;
  ms: number;
};

export async function runLLMPanel(prompt: string): Promise<EngineResult[]> {
  const calls: Array<Promise<EngineResult>> = [];
  if (config.anthropicApiKey) calls.push(withTimer('claude', () => callClaude(prompt)));
  if (config.openaiApiKey) calls.push(withTimer('openai', () => callOpenAI(prompt)));
  if (config.perplexityApiKey) calls.push(withTimer('perplexity', () => callPerplexity(prompt)));
  if (config.geminiApiKey) calls.push(withTimer('gemini', () => callGemini(prompt)));
  const results = await Promise.allSettled(calls);
  return results.map((r, idx) => r.status === 'fulfilled' ? r.value : {
    engine: ['claude', 'openai', 'perplexity', 'gemini'][idx] || 'unknown', ok: false, error: String(r.reason), ms: 0
  });
}

async function withTimer(engine: string, fn: () => Promise<any>): Promise<EngineResult> {
  const start = Date.now();
  try {
    const data = await timeout(fn(), config.llmTimeoutMs);
    return { engine, ok: true, score: extractScore(data), data, ms: Date.now() - start };
  } catch (err: any) {
    return { engine, ok: false, error: err?.message || String(err), ms: Date.now() - start };
  }
}

async function callClaude(prompt: string) {
  const client = new Anthropic({ apiKey: config.anthropicApiKey });
  const resp = await client.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 1600,
    temperature: 0,
    messages: [{ role: 'user', content: prompt }]
  });
  const text = resp.content.map((c: any) => c.type === 'text' ? c.text : '').join('\n');
  return parseJson(text);
}

async function callOpenAI(prompt: string) {
  const client = new OpenAI({ apiKey: config.openaiApiKey });
  const resp = await client.chat.completions.create({
    model: 'gpt-4o-mini', max_tokens: 1600, temperature: 0,
    messages: [{ role: 'user', content: prompt }]
  });
  return parseJson(resp.choices[0]?.message?.content || '{}');
}

async function callPerplexity(prompt: string) {
  const resp = await fetch('https://api.perplexity.ai/chat/completions', {
    method: 'POST', headers: { Authorization: `Bearer ${config.perplexityApiKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'sonar', max_tokens: 1600, temperature: 0, messages: [{ role: 'user', content: prompt }] })
  });
  const json = await resp.json();
  return parseJson(json.choices?.[0]?.message?.content || '{}');
}

async function callGemini(prompt: string) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${config.geminiApiKey}`;
  const resp = await fetch(url, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }], generationConfig: { maxOutputTokens: 1600, temperature: 0 } })
  });
  const json = await resp.json();
  const raw = json.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
  return parseJson(raw);
}

function parseJson(text: string) {
  const clean = String(text || '').replace(/```json\s*/gi, '').replace(/```/g, '').trim();
  const start = clean.indexOf('{');
  const end = clean.lastIndexOf('}');
  if (start >= 0 && end > start) return JSON.parse(clean.slice(start, end + 1));
  return {};
}

function extractScore(data: any): number | undefined {
  const n = Number(data?.aeoReadinessScore ?? data?.axoScore ?? data?.score);
  return Number.isFinite(n) ? Math.max(0, Math.min(100, n)) : undefined;
}

function timeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timed out after ${ms}ms`)), ms);
    promise.then(v => { clearTimeout(timer); resolve(v); }, e => { clearTimeout(timer); reject(e); });
  });
}
