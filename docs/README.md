# AXO Diagnostic Engine

Render-ready AXO diagnostic engine with:

- Fastify API
- BullMQ worker
- Redis queue
- Postgres state/results
- concurrent crawler
- bulk page inserts
- domain result cache
- parallel four-LLM scoring
- deterministic competitor scoring
- n8n handoff webhook support

Start with `ARCHITECTURE_AND_SETUP.md`.

## v4 Notes

v4 adds crawl prioritization, optional embeddings, AI citation simulation, and content-hash diffing. See `ARCHITECTURE_AND_SETUP.md` for implementation and deployment details.
