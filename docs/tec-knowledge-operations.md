# TEC consultation library — 2026-10-25

## Design

The authenticated Coach uses `src/knowledge/coach-knowledge.js` on every AI request, including turbo mode. Exact SKU/name and weighted, accent-normalized terms retrieve complete relevant records from an explicit eight-source allowlist. No embedding API, website scrape or Mongo query occurs in the retrieval request path. The existing public Chef flow is unchanged.

The legacy global Mongo library remains untouched: it includes unrelated businesses and historical customer notes. TEC releases live in `tec_knowledge_releases`, keyed by SHA-256 of the complete approved records. Startup uses the bundled index immediately, then can warm the exact, hash-verified Mongo snapshot asynchronously. A missing, slow or corrupt Mongo release does not stop the Coach. Releases are immutable and deployment-pinned; there is no remotely editable pointer to unreviewed sources.

Cache keys include full question, account scope, history/context, prompt version and knowledge hash. Old abbreviated cache entries cannot answer new questions. RoyalOne returns an explicit documentation-pending context, never DocuCite instructions masquerading as RoyalOne.

## Content

- Public US products and useful care tips refreshed October 25, 2026. Both current site navigation and sitemap are consulted. Retired HTTP 404 pages are reported, not fabricated; other fetch failures abort the refresh.
- Official US warranty version 28, including Elite and Chef Assist, complete sections and general restrictions. Pressure-cooker warranty is its own section (corrected HTML parser).
- Official US refund/exchange policy, distinct from warranty service.
- Existing local INNOVE catalog records and price list; 161 full public recipe records, with original Mexico recipe URLs. Recipe geography never determines US warranty/return policy.
- Public DocuCite material only. RoyalOne private workflow remains deferred.
- Duplicate identical price rows are deduplicated for retrieval only. CO9555 and WF0451 contain conflicting records in the source list. The assistant flags these for office confirmation; source prices/calculator files were not changed.

## Refresh / publication

1. Run `node scripts/refresh-tec-knowledge.mjs` explicitly. Review changed source files and validation counts. Do not run it on customer requests or as an unattended auto-publish.
2. Run `npm test`, `node scripts/verify-tec-knowledge.mjs`, and review representative question results. Test with deployment Node 20 as well as available local runtime when possible.
3. Commit reviewed files and deploy the existing Render branch. Health checks and graceful shutdown remain unchanged. No session, lead, recording, membership or schema migration is involved.
4. In the deployed service shell, run `node scripts/publish-tec-knowledge.mjs`. It reads the service's existing Mongo secret without displaying it and upserts only this immutable TEC release. Verify the returned hash/count. No legacy sources are deleted.
5. `node scripts/verify-tec-knowledge.mjs --live` runs seven synthetic questions using the existing OpenAI service credential, without creating chats/leads/orders. Review actual answers and completion reasons; reported API time is not a full browser-to-answer benchmark.

## Rollback and limits

Revert the implementation commit and redeploy using the normal branch workflow. Existing snapshots are retained; deploying a prior code/content version automatically selects its matching library. Do not drop Mongo collections or clear customer PWA storage.

Retrieval is deterministic and local, not open-ended semantic web search. Unknown or ambiguous products require clarification. Updates to official policies still require a reviewed refresh. Stored knowledge lookup time and full AI answer time are different metrics. The assistant remains an AI and cannot guarantee perfect answers; tests cover full source records, aliases, missing evidence, follow-ups, exclusions, scope/cache isolation and Mongo fallback.
