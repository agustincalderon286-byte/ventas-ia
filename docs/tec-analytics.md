# TEC usage analytics — 2026-11-04

Google Analytics property **562322027**, web stream **16271808920**, public measurement ID **G-DZCX3RMST7**. Created as a separate property under the owner's existing Analytics account. The pre-existing Chicago-railings property was not repurposed. Enhanced Measurement was turned **off on the persisted TEC web stream**, and there are no connected site tags. The general TEC integration uses no API secret, OAuth token or reporting service account. The separate audiobook payment integration below uses a Measurement Protocol secret held only on Render.

## Collection

- Website/PWA only: home, login, plans, Coach, privacy. Native wrappers with `TecNativeOffline` or the listed bridge markers skip this integration. No native builds changed.
- Explicit opt-in before Google script or usage POST. Rejecting keeps TEC operational; preferences live at the page footer. Honor GPC / DNT. Changing preferences never clears operational storage.
- Fixed event + area + web/PWA vocabulary, validated again by server. No form values, names, email, owner names, coordinates, addresses, prices, chat, audio, transcript, record IDs, full URL/query/hash/referrer or custom account ID. Fixed page title `TEC` and canonical synthetic URL.
- Google receives browser/network technical information after consent, including the connection IP and Analytics cookie identifiers. This is disclosed; do not call Google collection completely anonymous. Google Signals and ads permissions disabled. Analytics cookie expiry 30 days.
- Only fixed semantic button selectors and form selectors. Attempts are labelled as attempts, not completed saves/payments. No global fetch or media hooks.
- Batches up to eight events, one request per 2.5 seconds; first-party request timeout three seconds. No retries or persistent queue. Drop pending counts offline or during capture/processing. Observe only `data-capture-busy` to disable Google measurement during recording. Existing forms/audio queue is untouched.
- Google delivery is best effort. Blocking Google does not affect TEC. GA report settings should remain without automatic Enhanced Measurement.

## Torre

`/api/control/tec/usage` reuses administrator authorization. Shows **TEC aggregate counters**, not Google Data API results. No cross-user activity identifiers in this collection. Link opens the owner's full Google report, requiring a Google account authorized for that property. Google and TEC counters can differ (blocking/consent/delivery). Unique users, paid subscriptions and revenue are not inferred from clicks. Existing memberships/billing remain authoritative.

Daily collection `TecUsageDaily` contains only UTC date, fixed event, fixed area, web/PWA and count; TTL 90 days. An in-memory bounded per-IP rate limiter protects the public same-origin collector; IP is not persisted in this collection. Never log telemetry bodies. Only daily aggregates are stored, not per-event time or session ID. Future direct Google reports in Torre require a separate read-only Data API authorization; do not substitute counters and claim they are Google reports.

## Verification

Run `npm test`. `tests/tec-analytics.test.js` covers refusal/default denial, no data from fake private DOM values, offline, storage failures, recording transitions, network/tag failures, bounded queues, strict server schema/origin guards, admin-only reports and worker behavior. Existing map/recording/offline suites remain included. Chrome also exercises the real map module with fifty fictitious owners, a mocked Google transport and synthetic GPS, verifying walking/driving, saving notes and GPS resumption. This is not a physical GPS or microphone hardware test.

Deploy only the established Render branch. Verify health and public asset hashes; verify a deliberate non-private consented production page view in Google Realtime. The service worker does not force `skipWaiting`, so existing active sessions are not forcibly refreshed.


## Audiolibros: social visits and settled memberships — 2026-11-13

The audiobook integration retains explicit consent version 2, a 30-day campaign lifetime, GPC/DNT, exclusion of free/operator test accounts, no PII, and verified settled Stripe invoice deduplication. Nothing here grants access to complete audio or changes R2 delivery.

- Public profile links: `/audiolibros/fb`, `/audiolibros/ig`, `/audiolibros/yt`, `/audiolibros/tt`. Each opens a network-specific selection page. A visitor selecting a story carries its fixed piece ID into the library and its approved book sample. A generic profile visit remains `perfil`; a recognized untagged social referrer is `origen_sin_reel`. Never infer an individual Reel from a shared bio link alone.
- Individual publication links: `/audiolibros/r/NETWORK/PIECE_ID`. The reviewed registry is `public/audiolibros/social-campaigns.js`. It contains only public labels and publication IDs. Add new pieces there before publishing; do not put personal information, arbitrary captions, or customer IDs into UTM parameters. Future scheduled TikTok entries appear in the selection page only after their recorded publication time.
- Organic social vocabulary: `utm_source=facebook|instagram|youtube|tiktok`, `utm_medium=social`, `utm_campaign=tec_redes_es`, `utm_content=PIECE_ID`. Meta paid ads use `utm_source={{site_source_name}}&utm_medium=paid_social&utm_campaign=tec_meta_es&utm_content={{ad.id}}`; fb/ig/msg/an are normalized, and the ad ID must exist in the registry. Existing ChatGPT and legacy Meta labels continue to work.
- The same reviewed values accompany page views, sample plays, membership clicks, `begin_checkout`, stored Stripe payment context, and settled `purchase`. GA event dimensions: `tec_audio_source`, `tec_audio_medium`, `tec_audio_campaign`, `tec_audio_content` (exact fixed ID), `tec_audio_piece` (public title), `tec_audio_payment_type` (initial payment/renewal). For paid memberships compare `purchase` count and purchase revenue by the TEC source/content/piece dimensions. Filter payment type when counting new memberships separately from renewals. Do not count membership clicks or checkout creation as sales.
- Attribution uses the last recognized campaign within 30 days; an internal page or a direct return does not refresh the original expiry. Another explicit campaign replaces it. A new unknown external campaign clears the older label. Renewals retain original eligible custom attribution without injecting a new GA campaign/session interaction. Consent withdrawal clears payment measurement context.
- GA4 reporting is prospective, subject to consent/ad blockers, and new custom dimensions may take time to become usable in reports. This cannot reconstruct the specific Reel for historical untagged direct visits, cannot identify visits without consent, and does not promise a sale merely because tracking is installed. Stripe remains the authority for actual billing.

Validation: registry round-trip and URL injection guards, profile HTTP redirects and future visibility, consented browser page/sample/checkout continuity, all settled-invoice event variants with exact Reel preservation, deduplication and consent revocation. No live charge or synthetic production purchase is needed.
