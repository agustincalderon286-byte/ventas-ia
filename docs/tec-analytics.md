# TEC usage analytics — 2026-11-04

Google Analytics property **562322027**, web stream **16271808920**, public measurement ID **G-DZCX3RMST7**. Created as a separate property under the owner's existing Analytics account. The pre-existing Chicago-railings property was not repurposed. Enhanced Measurement was turned **off on the persisted TEC web stream**, and there are no connected site tags. No API secret, OAuth token or reporting service account is deployed.

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
