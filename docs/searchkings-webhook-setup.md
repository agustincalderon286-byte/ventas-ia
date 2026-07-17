# SearchKings Webhook Setup

Last updated: July 17, 2026

## Webhook endpoint for SearchKings

- Production endpoint URL: `https://cmwf-crm-api.onrender.com/api/integrations/searchkings/webhook`
- Alias endpoint URL: `https://cmwf-crm-api.onrender.com/integrations/searchkings/webhook`

## Recommended authorization for SearchKings

Use token auth.

The receiver accepts either:

- Query param: `?token=<SEARCHKINGS_WEBHOOK_TOKEN>`
- Header: `x-searchkings-webhook-token: <SEARCHKINGS_WEBHOOK_TOKEN>`
- Or standard bearer auth: `Authorization: Bearer <SEARCHKINGS_WEBHOOK_TOKEN>`

## Render environment variables

Add or confirm this on Render for `cmwf-crm-api`:

- `SEARCHKINGS_WEBHOOK_TOKEN=<strong random password>`

## Supported payload style

The receiver is intentionally flexible. It tries to normalize common call fields such as:

- `callId`, `id`, `sid`, `leadId`
- `callerName`, `contactName`, `name`
- `callerPhone`, `callerNumber`, `fromNumber`, `phone`
- `campaign`, `campaignName`
- `adGroup`, `adGroupName`
- `keyword`
- `summary`, `callSummary`, `aiSummary`
- `transcript`
- `recordingUrl`
- `outcome`, `status`, `callOutcome`
- `timestamp`, `callTime`, `receivedAt`, `createdAt`

It stores the lead in Metal Works CRM as:

- `externalSystem: "searchkings"`
- `sourceType: "searchkings_call"`
- source group attribution aligned with `search_kings_google_ads`

## Example test with curl

Replace `<token>` with your real Render env value:

```bash
curl -i \
  -X POST "https://cmwf-crm-api.onrender.com/api/integrations/searchkings/webhook?token=<token>" \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "call.completed",
    "call": {
      "callId": "sk_call_001",
      "callerName": "Maria Lopez",
      "callerPhone": "+1 (312) 555-0188",
      "campaignName": "Chicago Metal Repair",
      "adGroupName": "Railing Repair",
      "keyword": "metal railing repair chicago",
      "summary": "Caller needs an estimate for a rusted exterior stair railing.",
      "recordingUrl": "https://example.com/recording/sk_call_001",
      "transcript": "Hi, I need help with a railing repair...",
      "outcome": "missed_call",
      "timestamp": "2026-07-17T10:15:00.000Z",
      "duration": "42"
    }
  }'
```

Expected response:

```json
{
  "ok": true,
  "eventType": "call.completed",
  "entityType": "call"
}
```
