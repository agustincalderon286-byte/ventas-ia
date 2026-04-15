# Thumbtack Webhook Setup

Last updated: April 15, 2026

## Webhook endpoint for Thumbtack

- Production endpoint URL: `https://cmwf-crm-api.onrender.com/api/integrations/thumbtack/webhook`
- Alias endpoint URL: `https://cmwf-crm-api.onrender.com/integrations/thumbtack/webhook`

## Recommended authorization in Thumbtack

Thumbtack's Business Webhook docs support optional `Basic` authentication for webhook delivery.

Use this in Thumbtack:

- Authorization type: `Basic`
- Username: `thumbtack`
- Password: value of `THUMBTACK_WEBHOOK_PASSWORD`

If you prefer token-based testing outside of Thumbtack, the endpoint also accepts a bearer token via `THUMBTACK_WEBHOOK_TOKEN`, but Basic Auth is the best fit for the Thumbtack UI.

## Render environment variables

Add or confirm these on Render for `cmwf-crm-api`:

- `THUMBTACK_WEBHOOK_USERNAME=thumbtack`
- `THUMBTACK_WEBHOOK_PASSWORD=<strong random password>`
- `THUMBTACK_WEBHOOK_TOKEN=<optional fallback token for manual testing>`

## What to select in Thumbtack

If your main goal is lead intake into the CRM, start with:

- `Lead details`

Optional extras already supported by the receiver:

- `Messages`
- `Reviews`

The CRM will:

- upsert Thumbtack leads into Metal Works CRM
- attach message and review activity for debugging and follow-up
- accept message-first webhooks even if the negotiation arrives later

## Test with curl

Replace `<password>` with your real Render env value:

```bash
curl -i \
  -X POST https://cmwf-crm-api.onrender.com/api/integrations/thumbtack/webhook \
  -u thumbtack:<password> \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "NegotiationCreatedV4",
    "negotiation": {
      "negotiationID": "neg_demo_001",
      "jobStatus": "active",
      "description": "Need exterior handrail repair in Chicago.",
      "customer": {
        "customerID": "cust_demo_001",
        "displayName": "Demo Customer",
        "phone": "+1 773 555 0119"
      },
      "category": {
        "name": "Metal Fabricators"
      },
      "services": [
        { "name": "Handrail Installation" }
      ],
      "location": {
        "city": "Chicago",
        "state": "IL",
        "zipCode": "60632"
      }
    }
  }'
```

Expected response:

```json
{
  "ok": true,
  "eventType": "NegotiationCreatedV4",
  "entityType": "negotiation"
}
```

## Official references

- Businesses webhooks: `https://developers.thumbtack.com/docs/businesses`
- Negotiations implementation guide: `https://developers.thumbtack.com/docs/negotiations/implementation`
- Messages troubleshooting: `https://developers.thumbtack.com/docs/messages/troubleshooting`
