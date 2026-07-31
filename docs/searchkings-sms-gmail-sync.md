# SearchKings SMS Gmail Sync

This fallback imports only SearchKings SMS alert emails from Gmail into Metal Works CRM.

## One-time setup

1. In Render, add a strong random value as `SEARCHKINGS_SMS_EMAIL_TOKEN` to `cmwf-crm-api` and redeploy.
2. Create a standalone project in [Google Apps Script](https://script.google.com/) and paste `integrations/searchkings-sms-gmail-sync.gs`.
3. In Project Settings, add Script Property `SEARCHKINGS_SMS_EMAIL_TOKEN` with the same Render value.
4. Run `syncSearchKingsSmsLeads` once and approve Gmail and external-request permissions.
5. Add a time-driven trigger for `syncSearchKingsSmsLeads` every 5 minutes.

Only emails from `calls@searchkings.com` with `New SMS Lead` in the subject are imported. The CRM deduplicates with the Gmail message ID and saves the phone, ZIP, original SMS, and media URLs as `searchkings_sms`.
