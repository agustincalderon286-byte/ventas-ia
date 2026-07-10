# Atlas CRM Bridge

This repo now includes a small CLI bridge so Atlas can read and edit the Chicago Metal Works CRM without scraping the browser.

## Files

- CLI: `/Users/monse/Documents/New project/scripts/metalworks-crm-cli.js`
- NPM shortcut: `npm run metalworks:crm -- <command>`

## Required env vars

Add these where Atlas will run the command:

```bash
METALWORKS_CRM_API_BASE_URL=https://cmwf-crm-api.onrender.com
METALWORKS_CRM_LOGIN_EMAIL=agustincalderon286@gmail.com
METALWORKS_CRM_LOGIN_PASSWORD=your-crm-password
```

`METALWORKS_CRM_LOGIN_PASSWORD` is preferred over passing `--password` on the command line because shell history and process lists are easier to leak.

## What Atlas can do with it

### Check CRM auth

```bash
npm run metalworks:crm -- me
```

### Search leads

```bash
npm run metalworks:crm -- search-leads --query "Rigoberto" --json
```

```bash
npm run metalworks:crm -- search-leads --query "7737984107" --status contacted --json
```

### Recent leads

```bash
npm run metalworks:crm -- recent-leads --limit 5 --json
```

### Follow-up queue

```bash
npm run metalworks:crm -- followup-queue --bucket today --json
```

```bash
npm run metalworks:crm -- followup-queue --bucket stale --days-without-contact 3 --json
```

### Load one lead

```bash
npm run metalworks:crm -- get-lead --lead-id LEAD_ID --json
```

### Create one lead

Use this when Atlas creates a new company or customer directly in the CRM.

```bash
npm run metalworks:crm -- create-lead \
  --full-name "ABC Property Management" \
  --email-value vendor@example.com \
  --project-type "Property management outreach" \
  --source-group atlas_commercial_outreach \
  --status contacted \
  --details "Property management outreach campaign. Intro email sent 2026-07-10 to vendor@example.com. Offer: metal repairs, welding, railings, stairs, gates, fences, rust repair, and maintenance." \
  --next-action "Follow up on property management outreach if no reply." \
  --next-action-at "2026-07-14T09:00:00-05:00" \
  --json
```

### Update one lead

```bash
npm run metalworks:crm -- update-lead \
  --lead-id LEAD_ID \
  --status contacted \
  --next-action "Send quote after photos arrive" \
  --next-action-at "2026-06-13T09:00:00-05:00" \
  --private-notes "Customer texted from the field line." \
  --json
```

### Apply one Atlas workflow action

Use this when Atlas already understands the lead and only needs to move the operational state in a consistent way.

```bash
npm run metalworks:crm -- atlas-workflow \
  --lead-id LEAD_ID \
  --action waiting-photos \
  --json
```

Supported workflow actions:

- `waiting-customer`
- `waiting-photos`
- `waiting-price`
- `ready-to-schedule`
- `followup`
- `quoted`
- `booked`
- `won`
- `lost`

### Import a text thread into private notes

```bash
npm run metalworks:crm -- update-lead \
  --lead-id LEAD_ID \
  --text-thread-import-file /absolute/path/to/thread.txt \
  --text-thread-import-source "Rigoberto WhatsApp summary" \
  --json
```

## Recommended Atlas workflow

1. Search the CRM first by name or phone.
2. Read the lead detail before changing anything important.
3. Prefer the derived `lead.atlas` state when deciding what to do next:
   - `lead.atlas.stage`
   - `lead.atlas.missingFields`
   - `lead.atlas.suggestedActions`
   - `lead.atlas.brief`
4. Update only safe fields unless Agustin or Rigoberto explicitly asks for more:
   - `status`
   - `nextAction`
   - `nextActionAt`
   - `privateNotes`
   - `textThreadImport`
5. Use `atlas-workflow` for repeated operational moves like “waiting on photos” or “need price from Agustin” so Atlas does not invent different wording each time.
6. If multiple leads match, stop and ask which one is correct.

## Atlas Commercial Outreach

Atlas should run commercial outreach as a separate lane from the main Hot Schedule.

Daily routine when Agustin asks for outreach:

1. Find up to 20 new Chicago-area property managers, HOAs, condo associations, apartment operators, facility managers, or commercial maintenance contacts.
2. Search the CRM first by company name, email, phone, and website to avoid duplicates.
3. Send the intro email only after verifying the contact looks relevant.
4. Create the CRM lead with `sourceGroup=atlas_commercial_outreach`, status `contacted`, and project type `Property management outreach`.
5. Put cold follow-ups in Atlas Outreach with a simple next action like `Follow up on property management outreach if no reply.`
6. Do not treat cold follow-ups as Hot Schedule work for Agustin or Rigoberto.
7. Handoff to Agustin/Rigoberto when a prospect replies with real signal: quote request, photos, W-9/COI request, vendor onboarding, property/address, call request, or actual repair scope.
8. End each outreach run with a summary: emails sent, duplicates skipped, replies, hot handoffs, pending follow-ups, and high-value companies.

When a prospect becomes hot, update the lead with a concrete next action:

```bash
npm run metalworks:crm -- update-lead \
  --lead-id LEAD_ID \
  --source-group atlas_commercial_outreach \
  --status new \
  --next-action "Review photos and prepare quote for property manager." \
  --private-notes "Prospect replied with an active repair request. Summarize the email reply here." \
  --json
```

## Natural WhatsApp phrases Atlas should understand

These are good examples for Agustin and Rigoberto to send through WhatsApp:

```text
Atlas, busca este telefono en el CRM: 7737984107
Atlas, busca a Juan Perez en el CRM
Atlas, abre el lead de 7737984107
Atlas, dime el status y la proxima accion del lead de Juan Perez
Atlas, saca mis leads pendientes de hoy
Atlas, que clientes llevan 3 dias sin respuesta
Atlas, ensename los leads mas recientes
Atlas, agrega esta nota al lead de 7737984107: cliente pidio estimate por texto
Atlas, marca el lead de 7737984107 como contacted
Atlas, pon siguiente accion para el lead de Juan Perez: llamar manana a las 9am
Atlas, resume el lead mas reciente
```

Recommended pattern:

1. Search first by name or phone.
2. If exactly one match exists, Atlas can read or update it.
3. If multiple matches exist, Atlas should ask which lead is correct.
4. Atlas should never say an update succeeded unless the CRM bridge returned success.
5. For operational questions, Atlas can use:
   - `recent-leads` for the newest leads
   - `followup-queue --bucket today` for follow-ups scheduled today
   - `followup-queue --bucket stale --days-without-contact 3` for leads that have gone quiet

## Notes

- The CRM uses a session cookie, so this CLI logs in first and then calls the same authenticated endpoints the CRM UI uses.
- This is meant to be the safe first bridge for Atlas. Later, if you want, we can add higher-level commands like `find-by-phone-and-update` or direct message-to-CRM sync.
