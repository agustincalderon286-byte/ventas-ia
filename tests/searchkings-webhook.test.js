import test from "node:test";
import assert from "node:assert/strict";

import {
  buildSearchKingsLeadCandidate,
  buildSearchKingsWebhookEvent,
  getSearchKingsEventType,
} from "../src/searchkings-webhook.js";

test("normaliza una llamada de SearchKings a lead del CRM", () => {
  const payload = {
    eventType: "call.completed",
    call: {
      callId: "sk_call_123",
      callerName: "Maria Lopez",
      callerPhone: "+1 (312) 555-0188",
      campaignName: "Chicago Metal Repair",
      adGroupName: "Railing Repair",
      keyword: "metal railing repair chicago",
      summary: "Caller needs an estimate for a rusted exterior stair railing.",
      recordingUrl: "https://example.com/recordings/sk_call_123",
      transcript: "Hi, I need help with a railing repair.",
      outcome: "missed_call",
      timestamp: "2026-07-17T10:15:00.000Z",
      duration: "42",
      city: "Chicago",
      state: "IL",
      zipCode: "60632",
    },
  };

  const lead = buildSearchKingsLeadCandidate(payload);

  assert.ok(lead);
  assert.equal(lead.eventType, "call.completed");
  assert.equal(lead.entityType, "call");
  assert.equal(lead.externalLeadId, "sk_call_123");
  assert.equal(lead.externalSystem, "searchkings");
  assert.equal(lead.sourceType, "searchkings_call");
  assert.equal(lead.fullName, "Maria Lopez");
  assert.equal(lead.phone, "3125550188");
  assert.equal(lead.projectType, "Google Ads phone call / Chicago Metal Repair");
  assert.equal(lead.city, "Chicago");
  assert.equal(lead.zipCode, "60632");
  assert.match(lead.location, /Chicago, IL, 60632/);
  assert.match(lead.details, /Call ID: sk_call_123/);
  assert.match(lead.details, /Campaign: Chicago Metal Repair/);
  assert.match(lead.details, /Summary: Caller needs an estimate/);
});

test("acepta nombres alternos de campos para llamadas de SearchKings", () => {
  const payload = {
    type: "lead.created",
    data: {
      call: {
        id: "sk_alt_001",
        contact: {
          firstName: "Chris",
          lastName: "Pena",
          phone: "(773) 555-0199",
          email: "chris@example.com",
        },
        campaign: "Metal Fence Leads",
        source: "Google Ads",
        callSummary: "Asked for fence repair pricing.",
        callOutcome: "answered",
      },
    },
  };

  const event = buildSearchKingsWebhookEvent(payload);

  assert.equal(getSearchKingsEventType(payload), "lead.created");
  assert.ok(event.leadCandidate);
  assert.equal(event.leadCandidate.externalLeadId, "sk_alt_001");
  assert.equal(event.leadCandidate.fullName, "Chris Pena");
  assert.equal(event.leadCandidate.phone, "7735550199");
  assert.equal(event.leadCandidate.email, "chris@example.com");
  assert.match(event.activity.body, /Asked for fence repair pricing/);
});

test("acepta payload de Call Tracking Metrics con snake_case", () => {
  const payload = {
    id: 4385277242,
    sid: "CA5144d115d2dff6d16c056fd792bceb9d",
    name: "Private",
    source: "Google Ads",
    city: "Arlington Heights",
    state: "IL",
    postal_code: "60004",
    called_at: "2026-07-30 09:08 AM -05:00",
    duration: 180,
    status: "answered",
    caller_number: "+12244864244",
    caller_number_format: "(224) 486-4244",
    contact_number: "+12244864244",
    audio: "https://calls.searchkings.com/api/v1/accounts/597554/calls/example/recording",
    location: "https://www.chicagometalworksandfencing.com/google-ads-metalwork-chicago.html?gclid=test",
    transcription_text: "Jonathan requested a repair estimate for a car trunk metal repair.",
    summary:
      "Agent Sofia took a message for Rigo from caller Jonathan, who requested a repair estimate for a car trunk metal repair.",
  };

  const event = buildSearchKingsWebhookEvent(payload);

  assert.ok(event.leadCandidate);
  assert.equal(event.leadCandidate.externalLeadId, "4385277242");
  assert.equal(event.leadCandidate.phone, "2244864244");
  assert.equal(event.leadCandidate.phoneDisplay, "+12244864244");
  assert.equal(event.leadCandidate.city, "Arlington Heights");
  assert.equal(event.leadCandidate.zipCode, "60004");
  assert.match(event.leadCandidate.details, /Call Time: 2026-07-30 09:08 AM -05:00/);
  assert.match(event.leadCandidate.details, /Recording: https:\/\/calls.searchkings.com/);
  assert.match(event.leadCandidate.meta.landingUrl, /google-ads-metalwork-chicago/);
  assert.match(event.activity.body, /Agent Sofia took a message/);
});
