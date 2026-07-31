import test from "node:test";
import assert from "node:assert/strict";

import {
  buildGoogleCalendarEventForLead,
  clearCrmLoginFailures,
  getCrmLoginThrottle,
  recordCrmLoginFailure,
  shouldSyncLeadToGoogleCalendar,
} from "../src/metalworks-crm.js";

test("keeps Atlas Outreach follow-ups out of Google Calendar", () => {
  const startsAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();

  assert.equal(
    shouldSyncLeadToGoogleCalendar({
      sourceType: "atlas_commercial_outreach",
      sourceExternalSystem: "atlas_outreach",
      projectType: "Property management outreach",
      status: "booked",
      appointmentStatus: "scheduled",
      nextActionAt: startsAt,
    }),
    false,
  );

  assert.equal(
    shouldSyncLeadToGoogleCalendar({
      sourceType: "website_form",
      projectType: "Railing repair",
      status: "booked",
      appointmentStatus: "scheduled",
      nextActionAt: startsAt,
    }),
    true,
  );
});

test("does not copy private CRM notes into Google Calendar", () => {
  const event = buildGoogleCalendarEventForLead({
    _id: "lead-123",
    fullName: "Maria Lopez",
    projectType: "Railing repair",
    nextActionAt: "2026-08-04T15:00:00.000Z",
    appointmentStatus: "scheduled",
    details: "Replace the rusted front railing.",
    privateNotes: "Private payment discussion that must remain in the CRM.",
  });

  assert.ok(event);
  assert.match(event.description, /Replace the rusted front railing/);
  assert.doesNotMatch(event.description, /Private payment discussion/);
  assert.doesNotMatch(event.description, /Private notes/);
});

test("temporarily blocks repeated CRM login failures", () => {
  const request = {
    headers: {},
    ip: "198.51.100.24",
    socket: { remoteAddress: "198.51.100.24" },
  };
  const email = "security-test@example.com";
  const now = Date.now();

  clearCrmLoginFailures(request, email);

  for (let attempt = 0; attempt < 4; attempt += 1) {
    assert.equal(recordCrmLoginFailure(request, email, now).blocked, false);
  }

  assert.equal(recordCrmLoginFailure(request, email, now).blocked, true);
  assert.equal(getCrmLoginThrottle(request, email, now).blocked, true);

  clearCrmLoginFailures(request, email);
  assert.equal(getCrmLoginThrottle(request, email, now).blocked, false);
});
