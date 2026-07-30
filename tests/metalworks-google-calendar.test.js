import test from "node:test";
import assert from "node:assert/strict";

import { shouldSyncLeadToGoogleCalendar } from "../src/metalworks-crm.js";

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
