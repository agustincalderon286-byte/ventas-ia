import test from "node:test";
import assert from "node:assert/strict";

import {
  buildThumbtackExternalEventKey,
  parseCrmDatetimeInput,
  resolveExternalLeadCreateStatus,
} from "../src/metalworks-crm.js";

test("parseCrmDatetimeInput treats naive CRM datetimes as Chicago local time", () => {
  const parsed = parseCrmDatetimeInput("2026-04-19T09:00", "America/Chicago");

  assert.ok(parsed instanceof Date);
  assert.equal(parsed?.toISOString(), "2026-04-19T14:00:00.000Z");
});

test("parseCrmDatetimeInput preserves ISO timestamps with timezone", () => {
  const parsed = parseCrmDatetimeInput("2026-04-19T14:00:00.000Z", "America/Chicago");

  assert.ok(parsed instanceof Date);
  assert.equal(parsed?.toISOString(), "2026-04-19T14:00:00.000Z");
});

test("buildThumbtackExternalEventKey uses negotiation and message ids", () => {
  const key = buildThumbtackExternalEventKey({
    eventType: "MessageCreatedV4",
    entityType: "message",
    leadCandidate: {
      externalLeadId: "577384912670040071",
    },
    activity: {
      meta: {
        negotiationId: "577384912670040071",
        messageId: "577401801191514123",
      },
    },
  });

  assert.equal(key, "thumbtack:577384912670040071:message:577401801191514123");
});

test("resolveExternalLeadCreateStatus keeps first Thumbtack message leads as new", () => {
  assert.equal(
    resolveExternalLeadCreateStatus("contacted", "thumbtack", "thumbtack_message"),
    "new",
  );
  assert.equal(
    resolveExternalLeadCreateStatus("quoted", "thumbtack", "thumbtack_negotiation"),
    "quoted",
  );
});
