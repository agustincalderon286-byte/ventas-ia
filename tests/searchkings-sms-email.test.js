import test from "node:test";
import assert from "node:assert/strict";
import { buildSearchKingsSmsEmailCandidate } from "../src/searchkings-sms-email.js";

test("convierte una alerta de Gmail de SearchKings en un lead SMS", () => {
  const candidate = buildSearchKingsSmsEmailCandidate({
    gmailMessageId: "19fb4505eababd72", from: "SearchKings <calls@searchkings.com>",
    subject: "SearchKings: New SMS Lead from +17739575001", receivedAt: "2026-07-30T18:36:33-05:00",
    body: `Text Message\n\nFrom: +17739575001\nTo: +17732957583\nMessage: Hi Rigo, I need a quote for metalwork in Chicago. We need the gap in our security gate welded close. My zip code is 60637\nMedia:\n[Attachment 0](https://media.plivo.com/Account/example/Media/file)\n\nSearchKings`,
  });
  assert.ok(candidate);
  assert.equal(candidate.entityType, "sms");
  assert.equal(candidate.externalLeadId, "gmail-19fb4505eababd72");
  assert.equal(candidate.sourceType, "searchkings_sms");
  assert.equal(candidate.phone, "7739575001");
  assert.equal(candidate.zipCode, "60637");
  assert.equal(candidate.city, "Chicago");
  assert.match(candidate.details, /security gate welded close/);
  assert.match(candidate.details, /media\.plivo\.com/);
});

test("rechaza emails que no son alertas SMS de SearchKings", () => {
  assert.equal(buildSearchKingsSmsEmailCandidate({
    gmailMessageId: "not-a-lead", from: "someone@example.com", subject: "New SMS Lead",
    body: "From: 7735550199\nMessage: Test",
  }), null);
});

test("acepta el formato de remitente que Gmail usa sin corchetes", () => {
  const candidate = buildSearchKingsSmsEmailCandidate({
    gmailMessageId: "display-sender", from: "SearchKings calls@searchkings.com",
    subject: "SearchKings: New SMS Lead from +17731234567",
    body: "From: +17731234567\nMessage: Need a gate repair in Chicago",
  });

  assert.ok(candidate);
  assert.equal(candidate.phone, "7731234567");
});
