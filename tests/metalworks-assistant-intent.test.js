import test from "node:test";
import assert from "node:assert/strict";

import {
  detectAssistantProjectLeadIntent,
  detectEmploymentCorrection,
  detectEmploymentIntent,
} from "../src/metalworks-crm.js";

test("does not classify a guardrail project opening as a hiring inquiry", () => {
  const message =
    "We have this large opening that is a safety issue and would like to review some sort of guardrails.";

  assert.equal(detectEmploymentIntent(message), false);
  assert.equal(detectAssistantProjectLeadIntent(message), true);
});

test("keeps strong hiring language classified as employment intent", () => {
  assert.equal(detectEmploymentIntent("Are you hiring welders right now?"), true);
  assert.equal(detectEmploymentIntent("Busco trabajo de soldador"), true);
});

test("recognizes an explicit correction from hiring to project intent", () => {
  const message = "This isn’t for hiring it’s for a project.";

  assert.equal(detectEmploymentCorrection(message), true);
  assert.equal(detectEmploymentIntent(message), false);
});
