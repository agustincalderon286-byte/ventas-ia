import test from "node:test";
import assert from "node:assert/strict";

import { mergeLeadTextImportIntoPrivateNotes } from "../src/metalworks-crm.js";

test("mergeLeadTextImportIntoPrivateNotes prepends a dated text import block", () => {
  const result = mergeLeadTextImportIntoPrivateNotes(
    "Existing note about pricing.",
    "Customer: Can you do Tuesday?\nMe: Yes, after 4 pm works.",
    {
      sourceLabel: "iMessage",
      importedAt: new Date("2026-04-17T22:15:00.000Z"),
      timeZone: "America/Chicago",
    },
  );

  assert.match(result, /^\[iMessage import • /);
  assert.match(result, /Customer: Can you do Tuesday\?/);
  assert.match(result, /Me: Yes, after 4 pm works\./);
  assert.match(result, /Existing note about pricing\./);
});

test("mergeLeadTextImportIntoPrivateNotes keeps existing notes untouched when import is empty", () => {
  const result = mergeLeadTextImportIntoPrivateNotes("Keep this manual note.", "", {
    sourceLabel: "Text",
  });

  assert.equal(result, "Keep this manual note.");
});
