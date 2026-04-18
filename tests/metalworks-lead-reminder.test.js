import test from "node:test";
import assert from "node:assert/strict";

import {
  collectDueLeadReminderOffsets,
  formatLeadReminderOffsetLabel,
  normalizeLeadReminderOffsets,
} from "../src/metalworks-crm.js";

test("normalizeLeadReminderOffsets keeps only supported unique values", () => {
  assert.deepEqual(normalizeLeadReminderOffsets(["1440", "60", "1440", "25", "", "120"]), [
    60,
    120,
    1440,
  ]);
});

test("collectDueLeadReminderOffsets returns only reminders due inside grace window", () => {
  const nextActionAt = new Date("2026-04-18T18:00:00.000Z");
  const dueReminders = collectDueLeadReminderOffsets({
    nextActionAt,
    reminderOffsets: [60, 120, 1440],
    sentKeys: [`${nextActionAt.toISOString()}|120`],
    now: new Date("2026-04-18T17:01:30.000Z"),
    graceMs: 2 * 60 * 1000,
  });

  assert.deepEqual(
    dueReminders.map((reminder) => ({
      offsetMinutes: reminder.offsetMinutes,
      label: reminder.label,
      key: reminder.key,
    })),
    [
      {
        offsetMinutes: 60,
        label: "1 hour before",
        key: `${nextActionAt.toISOString()}|60`,
      },
    ],
  );
  assert.equal(formatLeadReminderOffsetLabel(1440), "1 day before");
});
