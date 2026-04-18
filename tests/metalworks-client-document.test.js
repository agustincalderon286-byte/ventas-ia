import test from "node:test";
import assert from "node:assert/strict";

import {
  buildMetalworksClientDocumentSnapshot,
  buildMetalworksEstimateEmail,
} from "../src/metalworks-crm.js";

test("invoice snapshot tracks deposit and balance due", () => {
  const snapshot = buildMetalworksClientDocumentSnapshot({
    fullName: "Gerad O'Donnell",
    clientDocumentType: "invoice",
    projectType: "Railing replacement and awning",
    estimateAmount: 1850,
    invoiceDepositAmount: 450,
  });

  assert.equal(snapshot.documentType, "invoice");
  assert.equal(snapshot.totalAmount, 1850);
  assert.equal(snapshot.depositAmount, 450);
  assert.equal(snapshot.balanceDueAmount, 1400);
  assert.equal(snapshot.deposit, "$450.00");
  assert.equal(snapshot.balanceDue, "$1,400.00");
});

test("invoice email includes deposit and balance due lines", () => {
  const email = buildMetalworksEstimateEmail(
    {
      fullName: "Gerad O'Donnell",
      email: "gerad@sbcglobal.net",
      clientDocumentType: "invoice",
      projectType: "Railing replacement",
      estimateAmount: 1850,
      invoiceDepositAmount: 450,
      clientDocumentDescription: "Replace damaged railing and install new anchors.",
    },
    "agustincalderon286@gmail.com",
  );

  assert.match(email.text, /Total project amount: \$1,850/);
  assert.match(email.text, /Deposit received: \$450/);
  assert.match(email.text, /Balance due: \$1,400/);
  assert.match(email.html, /Deposit received:/);
  assert.match(email.html, /Balance due:/);
});
