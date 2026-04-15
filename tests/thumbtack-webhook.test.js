import test from "node:test";
import assert from "node:assert/strict";

import {
  buildThumbtackLeadCandidate,
  buildThumbtackWebhookEvent,
  getThumbtackEventType,
} from "../src/thumbtack-webhook.js";

test("normaliza un negotiation webhook de Thumbtack a lead del CRM", () => {
  const payload = {
    eventType: "NegotiationCreatedV4",
    negotiation: {
      negotiationID: "neg_123",
      requestID: "req_456",
      jobStatus: "active",
      description: "Need a new front stair railing and welding repair.",
      customer: {
        customerID: "cust_789",
        displayName: "Olivia Y.",
        phone: "+1 (773) 555-0199",
        email: "olivia@example.com",
      },
      category: {
        name: "Metal Fabricators",
      },
      services: [{ name: "Handrail Installation" }],
      location: {
        city: "Chicago",
        state: "IL",
        zipCode: "60632",
      },
    },
  };

  const lead = buildThumbtackLeadCandidate(payload);

  assert.ok(lead);
  assert.equal(lead.eventType, "NegotiationCreatedV4");
  assert.equal(lead.entityType, "negotiation");
  assert.equal(lead.externalLeadId, "neg_123");
  assert.equal(lead.externalSystem, "thumbtack");
  assert.equal(lead.sourceType, "thumbtack_negotiation");
  assert.equal(lead.fullName, "Olivia Y.");
  assert.equal(lead.phone, "7735550199");
  assert.equal(lead.email, "olivia@example.com");
  assert.equal(lead.projectType, "Metal Fabricators / Handrail Installation");
  assert.match(lead.location, /Chicago, IL, 60632/);
  assert.match(lead.details, /Negotiation ID: neg_123/);
  assert.match(lead.details, /Description: Need a new front stair railing/);
});

test("normaliza un customer message webhook con negotiation id", () => {
  const payload = {
    type: "MessageCreatedV4",
    data: {
      message: {
        messageID: "msg_001",
        negotiationID: "neg_777",
        from: "Customer",
        text: "Can you come by tomorrow afternoon?",
        sentAt: "2026-04-15T15:30:00.000Z",
        customer: {
          customerID: "cust_222",
          displayName: "Chris P.",
        },
      },
    },
  };

  const event = buildThumbtackWebhookEvent(payload);

  assert.equal(getThumbtackEventType(payload), "MessageCreatedV4");
  assert.equal(event.entityType, "message");
  assert.ok(event.leadCandidate);
  assert.equal(event.leadCandidate.externalLeadId, "neg_777");
  assert.equal(event.leadCandidate.sourceType, "thumbtack_message");
  assert.equal(event.leadCandidate.crmStatus, "contacted");
  assert.equal(event.leadCandidate.fullName, "Chris P.");
  assert.match(event.activity.body, /Can you come by tomorrow afternoon/);
});

test("ignora mensajes automáticos de Thumbtack para no crear leads basura", () => {
  const payload = {
    eventType: "MessageCreatedV4",
    message: {
      messageID: "msg_auto",
      negotiationID: "neg_auto",
      from: "Customer",
      messageType: "quick_reply",
      text: "Thanks for reaching out!",
    },
  };

  const lead = buildThumbtackLeadCandidate(payload);

  assert.equal(lead, null);
});

test("clasifica reviews sin crear un lead nuevo", () => {
  const payload = {
    eventType: "ReviewCreatedV4",
    review: {
      reviewID: "rev_101",
      reviewerName: "EB",
      rating: 5,
      reviewText: "Fast, clean work and great communication.",
    },
  };

  const event = buildThumbtackWebhookEvent(payload);

  assert.equal(event.entityType, "review");
  assert.equal(event.leadCandidate, null);
  assert.equal(event.activity.activityType, "thumbtack_review");
  assert.match(event.activity.body, /Rating: 5/);
  assert.match(event.activity.body, /Fast, clean work/);
});
