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
  assert.equal(lead.crmStatus, "new");
  assert.equal(lead.fullName, "Olivia Y.");
  assert.equal(lead.phone, "7735550199");
  assert.equal(lead.email, "olivia@example.com");
  assert.equal(lead.projectType, "Metal Fabricators / Handrail Installation");
  assert.equal(lead.city, "Chicago");
  assert.equal(lead.zipCode, "60632");
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

test("extrae attachments de mensajes de Thumbtack cuando el webhook trae URLs de fotos", () => {
  const payload = {
    event: {
      eventType: "MessageCreatedV4",
    },
    data: {
      messageID: "msg_attach_001",
      negotiationID: "neg_attach_001",
      from: "Customer",
      text: "Here are the project photos.",
      sentAt: "2026-04-17T04:00:00.000Z",
      customer: {
        customerID: "cust_attach_001",
        displayName: "Photo Customer",
      },
      attachments: [
        {
          fileName: "gate-photo.png",
          mimeType: "image/png",
          url: "https://thumbtack.example.com/uploads/gate-photo.png",
        },
      ],
    },
  };

  const event = buildThumbtackWebhookEvent(payload);

  assert.ok(event.leadCandidate);
  assert.equal(event.leadCandidate.externalLeadId, "neg_attach_001");
  assert.equal(event.leadCandidate.attachments.length, 1);
  assert.equal(
    event.leadCandidate.attachments[0]?.url,
    "https://thumbtack.example.com/uploads/gate-photo.png",
  );
  assert.equal(event.leadCandidate.attachments[0]?.fileName, "gate-photo.png");
  assert.equal(event.leadCandidate.meta.attachmentCount, 1);
  assert.equal(event.activity.meta.attachmentCount, 1);
});

test("normaliza campos alternos de customer y address para guardar nombre, telefono y zipcode", () => {
  const payload = {
    eventType: "NegotiationUpdatedV4",
    data: {
      negotiation: {
        negotiationID: "neg_alt_001",
        status: "active",
        request: {
          description: "Need fence repair after storm damage.",
          category: {
            name: "Fence and Gate Installation",
          },
          services: [{ label: "Fence Repair" }],
          location: {
            address1: "1234 W 47th St",
            locality: "Chicago",
            administrativeArea: "IL",
            postalCode: "60609",
          },
        },
        customer: {
          firstName: "Maria",
          lastName: "Lopez",
          phoneNumber: {
            formattedNumber: "+1 (312) 555-0188",
          },
          emailAddress: "maria@example.com",
        },
      },
    },
  };

  const lead = buildThumbtackLeadCandidate(payload);

  assert.ok(lead);
  assert.equal(lead.externalLeadId, "neg_alt_001");
  assert.equal(lead.fullName, "Maria Lopez");
  assert.equal(lead.phone, "3125550188");
  assert.equal(lead.phoneDisplay, "+1 (312) 555-0188");
  assert.equal(lead.email, "maria@example.com");
  assert.equal(lead.addressLine, "1234 W 47th St");
  assert.equal(lead.city, "Chicago");
  assert.equal(lead.zipCode, "60609");
  assert.match(lead.location, /1234 W 47th St, Chicago, IL, 60609/);
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
    data: {
      reviewID: "rev_101",
      reviewerName: "EB",
      rating: 5,
      reviewText: "Fast, clean work and great communication.",
      negotiationID: "neg_review_101",
    },
  };

  const event = buildThumbtackWebhookEvent(payload);

  assert.equal(event.entityType, "review");
  assert.equal(event.leadCandidate, null);
  assert.equal(event.activity.activityType, "thumbtack_review");
  assert.equal(event.activity.meta.negotiationId, "neg_review_101");
  assert.match(event.activity.body, /Rating: 5/);
  assert.match(event.activity.body, /Fast, clean work/);
});
