import test from "node:test";
import assert from "node:assert/strict";

import { registerMetalworksSearchKingsRoutes } from "../src/metalworks-crm/searchkings-routes.js";

test("adds a later SearchKings SMS media alert to the caller's existing lead", async () => {
  const routes = [];
  const queries = [];
  const activities = [];
  let saveCount = 0;
  let createCount = 0;
  const existingLead = {
    _id: "lead-1",
    fullName: "SearchKings SMS lead",
    phone: "7737984107",
    phoneDisplay: "+17737984107",
    projectType: "Google Ads SMS lead",
    location: "IL",
    zipCode: "",
    city: "",
    sourceType: "searchkings_sms",
    details: "Gmail Message ID: first-message\nMessage: Initial text",
    save: async () => {
      saveCount += 1;
    },
    toObject: () => ({ id: "lead-1", phone: "7737984107" }),
  };
  const app = {
    post: (path, handler) => routes.push({ path, handler }),
  };
  const MetalworksLead = {
    findOne: (query) => {
      queries.push(query);
      return {
        sort: async () => (query.sourceExternalId ? null : existingLead),
      };
    },
    create: async () => {
      createCount += 1;
      return null;
    },
  };

  registerMetalworksSearchKingsRoutes(app, {
    searchKingsWebhookConfigured: () => true,
    searchKingsSmsEmailConfigured: () => true,
    respondError: (_res, status, error) => ({ status, error }),
    requestHasSearchKingsWebhookAccess: () => true,
    requestHasSearchKingsSmsEmailAccess: () => true,
    parseSearchKingsWebhookBody: (value) => value,
    cleanText: (value, maxLength = 0) => String(value || "").trim().slice(0, maxLength || undefined),
    normalizePhone: (value) => String(value || "").replace(/\D/g, "").replace(/^1(?=\d{10}$)/, ""),
    normalizeEmail: (value) => String(value || "").trim().toLowerCase(),
    buildTrackingPayload: (value) => value,
    MetalworksLead,
    getClientIp: () => "127.0.0.1",
    appendActivity: async (activity) => activities.push(activity),
    sendMetalworksPushAlert: async () => ({ delivered: true }),
    cleanExternalLeadReceipt: (lead) => lead,
  });

  const route = routes.find((item) => item.path === "/api/integrations/searchkings/sms-email");
  const responses = [];

  await route.handler(
    {
      body: {
        gmailMessageId: "photo-message",
        from: "SearchKings calls@searchkings.com",
        subject: "SearchKings: New SMS Lead from +17737984107",
        receivedAt: "2026-07-31T03:54:28.000Z",
        body: "Text Message\nFrom: +17737984107\nTo: +17732957583\nMessage: media\nMedia: https://media.example/photo.jpg\nSearchKings",
      },
      headers: {},
      query: {},
    },
    { json: (body) => responses.push(body) },
  );

  assert.equal(createCount, 0);
  assert.equal(saveCount, 1);
  assert.equal(queries.length, 2);
  assert.match(existingLead.details, /Initial text/);
  assert.match(existingLead.details, /https:\/\/media\.example\/photo\.jpg/);
  assert.equal(activities.length, 1);
  assert.equal(activities[0].activityType, "searchkings_sms");
  assert.equal(responses[0].duplicate, true);
});
