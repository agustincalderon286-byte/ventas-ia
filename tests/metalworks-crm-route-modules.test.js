import test from "node:test";
import assert from "node:assert/strict";

import { registerMetalworksCrmAccessRoutes } from "../src/metalworks-crm/access-routes.js";
import { registerMetalworksCrmAgendaRoutes } from "../src/metalworks-crm/agenda-routes.js";
import { registerMetalworksCrmCalendarRoutes } from "../src/metalworks-crm/calendar-routes.js";
import { registerMetalworksCrmLeadRoutes } from "../src/metalworks-crm/lead-routes.js";
import { registerMetalworksCrmLeadOperationRoutes } from "../src/metalworks-crm/lead-operation-routes.js";
import { registerMetalworksSearchKingsRoutes } from "../src/metalworks-crm/searchkings-routes.js";

function captureRoutes(register) {
  const routes = [];
  const app = {
    get: (path) => routes.push({ method: "get", path }),
    post: (path) => routes.push({ method: "post", path }),
    patch: (path) => routes.push({ method: "patch", path }),
    delete: (path) => routes.push({ method: "delete", path }),
  };

  register(app, {});
  return routes;
}

function captureRouteHandlers(register, dependencies = {}) {
  const routes = [];
  const app = {
    get: (path, handler) => routes.push({ method: "get", path, handler }),
    post: (path, handler) => routes.push({ method: "post", path, handler }),
    patch: (path, handler) => routes.push({ method: "patch", path, handler }),
    delete: (path, handler) => routes.push({ method: "delete", path, handler }),
  };

  register(app, dependencies);
  return routes;
}

test("registers the CRM feature routes from dedicated modules", () => {
  assert.deepEqual(captureRoutes(registerMetalworksCrmAccessRoutes), [
    { method: "post", path: "/api/metalworks-crm/login" },
    { method: "post", path: "/api/metalworks-crm/logout" },
  ]);

  assert.deepEqual(captureRoutes(registerMetalworksCrmAgendaRoutes), [
    { method: "get", path: "/api/metalworks-crm/agenda-events" },
    { method: "post", path: "/api/metalworks-crm/agenda-events" },
  ]);

  assert.deepEqual(captureRoutes(registerMetalworksCrmCalendarRoutes), [
    { method: "post", path: "/api/metalworks-crm/google-calendar/cleanup-atlas-outreach" },
  ]);

  assert.deepEqual(captureRoutes(registerMetalworksCrmLeadRoutes), [
    { method: "post", path: "/api/metalworks-crm/leads" },
  ]);

  assert.deepEqual(captureRoutes(registerMetalworksCrmLeadOperationRoutes), [
    { method: "get", path: "/api/metalworks-crm/leads/:leadId" },
    { method: "patch", path: "/api/metalworks-crm/leads/:leadId" },
    { method: "delete", path: "/api/metalworks-crm/leads/:leadId" },
    { method: "post", path: "/api/metalworks-crm/leads/:leadId/assets" },
    { method: "post", path: "/api/metalworks-crm/leads/:leadId/live-chat-reply" },
    { method: "get", path: "/api/metalworks-crm/assets/:assetId/content" },
    { method: "post", path: "/api/metalworks-crm/leads/:leadId/send-estimate" },
  ]);

  assert.deepEqual(captureRoutes(registerMetalworksSearchKingsRoutes), [
    {
      method: "post",
      path: ["/integrations/searchkings/webhook", "/api/integrations/searchkings/webhook"],
    },
  ]);
});

test("lead operation routes preserve invalid-ID validation outside the main file", async () => {
  const responses = [];
  const routes = captureRouteHandlers(registerMetalworksCrmLeadOperationRoutes, {
    requireAuth: async () => ({ email: "agustin@example.com" }),
    mongoose: { Types: { ObjectId: { isValid: () => false } } },
    respondError: (res, status, error) => {
      responses.push({ status, error });
      return res;
    },
  });
  const updateRoute = routes.find(
    (route) => route.method === "patch" && route.path === "/api/metalworks-crm/leads/:leadId",
  );

  await updateRoute.handler({ params: { leadId: "not-an-object-id" }, body: {} }, {});

  assert.deepEqual(responses, [{ status: 400, error: "Lead invalido." }]);
});
