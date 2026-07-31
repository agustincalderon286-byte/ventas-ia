import test from "node:test";
import assert from "node:assert/strict";

import { registerMetalworksCrmAccessRoutes } from "../src/metalworks-crm/access-routes.js";
import { registerMetalworksCrmAgendaRoutes } from "../src/metalworks-crm/agenda-routes.js";
import { registerMetalworksCrmCalendarRoutes } from "../src/metalworks-crm/calendar-routes.js";
import { registerMetalworksCrmLeadRoutes } from "../src/metalworks-crm/lead-routes.js";
import { registerMetalworksSearchKingsRoutes } from "../src/metalworks-crm/searchkings-routes.js";

function captureRoutes(register) {
  const routes = [];
  const app = {
    get: (path) => routes.push({ method: "get", path }),
    post: (path) => routes.push({ method: "post", path }),
  };

  register(app, {});
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

  assert.deepEqual(captureRoutes(registerMetalworksSearchKingsRoutes), [
    {
      method: "post",
      path: ["/integrations/searchkings/webhook", "/api/integrations/searchkings/webhook"],
    },
  ]);
});
