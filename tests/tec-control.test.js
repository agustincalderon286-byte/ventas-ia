import test from "node:test";
import assert from "node:assert/strict";
import mongoose from "mongoose";
import { readFileSync } from "node:fs";
import { actorExpression, userActivityQuery, controlPeriod, membershipKind, readTecInvoices, registerTecControlRoutes } from "../src/tec-control.js";

test("membership separates perpetual tests, admins, seats, trials and active paid cycles", () => {
  const options = { isTest: email => email === "test@example.com", isAdmin: user => user.email === "admin@example.com", monthlyPrice: "monthly", annualPrice: "annual" };
  const active = { stripeSubscriptionId: "sub_x", subscriptionStatus: "active", stripePriceId: "monthly" };
  assert.equal(membershipKind(active, options), "monthly");
  assert.equal(membershipKind({ ...active, stripePriceId: "annual" }, options), "annual");
  assert.equal(membershipKind({ ...active, email: "test@example.com" }, options), "test");
  assert.equal(membershipKind({ ...active, email: "admin@example.com" }, options), "admin");
  assert.equal(membershipKind({ ...active, accountType: "seat" }, options), "team");
  assert.equal(membershipKind({ ...active, subscriptionStatus: "trialing" }, options), "trial");
  assert.equal(membershipKind({ ...active, subscriptionStatus: "past_due" }, options), "inactive");
  assert.equal(membershipKind({ subscriptionActive: true }, options), "inactive");
});
test("periods are bounded and actor attribution does not double-credit upline", () => {
  assert.equal(controlPeriod("9999").days, 30);
  assert.equal(controlPeriod("all").from.toISOString(), "2000-01-01T00:00:00.000Z");
  assert.equal(controlPeriod("7", new Date("2026-10-22T15:00:00Z")).from.toISOString(), "2026-10-15T15:00:00.000Z");
  assert.deepEqual(actorExpression, { $ifNull: ["$generatedByUserId", "$ownerUserId"] });
  assert.deepEqual(userActivityQuery("lorena"), { $or: [{ generatedByUserId: "lorena" }, { generatedByUserId: null, ownerUserId: "lorena" }] });
});
test("billing reads only real eligible USD invoices, excludes out-of-band and other apps, and flags truncation", async () => {
  let calls = 0;
  const stripe = { invoices: { list: async params => {
    calls++; assert.equal(params.status, "paid"); assert.ok(params.created.gte);
    const row = { id: `in_${calls}`, customer: "cus_tec", currency: "usd", amount_paid: 5000, livemode: true };
    return { has_more: true, data: [row, { ...row, customer: "cus_unrelated" }, { ...row, livemode: false }, { ...row, paid_out_of_band: true }] };
  } } };
  const data = await readTecInvoices(stripe, ["cus_tec"], controlPeriod(30));
  assert.equal(calls, 3); assert.equal(data.grossUsd, 150); assert.equal(data.invoices, 3); assert.equal(data.incomplete, true);
  assert.match(data.basis, /No incluye facturas antiguas/);
  assert.equal((await readTecInvoices(null, [], controlPeriod(30))).available, false);
  assert.equal((await readTecInvoices({ invoices: { list() { throw Error("outage"); } } }, ["cus_tec"], controlPeriod(30))).available, false);
});
function harness(authorized = false) {
  const routes = new Map(), queries = [];
  const david = { _id: new mongoose.Types.ObjectId(), name: "David", email: "david@example.com", accountType: "owner" };
  const lorena = { _id: new mongoose.Types.ObjectId(), name: "Lorena", email: "lorena@example.com", accountType: "seat", teamOwnerUserId: david._id };
  const query = rows => ({ select(fields) { queries.push({ fields }); return this; }, sort() { return this; }, skip() { return this; }, limit() { return this; }, async lean() { return rows; } });
  const users = { find(filter) { queries.push({ users: filter }); return query([david, lorena]); }, countDocuments: async () => 2,
    findById: () => query(lorena) };
  const empty = { find(filter) { queries.push({ filter }); return query([]); }, aggregate: async stages => { queries.push({ stages }); return []; } };
  registerTecControlRoutes({ get: (path, handler) => routes.set(path, handler) }, {
    models: { users, profiles: empty, leads: empty, programs: empty, surveys: empty, outcomes: empty, analyses: { ...empty, findById: () => query(null) } },
    requireAdmin: async (_req, res) => authorized ? { user: { _id: david._id } } : (res.status(403).json({ error: "admin only" }), null),
    isTest: () => true, isAdmin: () => false, mongoose, cleanAnalysis: x => x, stripe: null
  });
  async function run(path, { params = {}, query = {} } = {}) {
    const res = { statusCode: 200, set() { return this; }, status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; } };
    await routes.get(path)({ params, query }, res); return res;
  }
  return { run, routes, queries, david, lorena };
}
test("every new tower GET rejects non-admins before any database read", async () => {
  const h = harness();
  for (const path of h.routes.keys()) assert.equal((await h.run(path)).statusCode, 403);
  assert.deepEqual(h.queries, []);
});
test("admin sees all accounts including Lorena, with upline names and no credentials", async () => {
  const h = harness(true);
  const res = await h.run("/api/control/tec/overview", { query: { days: 30, search: ".*" } });
  assert.equal(res.statusCode, 200); assert.equal(res.body.users.length, 2);
  assert.equal(res.body.users[1].uplineName, "David");
  assert.equal(res.body.membership.test, 2);
  assert.equal(res.body.users[1].membership, "test");
  assert.equal(res.body.summary.closeRate, null);
  assert.ok(h.queries[0].users.$or[0].name.test("literal .* text"));
  assert.equal(h.queries[0].users.$or[0].name.test("David"), false, "search is literal, not arbitrary regex");
  assert.doesNotMatch(JSON.stringify(res.body), /password|stripeCustomerId/);
});
test("admin detail selects actor-specific recent data, not the entire upline workspace", async () => {
  const h = harness(true);
  const res = await h.run("/api/control/tec/users/:id/activity", { params: { id: String(h.lorena._id) } });
  assert.equal(res.statusCode, 200); assert.equal(res.body.limitPerSection, 30);
  const leadFilter = h.queries.find(item => item.filter?.$or)?.filter;
  assert.equal(String(leadFilter.$or[0].generatedByUserId), String(h.lorena._id));
  assert.equal(leadFilter.$or[1].generatedByUserId, null);
  assert.ok(leadFilter.createdAt.$gte instanceof Date);
});
test("new tower replaces legacy UI without deleting legacy routes and keeps audio lab separate", () => {
  const html = readFileSync(new URL("../private/tec-control-tower.html", import.meta.url), "utf8");
  assert.doesNotMatch(html, /Abrir Chef|HighLevel|Replies WhatsApp|OPTIMO/);
  assert.match(html, /id="audio-lab"[^>]*hidden/);
  assert.match(html, /no cambia el grabador/i);
  assert.match(html, /id="dashboard" hidden/);
  const server = readFileSync(new URL("../server.js", import.meta.url), "utf8");
  assert.match(server, /sendFile\(path.join\(PRIVATE_DIR, "tec-control-tower.html"\)\)/);
  assert.match(server, /app.get\("\/api\/control\/overview"/);
});
