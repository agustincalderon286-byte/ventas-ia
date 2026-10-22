import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createCoachFormWriteGuard } from "../src/coach-form-write-guard.js";

const accountId = "000000000000000000000001";
function setup() {
  const rows = new Map();
  let authenticated = true, time = 0, failWrite = false;
  const model = {
    async create(row) { if (rows.has(row._id)) throw Object.assign(new Error("duplicate"), { code: 11000 }); rows.set(row._id, row); },
    findById(id) { return { lean: async () => rows.get(id) }; },
    async updateOne(query, update) { if (failWrite) throw new Error("DB down"); Object.assign(rows.get(query._id), update.$set); }
  };
  const guard = () => createCoachFormWriteGuard({ model, now: () => time,
    requireAuth: async (_req, res) => authenticated ? { user: { _id: accountId } } : (res.status(401).json({ error: "login" }), null) });
  const run = async (write, { body = { accountId, fullName: "Fictitious Test" }, path = "/api/coach/health-surveys", id = "form-test-123456" } = {}) => {
    let finish;
    const done = new Promise(resolve => { finish = resolve; });
    const res = { statusCode: 200, status(code) { this.statusCode = code; return this; }, json(body) { finish({ status: this.statusCode, body }); return this; } };
    await guard()({ path, headers: { "x-tec-request-id": id }, body }, res, () => write(res));
    return done;
  };
  return { run, rows, set authenticated(value) { authenticated = value; }, set time(value) { time = value; }, set failWrite(value) { failWrite = value; } };
}

test("a lost response and a fresh guard instance replay the committed response without writing again", async () => {
  const h = setup(); let writes = 0;
  const first = await h.run(res => { writes++; res.json({ survey: { id: "one" } }); });
  const retry = await h.run(() => { writes++; });
  assert.deepEqual(retry, first);
  assert.equal(writes, 1);
});

test("simultaneous browser and worker retries cannot execute the same form twice", async () => {
  const h = setup(); let reply, writes = 0;
  const first = h.run(res => { writes++; reply = res; });
  await new Promise(setImmediate);
  const concurrent = await h.run(() => { writes++; });
  assert.equal(concurrent.status, 503);
  assert.equal(writes, 1);
  reply.json({ created: true });
  assert.equal((await first).status, 200);
  assert.equal((await h.run(() => { writes++; })).status, 200);
  assert.equal(writes, 1);
});

test("account ownership, login and changed payloads are checked before replay", async () => {
  const h = setup(); let writes = 0;
  await h.run(res => { writes++; res.json({ ok: true }); });
  assert.equal((await h.run(() => { writes++; }, { body: { accountId, fullName: "Changed" } })).status, 409);
  assert.equal((await h.run(() => { writes++; }, { body: { accountId: "another" } })).status, 403);
  h.authenticated = false;
  assert.equal((await h.run(() => { writes++; })).status, 401);
  assert.equal(writes, 1);
});

test("an uncertain server operation retains its receipt and never silently creates a second record", async () => {
  const h = setup(); let writes = 0;
  await h.run(res => { writes++; res.status(500).json({ error: "Failure after write" }); });
  const retry = await h.run(() => { writes++; });
  assert.equal(retry.status, 409);
  assert.equal(retry.body.code, "form_uncertain");
  assert.equal(writes, 1);
  assert.equal(h.rows.size, 1);
});

test("failure to persist a confirmation does not report success or re-execute the operation", async () => {
  const h = setup(); let writes = 0;
  h.failWrite = true;
  assert.equal((await h.run(res => { writes++; res.json({ ok: true }); })).status, 503);
  h.time = 130000;
  assert.equal((await h.run(() => { writes++; })).body.code, "form_uncertain");
  assert.equal(writes, 1);
});

test("validation errors are replayed safely and different form routes have independent receipts", async () => {
  const h = setup(); let writes = 0;
  await h.run(res => res.status(400).json({ error: "Invalid phone" }));
  assert.equal((await h.run(() => { writes++; })).status, 400);
  assert.equal((await h.run(res => { writes++; res.json({ outcome: "one" }); }, { path: "/api/coach/demo-outcomes" })).status, 200);
  assert.equal(writes, 1);
  const source = readFileSync(new URL("../server.js", import.meta.url), "utf8");
  for (const route of ["health-surveys", "recruitment-applications", "demo-outcomes"]) {
    assert.ok(source.includes(`app.post("/api/coach/${route}", guardCoachFormWrite,`));
  }
});
