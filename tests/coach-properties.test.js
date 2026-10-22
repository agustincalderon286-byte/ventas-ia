import test from "node:test";
import assert from "node:assert/strict";
import { createCoachPropertyService, normalizePropertyAddress, normalizePropertyCity, propertyDistanceMeters, registerCoachPropertyRoutes } from "../src/coach-properties.js";

const anchorPin = "00000000000001";
const property = (pin, address = "100 TEST ST", owner = "TEST OWNER") => ({ pin, year: "2026", prop_address_full: address, prop_address_city_name: "HILLSIDE", prop_address_zipcode_1: "60162", owner_address_name: owner, mail_address_name: "DIFFERENT TAXPAYER" });
const coordinate = (pin, lat = 41.87, lon = -87.9) => ({ pin, year: "2026", lat: String(lat), lon: String(lon) });
const ok = rows => ({ ok: true, json: async () => rows });

test("normalizes a US street without accepting arbitrary query content", () => {
  assert.equal(normalizePropertyAddress(" 100 north Test Street. "), "100 N TEST ST");
  assert.equal(normalizePropertyCity("Hillside"), "HILLSIDE");
  assert.equal(normalizePropertyAddress("100 O'NEIL RD"), "100 O'NEIL RD");
  for (const input of [null, {}, "Test Street", "100 TEST ST; SELECT *", "100 TEST ST, HILLSIDE IL"]) assert.throws(() => normalizePropertyAddress(input), { status: 400 });
  assert.throws(() => normalizePropertyCity({}), { status: 400 });
});

test("address search joins by the full PIN and never substitutes the taxpayer for the owner", async () => {
  let calls = 0;
  const service = createCoachPropertyService({ fetchImpl: async url => {
    calls++;
    const select = url.searchParams.get("$select");
    if (select === "max(year) as year") return ok([{ year: "2026" }]);
    if (url.pathname.includes("pabr-t5kh")) return ok([coordinate(anchorPin)]);
    assert.match(url.searchParams.get("$where"), /prop_address_full = '100 O''NEIL RD'/);
    return ok([property(anchorPin, "100 O'NEIL RD", "")]);
  } });
  const result = await service.search({ address: "100 O'NEIL RD", city: "HILLSIDE" });
  assert.equal(result.candidates.length, 1);
  assert.equal(result.candidates[0].pin, anchorPin);
  assert.equal(result.candidates[0].ownerName, "");
  assert.equal(result.candidates[0].lat, 41.87);
  assert.equal(result.candidates[0].formattedAddress, "100 O'NEIL RD, HILLSIDE, IL, 60162");
  assert.equal("mail_address_name" in result.candidates[0], false);
  await service.search({ address: "100 O'NEIL RD", city: "HILLSIDE" });
  assert.equal(calls, 3, "cached searches do not call the county again");
});

test("missing coordinates are reported rather than creating a marker at zero", async () => {
  const service = createCoachPropertyService({ fetchImpl: async url => {
    if (url.searchParams.get("$select") === "max(year) as year") return ok([{ year: "2026" }]);
    if (url.pathname.includes("pabr-t5kh")) return ok([{ pin: anchorPin, lat: null, lon: "-87.9" }]);
    return ok([property(anchorPin)]);
  } });
  const result = await service.search({ address: "100 TEST ST", city: "HILLSIDE" });
  assert.equal(result.candidates.length, 0);
  assert.equal(result.missingCoordinates, 1);
});

test("nearby search filters the circle, excludes the appointment, and groups same-address parcels", async () => {
  const pins = [anchorPin, "00000000000002", "00000000000003", "00000000000004", "00000000000005"];
  const service = createCoachPropertyService({ fetchImpl: async url => {
    const where = url.searchParams.get("$where");
    if (url.pathname.includes("3723-97qp")) {
      if (where.startsWith("pin =")) return ok([property(anchorPin)]);
      return ok([property(pins[1], "102 TEST ST", "OWNER A"), property(pins[2], "102 TEST ST", "OWNER B"), property(pins[3], "104 TEST ST", "NULL")]);
    }
    if (where.startsWith("pin in")) return ok([coordinate(anchorPin)]);
    return ok([coordinate(anchorPin), coordinate(pins[1], 41.8701), coordinate(pins[2], 41.8701), coordinate(pins[3], 41.8702), coordinate(pins[4], 41.88)]);
  } });
  const result = await service.nearby({ pin: anchorPin, radius: 200 });
  assert.equal(result.properties.length, 2);
  assert.deepEqual(result.properties[0].ownerNames, ["OWNER A", "OWNER B"]);
  assert.equal(result.properties[0].parcelCount, 2);
  assert.equal(result.properties[1].ownerName, "");
  assert.ok(result.properties.every(row => row.pin !== anchorPin && row.distanceMeters <= 200));
  assert.equal(result.truncated, false);
});

test("dense areas are bounded and explicitly marked as incomplete", async () => {
  const rows = Array.from({ length: 501 }, (_, i) => coordinate(String(i + 2).padStart(14, "0"), 41.8701));
  const service = createCoachPropertyService({ fetchImpl: async url => {
    if (url.pathname.includes("3723-97qp")) {
      if (url.searchParams.get("$where").startsWith("pin =")) return ok([property(anchorPin)]);
      return ok(rows.slice(0, 100).map(row => property(row.pin, `${Number(row.pin)} TEST ST`)));
    }
    if (url.searchParams.get("$where").startsWith("pin in")) return ok([coordinate(anchorPin)]);
    assert.equal(url.searchParams.get("$limit"), "501");
    return ok(rows);
  } });
  const result = await service.nearby({ pin: anchorPin, radius: 100 });
  assert.equal(result.properties.length, 60);
  assert.equal(result.truncated, true);
});

test("invalid nearby queries never reach the upstream API", async () => {
  const service = createCoachPropertyService({ fetchImpl: () => { throw new Error("should not fetch"); } });
  await assert.rejects(service.nearby({ pin: "1", radius: 200 }), { status: 400 });
  await assert.rejects(service.nearby({ pin: anchorPin, radius: 10000 }), { status: 400 });
  assert.equal(propertyDistanceMeters({ lat: 41.87, lon: -87.9 }, { lat: 41.87, lon: -87.9 }), 0);
});

test("upstream errors are safe and failed queries can be retried", async () => {
  let fail = true;
  const service = createCoachPropertyService({ fetchImpl: async () => {
    if (fail) return { ok: false, status: 429 };
    return ok([]);
  } });
  await assert.rejects(service.search({ address: "100 TEST ST", city: "HILLSIDE" }), { status: 503 });
  fail = false;
  await assert.rejects(service.search({ address: "100 TEST ST", city: "HILLSIDE" }), { status: 404 });
});

test("property endpoints require an active account and rate-limit each account independently", async () => {
  const handlers = new Map(); let calls = 0; let authenticated = false;
  registerCoachPropertyRoutes({ get: (path, handler) => handlers.set(path, handler) }, {
    requireCoachActivo: async (req, res) => authenticated ? { user: { _id: req.userId } } : (res.status(401).json({ error: "login" }), null),
    service: { search: async () => { calls++; return { candidates: [] }; } }
  });
  const run = async userId => {
    const res = { headers: {}, code: 200, set(key, value) { this.headers[key] = value; return this; }, status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; } };
    await handlers.get("/api/coach/properties/search")({ query: {}, userId }, res, error => { throw error; });
    return res;
  };
  assert.equal((await run("one")).code, 401);
  assert.equal(calls, 0);
  authenticated = true;
  for (let i = 0; i < 30; i++) assert.equal((await run("one")).code, 200);
  const limited = await run("one");
  assert.equal(limited.code, 429);
  assert.ok(Number(limited.headers["Retry-After"]) > 0);
  assert.equal(limited.headers["Cache-Control"], "private, no-store");
  assert.equal((await run("two")).code, 200);
});
