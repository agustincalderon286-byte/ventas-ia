import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";
import { routeNavigationUrl } from "../public/coach/property-route-state.js";
test("both navigation apps open one destination from the current location, in walking or driving mode", () => {
 const stop = {property:{formattedAddress:"101 TEST ST, HILLSIDE, IL, 60162"}};
 for (const provider of ['google','apple']) for (const mode of ['walking','driving']) {
  const url=new URL(routeNavigationUrl(provider,stop,mode));
  assert.equal(url.searchParams.get(provider==='apple'?'daddr':'destination'),stop.property.formattedAddress);
  assert.equal(url.searchParams.get(provider==='apple'?'dirflg':'travelmode'),provider==='apple'?(mode==='walking'?'w':'d'):mode);
  assert.equal(url.searchParams.has('origin'),false);assert.equal(url.searchParams.has('saddr'),false);assert.equal(url.searchParams.has('waypoints'),false);
 }
 assert.equal(routeNavigationUrl('google',null),'');assert.equal(routeNavigationUrl('apple',null),'');
});
test("both maps are available in property routes and agenda and PWA updates do not force takeover", () => {
  const html = readFileSync(new URL("../private/coach-app.html", import.meta.url), "utf8");
  const app = readFileSync(new URL("../public/coach/app.js", import.meta.url), "utf8");
  const worker = readFileSync(new URL("../public/coach/sw.js", import.meta.url), "utf8");
  assert.match(html, /data-property-complete/); assert.match(html, /data-property-apple-route/);
  assert.match(app, /record\.appleMapsUrl/); assert.match(app, />Google Maps<\/a>/);
  assert.match(app, />Apple Maps<\/a>/);
  const version = html.match(/property-map\.js\?v=([^" ]+)/)?.[1];
  assert.ok(version && worker.includes(`/coach/property-map.js?v=${version}`));
  assert.doesNotMatch(worker, /self\.skipWaiting\(/);
});
