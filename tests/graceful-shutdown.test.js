import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { installGracefulShutdown } from "../src/graceful-shutdown.js";

test("shutdown stops new jobs but keeps the database open until the current request and worker finish", async () => {
  const events = [], signals = new EventEmitter(); let close, busy = true, wake;
  const drain = installGracefulShutdown({ signals,
    server: { close(callback) { events.push("close"); close = callback; } },
    setDraining: () => events.push("draining"), stopWorker: () => events.push("stop jobs"), workerBusy: () => busy,
    disconnect: async () => events.push("disconnect"), exit: code => events.push(`exit ${code}`),
    delay: async () => new Promise(resolve => { wake = resolve; }), schedule: () => ({}), cancel() {} });
  const first = drain(); const second = drain();
  assert.equal(first, second);
  assert.deepEqual(events, ["draining", "stop jobs", "close"]);
  close(); await new Promise(setImmediate); assert.equal(events.includes("disconnect"), false);
  busy = false; wake(); await first;
  assert.deepEqual(events.slice(-2), ["disconnect", "exit 0"]);
});

test("termination signals invoke a single drain and a deadline bounds stalled work", async () => {
  const signals = new EventEmitter(); let forced = 0, deadline, exits = [];
  installGracefulShutdown({ signals, server: { close() {}, closeAllConnections() { forced++; } },
    setDraining() {}, stopWorker() {}, workerBusy: () => false, disconnect: async () => {},
    exit: code => exits.push(code), schedule: callback => { deadline = callback; return {}; }, cancel() {} });
  signals.emit("SIGTERM"); signals.emit("SIGINT"); deadline();
  assert.equal(forced, 1); assert.deepEqual(exits, [1]);
});
