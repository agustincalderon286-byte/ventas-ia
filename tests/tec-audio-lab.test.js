import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { Readable, Writable } from "node:stream";
import mongoose from "mongoose";
import { canUseAudioLab, parseAudioRange, registerTecAudioLab } from "../src/tec-audio-lab.js";

test("audio byte ranges support iPhone seeks and reject malformed or out-of-bounds ranges", () => {
  assert.equal(parseAudioRange(undefined, 100), null);
  assert.deepEqual(parseAudioRange("bytes=0-1", 100), { start: 0, end: 1 });
  assert.deepEqual(parseAudioRange("bytes=10-", 100), { start: 10, end: 99 });
  assert.deepEqual(parseAudioRange("bytes=-5", 100), { start: 95, end: 99 });
  for (const value of ["bytes=100-", "bytes=5-2", "bytes=-0", "bytes=1-2,4-5", "bytes=-", "no"]) assert.throws(() => parseAudioRange(value, 100));
  assert.equal(canUseAudioLab({ email: "OtherAdmin@example.com" }, ["owner@example.com"]), false);
});

test("private audio upload, full playback, seeking and deletion round-trip without exposing it to upline or other admins", async () => {
  const files = new Map(), ownerId = new mongoose.Types.ObjectId();
  const bucket = {
    find(filter) {
      const rows = [...files.values()].filter(file => (!filter._id || String(file._id) === String(filter._id)) && file.metadata.ownerUserId === filter["metadata.ownerUserId"] && file.metadata.testOnly);
      return { sort() { return this; }, limit() { return this; }, async toArray() { return rows; } };
    },
    openUploadStream(_name, { metadata }) {
      const id = new mongoose.Types.ObjectId(), parts = [];
      const upload = new Writable({ write(chunk, _encoding, done) { parts.push(chunk); done(); }, final(done) {
        const bytes = Buffer.concat(parts); files.set(String(id), { _id: id, length: bytes.length, metadata, uploadDate: new Date(), bytes }); done();
      } });
      upload.id = id; upload.abort = async () => { files.delete(String(id)); }; return upload;
    },
    openDownloadStream(id, range = {}) { const bytes = files.get(String(id)).bytes; return Readable.from(bytes.subarray(range.start || 0, range.end ?? bytes.length)); },
    async delete(id) { files.delete(String(id)); }
  };
  const app = express();
  registerTecAudioLab(app, { ObjectId: mongoose.Types.ObjectId, getBucket: () => bucket,
    emails: ["owner@example.com"], analyses: {}, requireAdmin: async (req, res) => {
      const role = req.get("X-Test-Role");
      if (role !== "owner" && role !== "other-admin") { res.status(role ? 403 : 401).json({ error: "not admin" }); return null; }
      return { user: { _id: ownerId, email: role === "owner" ? "owner@example.com" : "other@example.com" } };
    } });
  const server = await new Promise(resolve => { const instance = app.listen(0, "127.0.0.1", () => resolve(instance)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const headers = { "X-Test-Role": "owner", "X-TEC-Audio-Test": "own-test-only", "Content-Type": "audio/wav" };
  try {
    assert.equal((await fetch(`${base}/api/control/audio-lab`)).status, 401);
    assert.equal((await fetch(`${base}/api/control/audio-lab`, { headers: { "X-Test-Role": "david" } })).status, 403);
    assert.deepEqual(await (await fetch(`${base}/api/control/audio-lab`, { headers: { "X-Test-Role": "other-admin" } })).json(), { enabled: false });
    const bytes = Buffer.from("RIFF-fictitious-audio-bytes-for-server-roundtrip");
    const created = await fetch(`${base}/api/control/audio-lab?label=Fictional`, { method: "POST", headers, body: bytes });
    assert.equal(created.status, 201); const { id } = await created.json();
    const url = `${base}/api/control/audio-lab/${id}/audio`;
    const all = await fetch(url, { headers });
    assert.equal(all.status, 200); assert.match(all.headers.get("cache-control"), /no-store/);
    assert.deepEqual(Buffer.from(await all.arrayBuffer()), bytes);
    const range = await fetch(url, { headers: { ...headers, Range: "bytes=0-3" } });
    assert.equal(range.status, 206); assert.equal(range.headers.get("content-range"), `bytes 0-3/${bytes.length}`);
    assert.deepEqual(Buffer.from(await range.arrayBuffer()), bytes.subarray(0, 4));
    assert.equal((await fetch(url, { headers: { ...headers, Range: "bytes=999-" } })).status, 416);
    assert.equal((await fetch(url, { headers: { "X-Test-Role": "david" } })).status, 403);
    assert.equal((await fetch(url, { headers: { "X-Test-Role": "other-admin" } })).status, 403);
    assert.equal((await fetch(url)).status, 401);
    assert.equal((await fetch(`${base}/api/control/audio-lab`, { method: "POST", headers: { ...headers, "Sec-Fetch-Site": "cross-site" }, body: bytes })).status, 403);
    assert.equal((await fetch(`${base}/api/control/audio-lab`, { method: "POST", headers: { ...headers, "Content-Type": "text/html" }, body: bytes })).status, 415);
    assert.equal((await fetch(`${base}/api/control/audio-lab/${id}`, { method: "DELETE", headers })).status, 200);
    assert.equal(files.size, 0); assert.equal((await fetch(url, { headers })).status, 404);
  } finally { await new Promise(resolve => server.close(resolve)); }
});
