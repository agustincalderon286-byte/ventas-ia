import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { registerCoachAnnouncementRead } from '../src/coach-announcement-read.js';
import { createCoachRequestGuard } from '../src/tec-security.js';
const a = 'a'.repeat(24), b = 'b'.repeat(24), globalId = 'c'.repeat(24);
async function fixture(t) {
  const saved = new Map(); let failed = false, writes = 0;
  const authorize = async (req, res) => {
    const id = req.get('X-Fixture-User'); if (!['alice', 'bob'].includes(id)) { res.status(401).json({ error: 'Sin sesión' }); return null; }
    return { user: { _id: id } };
  };
  const app = express(); app.use(express.json()); app.use('/api/coach', createCoachRequestGuard({ requireActive: authorize }));
  registerCoachAnnouncementRead(app, { requireActive: authorize,
    getOverview: async user => ({ announcements: [user._id === 'alice' ? a : b, globalId].map(id => ({ id, read: saved.has(`${user._id}:${id}`) })), unread: { direct: 3, support: 2 } }),
    receipts: { async updateOne(filter, update, options) { if (failed) throw new Error('Database unavailable'); writes++; assert.equal(options.upsert, true); saved.set(`${filter.userId}:${filter.announcementId}`, update.$set.readAt); } }
  });
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(() => { server.closeAllConnections(); server.close(); });
  const send = (body, user = 'alice', origin = 'https://totalelitecoaching.com') => fetch(`http://127.0.0.1:${server.address().port}/api/coach/announcements/read`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Fixture-User': user, Origin: origin }, body: JSON.stringify(body)
  });
  return { saved, send, get writes() { return writes; }, fail() { failed = true; } };
}
test('read receipts persist for exactly the loaded bulletins and the signed-in account; retries are idempotent', async t => {
  const f = await fixture(t); let response = await f.send({ accountId: 'alice', ids: [a] });
  assert.equal(response.status, 200); assert.deepEqual((await response.json()).readIds, [a]);
  assert.equal(response.headers.get('cache-control'), 'private, no-store');
  assert.ok(f.saved.has(`alice:${a}`)); assert.equal(f.saved.has(`alice:${globalId}`), false); assert.equal(f.saved.has(`bob:${a}`), false);
  response = await f.send({ accountId: 'alice', ids: [a] }); assert.equal(response.status, 200); assert.equal(f.writes, 1);
  response = await f.send({ accountId: 'bob', ids: [globalId] }, 'bob'); assert.equal(response.status, 200); assert.equal(f.saved.has(`alice:${globalId}`), false);
});
test('foreign bulletins, stale account tabs, unsigned users and foreign-origin writes cannot alter read state', async t => {
  const f = await fixture(t);
  assert.equal((await f.send({ accountId: 'alice', ids: [a, b] })).status, 403);
  assert.equal((await f.send({ accountId: 'alice', ids: [globalId] }, 'bob')).status, 403);
  assert.equal((await f.send({ accountId: 'alice', ids: [a] }, 'unsigned')).status, 401);
  assert.equal((await f.send({ accountId: 'alice', ids: [a] }, 'alice', 'https://foreign.example')).status, 403);
  assert.equal(f.saved.size, 0);
});
test('invalid receipt lists and storage failures never claim successful reading', async t => {
  const f = await fixture(t);
  for (const ids of [null, a, {}, [a, a], ['bad-id'], [3], Array(21).fill(a)]) assert.equal((await f.send({ accountId: 'alice', ids })).status, 400);
  f.fail(); const response = await f.send({ accountId: 'alice', ids: [a] }); assert.equal(response.status, 503); assert.equal(f.saved.size, 0);
  assert.equal((await response.json()).readIds, undefined);
});
