import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import mongoose from 'mongoose';
import { createCoachTeamChatModels, registerCoachTeamChat } from '../src/coach-team-chat.js';
import { createCoachRequestGuard } from '../src/tec-security.js';

const id = n => n.toString(16).padStart(24, '0');
const alice = id(1001), bob = id(1002), carol = id(1003);
const token = n => `12345678-1234-4123-8123-${n.toString(16).padStart(12, '0')}`;
const users = { alice: { _id: alice, name: 'Alice', team: 'one' }, bob: { _id: bob, name: 'Bob', team: 'one' }, carol: { _id: carol, name: 'Carol', team: 'two' }, unassigned: { _id: id(1004), name: 'No team', team: '' } };
function matches(row, filter) {
  return Object.entries(filter).every(([key, value]) => {
    if (value && typeof value === 'object') return Object.entries(value).every(([op, bound]) => op === '$ne' ? String(row[key]) !== String(bound) : op === '$gt' ? String(row[key]) > String(bound) : op === '$lt' ? String(row[key]) < String(bound) : false);
    return String(row[key]) === String(value);
  });
}
async function fixture(t) {
  const rows = [], receipts = new Map();
  let sequence = 0, failed = '', duplicateReceipt = false, at = 1800000000000;
  const check = operation => { if (failed === operation) throw new Error('fixture storage unavailable'); };
  const messages = {
    findOne(filter) { return { async lean() { check('load'); return rows.find(row => matches(row, filter)) || null; } }; },
    find(filter) {
      let limit = Infinity;
      return { sort(sort) { assert.deepEqual(sort, { _id: -1 }); return this; }, limit(n) { limit = n; return this; }, async lean() { check('load'); return rows.filter(row => matches(row, filter)).sort((a, b) => b._id.localeCompare(a._id)).slice(0, limit); } };
    },
    async countDocuments(filter) { check('count'); return rows.filter(row => matches(row, filter)).length; },
    async create(value) {
      check('create');
      if (rows.some(row => row.roomId === value.roomId && row.senderId === value.senderId && row.clientMessageId === value.clientMessageId)) throw Object.assign(new Error('duplicate'), { code: 11000 });
      const row = { ...value, _id: id(++sequence) }; rows.push(row); return row;
    }
  };
  const readers = {
    findOne(filter) { return { async lean() { check('receipt-load'); return receipts.get(`${filter.roomId}:${filter.accountId}`) || null; } }; },
    async updateOne(filter, update, options) {
      check('receipt');
      if (duplicateReceipt && options.upsert) { duplicateReceipt = false; throw Object.assign(new Error('concurrent upsert'), { code: 11000 }); }
      const key = `${filter.roomId}:${filter.accountId}`, previous = receipts.get(key);
      const lastReadMessageId = previous?.lastReadMessageId > update.$max.lastReadMessageId ? previous.lastReadMessageId : update.$max.lastReadMessageId;
      receipts.set(key, { ...filter, lastReadMessageId });
    }
  };
  const authorize = async (req, res) => {
    const user = users[req.get('X-Fixture-User')];
    if (!user) { res.status(401).json({ error: 'Sin sesión activa' }); return null; }
    return { user };
  };
  const app = express(); app.use(express.json());
  app.use('/api/coach', createCoachRequestGuard({ requireActive: authorize }));
  registerCoachTeamChat(app, { requireActive: authorize, getTeamKey: user => user.team, messages, readers, now: () => at });
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(() => { server.closeAllConnections(); server.close(); });
  async function request(path = '', { user = 'alice', body, origin = 'https://totalelitecoaching.com', headers = {} } = {}) {
    const response = await fetch(`http://127.0.0.1:${server.address().port}/api/coach/team-chat${path}`, { method: body === undefined ? 'GET' : 'POST', headers: { 'X-Fixture-User': user, Origin: origin, ...(body === undefined ? {} : { 'Content-Type': 'application/json' }), ...headers }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
    const json = await response.json(); return { status: response.status, json, headers: response.headers };
  }
  const roomId = (await request()).json.roomId;
  const roomTwo = (await request('', { user: 'carol' })).json.roomId;
  const send = (n, body = 'Hola equipo', options = {}) => request('/messages', { body: { accountId: users[options.user || 'alice']?._id, roomId: options.user === 'carol' ? roomTwo : roomId, clientMessageId: token(n), body, ...options.fields }, ...options });
  const read = (lastMessageId, options = {}) => request('/read', { body: { accountId: users[options.user || 'alice']?._id, roomId, lastMessageId, ...options.fields }, ...options });
  return { request, send, read, rows, receipts, roomId, roomTwo, fail(operation) { failed = operation; }, raceReceipt() { duplicateReceipt = true; }, advance() { at += 60001; } };
}

test('model factory creates private indexed collections with bounded message fields', () => {
  const models = createCoachTeamChatModels(mongoose);
  assert.equal(createCoachTeamChatModels(mongoose).messages, models.messages);
  assert.ok(models.messages.schema.indexes().some(([keys, options]) => keys.roomId === 1 && keys.senderId === 1 && keys.clientMessageId === 1 && options.unique));
  assert.ok(models.messages.schema.indexes().some(([keys]) => keys.roomId === 1 && keys._id === -1));
  assert.ok(models.readers.schema.indexes().some(([keys, options]) => keys.roomId === 1 && keys.accountId === 1 && options.unique));
  assert.equal(models.messages.schema.path('body').options.maxlength, 2000);
  assert.equal(models.readers.schema.path('lastReadMessageId').instance, 'ObjectId');
});

test('room scope and authenticated sender survive forged fields; own messages never count unread', async t => {
  const f = await fixture(t);
  const sent = await f.send(1, '  Reporte de ventas  ', { fields: { senderId: carol, senderName: 'Falso' } });
  assert.equal(sent.status, 201); assert.deepEqual(Object.keys(sent.json.message), ['id', 'senderId', 'senderName', 'body', 'createdAt']);
  assert.equal(sent.json.message.senderId, alice); assert.equal(sent.json.message.senderName, 'Alice'); assert.equal(sent.json.message.body, 'Reporte de ventas');
  assert.equal((await f.request()).json.unreadCount, 0);
  const teammate = await f.request('', { user: 'bob' });
  assert.equal(teammate.json.roomId, f.roomId); assert.equal(teammate.json.unreadCount, 1);
  assert.equal(teammate.headers.get('cache-control'), 'private, no-store'); assert.match(teammate.headers.get('vary'), /Cookie/);
  const other = await f.request('', { user: 'carol' }); assert.notEqual(other.json.roomId, f.roomId); assert.deepEqual(other.json.messages, []); assert.equal(other.json.unreadCount, 0);
});

test('all routes reject unsigned, stale account, foreign room, and cross-origin writes', async t => {
  const f = await fixture(t);
  assert.equal((await f.request('', { user: 'unsigned' })).status, 401);
  assert.equal((await f.request('', { user: 'unassigned' })).status, 403);
  assert.equal((await f.request('', { headers: { 'X-TEC-Account-ID': bob } })).status, 403);
  assert.equal((await f.request(`?roomId=${encodeURIComponent(f.roomTwo)}`)).status, 403);
  for (const path of ['/messages', '/read']) {
    const base = { accountId: alice, roomId: f.roomId, clientMessageId: token(1), body: 'Hello', lastMessageId: id(1) };
    assert.equal((await f.request(path, { user: 'unsigned', body: base })).status, 401);
    assert.equal((await f.request(path, { body: { ...base, accountId: bob } })).status, 403);
    assert.equal((await f.request(path, { body: { ...base, roomId: f.roomTwo } })).status, 403);
    assert.equal((await f.request(path, { body: base, headers: { 'X-TEC-Account-ID': bob } })).status, 403);
    assert.equal((await f.request(path, { body: base, origin: 'https://foreign.example' })).status, 403);
  }
  assert.equal(f.rows.length, 0); assert.equal(f.receipts.size, 0);
});

test('message UUID retries and concurrent sends are idempotent per sender and room', async t => {
  const f = await fixture(t);
  const responses = await Promise.all([f.send(1), f.send(1)]);
  assert.ok(responses.every(response => [200, 201].includes(response.status)));
  assert.equal(responses[0].json.message.id, responses[1].json.message.id); assert.equal(f.rows.length, 1);
  assert.equal((await f.send(1, 'Edited retry')).status, 409);
  assert.equal((await f.send(1, 'Hola equipo', { user: 'bob' })).status, 201);
  assert.equal((await f.send(1, 'Hola equipo', { user: 'carol' })).status, 201);
  assert.equal(f.rows.length, 3);
});

test('invalid message and pagination inputs cannot write or select another team', async t => {
  const f = await fixture(t);
  for (const body of ['', ' ', 'x'.repeat(2001), {}, null]) assert.equal((await f.send(1, body)).status, 400);
  for (const clientMessageId of ['', 'bad-id', {}, '12345678-1234-0123-8123-123456789012']) assert.equal((await f.send(1, 'Text', { fields: { clientMessageId } })).status, 400);
  assert.equal((await f.send(2, 'x'.repeat(2000))).status, 201);
  for (const query of ['?before=invalid', '?before[x]=1']) assert.equal((await f.request(query)).status, 400);
  const foreign = await f.send(3, 'Other team', { user: 'carol' });
  assert.equal((await f.request(`?before=${foreign.json.message.id}`)).status, 404);
  assert.equal((await f.read(foreign.json.message.id)).status, 404);
  assert.equal((await f.read(id(999))).status, 404);
  assert.equal((await f.read('invalid')).status, 400); assert.equal(f.receipts.size, 0);
});

test('pagination returns newest fifty ascending then older messages without gaps', async t => {
  const f = await fixture(t);
  for (let n = 1; n <= 55; n++) { await f.send(n, `Message ${n}`, { user: 'bob' }); if (n % 25 === 0) f.advance(); }
  const first = await f.request(); assert.equal(first.json.messages.length, 50); assert.equal(first.json.hasMore, true); assert.equal(first.json.unreadCount, 55);
  assert.equal(first.json.messages[0].body, 'Message 6'); assert.equal(first.json.messages.at(-1).body, 'Message 55');
  const second = await f.request(`?before=${first.json.messages[0].id}`); assert.deepEqual(second.json.messages.map(row => row.body), ['Message 1', 'Message 2', 'Message 3', 'Message 4', 'Message 5']); assert.equal(second.json.hasMore, false);
});

test('receipt cursor persists per account+team and older acknowledgments cannot regress it', async t => {
  const f = await fixture(t);
  const first = (await f.send(1, 'One', { user: 'bob' })).json.message.id;
  const second = (await f.send(2, 'Two', { user: 'bob' })).json.message.id;
  const third = (await f.send(3, 'Three', { user: 'bob' })).json.message.id;
  f.raceReceipt(); let receipt = await f.read(second); assert.equal(receipt.status, 200); assert.equal(receipt.json.lastMessageId, second); assert.equal(receipt.json.unreadCount, 1);
  receipt = await f.read(first); assert.equal(receipt.json.lastMessageId, second); assert.equal(receipt.json.unreadCount, 1);
  await Promise.all([f.read(third), f.read(first)]);
  assert.equal((await f.request()).json.unreadCount, 0);
  assert.equal((await f.read(first)).json.lastMessageId, third);
  assert.equal((await f.request('', { user: 'bob' })).json.unreadCount, 0);
  assert.equal((await f.request('', { user: 'carol' })).json.unreadCount, 0);
  assert.equal(f.receipts.size, 1);
});

test('failed storage never claims delivery, reading, or fresh message data', async t => {
  const f = await fixture(t);
  f.fail('create'); let response = await f.send(1); assert.equal(response.status, 503); assert.equal(response.json.message, undefined); assert.equal(f.rows.length, 0);
  f.fail(''); const messageId = (await f.send(1)).json.message.id;
  f.fail('receipt'); response = await f.read(messageId); assert.equal(response.status, 503); assert.equal(response.json.lastMessageId, undefined); assert.equal(f.receipts.size, 0);
  f.fail('load'); response = await f.request(); assert.equal(response.status, 503); assert.equal(response.json.messages, undefined);
  f.fail('count'); assert.equal((await f.request()).status, 503);
});

test('rate limits are per account, retry existing sends, and expire after one minute', async t => {
  const f = await fixture(t);
  for (let n = 1; n <= 30; n++) assert.equal((await f.send(n)).status, 201);
  let response = await f.send(31); assert.equal(response.status, 429); assert.equal(response.headers.get('retry-after'), '60');
  assert.equal((await f.send(1)).status, 200); assert.equal((await f.send(31, 'Bob', { user: 'bob' })).status, 201);
  f.advance(); assert.equal((await f.send(31)).status, 201);
});
