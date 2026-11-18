import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import express from 'express';
import { registerCoachClosingSupervisor } from '../src/coach-closing-supervisor.js';
import { createCoachRequestGuard } from '../src/tec-security.js';

async function fixture(t) {
  const records = new Map([['seller-1', {}], ['seller-2', {}]]);
  const requireActive = async (req, res) => {
    const id = req.get('X-Fixture-User');
    if (records.has(id)) return { user: { _id: id, teamOwnerUserId: 'shared' } };
    res.status(401).json({ error: 'Sin sesión activa' });
  };
  let fail = false;
  const users = {
    findById(id) {
      return { select(fields) {
        assert.equal(fields, '+coachClosingSupervisor');
        return { async lean() { return records.get(id); } };
      } };
    },
    async updateOne({ _id }, { $set }) {
      if (fail) throw new Error('storage unavailable');
      Object.assign(records.get(_id), $set);
      return { matchedCount: 1 };
    }
  };
  const app = express();
  app.use(express.json());
  app.use('/api/coach', createCoachRequestGuard({ requireActive }));
  registerCoachClosingSupervisor(app, { requireActive, users });
  const server = await new Promise(resolve => {
    const instance = app.listen(0, '127.0.0.1', () => resolve(instance));
  });
  t.after(() => { server.closeAllConnections(); server.close(); });

  function send({ body, user = 'seller-1', account = user, origin = 'https://totalelitecoaching.com' } = {}) {
    return fetch('http://127.0.0.1:' + server.address().port + '/api/coach/closing-supervisor', {
      method: body ? 'PUT' : 'GET',
      headers: { 'Content-Type': 'application/json', 'X-Fixture-User': user, 'X-TEC-Account-ID': account, Origin: origin },
      body: body ? JSON.stringify(body) : undefined
    });
  }
  return { send, fail: () => { fail = true; } };
}

test('closing card stores an editable dialable supervisor separately for each seller', async t => {
  const { send } = await fixture(t);
  assert.deepEqual(await (await send()).json(), { contact: null });
  const saved = await send({ body: { name: '  José   Pérez García ', phone: '(312) 555-0101' } });
  assert.equal(saved.status, 200);
  assert.deepEqual(await saved.json(), { contact: { name: 'José Pérez García', phone: '+13125550101' } });
  const read = await send();
  assert.equal(read.headers.get('cache-control'), 'private, no-store');
  assert.match(read.headers.get('vary'), /Cookie/);
  assert.deepEqual(await read.json(), { contact: { name: 'José Pérez García', phone: '+13125550101' } });
  assert.deepEqual(await (await send({ user: 'seller-2' })).json(), { contact: null });

  const edited = await send({ body: { name: 'María López', phone: '+52 55 1234 5678' } });
  assert.equal(edited.status, 200);
  assert.deepEqual(await (await send()).json(), { contact: { name: 'María López', phone: '+525512345678' } });
});

test('closing supervisor reads and writes require the active account and safe contact fields', async t => {
  const { send, fail } = await fixture(t);
  const valid = { name: 'María López', phone: '+13125550101' };
  assert.equal((await send({ user: 'signed-out' })).status, 401);
  assert.equal((await send({ account: 'seller-2' })).status, 403);
  assert.equal((await send({ account: 'seller-2', body: valid })).status, 403);
  assert.equal((await send({ body: valid, origin: 'https://foreign.example' })).status, 403);
  for (const body of [
    { ...valid, name: ' ' }, { ...valid, name: 'Name\nTwo' }, { ...valid, name: {} },
    { ...valid, phone: '+1 5550000' }, { ...valid, phone: '+0525512345678' }, { ...valid, phone: '555-0101;ext=99' },
    { ...valid, phone: '+1 312-555-0101,911' }, { ...valid, phone: 'javascript:alert(1)' }
  ]) assert.equal((await send({ body })).status, 400);
  fail();
  assert.equal((await send({ body: valid })).status, 503);
  assert.deepEqual(await (await send()).json(), { contact: null });
});

test('closing supervisor belongs to the order review card and enables calling only on click', () => {
  const html = fs.readFileSync(new URL('../private/coach-app.html', import.meta.url), 'utf8');
  const card = html.match(/<article\s+id="paso-cierre"[\s\S]*?<\/article>/)[0];
  assert.match(card, /data-closing-supervisor.*hidden/);
  assert.match(card, /<a[^>]*data-supervisor-call hidden>/);
  assert.match(card, /data-order-calc-toggle/);
  assert.equal((html.match(/data-closing-supervisor/g) || []).length, 1);
});
