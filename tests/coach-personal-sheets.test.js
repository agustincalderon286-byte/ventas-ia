import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createHash } from 'node:crypto';
import mongoose from 'mongoose';

process.env.NODE_ENV = 'test';
const { app } = await import('../server.js');
const id = () => new mongoose.Types.ObjectId();
const base = '/api/coach/program-4-in-14';
const ownerId = id();
const users = ['seat', 'leader', 'owner'].map((accountType, i) => ({
  _id: accountType === 'owner' ? ownerId : id(), accountType, teamOwnerUserId: ownerId,
  name: 'Vendedor ' + i, email: 'vendedor' + i + '@example.test', subscriptionActive: true
}));
const tokens = users.map((_, i) => String.fromCharCode(65 + i).repeat(43));
const hash = token => createHash('sha256').update(token).digest('hex');

// Exercise the actual Express routes, session validation, Mongoose hydration,
// validation, casting and document writes. Only the database transport is local.
function memoryCollection(t, model, initial = []) {
  const rows = [...initial];
  const copy = doc => mongoose.mongo.BSON.deserialize(mongoose.mongo.BSON.serialize(doc));
  const matches = (doc, query) => Object.entries(query).every(([key, condition]) => {
    const values = key === 'referrals.phone' ? doc.referrals.map(row => row.phone) : [doc[key] ?? null];
    const options = condition?.$in || [condition];
    return values.some(value => options.some(option => String(value) === String(option)));
  });
  const selected = query => rows.filter(doc => matches(doc, query));
  const collection = model.collection;
  t.mock.method(collection, 'findOne', async query => {
    const doc = selected(query)[0];
    return doc ? copy(doc) : null;
  });
  t.mock.method(collection, 'find', (query, options = {}) => ({
    toArray: async () => selected(query).sort((a, b) => b.updatedAt - a.updatedAt)
      .slice(0, options.limit || rows.length).map(copy)
  }));
  t.mock.method(collection, 'distinct', async (key, query) => {
    assert.equal(key, 'referrals.phone');
    return [...new Set(selected(query).flatMap(doc => doc.referrals.map(row => row.phone)))];
  });
  t.mock.method(collection, 'insertOne', async doc => {
    if (doc.offlineRequestId && selected({ ownerUserId: doc.ownerUserId, offlineRequestId: doc.offlineRequestId }).length) {
      throw Object.assign(new Error('duplicate key'), { code: 11000 });
    }
    rows.push(copy(doc));
    return { acknowledged: true, insertedId: doc._id };
  });
  t.mock.method(collection, 'updateOne', async (filter, update) => {
    const doc = selected(filter)[0];
    if (!doc) return { matchedCount: 0, modifiedCount: 0 };
    for (const [path, value] of Object.entries(update.$set || {})) {
      const keys = path.split('.');
      const last = keys.pop();
      keys.reduce((obj, key) => obj[key], doc)[last] = value;
    }
    return { acknowledged: true, matchedCount: 1, modifiedCount: 1 };
  });
  return rows;
}

async function fixture(t) {
  const { CoachSession, CoachProgramSheet, CoachPersonalProgramSheet, CoachLeadInbox, CoachCrmRecord } = mongoose.models;
  assert.notEqual(CoachPersonalProgramSheet.collection.name, CoachProgramSheet.collection.name);
  assert.ok(CoachPersonalProgramSheet.schema.indexes().some(([keys, opts]) =>
    keys.ownerUserId === 1 && keys.offlineRequestId === 1 && opts.unique && opts.partialFilterExpression));
  const sessionModes = users.map(() => 'direct');
  t.mock.method(CoachSession, 'findOne', query => ({
    populate: async () => {
      const index = tokens.findIndex(token => hash(token) === query.tokenHash);
      return index < 0 ? null : { userId: users[index], accessMode: sessionModes[index], save: async () => {} };
    }
  }));
  const historical = [users[0]._id, users[1]._id, null].map((generatedByUserId, i) =>
    new CoachProgramSheet({
      ownerUserId: ownerId, generatedByUserId, hostName: 'Histórico ' + i, hostPhone: '3125550110',
      referrals: [{ fullName: 'Referido histórico ' + i, phone: '312555011' + i }],
      updatedAt: new Date('2020-01-01')
    }).toObject());
  memoryCollection(t, CoachProgramSheet, historical);
  const captured = memoryCollection(t, CoachPersonalProgramSheet);
  const leads = memoryCollection(t, CoachLeadInbox);
  const crm = memoryCollection(t, CoachCrmRecord);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  async function request(seller, url, { method = 'GET', body, requestId, visiting = false, accountHeader } = {}) {
    const headers = { Origin: 'https://totalelitecoaching.com', 'Content-Type': 'application/json' };
    if (seller !== null) headers.Cookie = 'agustin_coach_session=' + tokens[seller] +
      (visiting ? '; agustin_coach_session_return=' + tokens[2] : '');
    if (requestId) headers['X-TEC-Request-ID'] = requestId;
    if (accountHeader) headers['X-TEC-Account-ID'] = accountHeader;
    const res = await fetch('http://127.0.0.1:' + server.address().port + url, {
      method, headers, ...(body ? { body: JSON.stringify(body) } : {})
    });
    return { status: res.status, data: await res.json() };
  }
  return { request, captured, historical, leads, crm, sessionModes };
}

const who = seller => String(users[seller]._id);
const own = seller => base + '?scope=mine&accountId=' + who(seller);
const input = (seller, override = {}) => ({
  captureMode: 'tec_sheet', accountId: who(seller), activeCrmRecordId: String(id()),
  hostName: 'Anfitriona Sintética', hostPhone: '312–555–0100',
  referrals: [{ fullName: 'Referida Sintética', phone: '312-555-0140', address: 'Avenida de Prueba 1' }],
  ...override
});

test('own form saves contacts atomically, survives reload, and keeps team inbox/CRM and legacy sheets untouched', async t => {
  const f = await fixture(t);
  const body = input(0);
  body.ownerUserId = who(1);
  body.generatedByUserId = who(1);
  body.referrals[0].createdLeadId = String(id());
  const saved = await f.request(0, base, { method: 'POST', body, requestId: 'test-first-save' });
  assert.equal(saved.status, 200);
  assert.equal(saved.data.savedReferralCount, 1);
  assert.equal(saved.data.createdLeadCount, 0);
  assert.equal(saved.data.duplicateCount, 0);
  assert.equal(saved.data.delivery.attempted, false);
  assert.equal(saved.data.sheet.generatedByUserId, who(0));
  assert.equal(saved.data.sheet.ownerUserId, who(0));
  assert.equal(saved.data.sheet.linkedCrmRecordId, '');
  assert.equal(saved.data.sheet.referrals[0].createdLeadId, '');
  assert.equal(f.captured.length, 1);
  assert.equal(f.leads.length + f.crm.length, 0);
  assert.equal(mongoose.models.CoachCrmRecord.collection.findOne.mock.callCount(), 0);
  const sheetId = saved.data.sheet.id;
  const list = (await f.request(0, own(0))).data.sheets;
  assert.deepEqual(list.map(s => s.hostName), ['Anfitriona Sintética', 'Histórico 0']);
  assert.equal(list[0].referrals[0].phone, '3125550140');
  for (const seller of [1, 2]) {
    assert.deepEqual((await f.request(seller, own(seller))).data.sheets.map(s => s.hostName), ['Histórico ' + seller]);
    const team = await f.request(seller, base);
    assert.equal(team.status, 200);
    assert.deepEqual(team.data.sheets.map(s => s.hostName).sort(), ['Histórico 0', 'Histórico 1', 'Histórico 2']);
    assert.deepEqual((await f.request(seller, '/api/coach/leads')).data.leads, []);
    assert.equal((await f.request(seller, '/api/coach/leads/' + sheetId, { method: 'PATCH', body: { notes: 'synthetic' } })).status, 404);
    for (const patch of [{ activate: true }, { activate: true, accountId: who(seller) }]) {
      const denied = await f.request(seller, base + '/' + sheetId + '/referrals/0/instant-call', { method: 'PATCH', body: patch });
      assert.equal(denied.status, 404);
      assert.equal(denied.data.sheet, undefined);
    }
  }
  assert.equal(f.captured[0].referrals[0].instantCallStatus, undefined);
  const patch = await f.request(0, base + '/' + sheetId + '/referrals/0/instant-call', {
    method: 'PATCH', body: { accountId: who(0), instantCallStatus: 'cita_lograda', appointmentDetails: 'Lunes' }
  });
  assert.equal(patch.status, 200);
  const updated = (await f.request(0, own(0))).data.sheets[0];
  assert.equal(updated.referrals[0].instantCallStatus, 'cita_lograda');
  assert.equal(updated.referrals[0].appointmentDetails, 'Lunes');
  assert.equal(f.crm.length + f.leads.length, 0);
});

test('personal save retries and duplicate warnings are account-scoped, even within the same team', async t => {
  const f = await fixture(t);
  const requestId = 'reused-client-id';
  const save = (seller, override, key = requestId) =>
    f.request(seller, base, { method: 'POST', body: input(seller, override), requestId: key });
  const [first, second] = await Promise.all([save(0), save(0)]);
  assert.equal(first.status, 200);
  assert.equal(second.status, 200);
  assert.equal(first.data.sheet.id, second.data.sheet.id);
  assert.deepEqual([first.data.duplicateRequest, second.data.duplicateRequest].sort(), [false, true]);
  assert.equal(f.captured.length, 1);
  assert.equal((await save(0, { hostName: 'Distinto' })).status, 409);
  assert.equal((await save(1)).data.duplicateCount, 0);
  assert.equal(f.captured.length, 2);
  assert.equal((await save(0, {}, 'another-sheet-id')).data.duplicateCount, 1);
  const previous = phone => ({ referrals: [{ fullName: 'Prueba', phone }] });
  assert.equal((await save(0, previous('3125550110'), 'old-own-contact')).data.duplicateCount, 1);
  assert.equal((await save(0, previous('3125550111'), 'old-other-contact')).data.duplicateCount, 0);
  assert.equal(f.captured.length, 5);
});

test('GET, POST, photo and personal PATCH require own active session and matching claimed account', async t => {
  const f = await fixture(t);
  const paths = [
    [own(0), 'GET', undefined], [base, 'POST', input(0)],
    [base + '/photo', 'POST', { photo: '', accountId: who(0) }],
    [base + '/' + id() + '/referrals/0/instant-call', 'PATCH', { accountId: who(0), activate: true }]
  ];
  for (const [url, method, body] of paths) {
    assert.equal((await f.request(null, url, { method, body })).status, 401);
    assert.equal((await f.request(1, url, { method, body })).status, 403);
    assert.equal((await f.request(0, url, { method, body, accountHeader: who(1) })).status, 403);
    assert.equal((await f.request(0, url, { method, body, visiting: true })).status, 403);
    for (const mode of ['team_visit', undefined]) {
      f.sessionModes[0] = mode;
      assert.equal((await f.request(0, url, { method, body })).status, 403);
    }
    f.sessionModes[0] = 'direct';
  }
  f.sessionModes[0] = undefined;
  assert.equal((await f.request(0, base)).status, 200);
  assert.equal(f.captured.length + f.leads.length + f.crm.length, 0);
});
