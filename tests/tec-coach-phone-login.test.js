import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFileSync } from 'node:fs';
import mongoose from 'mongoose';
import express from 'express';
import { parseCoachLogin } from '../src/tec-coach-login.js';
import { createCoachRequestGuard } from '../src/tec-security.js';

const source = readFileSync(new URL('../server.js', import.meta.url), 'utf8');
const client = readFileSync(new URL('../public/coach/app.js', import.meta.url), 'utf8');
function fn(text, name) {
  const start = text.indexOf('function ' + name + '(');
  const end = /\n(?:async )?function \w+\(/g; end.lastIndex = start + 10;
  return (text.slice(start - 6, start) === 'async ' ? 'async ' : '') + text.slice(start, end.exec(text)?.index || text.length);
}
test('US phone usernames normalize punctuation and country code, without treating them as email', () => {
  for (const value of ['8475550123', '(847) 555-0123', '1 847 555 0123', '+1 (847) 555-0123']) assert.deepEqual(parseCoachLogin(value), { field: 'username', value: '8475550123' });
  assert.deepEqual(parseCoachLogin(' PERSON@Example.test '), { field: 'email', value: 'person@example.test' });
  for (const value of ['', 'phone8475550123', '123', '+528475550123', '0000000000', { $ne: null }, null, 'a'.repeat(255)]) assert.equal(parseCoachLogin(value), null);
});
test('Coach schema permits a phone-only seat, requires an identity, and preserves ordinary email validation', async () => {
  const start = source.indexOf('const coachUserSchema =');
  const end = source.indexOf('const coachDistributorProfileSchema', start);
  const schema = vm.runInNewContext(source.slice(start, end) + '\ncoachUserSchema;', { mongoose });
  const User = mongoose.models.PhoneLoginFixture || mongoose.model('PhoneLoginFixture', schema);
  const base = { name: 'Provisional', passwordHash: 'hash', passwordSalt: 'salt', accountType: 'seat' };
  const phone = new User({ ...base, username: '8475550123' });
  await phone.validate(); assert.equal(phone.email, undefined);
  await assert.rejects(new User(base).validate());
  await assert.rejects(new User({ ...base, username: 'invalid' }).validate());
  await new User({ ...base, email: 'existing@example.test' }).validate();
});
test('actual login handler accepts phone or email without changing permissions or bypassing password checks', async t => {
  const marker = 'app.post("/api/coach/login", async (req, res) => {';
  const start = source.indexOf(marker) + marker.length;
  const body = source.slice(start, source.indexOf('\napp.post("/api/coach/logout"', start)).replace(/\n\}\);\s*$/, '');
  let sessionCount = 0, query, throttledIdentity;
  const user = { _id: 'seat-A', async save() {} };
  const handler = vm.runInNewContext(`async(req,res)=>{${body}}`, {
    parseCoachLogin, obtenerCoachLoginThrottle: (_req, id) => { throttledIdentity = id; return { blocked: false }; },
    registrarCoachLoginFallido: () => ({ blocked: false }), limpiarCoachLoginFallidos: () => {},
    CoachUser: { findOne: async value => { query = value; return user; } },
    verificarPasswordSeguro: async password => password === 'fixture-password', asegurarCoachUserBase: async value => value,
    asegurarCoachDistributorProfile: async () => ({}), aplicarCoachAccountPreset: async (userDoc, profileDoc) => ({ userDoc, profileDoc }),
    crearCoachSesion: async () => { sessionCount++; }, construirCoachUserView: async () => ({ id: 'seat-A', accountType: 'seat', canViewControlTower: false }),
    responderCoachError: (res, status, error) => res.status(status).json({ error }), console
  });
  const app = express(); app.use(express.json()); app.use(createCoachRequestGuard({ requireActive: async () => null }));
  app.post('/api/coach/login', handler); app.post('/api/coach/signup-checkout', (_req, res) => res.json({ ok: true }));
  const server = app.listen(0, '127.0.0.1'); await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const post = (path, data) => fetch(base + path, { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: 'https://totalelitecoaching.com' }, body: JSON.stringify(data) });
  for (const login of [{ email: '+1 (847) 555-0123' }, { username: '8475550123' }]) {
    const response = await post('/api/coach/login', { ...login, password: 'fixture-password' });
    assert.equal(response.status, 200); assert.equal((await response.json()).user.canViewControlTower, false);
    assert.equal(query.username, '8475550123'); assert.equal(query.email, undefined); assert.equal(throttledIdentity, '8475550123');
  }
  assert.equal((await post('/api/coach/login', { email: 'Original@Example.test', password: 'fixture-password' })).status, 200);
  assert.equal(query.email, 'original@example.test');
  assert.equal((await post('/api/coach/login', { email: '8475550123', password: 'wrong' })).status, 401);
  assert.equal(sessionCount, 3);
  assert.equal((await post('/api/coach/login', { email: { $ne: null }, password: 'fixture-password' })).status, 400);
  assert.equal((await post('/api/coach/signup-checkout', { email: '8475550123', password: 'fixture-password' })).status, 400);
  assert.equal(sessionCount, 3);
});
test('phone-only cached viewer and pending native writes remain isolated by immutable account scope', async () => {
  const writes = [], live = { authenticated: true, user: { id: 'A', email: '', username: '8475550123', storageIdentity: 'coach-id:a' } };
  const context = { window: { TecNativeOffline: {} }, TEC_VIEWER_CACHE_KEY: 'viewer', resetCoachChatForAccount: () => {}, tecOfflinePut: async (...args) => writes.push(args), apiRequest: async () => live, Date, Error };
  vm.createContext(context);
  vm.runInContext(fn(client, 'cacheTecViewer') + '\n' + fn(client, 'verifyTecNativeSyncOwner'), context);
  await context.cacheTecViewer(live); assert.equal(writes.length, 1);
  await context.verifyTecNativeSyncOwner('coach-id:a');
  await assert.rejects(context.verifyTecNativeSyncOwner('coach-id:b'), /cuenta/);
});
test('service worker syncs a phone account pending lead and keeps other accounts queued', async () => {
  const sw = readFileSync(new URL('../public/coach/sw.js', import.meta.url), 'utf8');
  const start = sw.indexOf('async function flushOutboxInServiceWorker()'), end = sw.indexOf('\nself.addEventListener("sync"', start);
  const sent = [], removed = [];
  const context = {
    fetch: async (url, options) => url === '/api/coach/me' ? { ok: true, json: async () => ({ user: { id: 'a', email: '', storageIdentity: 'coach-id:a' } }) } : (sent.push(options), { ok: true, json: async () => ({ ok: true }) }),
    getAllOutboxItems: async () => ['a', 'b'].map(id => ({ id, ownerEmail: 'coach-id:' + id, kind: 'lead', url: '/api/coach/leads', body: {} })),
    deleteOutboxItem: async id => removed.push(id), recordOfflineFormError: async () => {}, broadcastOfflineStatus: async () => {}, completeOfflineForm: async item => removed.push(item.id)
  };
  vm.createContext(context); vm.runInContext(sw.slice(start, end), context); await context.flushOutboxInServiceWorker();
  assert.equal(sent.length, 1); assert.equal(sent[0].headers['X-TEC-Account-ID'], 'a'); assert.deepEqual(removed, ['a']);
});
test('team credential handoff displays a phone username after an authorized reset', () => {
  const nodes = { '[data-team-created-card]': { hidden: true }, '[data-team-created-email]': {}, '[data-team-created-password]': {} };
  const context = { document: { querySelector: selector => nodes[selector] } };
  vm.createContext(context); vm.runInContext(fn(client, 'renderCoachTeamCreatedCredentials'), context);
  context.renderCoachTeamCreatedCredentials({ email: '', username: '8475550123', temporaryPassword: 'fixture-password' });
  assert.equal(nodes['[data-team-created-card]'].hidden, false);
  assert.equal(nodes['[data-team-created-email]'].textContent, '8475550123');
  assert.equal(nodes['[data-team-created-password]'].textContent, 'fixture-password');
});
