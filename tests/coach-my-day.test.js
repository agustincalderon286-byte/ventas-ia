import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import webpush from 'web-push';
import { randomBytes } from 'node:crypto';
import { createMyDay, calendarOf, dayMessage, STANDARD_WEEKLY_LEADS } from '../src/coach-my-day.js';
import { registerMyDayRoutes } from '../src/coach-my-day-routes.js';
import { createCoachDayPush, dueSlot, DAY_PUSH_SIGNAL } from '../src/coach-my-day-push.js';

// Datos inventados. Lunes 23 de noviembre de 2026, 10:00 en Chicago (16:00 UTC).
const MONDAY_10 = Date.parse('2026-11-23T16:00:00Z');
const SELLER = 'a'.repeat(24), OTHER = 'b'.repeat(24);
const id = n => n.toString(16).padStart(24, '0');

// Colección en memoria con lo que usa «Mi día»: rutas con punto, $in/$nin/$gte/$lt/$exists/$ne y arreglos.
function collection(rows = []) {
  let seq = 900;
  const get = (row, path) => path.split('.').reduce((v, k) => v?.[k], row);
  const set = (row, path, value) => { const keys = path.split('.'); let at = row; for (const k of keys.slice(0, -1)) at = at[k] ??= {}; at[keys.at(-1)] = value; };
  const unset = (row, path) => { const keys = path.split('.'); let at = row; for (const k of keys.slice(0, -1)) at = at?.[k]; if (at) delete at[keys.at(-1)]; };
  const cmp = (a, b) => (a instanceof Date || b instanceof Date) ? new Date(a) - new Date(b) : a < b ? -1 : a > b ? 1 : 0;
  const same = (a, b) => String(a) === String(b);
  const matches = (row, filter) => Object.entries(filter).every(([key, expected]) => {
    if (key === '$or') return expected.some(f => matches(row, f));
    const actual = get(row, key);
    if (expected && typeof expected === 'object' && !(expected instanceof Date) && !Array.isArray(expected) && Object.keys(expected).some(k => k.startsWith('$'))) {
      return Object.entries(expected).every(([op, value]) => {
        if (op === '$exists') return (actual !== undefined) === value;
        if (op === '$ne') return Array.isArray(actual) ? !actual.some(v => same(v, value)) : !same(actual, value);
        if (op === '$in') return value.some(v => same(v, actual));
        if (op === '$nin') return !value.some(v => (v == null ? actual == null : same(v, actual)));
        if (actual === undefined || actual === null) return false;
        return { $gte: cmp(actual, value) >= 0, $gt: cmp(actual, value) > 0, $lt: cmp(actual, value) < 0, $lte: cmp(actual, value) <= 0 }[op];
      });
    }
    if (Array.isArray(actual) && !Array.isArray(expected)) return actual.some(v => same(v, expected));
    if (expected === null) return actual == null;
    return same(actual, expected);
  });
  const apply = (row, update, inserting) => {
    if (inserting) for (const [k, v] of Object.entries(update.$setOnInsert || {})) set(row, k, structuredClone(v));
    for (const [k, v] of Object.entries(update.$set || {})) set(row, k, structuredClone(v));
    for (const [k, v] of Object.entries(update.$inc || {})) set(row, k, (get(row, k) || 0) + v);
    for (const k of Object.keys(update.$unset || {})) unset(row, k);
    for (const [k, v] of Object.entries(update.$push || {})) { const list = [...(get(row, k) || []), ...structuredClone(v.$each || [v])]; set(row, k, v.$slice ? list.slice(v.$slice) : list); }
    for (const [k, v] of Object.entries(update.$addToSet || {})) { const list = get(row, k) || []; for (const item of v.$each || [v]) if (!list.includes(item)) list.push(item); set(row, k, list); }
    for (const [k, v] of Object.entries(update.$pull || {})) set(row, k, (get(row, k) || []).filter(item => !matches(item, v)));
  };
  const query = produce => { let sort = null, limit = Infinity; const q = { select() { return q; }, sort(v) { sort = v; return q; }, limit(v) { limit = v; return q; },
    async lean() { let r = produce(); if (Array.isArray(r)) { if (sort) r = r.slice().sort((a, b) => { for (const [k, d] of Object.entries(sort)) { const c = cmp(get(a, k), get(b, k)); if (c) return c * d; } return 0; }); r = r.slice(0, limit); } return r == null ? null : structuredClone(r); },
    then(resolve, reject) { return q.lean().then(resolve, reject); } }; return q; };
  const upsertRow = (filter, update, options) => {
    let row = rows.find(r => matches(r, filter)); const inserting = !row;
    if (!row && !options?.upsert) return null;
    if (!row) { row = { _id: filter._id ?? id(++seq) }; for (const [k, v] of Object.entries(filter)) if (!k.startsWith('$') && (typeof v !== 'object' || v === null || v instanceof Date || typeof v.toHexString === 'function')) set(row, k, v); rows.push(row); }
    apply(row, update, inserting); return row;
  };
  return { rows,
    find: f => query(() => rows.filter(r => matches(r, f))), findOne: f => query(() => rows.find(r => matches(r, f)) || null),
    findById: key => query(() => rows.find(r => same(r._id, key)) || null),
    countDocuments: async f => rows.filter(r => matches(r, f)).length,
    findOneAndUpdate: (f, u, o) => query(() => upsertRow(f, u, o)),
    async updateOne(f, u, o) { const row = upsertRow(f, u, o); return { matchedCount: row ? 1 : 0 }; } };
}

function fixture({ at = MONDAY_10, sheetPool = null, events = [], agendaRows = [], goal = null } = {}) {
  let clock = at;
  const days = n => new Date(at + n * 86400000);
  const sources = {
    leads: collection([{ _id: id(1), generatedByUserId: SELLER, createdAt: days(0), fullName: 'Ana Prueba', phone: '5550100101', status: 'nuevo', source: 'captura_manual' },
      { _id: id(2), generatedByUserId: SELLER, createdAt: days(-1), fullName: 'Beto Prueba', phone: '5550100102', status: 'contactado', nextActionAt: days(-1) },
      { _id: id(3), generatedByUserId: SELLER, createdAt: days(-9), fullName: 'Vieja Prueba', phone: '5550100103', status: 'nuevo' },
      { _id: id(4), generatedByUserId: OTHER, createdAt: days(0), fullName: 'Otro Vendedor', phone: '5550100104', status: 'nuevo' }]),
    tickets: collection([{ _id: id(5), generatedByUserId: SELLER, createdAt: days(0), fullName: 'Carla Boleta', phone: '5550100105', status: 'nuevo', source: 'rifa_digital' }]),
    sheets: collection([{ _id: id(6), ownerUserId: SELLER, createdAt: days(0), referralCount: 7 }, { _id: id(7), ownerUserId: OTHER, createdAt: days(0), referralCount: 9 }]),
    outcomes: collection([{ _id: id(8), reportedByUserId: SELLER, createdAt: days(0), resultType: 'venta', saleAmount: 2400 },
      { _id: id(9), reportedByUserId: SELLER, createdAt: days(0), resultType: 'no_venta' }, { _id: id(10), reportedByUserId: SELLER, createdAt: days(0), resultType: 'no_atendio' }])
  };
  const models = { plans: collection(goal ? [{ _id: SELLER, weeklyLeadGoal: goal }] : []), logs: collection() };
  const calendar = { getConnection: async () => ({ grant: 'g' }),
    readAgenda: async () => ({ events: { status: 'fulfilled', value: events }, busy: { status: 'fulfilled', value: [{}, {}] }, availabilityEnabled: true }) };
  const sheets = { access: async () => ({ token: 't', grant: 'g', spreadsheetId: 'book' }), current: async () => true };
  const sheetTools = { agenda: async () => ({ rows: agendaRows }), nextCall: async () => ({ contacts: structuredClone(sheetPool || []), total: (sheetPool || []).length }) };
  const config = { pilot: ['*'], sheetsPilot: sheetPool ? ['*'] : [], sheetsReady: Boolean(sheetPool) };
  const service = createMyDay({ config, models, sources, calendar, sheets, sheetTools, now: () => clock });
  const actor = { owner: SELLER, email: 'vendedor@example.invalid', name: 'Ana Vendedora', zone: 'America/Chicago', session: 's' };
  return { service, models, sources, actor, advance(ms) { clock += ms; } };
}

test('la semana va de lunes a domingo en la zona del vendedor y el domingo cuenta como un día', () => {
  const monday = calendarOf(MONDAY_10, 'America/Chicago');
  assert.equal(monday.day, '2026-11-23'); assert.equal(monday.weekKey, '2026-11-23'); assert.equal(monday.workDaysLeft, 6);
  assert.equal(new Date(monday.weekStart).toISOString(), '2026-11-23T06:00:00.000Z');
  const sunday = calendarOf(Date.parse('2026-11-29T18:00:00Z'), 'America/Chicago');
  assert.equal(sunday.weekKey, '2026-11-23'); assert.equal(sunday.workDaysLeft, 1);
});

test('cuenta los leads del vendedor de la semana (capturas, boletas y referidos del 4 en 14) contra la meta estándar', async () => {
  const f = fixture();
  const s = await f.service.state(f.actor);
  // Ana y Carla + 7 referidos de su hoja 4 en 14. Beto es del domingo (semana anterior); no cuentan la de hace 9 días ni las de otro vendedor.
  assert.equal(s.goal.weekLeads, 9); assert.equal(s.goal.standard, STANDARD_WEEKLY_LEADS); assert.equal(s.goal.personal, null);
  assert.equal(s.goal.remaining, 91); assert.equal(s.goal.dailyTarget, 16);
  assert.equal(s.today.leads, 9); assert.equal(s.today.demos, 2); assert.equal(s.today.sales, 1); assert.equal(s.today.amount, 2400);
  assert.equal(s.askExhibitor, true);
});

test('la meta personal se guarda y se mide, sin ocultar la estándar', async () => {
  const f = fixture();
  await f.service.setGoal(f.actor, 40);
  const s = await f.service.state(f.actor);
  assert.equal(s.goal.personal, 40); assert.equal(s.goal.standard, 100); assert.equal(s.goal.remaining, 31);
  await assert.rejects(f.service.setGoal(f.actor, 5000), /meta/);
  await f.service.setGoal(f.actor, 0);
  assert.equal((await f.service.state(f.actor)).goal.personal, null);
});

test('recomienda atender citas si hay una más tarde y prospectar si hay exhibidor hoy', async () => {
  const later = fixture({ events: [{ title: 'Demo familia Prueba', start: new Date(MONDAY_10 + 3 * 3600000).toISOString() }] });
  const s = await later.service.state(later.actor);
  assert.equal(s.recommendation.mode, 'citas'); assert.match(s.recommendation.reason, /13:00/);
  assert.deepEqual(s.citas.events.map(e => e.time), ['13:00']); assert.equal(s.citas.busy, 2);
  const exhibitor = fixture();
  await exhibitor.service.addExhibitor(exhibitor.actor, { day: '2026-11-23', start: '11:00', end: '17:00', place: 'Fresh Market Prueba', address: '100 Calle Ficticia' });
  const t = await exhibitor.service.state(exhibitor.actor);
  assert.equal(t.recommendation.mode, 'prospectar'); assert.equal(t.askExhibitor, false); assert.equal(t.exhibitorsToday[0].place, 'Fresh Market Prueba');
});

test('el exhibidor exige día, lugar y dirección; sin exhibidor el vendedor puede decir por qué', async () => {
  const f = fixture();
  await assert.rejects(f.service.addExhibitor(f.actor, { day: '2026-11-20', place: 'X', address: 'Y' }), /día/);
  await assert.rejects(f.service.addExhibitor(f.actor, { day: '2026-11-24', place: '', address: 'Y' }), /lugar/);
  await assert.rejects(f.service.addExhibitor(f.actor, { day: '2026-11-24', start: '15:00', end: '14:00', place: 'X', address: 'Y' }), /hora/);
  await f.service.skipExhibitor(f.actor, 'No conseguí lugar');
  assert.equal((await f.service.state(f.actor)).askExhibitor, false);
});

const person = (n, status = '', bucket = 'overdue') => ({ identityKey: 'k' + n, fullName: 'Persona ' + n, phone: '+1555010' + String(n).padStart(4, '0'), kind: 'referrals', host: 'Anfitrión', status, bucket, reason: 'Motivo ' + n, date: '' });

test('los bloques de llamadas son de cinco, no repiten personas y saltan citas y no interesados', async () => {
  const pool = [person(1, 'Cita'), person(2, 'No interesado'), ...Array.from({ length: 8 }, (_, i) => person(i + 3))];
  const f = fixture({ sheetPool: pool });
  const first = await f.service.callBlock(f.actor);
  assert.equal(first.source, 'sheet'); assert.equal(first.people.length, 5); assert.equal(first.total, 8); assert.equal(first.left, 3);
  assert.ok(!first.people.some(p => /Persona [12]$/.test(p.name)));
  assert.ok(first.people.every(p => /^l[a-f0-9]{12}$/.test(p.ref) && p.phone && p.reason));
  const second = await f.service.callBlock(f.actor, { more: true });
  assert.equal(second.people.length, 3);
  assert.ok(!second.people.some(p => first.people.some(q => q.ref === p.ref)));
});

test('quien pidió llamadas en las tardes va al final por la mañana', async () => {
  const f = fixture({ sheetPool: [person(1, 'Llamar en las tardes'), person(2), person(3, 'Solo mañanas')] });
  const block = await f.service.callBlock(f.actor);
  assert.deepEqual(block.people.map(p => p.name), ['Persona 3', 'Persona 2', 'Persona 1']);
  assert.match(block.people[2].reason, /otro horario/);
});

test('sin hoja conectada usa los leads que el vendedor capturó en TEC, primero los pendientes', async () => {
  const f = fixture();
  const block = await f.service.callBlock(f.actor);
  assert.equal(block.source, 'tec');
  assert.deepEqual(block.people.map(p => p.name), ['Beto Prueba', 'Ana Prueba', 'Carla Boleta', 'Vieja Prueba']);
  assert.equal(block.people[0].phone, '+15550100102'); assert.match(block.people[0].reason, /pendiente/);
});

test('cada persona del bloque cuenta una sola llamada aunque cambie el resultado', async () => {
  const f = fixture();
  const block = await f.service.callBlock(f.actor);
  const [a, b] = block.people;
  assert.equal((await f.service.callResult(f.actor, { ref: a.ref, result: 'no_contesto' })).calls, 1);
  assert.equal((await f.service.callResult(f.actor, { ref: a.ref, result: 'cita' })).calls, 1);
  assert.equal((await f.service.callResult(f.actor, { ref: b.ref, result: 'volver' })).calls, 2);
  await assert.rejects(f.service.callResult(f.actor, { ref: a.ref, result: 'otro' }), /llamada/);
  assert.equal((await f.service.callResult(f.actor, { ref: 'l' + '0'.repeat(12), result: 'cita' })).calls, 2);
  const log = f.models.logs.rows[0];
  assert.equal(log.calls.cita, 1); assert.equal(log.calls.no_contesto, 0); assert.equal(log.calls.volver, 1);
  assert.ok(!JSON.stringify(log).includes('Prueba'), 'el registro del día no guarda nombres');
  assert.equal((await f.service.state(f.actor)).today.calls, 2);
});

test('la ruta de prospección cuenta los leads capturados desde que salió', async () => {
  const f = fixture();
  f.advance(60000);
  await f.service.route(f.actor, { action: 'start', place: 'lavanderias' });
  f.sources.leads.rows.push({ _id: id(20), generatedByUserId: SELLER, createdAt: new Date(MONDAY_10 + 120000), fullName: 'Nueva Prueba', phone: '5550100120', status: 'nuevo' });
  f.advance(3600000);
  const done = await f.service.route(f.actor, { action: 'finish', places: 3, note: 'Buena respuesta' });
  assert.deepEqual(done, { places: 3, leads: 1 });
  await assert.rejects(f.service.route(f.actor, { action: 'start', place: 'casino' }), /prospectar/);
});

test('los avisos de 8 am, 1 pm y 9 pm dicen lo que va a hacer, el empuje del mediodía y el cierre del día', async () => {
  const f = fixture();
  const s = await f.service.state(f.actor);
  assert.match(dayMessage(s, 8).body, /Llevas 9 de 100 leads esta semana\. Meta de hoy: 16 leads\. ¿Vas a atender citas, llamar o prospectar\?/);
  assert.match(dayMessage(s, 13).body, /horas pico/);
  assert.match(dayMessage(s, 21).body, /te faltan 91\. Mañana empiezas fresco/);
  assert.equal(dayMessage({ ...s, goal: { ...s.goal, remaining: 0 } }, 21).body.includes('¡llegaste a la meta!'), true);
});

test('las rutas exigen la cuenta de la sesión y el piloto', async t => {
  const f = fixture();
  const users = { a: { _id: SELLER, email: 'vendedor@example.invalid', name: 'Ana' }, b: { _id: OTHER, email: 'otro@example.invalid', name: 'Otro' } };
  const app = express(); app.use(express.json());
  registerMyDayRoutes(app, { requireActive: async req => ({ user: users[req.get('X-User')], session: { tokenHash: 'h' } }), service: f.service, allowed: auth => auth.user._id === SELLER });
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(() => server.close());
  const go = (path, user, body, account = users[user]._id) => fetch('http://127.0.0.1:' + server.address().port + '/api/coach/mi-dia' + path, {
    method: body ? 'POST' : 'GET', headers: { 'X-User': user, 'X-TEC-Account-ID': account, ...(body && { 'Content-Type': 'application/json' }) },
    ...(body && { body: JSON.stringify({ accountId: account, ...body }) }) });
  const ok = await go('?zone=America/Chicago', 'a');
  assert.equal(ok.status, 200); assert.equal((await ok.json()).goal.weekLeads, 9);
  assert.equal((await go('', 'a', null, OTHER)).status, 403);
  assert.equal((await go('', 'b')).status, 403);
  const bad = await go('/meta', 'a', { leads: -1 });
  assert.equal(bad.status, 422);
});

test('cada aviso sale una vez, a su hora, con una señal genérica; el texto lo pide el teléfono', async () => {
  let clock = Date.parse('2026-11-23T14:05:00Z'); // 8:05 am en Chicago
  const keys = webpush.generateVAPIDKeys();
  const devices = collection(), configs = collection([{ _id: 'team-chat-v1', ...keys }]), sent = [];
  const session = { _id: id(77), expiresAt: new Date(clock + 7 * 86400000) };
  const push = createCoachDayPush({ devices, configs, now: () => clock, requireActive: async () => ({ user: { _id: SELLER, name: 'Ana' }, session }),
    message: async ({ slot }) => ({ title: 'Aviso ' + slot, body: 'Texto ' + slot }), send: async (...args) => { sent.push(args); return { statusCode: 201 }; } });
  await push.initialize();
  const subscription = { endpoint: 'https://fcm.googleapis.com/fcm/send/' + randomBytes(24).toString('hex'), keys: { p256dh: webpush.generateVAPIDKeys().publicKey, auth: randomBytes(16).toString('base64url') } };
  const app = express(); app.use(express.json()); push.register(app);
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  const post = async (action, body) => (await fetch('http://127.0.0.1:' + server.address().port + '/api/coach/mi-dia/push/' + action, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ accountId: SELLER, subscription, ...body }) })).json();
  try {
    // Al activarlo a las 8:05 no se manda el aviso de las 8 que ya pasó.
    assert.equal((await post('subscribe', { zone: 'America/Chicago' })).enabled, true);
    assert.equal(await push.deliverOne(), false); assert.equal(sent.length, 0);
    clock = Date.parse('2026-11-23T19:10:00Z'); // 1:10 pm
    assert.equal(await push.deliverOne(), true); assert.equal(sent.length, 1);
    assert.equal(sent[0][1], DAY_PUSH_SIGNAL);
    assert.equal(await push.deliverOne(), false); assert.equal(sent.length, 1);
    assert.deepEqual(await post('inbox'), { notify: true, title: 'Aviso 13', body: 'Texto 13', url: '/coach/app/?miDia=1' });
    clock = Date.parse('2026-11-24T03:30:00Z'); // 9:30 pm
    await push.deliverOne(); assert.equal(sent.length, 2);
    clock = Date.parse('2026-11-24T08:00:00Z'); // 2 am: fuera de todo aviso
    assert.equal(await push.deliverOne(), false);
  } finally { server.close(); }
  assert.equal(dueSlot(Date.parse('2026-11-23T18:00:00Z'), 'America/Chicago'), null); // mediodía
  assert.equal(dueSlot(Date.parse('2026-11-23T16:30:00Z'), 'America/Chicago').slot, 8); // 10:30 am, dentro de la ventana
});
