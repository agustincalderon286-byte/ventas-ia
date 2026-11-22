import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { contactsGoogleClient, contactsScope, searchPageSize, samePhone, cleanPerson } from '../src/coach-contacts-google.js';

// Fictitious data only.
const config = { origin: 'https://coach.example', clientId: 'client-test', secret: 'secret-test' };
const json = (body, status = 200) => ({ ok: status >= 200 && status < 300, status, json: async () => body });
const contact = (n, extra = {}) => ({
  resourceName: 'people/c' + n, etag: 'etag-' + n,
  metadata: { sources: [{ type: 'CONTACT', id: 'src' + n, etag: 'src-etag-' + n }] },
  names: [{ displayName: 'Persona ' + n, metadata: { primary: true, source: { type: 'CONTACT', id: 'src' + n } } }],
  phoneNumbers: [{ value: '+52 55 0000 000' + n, canonicalForm: '+5255000000' + n, type: 'mobile', metadata: { source: { type: 'CONTACT', id: 'src' + n } } }],
  emailAddresses: [{ value: 'persona' + n + '@example.com', metadata: { source: { type: 'CONTACT', id: 'src' + n } } }],
  ...extra
});
const client = handler => {
  const fetcher = mock.fn(async (url, init) => handler(new URL(url), init));
  return { google: contactsGoogleClient(config, fetcher), fetcher };
};

test('authorize pide openid, email y contacts offline con PKCE y el redirect compartido', () => {
  const { google } = client(() => json({}));
  const url = new URL(google.authorize('people.abc', 'challenge', 'vendedor@example.com'));
  const p = url.searchParams;
  assert.equal(p.get('scope'), 'openid email ' + contactsScope);
  assert.equal(p.get('access_type'), 'offline');
  assert.equal(p.get('code_challenge_method'), 'S256');
  assert.equal(p.get('code_challenge'), 'challenge');
  assert.equal(p.get('state'), 'people.abc');
  assert.equal(p.get('redirect_uri'), 'https://coach.example/api/coach/conexiones/gmail/callback');
  assert.equal(p.get('include_granted_scopes'), null);
});

test('search calienta con consulta vacía, busca por nombre, teléfono y correo, deduplica y no escribe', async () => {
  const { google, fetcher } = client(url => {
    assert.equal(url.pathname, '/v1/people:searchContacts');
    const q = url.searchParams.get('query');
    if (q === '') return json({});
    if (q === 'Persona') return json({ results: [{ person: contact(1) }, { person: contact(2) }] });
    if (q === '525500000001') return json({ results: [{ person: contact(1) }] });
    if (q === 'persona3@example.com') return json({ results: [{ person: contact(3) }] });
    return json({});
  });
  const found = await google.search('access', { name: 'Persona', phone: '+52 55 0000 0001', email: 'Persona3@Example.com' });
  const queries = fetcher.mock.calls.map(c => new URL(c.arguments[0]).searchParams.get('query'));
  assert.deepEqual(queries, ['', 'Persona', '+52 55 0000 0001', '525500000001','persona3@example.com']);
  assert.ok(fetcher.mock.calls.every(c => !c.arguments[1].method || c.arguments[1].method === 'GET'));
  assert.deepEqual(found.map(c => [c.resourceName, c.matchedBy]), [
    ['people/c1', ['name', 'phone']], ['people/c2', ['name']], ['people/c3', ['email']]
  ]);
  assert.deepEqual(Object.keys(found[0]).sort(), ['emails', 'etag', 'matchedBy', 'name', 'phones', 'resourceName']);
  assert.deepEqual(found[0].phones, ['+52 55 0000 0001']);
});

test('search falla en vez de recortar candidatos cuando la página está llena', async () => {
  const many = Array.from({ length: searchPageSize }, (_, i) => ({ person: contact(i + 10) }));
  const { google } = client(url => json(url.searchParams.get('query') ? { results: many } : {}));
  await assert.rejects(google.search('access', { name: 'Persona' }), { code: 'contactsTooMany', status: 409 });
});

test('search rechaza filas incompletas en lugar de descartarlas', async () => {
  const { google } = client(url => json(url.searchParams.get('query') ? { results: [{ person: { resourceName: 'people/c1' } }] } : {}));
  await assert.rejects(google.search('access', { name: 'Persona' }), { code: 'contactsFailed' });
});

test('update agrega solo el teléfono nuevo, conserva los previos, no renombra y usa metadata de origen', async () => {
  let patch;
  const { google } = client((url, init) => {
    if (init.method === 'PATCH') {
      patch = { url, body: JSON.parse(init.body) };
      return json({ ...contact(1), etag: 'etag-new', phoneNumbers: patch.body.phoneNumbers });
    }
    return json(contact(1));
  });
  const result = await google.update('access', { resourceName: 'people/c1', etag: 'etag-1' },
    { name: 'Otro Nombre', phone: '+52 55 1111 2222', email: 'PERSONA1@example.com' });
  assert.equal(patch.url.pathname, '/v1/people/c1:updateContact');
  assert.equal(patch.url.searchParams.get('updatePersonFields'), 'phoneNumbers');
  assert.equal(patch.body.etag, 'etag-1');
  assert.deepEqual(patch.body.metadata, { sources: [{ type: 'CONTACT', id: 'src1', etag: 'src-etag-1' }] });
  assert.equal(patch.body.names, undefined);
  assert.equal(patch.body.emailAddresses, undefined);
  assert.deepEqual(patch.body.phoneNumbers.map(p => p.value), ['+52 55 0000 0001', '+52 55 1111 2222']);
  assert.equal(patch.body.phoneNumbers[0].type, 'mobile');
  assert.equal(result.name, 'Persona 1');
  assert.equal(result.etag, 'etag-new');
  assert.deepEqual(result.changed, ['phone']);
});

test('update no reenvía entradas de perfil ajenas al contacto', async () => {
  const row = contact(1);
  row.metadata.sources.push({ type: 'PROFILE', id: 'p1', etag: 'p' });
  row.emailAddresses.push({ value: 'perfil@example.com', metadata: { source: { type: 'PROFILE', id: 'p1' } } });
  let body;
  const { google } = client((_url, init) => {
    if (init.method === 'PATCH') { body = JSON.parse(init.body); return json({ ...row, emailAddresses: body.emailAddresses }); }
    return json(row);
  });
  await google.update('access', { resourceName: 'people/c1', etag: 'etag-1' }, { email: 'nuevo@example.com' });
  assert.deepEqual(body.emailAddresses.map(e => e.value), ['persona1@example.com', 'nuevo@example.com']);
});

test('update sin datos nuevos no escribe', async () => {
  const { google, fetcher } = client(() => json(contact(1)));
  const result = await google.update('access', { resourceName: 'people/c1', etag: 'etag-1' }, { phone: '+52 (55) 0000 0001' });
  assert.equal(fetcher.mock.callCount(), 1);
  assert.deepEqual(result.changed, []);
});

test('update con etag distinto devuelve conflicto sin escribir', async () => {
  const { google, fetcher } = client(() => json(contact(1)));
  await assert.rejects(google.update('access', { resourceName: 'people/c1', etag: 'viejo' }, { phone: '+52 55 1111 2222' }),
    { code: 'contactsConflict', status: 409 });
  assert.equal(fetcher.mock.callCount(), 1);
});

test('update mapea FAILED_PRECONDITION de Google a conflicto', async () => {
  const { google } = client((_url, init) => init.method === 'PATCH'
    ? json({ error: { status: 'FAILED_PRECONDITION' } }, 400) : json(contact(1)));
  await assert.rejects(google.update('access', { resourceName: 'people/c1', etag: 'etag-1' }, { email: 'x@example.com' }),
    { code: 'contactsConflict' });
});

test('401 y scope insuficiente piden reautorización', async () => {
  const { google } = client(() => json({}, 401));
  await assert.rejects(google.get('access', 'people/c1'), { code: 'contactsReconnect', reauthorize: true });
  const scoped = client(() => json({ error: { details: [{ reason: 'ACCESS_TOKEN_SCOPE_INSUFFICIENT' }] } }, 403)).google;
  await assert.rejects(scoped.get('access', 'people/c1'), { code: 'contactsReconnect' });
  const token = client(() => json({ error: 'invalid_grant' }, 400)).google;
  await assert.rejects(token.refresh('refresh-test'), { reauthorize: true });
});

test('create ambiguo queda incierto y no se reporta éxito', async () => {
  const { google } = client(() => json({}, 503));
  await assert.rejects(google.create('access', { name: 'Persona Nueva', phone: '+52 55 3333 4444' }),
    { code: 'contactsUncertain', uncertain: true });
  const timeout = contactsGoogleClient(config, async () => { throw new Error('timeout'); });
  await assert.rejects(timeout.create('access', { name: 'Persona Nueva' }), { code: 'contactsUncertain' });
});

test('create envía solo nombre, teléfono y correo', async () => {
  let body;
  const { google } = client((_url, init) => { body = JSON.parse(init.body); return json(contact(9)); });
  const card = await google.create('access', { name: 'Persona Nueva', phone: '+52 55 3333 4444', email: 'nueva@example.com' });
  assert.deepEqual(body, { names: [{ unstructuredName: 'Persona Nueva' }], phoneNumbers: [{ value: '+52 55 3333 4444' }],
    emailAddresses: [{ value: 'nueva@example.com' }] });
  assert.equal(card.resourceName, 'people/c9');
});

test('validación y comparación de teléfonos', () => {
  assert.throws(() => cleanPerson({}), { code: 'contactsInvalid' });
  assert.throws(() => cleanPerson({ phone: 'abc' }), { code: 'contactsInvalid' });
  assert.throws(() => cleanPerson({ email: 'no-correo' }), { code: 'contactsInvalid' });
  assert.throws(() => cleanPerson({ phone: '+52 55 1111 2222' }, { needName: true }), { code: 'contactsInvalid' });
  assert.ok(!samePhone('+52 55 1111 2222', '5511112222'));
  assert.ok(samePhone('+1 312 555 0191', '3125550191'));
  assert.ok(!samePhone('5511112222', '5511112223'));
  const { google } = client(() => json({}));
  return assert.rejects(google.get('access', '../people/c1'), { code: 'contactsInvalid' });
});

// --- Connections: requires the backend's real dependencies (mongoose, coach-hycite-gmail.js). ---
const connections=await import('../src/coach-contacts-connections.js');

function fakeStore() {
  const docs = new Map();
  const match = (doc, query) => Object.entries(query).every(([key, value]) => {
    if (value && typeof value === 'object' && !(value instanceof Date)) {
      if ('$gt' in value) return doc[key] > value.$gt;
      if ('$exists' in value) return (doc[key] !== undefined) === value.$exists;
    }
    return doc[key] === value;
  });
  const apply = (doc, update) => {
    Object.assign(doc, update.$set || {});
    for (const key of Object.keys(update.$unset || {})) delete doc[key];
  };
  const lean = value => ({ lean: async () => value && structuredClone(value) });
  return {
    docs,
    findOne: query => lean([...docs.values()].find(doc => match(doc, query))),
    findById: id => lean(docs.get(id)),
    exists: async query => [...docs.values()].some(doc => match(doc, query)),
    findOneAndUpdate(query, update) {
      const doc = [...docs.values()].find(item => match(item, query));
      const before = doc && structuredClone(doc);
      if (doc) apply(doc, update);
      return lean(before);
    },
    async updateOne(query, update, options = {}) {
      let doc = [...docs.values()].find(item => match(item, query));
      if (!doc && options.upsert) { doc = { _id: query._id }; docs.set(query._id, doc); }
      if (doc) apply(doc, update);
      return { matchedCount: doc ? 1 : 0 };
    }
  };
}

function harness(overrides = {}) {
  const store = fakeStore();
  const google = {
    authorize: mock.fn(state => 'https://accounts.example/auth?state=' + encodeURIComponent(state)),
    exchange: mock.fn(async () => ({ access_token: 'access-test', refresh_token: 'refresh-test', scope: 'openid email ' + contactsScope })),
    profile: mock.fn(async () => ({ sub: 'sub-test', email: 'Vendedor@example.com', email_verified: true })),
    refresh: mock.fn(async () => ({ access_token: 'access-renewed', scope: contactsScope })),
    search: mock.fn(async () => []),
    ...overrides
  };
  const config = { contactsReady: true, key: randomBytes(32), origin: 'https://coach.example' };
  const plugin = connections.createCoachContactsConnections({ config, store, google });
  const routes = {};
  const identity = { id: 'owner-1', session: 'session-1', email: 'vendedor@example.com' };
  plugin.register({ post: (path, handler) => { routes[path] = handler; } }, handler => (req, res) => handler(req, res, identity), (_res, code) => code);
  const res = { redirect: mock.fn((_status, url) => url) };
  const start = async () => {
    const url = await routes['/api/coach/conexiones/contactos/start']({ body: {} }, res);
    return new URL(url).searchParams.get('state');
  };
  const callback = (query, who = identity) => plugin.callback({ query }, res, who, (_res, code) => code);
  return { store, google, plugin, routes, identity, start, callback, res };
}

test('conexión: start, callback, token cifrado y servicio ligado al grant', async () => {
  const h = harness();
  const state = await h.start();
  assert.match(state, /^people\./);
  assert.equal(await h.callback({ state, code: 'code-test' }), 'contactsConnected');
  const doc = h.store.docs.get('owner-1');
  assert.ok(doc.encryptedToken && !doc.encryptedToken.includes('refresh-test'));
  assert.equal(doc.stateHash, undefined);
  const profile = await h.plugin.service.profile('owner-1');
  assert.deepEqual(profile, { grant: doc.grant, mailbox: 'vendedor@example.com' });
  await h.plugin.service.search('owner-1', profile.grant, { name: 'Persona' });
  assert.equal(h.google.refresh.mock.calls[0].arguments[0], 'refresh-test');
  assert.equal(h.google.search.mock.calls[0].arguments[0], 'access-renewed');
  assert.equal(await h.callback({ state, code: 'code-test' }), 'expired');
});

test('conexión: cuenta distinta, sesión distinta o scope faltante no conectan', async () => {
  const wrong = harness({ profile: mock.fn(async () => ({ sub: 's', email: 'otra@example.com', email_verified: true })) });
  assert.equal(await wrong.callback({ state: await wrong.start(), code: 'c' }), 'contactsWrongAccount');
  assert.equal(wrong.store.docs.get('owner-1').encryptedToken, undefined);

  const session = harness();
  const state = await session.start();
  assert.equal(await session.callback({ state, code: 'c' }, { ...session.identity, session: 'otra' }), 'expired');

  const scope = harness({ exchange: mock.fn(async () => ({ access_token: 'a', refresh_token: 'r', scope: 'openid email' })) });
  assert.equal(await scope.callback({ state: await scope.start(), code: 'c' }), 'contactsMissingScope');
  assert.equal(await scope.plugin.service.profile('owner-1'), null);
});

test('conexión: desconectar invalida el grant sin revocar en Google', async () => {
  const h = harness();
  await h.callback({ state: await h.start(), code: 'c' });
  const { grant } = await h.plugin.service.profile('owner-1');
  assert.equal(await h.routes['/api/coach/conexiones/contactos/disconnect']({}, h.res), 'contactsDisconnected');
  assert.equal(await h.plugin.service.profile('owner-1'), null);
  await assert.rejects(h.plugin.service.search('owner-1', grant, { name: 'Persona' }), { code: 'contactsChanged' });
  assert.equal(h.google.revoke, undefined);
  assert.equal(h.google.refresh.mock.callCount(), 0);
});

test('conexión: refresh rechazado pide reautorizar', async () => {
  const h = harness({ refresh: mock.fn(async () => { throw Object.assign(new Error('x'), { reauthorize: true }); }) });
  await h.callback({ state: await h.start(), code: 'c' });
  const { grant } = await h.plugin.service.profile('owner-1');
  await assert.rejects(h.plugin.service.update('owner-1', grant, { resourceName: 'people/c1', etag: 'e' }, { phone: '+52 55 1111 2222' }),
    { code: 'contactsReconnect' });
});
