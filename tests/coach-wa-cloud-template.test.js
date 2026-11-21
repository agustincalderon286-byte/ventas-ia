import test from 'node:test';
import assert from 'node:assert/strict';
import { waCloudConfig } from '../src/coach-wa-cloud.js';
import { createWaCloudTemplate, templateProblem } from '../src/coach-wa-cloud-template.js';

// Solo dobles de prueba: ningún token, número ni plantilla real.
const TOKEN = 'TEST_token_' + 'x'.repeat(30);
const config = waCloudConfig({
  COACH_WA_CLOUD_SELLER_EMAIL: 'vendedor@example.test',
  COACH_WA_CLOUD_PHONE: '+13125550100',
  COACH_WA_CLOUD_PHONE_NUMBER_ID: '1111111111',
  COACH_WA_CLOUD_ACCESS_TOKEN: TOKEN,
  COACH_WA_CLOUD_GRAPH_VERSION: 'v27.0'
});
const BODY = 'Hola, soy tu asesor de TEC.\nQueda pendiente tu seguimiento. ¿Te puedo escribir por aquí?  ';
const options = { wabaId: '2222222222', templateName: 'seguimiento_tec', language: 'es_MX', env: {} };

const reply = (status, data) => ({ ok: status >= 200 && status < 300, status, json: async () => data });

function fixture({ phones, templates, post, overrides = {} } = {}) {
  const calls = [];
  const state = {
    phones: phones ?? [{ id: '1111111111', display_phone_number: '+1 312-555-0100' }],
    templates: templates ?? [
      { name: 'seguimiento_tec_v2', language: 'es_MX', status: 'APPROVED', category: 'UTILITY', components: [{ type: 'BODY', text: 'Otro' }] },
      { name: 'seguimiento_tec', language: 'en_US', status: 'APPROVED', category: 'UTILITY', components: [{ type: 'BODY', text: 'English' }] },
      { name: 'seguimiento_tec', language: 'es_MX', status: 'APPROVED', category: 'UTILITY', components: [{ type: 'BODY', text: BODY }] }
    ],
    post: post ?? (() => reply(200, { messages: [{ id: 'wamid.TEST123', message_status: 'accepted' }] }))
  };
  const fetch = async (url, init) => {
    calls.push({ url, init });
    if (url.includes('/phone_numbers')) return typeof state.phones === 'function' ? state.phones() : reply(200, { data: state.phones });
    if (url.includes('/message_templates')) return typeof state.templates === 'function' ? state.templates() : reply(200, { data: state.templates });
    if (url.endsWith('/messages')) return state.post(init);
    throw new Error('URL inesperada ' + url);
  };
  const wa = createWaCloudTemplate(config, { fetch, ...options, ...overrides });
  return { wa, calls, state };
}

const withBody = (text, extra = {}) => [{ name: 'seguimiento_tec', language: 'es_MX', status: 'APPROVED', category: 'UTILITY', components: [{ type: 'BODY', text }], ...extra }];

test('prepare lee solo Graph HTTPS con Bearer y devuelve la plantilla exacta sin alterar el texto', async () => {
  const { wa, calls } = fixture();
  const template = await wa.prepare();
  assert.deepEqual({ ...template }, { name: 'seguimiento_tec', language: 'es_MX', category: 'UTILITY', body: BODY });
  assert.ok(Object.isFrozen(template));
  assert.equal(template.body, BODY, 'conserva espacios y saltos de línea');
  assert.deepEqual(calls.map(c => c.url), [
    'https://graph.facebook.com/v27.0/2222222222/phone_numbers?fields=id,display_phone_number',
    'https://graph.facebook.com/v27.0/2222222222/message_templates?name=seguimiento_tec&fields=name,status,category,language,components'
  ]);
  for (const { init } of calls) {
    assert.equal(init.method, 'GET');
    assert.equal(init.redirect, 'error');
    assert.equal(init.headers.Authorization, 'Bearer ' + TOKEN);
    assert.ok(init.signal);
  }
  assert.ok(!JSON.stringify(template).includes(TOKEN));
  assert.ok(!JSON.stringify(wa.settings).includes(TOKEN));
});

test('las opciones pueden venir de variables de entorno específicas', async () => {
  const fetch = async url => url.includes('/phone_numbers')
    ? reply(200, { data: [{ id: '1111111111', display_phone_number: '13125550100' }] })
    : reply(200, { data: withBody('Texto fijo') });
  const wa = createWaCloudTemplate(config, { fetch, env: {
    COACH_WA_CLOUD_WABA_ID: '2222222222', COACH_WA_CLOUD_TEMPLATE_NAME: 'seguimiento_tec', COACH_WA_CLOUD_TEMPLATE_LANGUAGE: 'es_MX'
  } });
  assert.equal(wa.settings.ready, true);
  assert.equal((await wa.prepare()).body, 'Texto fijo');
});

test('sin configuración completa, con base no HTTPS o Cloud sin preparar no llama a Meta', async () => {
  const cases = [
    { wabaId: '' }, { wabaId: '22a22' }, { templateName: 'Seguimiento TEC' }, { language: 'español' },
    { base: 'https://other.example.test' }, { base: 'http://graph.facebook.com' }, { base: 'https://graph.facebook.com/otra' }, { base: 'https://user:pw@graph.facebook.com' }
  ];
  for (const overrides of cases) {
    const { wa, calls } = fixture({ overrides });
    assert.equal(wa.settings.ready, false, JSON.stringify(overrides));
    assert.equal(await wa.prepare(), null);
    await assert.rejects(wa.send({ to: '+13125550199', template: { name: 'seguimiento_tec', language: 'es_MX', body: BODY } }),
      e => e.beforeSend && e.reason === 'notConfigured');
    assert.equal(calls.length, 0);
  }
  const calls = [];
  const notReady = createWaCloudTemplate(waCloudConfig({}), { fetch: async () => { calls.push(1); }, ...options });
  assert.deepEqual([...notReady.settings.problems], ['cloud']);
  assert.equal(await notReady.prepare(), null);
  assert.equal(calls.length, 0);
});

test('si la línea no pertenece exactamente a la WABA no consulta plantillas ni envía', async () => {
  for (const phones of [
    [], [{ id: '11111111110', display_phone_number: '+13125550100' }], [{ id: '111111111', display_phone_number: '+13125550100' }],
    [{ id: 1111111111, display_phone_number: '+13125550100' }], [{ id: '1111111111', display_phone_number: '+13125550101' }],
    [{ id: '1111111111' }]
  ]) {
    const { wa, calls } = fixture({ phones });
    assert.deepEqual(await wa.inspect(), { ok: false, reason: 'notOwned' });
    await assert.rejects(wa.send({ to: '+13125550199', template: { name: 'seguimiento_tec', language: 'es_MX', body: BODY } }),
      e => e.beforeSend && e.reason === 'notOwned');
    assert.ok(calls.every(c => c.url.includes('/phone_numbers')));
  }
});

test('rechaza plantillas no exactas, no aprobadas, no UTILITY o con algo distinto de un BODY fijo', async () => {
  const cases = [
    [[{ ...withBody(BODY)[0], name: 'seguimiento_tec_v2' }], 'notFound'],
    [[{ ...withBody(BODY)[0], name: 'Seguimiento_tec' }], 'notFound'],
    [[{ ...withBody(BODY)[0], language: 'es' }], 'notFound'],
    [[...withBody(BODY), ...withBody(BODY)], 'ambiguous'],
    [withBody(BODY, { status: 'PENDING' }), 'notApproved'],
    [withBody(BODY, { status: 'approved' }), 'notApproved'],
    [withBody(BODY, { category: 'MARKETING' }), 'category'],
    [withBody(BODY, { category: 'AUTHENTICATION' }), 'category'],
    [withBody(BODY, { components: [{ type: 'HEADER', format: 'TEXT', text: 'Hola' }, { type: 'BODY', text: BODY }] }), 'components'],
    [withBody(BODY, { components: [{ type: 'BODY', text: BODY }, { type: 'BUTTONS', buttons: [] }] }), 'components'],
    [withBody(BODY, { components: [{ type: 'BODY', text: BODY }, { type: 'FOOTER', text: 'TEC' }] }), 'components'],
    [withBody(BODY, { components: [{ type: 'BODY', text: 'Hola', example: { body_text: [['x']] } }] }), 'components'],
    [withBody(BODY, { components: [{ type: 'body', text: BODY }] }), 'components'],
    [withBody(BODY, { components: [] }), 'components'],
    [withBody(BODY, { components: null }), 'components'],
    [withBody('Hola {{1}}, ¿seguimos?'), 'components'],
    [withBody('Hola {{nombre}}'), 'components'],
    [withBody('   '), 'text'],
    [withBody(''), 'text'],
    [withBody('x'.repeat(1025)), 'text'],
    [withBody('Hola\u0000'), 'text'],
    [withBody(42), 'text'],
    [[], 'notFound']
  ];
  for (const [templates, reason] of cases) {
    const { wa, calls } = fixture({ templates });
    assert.deepEqual(await wa.inspect(), { ok: false, reason }, JSON.stringify(templates).slice(0, 120));
    assert.equal(await wa.prepare(), null);
    assert.ok(!calls.some(c => c.init.method === 'POST'));
  }
});

test('errores de lectura en Meta se distinguen y devuelven null', async () => {
  const cases = [
    [() => reply(401, { error: { code: 190 } }), 'token'],
    [() => reply(400, { error: { code: 190 } }), 'token'],
    [() => reply(503, {}), 'unreachable'],
    [() => reply(400, { error: { code: 100 } }), 'rejected'],
    [() => { throw new TypeError('red'); }, 'unreachable']
  ];
  for (const [templates, reason] of cases) {
    const { wa } = fixture({ templates });
    assert.deepEqual(await wa.inspect(), { ok: false, reason });
    assert.equal(await wa.prepare(), null);
  }
  const { wa } = fixture({ phones: () => reply(401, {}) });
  assert.deepEqual(await wa.inspect(), { ok: false, reason: 'token' });
});

test('send revalida en Meta y envía solo nombre e idioma de servidor, sin componentes', async () => {
  const { wa, calls } = fixture();
  const template = await wa.prepare();
  calls.length = 0;
  const result = await wa.send({ to: '+13125550199', template });
  assert.deepEqual(result, { id: 'wamid.TEST123', metaStatus: 'accepted' });
  assert.equal(calls.length, 3);
  assert.ok(calls[0].url.includes('/phone_numbers'));
  assert.ok(calls[1].url.includes('/message_templates'));
  const post = calls[2];
  assert.equal(post.url, 'https://graph.facebook.com/v27.0/1111111111/messages');
  assert.equal(post.init.method, 'POST');
  assert.equal(post.init.redirect, 'error');
  assert.equal(post.init.headers.Authorization, 'Bearer ' + TOKEN);
  assert.equal(post.init.headers['Content-Type'], 'application/json');
  assert.deepEqual(JSON.parse(post.init.body), {
    messaging_product: 'whatsapp', recipient_type: 'individual', to: '13125550199', type: 'template',
    template: { name: 'seguimiento_tec', language: { code: 'es_MX' } }
  });
});

test('send ignora campos extra del cliente: nombre/idioma salen del servidor', async () => {
  const { wa, calls } = fixture();
  const template = { ...(await wa.prepare()), components: [{ type: 'body', parameters: [{ type: 'text', text: 'inyección' }] }], namespace: 'x' };
  await wa.send({ to: '+13125550199', template });
  const body = JSON.parse(calls.at(-1).init.body);
  assert.deepEqual(body.template, { name: 'seguimiento_tec', language: { code: 'es_MX' } });
});

test('send no hace POST si cambió el texto, nombre o idioma, o si el destino no es válido', async () => {
  const good = { name: 'seguimiento_tec', language: 'es_MX', body: BODY };
  const cases = [
    [{ to: '+13125550199', template: { ...good, body: BODY.trim() } }, 'changed', 3],
    [{ to: '+13125550199', template: { ...good, body: 'Otro texto' } }, 'changed', 3],
    [{ to: '+13125550199', template: { ...good, name: 'seguimiento_tec_v2' } }, 'changed', 1],
    [{ to: '+13125550199', template: { ...good, language: 'es' } }, 'changed', 1],
    [{ to: '+13125550199', template: { ...good, body: 'Hola {{1}}' } }, 'changed', 1],
    [{ to: '+13125550199' }, 'changed', 1],
    [{ to: '3125550199', template: good }, 'invalid', 1],
    [{ to: '+1 312 555 0199', template: good }, 'invalid', 1],
    [{}, 'invalid', 1]
  ];
  for (const [args, reason, phase] of cases) {
    const { wa, calls } = fixture();
    await assert.rejects(wa.send(args), e => e.beforeSend === true && e.reason === reason && !e.providerRejected, JSON.stringify(args));
    assert.ok(!calls.some(c => c.init.method === 'POST'));
    // phase 1: rechazo local sin red; phase 3: rechazo tras revalidar (2 GET).
    assert.equal(calls.length, phase === 1 ? 0 : 2);
  }
});

test('send no envía si Meta dejó de aprobar la plantilla o cambió el texto tras la revisión', async () => {
  const { wa, calls, state } = fixture();
  const template = await wa.prepare();
  state.templates = withBody(BODY, { status: 'PAUSED' });
  await assert.rejects(wa.send({ to: '+13125550199', template }), e => e.beforeSend && e.reason === 'notApproved');
  state.templates = withBody(BODY + 'x');
  await assert.rejects(wa.send({ to: '+13125550199', template }), e => e.beforeSend && e.reason === 'changed');
  state.templates = () => { throw new Error('caída'); };
  await assert.rejects(wa.send({ to: '+13125550199', template }), e => e.beforeSend && e.reason === 'unreachable');
  assert.ok(!calls.some(c => c.init.method === 'POST'));
});

test('send distingue rechazo de Meta de resultado indeterminado', async () => {
  const rejected = [
    [400, { code: 132001 }, 'templateUnavailable'],
    [400, { code: 132015 }, 'templateUnavailable'],
    [400, { code: 131026 }, 'undeliverable'],
    [401, { code: 190 }, 'token'],
    [429, { code: 130429 }, 'rateLimit'],
    [400, { code: 131031 }, 'account'],
    [400, { code: 100 }, 'invalid'],
    [403, { code: 999999 }, 'rejected']
  ];
  for (const [status, error, reason] of rejected) {
    const { wa } = fixture({ post: () => reply(status, { error }) });
    const template = await wa.prepare();
    await assert.rejects(wa.send({ to: '+13125550199', template }),
      e => e.providerRejected === true && !e.beforeSend && e.reason === reason && e.code === error.code);
    assert.equal(typeof templateProblem(reason), 'string');
  }
  const indeterminate = [
    () => reply(500, { error: { code: 1 } }),
    () => reply(400, null),
    () => reply(200, {}),
    () => reply(200, { messages: [{ id: '' }] }),
    () => reply(200, { messages: [{ id: 'bad id with spaces' }] }),
    () => { throw new DOMException('timeout', 'TimeoutError'); },
    () => { throw new TypeError('redirect'); }
  ];
  for (const post of indeterminate) {
    const { wa } = fixture({ post });
    const template = await wa.prepare();
    await assert.rejects(wa.send({ to: '+13125550199', template }),
      e => e.indeterminate === true && !e.beforeSend && !e.providerRejected);
  }
});

test('templateProblem da mensajes propios y cae en los del piloto', () => {
  for (const reason of ['notConfigured', 'notOwned', 'notFound', 'ambiguous', 'notApproved', 'category', 'components', 'text', 'changed', 'templateUnavailable', 'unreachable']) {
    assert.ok(templateProblem(reason).length > 10, reason);
  }
  assert.match(templateProblem('token'), /autorización/);
  assert.equal(templateProblem('desconocido'), templateProblem('rejected'));
});


test('busca la línea y la plantilla en páginas posteriores del mismo endpoint oficial', async () => {
  const calls = [];
  const fetch = async input => {
    const url = new URL(input);
    assert.equal(url.origin, 'https://graph.facebook.com');
    calls.push(url.pathname);
    if (!url.searchParams.has('after')) {
      return reply(200, {data:[], paging:{next:'https://example.invalid/ignore', cursors:{after:'otro+cursor'}}});
    }
    assert.equal(url.searchParams.get('after'), 'otro+cursor');
    const data = url.pathname.endsWith('/phone_numbers')
      ? [{id:'1111111111', display_phone_number:'13125550100'}] : withBody(BODY);
    return reply(200, {data});
  };
  assert.equal((await createWaCloudTemplate(config, {fetch, ...options}).prepare()).body, BODY);
  assert.equal(calls.length, 4);
});
