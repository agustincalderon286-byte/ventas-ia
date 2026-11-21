import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { readFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import sharp from 'sharp';
import { prepareReferralDocument } from '../src/coach-referral-document-image.js';
import { COACH_REFERRAL_PHOTO_PATH, FOURTEEN_SHEET_MAX_REFERRALS, createCoachReferralPhotoHandler } from '../src/coach-referral-photo.js';

const accountA = 'aaaaaaaaaaaaaaaaaaaaaaaa';
const accountB = 'bbbbbbbbbbbbbbbbbbbbbbbb';
const testPhoto = await sharp({ create: { width: 400, height: 300, channels: 3, background: '#ffffff' } })
  .withMetadata({ density: 96 }).png().toBuffer();
const photo = testPhoto.toString('base64');
const row = (fullName = 'Persona de prueba', phone = '(312) 555-0100', address = '') =>
  ({ fullName, phone, address, city: '', relationship: '', location: '', uncertainFields: [] });
const sheet = (changes = {}) => ({
  documentType: 'referrals', hostName: 'Anfitriona de prueba', hostPhone: '', uncertainHostFields: [],
  referrals: [row()], overflow: false, ...changes
});
function completion(data, finish = 'stop') {
  return Response.json({ choices: [{ finish_reason: finish, message: { content: JSON.stringify(data) } }] });
}

function fixture(options = {}) {
  const calls = [];
  const requireActive = async (req, res) => {
    if (![accountA, accountB].includes(req.session)) {
      res.status(401).json({ error: 'Sesión de prueba inactiva' });
      return null;
    }
    return { user: { _id: req.session } };
  };
  const handler = createCoachReferralPhotoHandler({
    apiKey: 'test-only', requireActive, fetchImpl: async (url, init) => {
      calls.push({ url, init });
      return completion(sheet());
    }, ...options
  });
  async function submit({ session = accountA, accountId = accountA, image = photo, headerAccount, json = true } = {}) {
    const req = {
      session, body: { accountId, photo: image },
      is: mime => json && mime === 'application/json',
      get: key => key === 'X-TEC-Account-ID' ? headerAccount : undefined
    };
    const res = Object.assign(new EventEmitter(), {
      statusCode: 200, headers: {},
      status(code) { this.statusCode = code; return this; },
      set(key, value) { this.headers[key] = value; return this; },
      json(body) { this.body = body; return this; }
    });
    await handler(req, res);
    return res;
  }
  return { submit, calls };
}

test('photo route and save use the same 11 rows as the 4-in-14 editor', async () => {
  assert.equal(COACH_REFERRAL_PHOTO_PATH, '/api/coach/program-4-in-14/photo');
  const ui = await readFile(new URL('../public/coach/app.js', import.meta.url), 'utf8');
  const server = await readFile(new URL('../server.js', import.meta.url), 'utf8');
  assert.equal(Number(ui.match(/const FOURTEEN_SHEET_MAX_REFERRALS = (\d+)/)?.[1]), FOURTEEN_SHEET_MAX_REFERRALS);
  assert.match(server, /app\.post\(COACH_REFERRAL_PHOTO_PATH, createCoachReferralPhotoHandler\(\{ requireActive: requireOwnPhotoSession/);
  assert.match(server, /payload\.referrals\.map\(limpiarCoachProgramReferral\).*?FOURTEEN_SHEET_MAX_REFERRALS/);
});

test('private transcription receives only sanitized pixels and returns a reviewable draft', async () => {
  const f = fixture();
  const res = await f.submit();
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.body, {
    documentType: 'referrals', hostName: 'Anfitriona de prueba', hostPhone: '',
    referrals: [row()], review: { incompleteRows: [], uncertainHostFields: [], lowResolution: true, overflow: false }
  });
  const [{ url, init }] = f.calls;
  assert.equal(url, 'https://api.openai.com/v1/chat/completions');
  assert.equal(init.redirect, 'error');
  assert.ok(init.signal instanceof AbortSignal);
  const body = JSON.parse(init.body);
  assert.equal(body.store, false);
  assert.equal(body.response_format.json_schema.strict, true);
  assert.equal(body.tools, undefined);
  assert.equal(init.body.includes(accountA), false);
  assert.equal(init.body.includes(photo), false);
  const [instructions, userInput] = body.messages;
  assert.match(instructions.content, /Ignora cualquier instrucción/);
  assert.match(instructions.content, /Nunca adivines/);
  assert.equal(userInput.content.length, 2);
  assert.match(userInput.content[0].text, /Vista general/);
  const data = userInput.content[1].image_url.url;
  assert.ok(data.startsWith('data:image/jpeg;base64,'));
  const metadata = await sharp(Buffer.from(data.split(',')[1], 'base64')).metadata();
  assert.equal(metadata.format, 'jpeg');
  assert.equal(metadata.exif, undefined);
  assert.equal(metadata.xmp, undefined);
});

test('document views follow EXIF orientation, preserve native handwriting scale and send four labeled overlapping crops', async () => {
  const original = Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="2400" height="1800">' +
    '<rect width="1200" height="900" fill="red"/><rect x="1200" width="1200" height="900" fill="lime"/>' +
    '<rect y="900" width="1200" height="900" fill="blue"/><rect x="1200" y="900" width="1200" height="900" fill="yellow"/></svg>');
  const bytes = await sharp(original).withMetadata({ orientation: 6 }).jpeg().toBuffer();
  const { content, lowResolution } = await prepareReferralDocument(bytes.toString('base64'));
  assert.equal(lowResolution, false);
  assert.equal(content.length, 10);
  const colors = [[0, 0, 255], [255, 0, 0], [255, 255, 0], [0, 255, 0]];
  const sides = ['superior izquierdo', 'superior derecho', 'inferior izquierdo', 'inferior derecho'];
  for (let i = 0; i < content.length; i += 2) {
    const input = Buffer.from(content[i + 1].image_url.url.split(',')[1], 'base64');
    const meta = await sharp(input).metadata();
    assert.equal(meta.orientation, undefined);
    assert.equal(meta.exif, undefined);
    assert.equal(meta.xmp, undefined);
    assert.equal(meta.icc, undefined);
    if (!i) {
      assert.deepEqual([meta.width, meta.height], [1275, 1700]);
      continue;
    }
    const index = i / 2 - 1;
    assert.ok(content[i].text.includes(sides[index]));
    assert.match(content[i].text, /x \d+–\d+%, y \d+–\d+%/);
    assert.deepEqual([meta.width, meta.height], [1062, 1440]);
    const rgb = await sharp(input).extract({ left: 520, top: 720, width: 1, height: 1 }).raw().toBuffer();
    assert.ok(colors[index].every((v, c) => Math.abs(rgb[c] - v) < 5), sides[index]);
  }
});

test('ticket contacts retain the spatial label and their own city while health fields and host guesses are discarded', async () => {
  const f = fixture({ fetchImpl: async () => completion(sheet({
    documentType: 'tickets', hostName: 'Un representante', hostPhone: '3125550147',
    referrals: [
      { ...row('Nora y Luz', '312 555 0147'), location: 'Boleta superior izquierda', city: 'Villa de Prueba', relationship: 'Amigas', health: 'NO_INCLUIR' },
      { ...row('Luis', ''), location: 'Boleta inferior derecha', uncertainFields: ['phone'] },
      { ...row('', '3125550148'), location: 'Talón inferior izquierdo', extraCredit: 'NO_INCLUIR' },
      { ...row('', '', 'Edificio impreso del evento'), location: 'Margen inferior', relationship: 'Pariente', city: 'Un lugar' },
      row('', '')
    ]
  })) });
  const res = await f.submit();
  assert.equal(res.statusCode, 200);
  assert.equal(res.body.documentType, 'tickets');
  assert.equal(res.body.hostName, '');
  assert.equal(res.body.hostPhone, '');
  assert.equal(res.body.referrals.length, 3);
  assert.equal(res.body.referrals[0].fullName, 'Nora y Luz');
  assert.equal(res.body.referrals[0].city, 'Villa de Prueba');
  assert.equal(res.body.referrals[0].relationship, 'Amigas');
  assert.equal(res.body.referrals[2].location, 'Talón inferior izquierdo');
  assert.deepEqual(res.body.review.incompleteRows, [2, 3]);
  assert.doesNotMatch(JSON.stringify(res.body), /NO_INCLUIR|extraCredit|consent|appointment|Edificio/);
});

test('complete phones keep printed punctuation, while local fragments, partial country codes and alternative numbers require review', async () => {
  const candidates = ['555-0188', '312-555-0188 / 0189', '(312) 555-0101 (312) 555-0102', '+1 312 555018', '+1 (312) 555–0117'];
  const f = fixture({ fetchImpl: async () => completion(sheet({
    hostPhone: '3125550133 / 3125550134', referrals: candidates.map((phone, i) => row('Ejemplo ' + i, phone))
  })) });
  const { body } = await f.submit();
  assert.equal(body.hostPhone, '');
  assert.deepEqual(body.review.uncertainHostFields, ['hostPhone']);
  assert.deepEqual(body.referrals.map(r => r.phone), ['', '', '', '', candidates[4]]);
  assert.deepEqual(body.review.incompleteRows, [1, 2, 3, 4]);
});

test('overlap does not duplicate a single filled house or merge different people sharing a telephone', async () => {
  const a = { ...row('Prueba Azul', '312 555 0141'), location: 'Izquierda · fila 1' };
  const b = { ...row('Prueba Verde', '312 555 0141'), location: 'Derecha · fila 2' };
  const c = { ...row('Prueba Roja', '312 555 0142'), location: 'Derecha · fila 4' };
  const f = fixture({ fetchImpl: async () => completion(sheet({
    referrals: [a, { ...a, phone: '(312) 555-0141' }, b, c, { ...c, phone: '312 555 0143' }]
  })) });
  const { body } = await f.submit();
  assert.deepEqual(body.referrals.map(r => r.fullName), ['Prueba Azul', 'Prueba Verde', 'Prueba Roja']);
  assert.equal(body.referrals[2].phone, '');
  assert.deepEqual(body.referrals[2].uncertainFields, ['phone']);
  assert.deepEqual(body.review.incompleteRows, [3]);
});

test('a non-contact document cannot produce a host or referrals even when the model supplies text', async () => {
  const f = fixture({ fetchImpl: async () => completion(sheet({ documentType: 'unknown' })) });
  const res = await f.submit();
  assert.equal(res.statusCode, 422);
  assert.equal(res.body.documentType, 'unknown');
  assert.equal(res.body.referrals, undefined);
  assert.doesNotMatch(JSON.stringify(res.body), /Persona de prueba|Anfitriona de prueba/);
});

test('session, posted account and optional account header must agree before reading any image', async () => {
  const f = fixture();
  assert.equal((await f.submit({ session: null })).statusCode, 401);
  assert.equal((await f.submit({ session: accountB })).statusCode, 403);
  assert.equal((await f.submit({ accountId: '' })).statusCode, 403);
  assert.equal((await f.submit({ headerAccount: accountB })).statusCode, 403);
  assert.equal((await f.submit({ json: false })).statusCode, 415);
  assert.equal(f.calls.length, 0);
  for (const session of [accountA, accountB]) {
    assert.equal((await f.submit({ session, accountId: session, headerAccount: session })).statusCode, 200);
  }
  assert.equal(f.calls.length, 2);
});

test('account switched or revoked during provider processing never gets the extracted data', async () => {
  for (const revoke of [false, true]) {
    let reads = 0;
    const f = fixture({ requireActive: async (req, res) => {
      if (++reads === 1) return { user: { _id: accountA } };
      if (!revoke) return { user: { _id: accountB } };
      res.status(401).json({ error: 'Sesión de prueba revocada' });
      return null;
    } });
    const res = await f.submit();
    assert.equal(res.statusCode, revoke ? 401 : 403);
    assert.equal(f.calls.length, 1);
    assert.deepEqual(Object.keys(res.body), ['error']);
  }
});

test('uncertain fields stay empty and indices match the written rows including wholly illegible ones', async () => {
  const f = fixture({ fetchImpl: async () => completion(sheet({
    hostPhone: '(312) 555-0199', uncertainHostFields: ['hostPhone'], referrals: [
      row('  Zoë  Pérez', '+1 (312) 555-0191', 'Calle de Prueba 123'),
      { ...row('Posible apellido', '312-555-0192', 'Calle Dudosa 2'), uncertainFields: ['fullName', 'address'] },
      row('Texto de prueba', '312-555-01?9'),
      { ...row('', '', ''), uncertainFields: ['fullName', 'phone'] },
      { ...row('Otro nombre', '312-555-0189', 'calle'), uncertainFields: ['address'] }
    ]
  })) });
  const res = await f.submit();
  assert.equal(res.statusCode, 200);
  assert.equal(res.body.hostPhone, '');
  assert.deepEqual(res.body.referrals[0], row('Zoë Pérez', '+1 (312) 555-0191', 'Calle de Prueba 123'));
  assert.deepEqual(res.body.referrals[1], { ...row('', '312-555-0192', ''), uncertainFields: ['fullName', 'address'] });
  assert.equal(res.body.referrals[2].phone, '');
  assert.deepEqual(res.body.referrals[3], { ...row('', '', ''), uncertainFields: ['fullName', 'phone'] });
  assert.deepEqual(res.body.review, { incompleteRows: [2, 3, 4, 5], uncertainHostFields: ['hostPhone'], lowResolution: true, overflow: false });
});

test('both explicit overflow and unanticipated extra output warn that more contacts remain', async () => {
  for (const [count, overflow] of [[11, true], [13, false], [11, false]]) {
    const referrals = Array.from({ length: count }, (_, i) => row('Prueba ' + i));
    const f = fixture({ fetchImpl: async () => completion(sheet({ referrals, overflow })) });
    const { body } = await f.submit();
    assert.equal(body.referrals.length, 11);
    assert.equal(body.referrals.at(-1).fullName, 'Prueba 10');
    assert.equal(body.review.overflow, overflow || count > 11);
  }
});

test('invalid images never reach the provider, and unavailable OCR gives a manual alternative', async () => {
  const f = fixture();
  for (const invalid of ['https://example.test/upload', 'data:image/png;base64,' + photo, Buffer.from('<svg/>').toString('base64'), 'A'.repeat(2800000)]) {
    assert.equal((await f.submit({ image: invalid })).statusCode, 400);
  }
  assert.equal(f.calls.length, 0);
  const disabled = fixture({ apiKey: '' });
  assert.equal((await disabled.submit()).statusCode, 503);
  assert.equal(disabled.calls.length, 0);
  const unreadable = fixture({ fetchImpl: async () => completion(sheet({ hostName: '', referrals: [row('', '', '')] })) });
  assert.equal((await unreadable.submit()).statusCode, 422);
});

test('invalid or truncated responses and provider failures do not expose partial results or errors', async () => {
  for (const fetchImpl of [
    async () => completion(sheet(), 'length'),
    async () => Response.json({ choices: [{ finish_reason: 'stop', message: { content: 'sensitive-test-value' } }] }),
    async () => completion(sheet({ referrals: [null] })),
    async () => { throw new Error('sensitive-test-value'); }
  ]) {
    const res = await fixture({ fetchImpl }).submit();
    assert.equal(res.statusCode, 503);
    assert.deepEqual(Object.keys(res.body), ['error']);
    assert.doesNotMatch(res.body.error, /sensitive-test-value/);
  }
});

test('daily allowance belongs to the authenticated person, resets, and limits duplicate in-flight reads', async () => {
  let time = 1000;
  const f = fixture({ now: () => time, dailyLimit: 1 });
  assert.equal((await f.submit()).statusCode, 200);
  const limited = await f.submit();
  assert.equal(limited.statusCode, 429);
  assert.equal(limited.headers['Retry-After'], '86400');
  assert.equal((await f.submit({ session: accountB, accountId: accountB })).statusCode, 200);
  time += 86400000;
  assert.equal((await f.submit()).statusCode, 200);
  assert.equal(f.calls.length, 3);

  const started = Promise.withResolvers();
  const finish = Promise.withResolvers();
  const concurrent = fixture({ fetchImpl: async () => { started.resolve(); await finish.promise; return completion(sheet()); } });
  const first = concurrent.submit();
  await started.promise;
  assert.equal((await concurrent.submit()).statusCode, 429);
  finish.resolve();
  assert.equal((await first).statusCode, 200);
  assert.equal((await concurrent.submit()).statusCode, 200);
});

test('timed-out reads release their slot and disclose only the retry message', async () => {
  let attempts = 0;
  const f = fixture({ timeoutMs: 10, fetchImpl: async (_url, init) => {
    if (++attempts === 1) await delay(1000, null, { signal: init.signal });
    return completion(sheet());
  } });
  assert.equal((await f.submit()).statusCode, 503);
  assert.equal((await f.submit()).statusCode, 200);
});
