import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { readFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import sharp from 'sharp';
import { COACH_REFERRAL_PHOTO_PATH, FOURTEEN_SHEET_MAX_REFERRALS, createCoachReferralPhotoHandler } from '../src/coach-referral-photo.js';

const accountA = 'aaaaaaaaaaaaaaaaaaaaaaaa';
const accountB = 'bbbbbbbbbbbbbbbbbbbbbbbb';
const testPhoto = await sharp({ create: { width: 64, height: 48, channels: 3, background: '#ffffff' } })
  .withMetadata({ density: 96 }).png().toBuffer();
const photo = testPhoto.toString('base64');
const row = (fullName = 'Persona de prueba', phone = '(312) 555-0100', address = '') =>
  ({ fullName, phone, address, uncertainFields: [] });
const sheet = (changes = {}) => ({
  hostName: 'Anfitriona de prueba', hostPhone: '', uncertainHostFields: [],
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
    hostName: 'Anfitriona de prueba', hostPhone: '',
    referrals: [{ fullName: 'Persona de prueba', phone: '(312) 555-0100', address: '' }],
    review: { incompleteRows: [], overflow: false }
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
  assert.equal(userInput.content.length, 1);
  const data = userInput.content[0].image_url.url;
  assert.ok(data.startsWith('data:image/jpeg;base64,'));
  const metadata = await sharp(Buffer.from(data.split(',')[1], 'base64')).metadata();
  assert.equal(metadata.format, 'jpeg');
  assert.equal(metadata.exif, undefined);
  assert.equal(metadata.xmp, undefined);
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
      row('', '', ''), { ...row('Otro nombre', '312-555-0189', 'calle'), uncertainFields: ['address'] }
    ]
  })) });
  const res = await f.submit();
  assert.equal(res.statusCode, 200);
  assert.equal(res.body.hostPhone, '');
  assert.deepEqual(res.body.referrals[0], { fullName: 'Zoë Pérez', phone: '+1 (312) 555-0191', address: 'Calle de Prueba 123' });
  assert.deepEqual(res.body.referrals[1], { fullName: '', phone: '312-555-0192', address: '' });
  assert.equal(res.body.referrals[2].phone, '');
  assert.deepEqual(res.body.referrals[3], { fullName: '', phone: '', address: '' });
  assert.deepEqual(res.body.review, { incompleteRows: [2, 3, 4, 5], overflow: false });
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
