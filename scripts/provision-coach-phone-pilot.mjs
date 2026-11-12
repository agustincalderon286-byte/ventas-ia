// Operator-only: run inside the existing TEC service. Never exports DB secrets.
// Usage: node scripts/provision-coach-phone-pilot.mjs inspect|apply PHONE PARENT_EMAIL
import mongoose from 'mongoose';
import crypto from 'node:crypto';
import { promisify } from 'node:util';
import { parseCoachLogin } from '../src/tec-coach-login.js';

const [mode = 'inspect', phone, parentEmail = ''] = process.argv.slice(2);
let cookie = '';
const base = 'http://127.0.0.1:' + (process.env.PORT || 10000);
try {
  const identity = parseCoachLogin(phone);
  if (!['inspect', 'apply'].includes(mode) || identity?.field !== 'username' || !parentEmail.includes('@')) throw Error('Argumentos inválidos.');
  await mongoose.connect(process.env.MONGODB_URI);
  const users = mongoose.connection.collection('coachusers');
  const parent = await users.findOne({ email: parentEmail.trim().toLowerCase() });
  const owner = parent?.teamOwnerUserId && await users.findOne({ _id: parent.teamOwnerUserId });
  const permanent = (process.env.COACH_TEST_ACCESS_EMAILS || '').split(/[\n,;]+/).map(x => x.trim().toLowerCase());
  if (!parent || parent.seatStatus !== 'active' || !parent.teamSupervisionEnabled || !owner || owner.accountType !== 'owner' || !permanent.includes(owner.email)) throw Error('El equipo no corresponde a un piloto permanente con supervisión activa.');
  if (await users.findOne({ username: identity.value })) throw Error('El usuario ya existe; no se modificó su contraseña.');
  if (mode === 'inspect') {
    console.log(JSON.stringify({ ready: true, username: identity.value, parent: parent.name, teamOwner: owner.name, permanentPilot: true, sellerOnly: true, emailInvented: false }));
  } else {
    // Build the replacement unique index BEFORE retiring the legacy email index.
    // This permits multiple accounts without an email while preserving uniqueness
    // of every real email, including concurrent signup requests.
    await users.createIndex({ email: 1 }, { name: 'coach_email_present_unique', unique: true, partialFilterExpression: { email: { $type: 'string' } } });
    await users.createIndex({ username: 1 }, { name: 'coach_username_unique', unique: true, partialFilterExpression: { username: { $type: 'string' } } });
    const indexes = await users.indexes();
    const legacy = indexes.find(index => index.name === 'email_1');
    if (legacy) {
      if (legacy.unique !== true || legacy.key.email !== 1 || Object.keys(legacy.key).length !== 1) throw Error('El índice anterior requiere revisión.');
      await users.dropIndex(legacy.name);
    }
    const password = 'Casa-' + crypto.randomBytes(9).toString('base64url') + '!';
    const salt = crypto.randomBytes(16).toString('hex');
    const hash = (await promisify(crypto.scrypt)(password, salt, 64)).toString('hex');
    const now = new Date(), _id = new mongoose.Types.ObjectId();
    const name = 'Vendedora · ' + identity.value.slice(-4);
    await users.insertOne({ _id, name, username: identity.value, passwordHash: hash, passwordSalt: salt,
      accountType: 'seat', teamSupervisionEnabled: false, parentUserId: parent._id,
      teamOwnerUserId: owner._id, billingOwnerUserId: owner._id,
      sponsorUserId: parent.sponsorUserId || owner.sponsorUserId || null,
      sponsorName: parent.sponsorName || owner.sponsorName || '',
      sponsorCommissionRate: Number(parent.sponsorCommissionRate || owner.sponsorCommissionRate || 0),
      seatStatus: 'active', officeId: parent.officeId || owner.officeId || '', territoryId: parent.territoryId || owner.territoryId || '',
      subscriptionStatus: 'team_access', subscriptionActive: false, subscriptionCurrentPeriodEnd: null,
      subscriptionCancelAtPeriodEnd: false, stripeSyncRevision: 0, createdAt: now, updatedAt: now });
    console.log('CREDENCIALES_NUEVA_CUENTA ' + JSON.stringify({ username: identity.value, password, name, login: 'https://totalelitecoaching.com/coach/login/' }));
    const response = await fetch(base + '/api/coach/login', { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: 'https://totalelitecoaching.com' }, body: JSON.stringify({ email: phone, password }), signal: AbortSignal.timeout(30000) });
    const data = await response.json();
    cookie = (response.headers.getSetCookie?.() || []).map(value => value.split(';')[0]).find(value => value.startsWith('agustin_coach_session=')) || '';
    if (response.status !== 200 || data.user?.id !== String(_id) || !data.user.accessGranted || !cookie || data.user.accountType !== 'seat' || data.user.canViewControlTower !== false || data.user.canViewTeam !== false || data.user.storageIdentity !== 'coach-id:' + _id) throw Error('Cuenta creada; falta verificar el acceso.');
    const checks = [];
    for (const [path, expected] of [['/coach/app/', 200], ['/api/coach/leads', 200], ['/api/control/overview', 403], ['/api/chef/admin', 403], ['/api/coach/team', 403]]) {
      const r = await fetch(base + path, { redirect: 'manual', headers: { Cookie: cookie }, signal: AbortSignal.timeout(30000) });
      await r.arrayBuffer(); checks.push({ path, status: r.status, expected });
      if (r.status !== expected) throw Error('Cuenta creada; falta verificar los permisos.');
    }
    console.log('VERIFICACION_PILOTO ' + JSON.stringify({ ok: true, username: identity.value, parent: parent.name, teamOwner: owner.name, noExpiry: true, noCharge: true, admin: false, checks }));
  }
} catch (error) {
  const safe = /^(Argumentos|El equipo|El usuario|El índice|Cuenta creada)/.test(error.message || '');
  console.error(safe ? error.message : error.code === 11000 ? 'Usuario duplicado; no se sobrescribió ninguna cuenta.' : 'No se completó la operación. No se muestran datos de conexión.');
  process.exitCode = 1;
} finally {
  if (cookie) try { await fetch(base + '/api/coach/logout', { method: 'POST', headers: { Cookie: cookie, 'Content-Type': 'application/json', Origin: 'https://totalelitecoaching.com' }, body: '{}', signal: AbortSignal.timeout(10000) }); } catch {}
  await mongoose.disconnect();
}
