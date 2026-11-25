import { createHash, randomBytes } from 'node:crypto';
import mongoose from 'mongoose';
import { openToken, sealToken } from './coach-hycite-gmail.js';
import { isCompletePhone } from './coach-referral-photo.js';
import { createSheetsGoogle } from './coach-sheets-google.js';

const minute = 60_000;
const leaseMs = 5 * minute; // Exceeds the Sheets API's 180-second maximum processing time.
const hash = value => createHash('sha256').update(value).digest('hex');
const fail = (status, message, options = {}) => Object.assign(new Error(message), { status, ...options });
const keyForName = text => text.normalize('NFD').replace(/\p{M}+/gu, '').toLocaleLowerCase('es')
  .replace(/[^\p{L}\p{N}]+/gu, ' ').trim();

export function sheetsPhoneKey(value) {
  const raw = String(value ?? '').trim();
  if (!isCompletePhone(raw)) return '';
  const digits = raw.replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('1')) return digits.slice(1);
  return raw.startsWith('+') ? '+' + digits : digits;
}

function textField(value, limit) {
  if (value == null) return '';
  if (typeof value !== 'string' || value.length > limit || /[\u0000-\u001f\u007f]/u.test(value)) {
    throw fail(422, 'Revisa los campos del lote. Cada dato debe ocupar una sola línea.');
  }
  return value.trim().replace(/\s+/gu, ' ');
}

function cleanInput(input, sellerName) {
  if (!['tickets','referrals'].includes(input?.kind) || !Array.isArray(input.referrals) ||
      !input.referrals.length || input.referrals.length > 11) throw fail(422, 'Selecciona entre 1 y 11 contactos para revisar.');
  const referrals = input.referrals.map(row => {
    const clean = {};
    for (const [field, limit] of Object.entries({ fullName:120, phone:40, address:240, city:100, relationship:80 })) {
      clean[field] = textField(row?.[field], limit);
    }
    if (input.kind === 'referrals' && row?.note) clean.note = textField(row.note, 640);
    if (!keyForName(clean.fullName) || !sheetsPhoneKey(clean.phone)) throw fail(422, 'Cada contacto necesita nombre y teléfono completo.');
    return clean;
  });
  const hostName = input.kind === 'referrals' ? textField(input.hostName, 120) : '';
  const hostPhone = input.kind === 'referrals' ? textField(input.hostPhone, 40) : '';
  if (input.kind === 'referrals' && !keyForName(hostName)) throw fail(422, 'Escribe el anfitrión del 4 en 14 para conservar su grupo.');
  if (hostPhone && !sheetsPhoneKey(hostPhone)) throw fail(422, 'Revisa el teléfono del anfitrión.');
  let startDate = '';
  if (input.kind === 'referrals' && input.startDate) {
    startDate = textField(input.startDate, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(startDate) || !Number.isFinite(Date.parse(startDate)) ||
        new Date(startDate).toISOString().slice(0,10) !== startDate) throw fail(422, 'Revisa la fecha de inicio del 4 en 14.');
  }
  return { kind:input.kind, referrals, hostName, hostPhone, startDate, sellerName:textField(sellerName,120) };
}

function findMatches(input, snapshot) {
  const byPhone = new Map(), byName = new Map();
  function add(record) {
    const name = keyForName(record.fullName);
    const phones = new Set(String(record.phone).split(/[,;/|\n]|\s+(?:o|y)\s+/i).map(sheetsPhoneKey).filter(Boolean));
    const entry = { ...record, name, phones };
    if (name) { if (!byName.has(name)) byName.set(name, []); byName.get(name).push(entry); }
    for (const phone of phones) {
      if (!byPhone.has(phone)) byPhone.set(phone, []);
      byPhone.get(phone).push(entry);
    }
  }
  for (const record of snapshot.contacts) add(record);
  return input.referrals.map((contact, index) => {
    const name = keyForName(contact.fullName), phone = sheetsPhoneKey(contact.phone);
    const related = new Set([...(byPhone.get(phone) || []), ...(byName.get(name) || [])]);
    let exact = false;
    const matches = [];
    for (const entry of related) {
      const reason = entry.name === name && entry.phones.has(phone) ? 'name_phone' : entry.phones.has(phone) ? 'phone' : 'name';
      if (reason === 'name_phone') exact = true;
      matches.push({ sheetTitle:entry.sheetTitle, row:entry.row, reason, ...(entry.batch && { batch:true }),
        ...(Number.isInteger(entry.sheetId) && { sheetId:entry.sheetId }), fullName:entry.fullName });
    }
    add({ ...contact, sheetTitle:'Este lote', row:index+1, batch:true });
    return { index, fullName:contact.fullName, phone:contact.phone,
      status:exact ? 'duplicate' : matches.length ? 'possible' : 'new', matchCount:matches.length, matches:matches.slice(0,12),
      matchProof:hash(JSON.stringify(matches)) };
  });
}

const draftSchema = new mongoose.Schema({ _id:String, owner:String, session:String, grant:String, file:String,
  payload:String, selection:String, written:Number, sheetTitle:String, expiresAt:{ type:Date, expires:0 }
}, { versionKey:false, writeConcern:{ w:'majority' } });
const leaseSchema = new mongoose.Schema({ _id:String, nonce:String, job:String, until:Date }, { versionKey:false, writeConcern:{ w:'majority' } });
const Draft = mongoose.models.CoachSheetsPreview || mongoose.model('CoachSheetsPreview', draftSchema);
const Lease = mongoose.models.CoachSheetsFileLease || mongoose.model('CoachSheetsFileLease', leaseSchema);

function mongoStore() {
  return {
    create: doc => Draft.create(doc),
    get: filter => Draft.findOne(filter).lean(),
    async bind(filter, choice) {
      return Boolean(await Draft.findOneAndUpdate({ ...filter, $or:[{ selection:null }, { selection:choice }] },
        { $set:{ selection:choice } }).lean());
    },
    finish: (filter, written) => Draft.updateOne(filter, { $set:{ written }, $unset:{ payload:1 } }),
    async lock(file, nonce, now, job) {
      try {
        return Boolean(await Lease.findOneAndUpdate({ _id:file, until:{ $lte:new Date(now) } },
          { $set:{ nonce, job, until:new Date(now + leaseMs) } }, { upsert:true, new:true }).lean());
      } catch (error) { if (error.code === 11000) return false; throw error; }
    },
    owns: (file, nonce, now) => Lease.exists({ _id:file, nonce, until:{ $gt:new Date(now + 30000) } }),
    unlockJob: (file, job) => Lease.deleteOne({ _id:file, job }),
    unlock: (file, nonce) => Lease.deleteOne({ _id:file, nonce })
  };
}

function scope(ctx) {
  if (!ctx?.owner || !ctx.session || !ctx.access || !ctx.grant || !ctx.workbook?.spreadsheetId) {
    throw fail(403, 'Conecta tu cuenta de Google Sheets y elige un archivo.');
  }
  return { owner:String(ctx.owner), session:hash(String(ctx.session)), grant:hash(String(ctx.grant)), file:ctx.workbook.spreadsheetId };
}
const aad = d => JSON.stringify([d._id, d.owner, d.session, d.grant, d.file]);
function pick(input, size) {
  const selected = input?.selected;
  if (!Array.isArray(selected) || !selected.length || selected.length > 11 ||
      selected.some(i => !Number.isInteger(i) || i < 0 || i >= size) || new Set(selected).size !== selected.length) {
    throw fail(422, 'Elige los contactos que quieres guardar en la vista previa.');
  }
  return [...selected].sort((a,b) => a-b);
}

export function createSheetsTransfer({ google = createSheetsGoogle(), store = mongoStore(),
    key = Buffer.from(process.env.COACH_GMAIL_ENCRYPTION_KEY || '', 'base64'), now = Date.now } = {}) {
  async function check(ctx) {
    if (typeof ctx.isCurrent !== 'function' || !await ctx.isCurrent()) throw fail(409, 'La conexión cambió. Abre una nueva vista previa.');
  }
  async function snapshot(ctx) {
    await check(ctx);
    return google.read(ctx.access, ctx.workbook);
  }
  async function prepare(ctx, input, state) {
    const rows = findMatches(input, state);
    const target = state[input.kind];
    const filter = { _id:randomBytes(16).toString('hex'), ...scope(ctx) };
    const signature = JSON.stringify([target, rows.map(r => [r.status, r.matchProof])]);
    await store.create({ ...filter, sheetTitle:target.title, payload:sealToken(JSON.stringify({input, signature}), key, aad(filter)),
      expiresAt:new Date(now()+30*minute) });
    return { previewId:filter._id, kind:input.kind, title:state.title, sheetTitle:target.title,
      hostName:input.hostName, hostPhone:input.hostPhone, rows:rows.map(({matchProof, ...visible}) => visible) };
  }
  function receipt(id, written, sheetTitle) { return { previewId:id, sheetTitle, written, addedCount:written, createdCount:written }; }
  return {
    async preview(ctx, body) {
      scope(ctx);
      const input = cleanInput(body, ctx.sellerName);
      return prepare(ctx, input, await snapshot(ctx));
    },
    async confirm(ctx, body) {
      const filter = { ...scope(ctx), _id:body?.previewId };
      if (typeof filter._id !== 'string' || !/^[a-f0-9]{32}$/.test(filter._id)) throw fail(400, 'Abre una vista previa válida.');
      const nonce = randomBytes(16).toString('hex');
      const doc = await store.get(filter);
      if (!doc || Number(doc.expiresAt) <= now()) throw fail(409, 'La vista previa venció o pertenece a otra conexión. Prepárala de nuevo.');
      const selected = pick(body, 11);
      const choice = selected.join(',');
      if (doc.selection != null && doc.selection !== choice) throw fail(409, 'Esta revisión ya se confirmó con otra selección.');
      if (doc.written != null) return receipt(doc._id, doc.written, doc.sheetTitle);
      const { input, signature } = JSON.parse(openToken(doc.payload, key, aad(doc)));
      pick(body, input.referrals.length);
      await check(ctx);
      // A shared spreadsheet is one write destination, even when two sellers use separate OAuth grants.
      const marker = 'TEC:' + doc._id + ':' + choice;
      if (!await store.lock(filter.file, nonce, now(), doc._id)) {
        if (doc.selection === choice && (await snapshot(ctx)).notes.has(marker)) {
          await store.finish(filter, selected.length);
          await store.unlockJob(filter.file, doc._id).catch(() => {});
          return receipt(doc._id, selected.length, doc.sheetTitle);
        }
        throw fail(409, 'Hay un guardado en curso en esta hoja. Reintenta la misma revisión.', { retryable:true });
      }
      let release = true;
      try {
        const current = await store.get(filter);
        if (current?.selection != null && current.selection !== choice) throw fail(409, 'Esta revisión ya se confirmó con otra selección.');
        if (current?.written != null) return receipt(doc._id, current.written, current.sheetTitle);
        const state = await snapshot(ctx);
        // The note is appended atomically with its lead rows. It survives an API timeout or a failed Mongo acknowledgement.
        if (current?.selection === choice && state.notes.has(marker)) {
          await store.finish(filter, selected.length);
          return receipt(doc._id, selected.length, doc.sheetTitle);
        }
        const rows = findMatches(input, state);
        const target = state[input.kind];
        if (signature !== JSON.stringify([target, rows.map(r => [r.status, r.matchProof])])) {
          const preview = await prepare(ctx, input, state);
          throw fail(409, 'La hoja cambió. Revisa los duplicados y vuelve a confirmar.', { preview });
        }
        if (selected.some(i => rows[i].status === 'duplicate')) throw fail(422, 'Quita los duplicados exactos antes de guardar.');
        if (!await store.bind(filter, choice)) throw fail(409, 'Esta revisión ya se confirmó con otra selección.');
        await check(ctx);
        if (!await store.owns(filter.file, nonce, now())) throw fail(409, 'El turno de guardado venció. Reintenta.', { retryable:true });
        try { await google.append(ctx.access, ctx.workbook, target, input, selected, marker); }
        catch (error) {
          // In an ambiguous response the remote write may still be executing. Keep the lease until its safe expiry.
          if (!error.definite) release = false;
          throw error;
        }
        await store.finish(filter, selected.length);
        return receipt(doc._id, selected.length, doc.sheetTitle);
      } finally {
        if (release) await store.unlock(filter.file, nonce).catch(() => {});
      }
    }
  };
}
