import { createHash } from 'node:crypto';
import { isCompletePhone, FOURTEEN_SHEET_MAX_REFERRALS } from './coach-referral-photo.js';

const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const contactKey = (name, phone) => digest([name.trim().normalize('NFC').toLocaleLowerCase('es').replace(/\s+/gu, ' '), phone]);
const invalid = () => Object.assign(new Error('Revisa las boletas seleccionadas. Cada persona necesita nombre y teléfono completo.'), { status: 422 });

export function createRaffleLeadModels(mongoose, inboxSchema) {
  const schema = inboxSchema.clone();
  schema.add({ ticketContactKey: { type: String, required: true }, ticketImportId: { type: String, required: true }, relationship: String });
  schema.index({ ownerUserId: 1, ticketContactKey: 1 }, { unique: true });
  schema.index({ ownerUserId: 1, ticketImportId: 1 });
  const receiptSchema = new mongoose.Schema({
    _id: String, fingerprint: { type: String, required: true }, createdCount: Number, duplicateCount: Number
  });
  return {
    leads: mongoose.models.CoachPersonalTicketLead || mongoose.model('CoachPersonalTicketLead', schema),
    receipts: mongoose.models.CoachTicketImport || mongoose.model('CoachTicketImport', receiptSchema)
  };
}

function readContact(row) {
  if (!row || typeof row !== 'object' || Array.isArray(row)) throw invalid();
  const contact = {};
  for (const [field, max] of Object.entries({ fullName: 120, address: 240, city: 100, relationship: 80 })) {
    const value = row[field] ?? '';
    if (typeof value !== 'string' || value.length > max || /[\u0000-\u001f<>]/u.test(value)) throw invalid();
    contact[field] = value.trim().replace(/\s+/gu, ' ');
  }
  if (!contact.fullName || typeof row.phone !== 'string' || row.phone.length > 40 || !isCompletePhone(row.phone.trim())) throw invalid();
  const digits = row.phone.replace(/\D/g, '');
  contact.phone = row.phone.trim().startsWith('+') && !digits.startsWith('1')
    ? '+' + digits : digits.replace(/^1(\d{10})$/, '$1');
  return contact;
}

export async function importRaffleLeads({ leads, receipts, findExisting }, user, body) {
  if (!body || body.reviewed !== true || !Array.isArray(body.referrals) || !body.referrals.length) throw invalid();
  if (body.referrals.length > FOURTEEN_SHEET_MAX_REFERRALS) {
    throw Object.assign(new Error(`Elige hasta ${FOURTEEN_SHEET_MAX_REFERRALS} boletas por guardado.`), { status: 422 });
  }
  if (typeof body.requestId !== 'string' || !/^[a-zA-Z0-9_-]{8,100}$/.test(body.requestId)) {
    throw Object.assign(new Error('Vuelve a abrir la revisión para identificar este guardado.'), { status: 400 });
  }
  const contacts = body.referrals.map(readContact);
  const ownerUserId = user._id;
  const importId = digest([String(ownerUserId), body.requestId]);
  const fingerprint = digest(contacts);
  // Claim this payload before creating a contact. A partial import can resume,
  // including after process restart, with stable counts for every client retry.
  let receipt;
  try {
    receipt = await receipts.findOneAndUpdate({ _id: importId }, { $setOnInsert: { fingerprint } }, { upsert: true, new: true }).lean();
  } catch (error) {
    if (error.code !== 11000) throw error;
    receipt = await receipts.findById(importId).lean();
  }
  if (!receipt) throw new Error('receipt_unavailable');
  if (receipt.fingerprint !== fingerprint) {
    throw Object.assign(new Error('Este guardado ya se usó con otra selección de boletas.'), { status: 409 });
  }
  if (typeof receipt.createdCount === 'number') return { createdCount: receipt.createdCount, duplicateCount: receipt.duplicateCount };
  const distinct = new Map();
  for (const contact of contacts) {
    const key = contactKey(contact.fullName, contact.phone);
    if (!distinct.has(key)) distinct.set(key, contact);
  }
  const previous = await findExisting(user, [...new Set(contacts.map(contact => contact.phone))]);
  const legacyKeys = new Set(previous.filter(contact => typeof contact.fullName === 'string')
    .map(contact => contactKey(contact.fullName, contact.phone)));
  for (const [ticketContactKey, contact] of distinct) {
    if (legacyKeys.has(ticketContactKey)) continue;
    const key = { ownerUserId, ticketContactKey };
    try {
      await leads.updateOne(key, { $setOnInsert: {
        ...key, ...contact, ticketImportId: importId, generatedByUserId: ownerUserId, generatedByName: user.name,
        generatedByAccountType: user.accountType, ownerName: user.name, ownerEmail: user.email,
        source: 'rifa_digital', sourceDetail: 'Boleta de rifa', status: 'nuevo', consentGiven: false,
        notes: contact.relationship ? 'Relación: ' + contact.relationship : '', updatedAt: new Date()
      } }, { upsert: true });
    } catch (error) {
      if (error.code !== 11000 || !await leads.exists(key)) throw error;
    }
  }
  const createdCount = await leads.countDocuments({ ownerUserId, ticketImportId: importId });
  const result = { createdCount, duplicateCount: contacts.length - createdCount };
  await receipts.updateOne({ _id: importId, fingerprint }, { $set: result });
  return result;
}
