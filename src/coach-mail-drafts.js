import { createHash, randomBytes } from 'node:crypto';
import mongoose from 'mongoose';
import { isGmailAddress, openToken, sealToken } from './coach-hycite-gmail.js';

import { validatedThread } from './coach-gmail-thread.js';

const minute = 60_000;
const draftLifetime = 30 * minute;
const resultLifetime = 24 * 60 * minute;

const draftSchema = new mongoose.Schema({
  _id: { type: String, required: true },
  owner: { type: String, required: true, index: true },
  sessionDigest: { type: String, required: true },
  grantDigest: { type: String, required: true },
  state: { type: String, required: true },
  content: String,
  revisionCode: { type: String, unique:true, sparse:true },
  deadline: { type: Date, required: true },
  removeAfter: { type: Date, required: true, expires: 0 },
  claimedAt: Date,
  messageId: String
}, { versionKey: false, writeConcern: { w: 'majority' } });

const Store = mongoose.models.CoachMailDraft || mongoose.model('CoachMailDraft', draftSchema);
const hash = value => createHash('sha256').update(value).digest('base64url');

function ownerAndGrant({ owner, grant }) {
  const account = owner instanceof mongoose.Types.ObjectId ? owner.toHexString() : owner;
  if (typeof account !== 'string' || !account || typeof grant !== 'string' || !grant) {
    throw new Error('Falta una conexión de Gmail activa.');
  }
  return { owner: account, grantDigest: hash(grant) };
}

function scope(input) {
  if (typeof input.session !== 'string' || !input.session) throw new Error('Falta la sesión del vendedor.');
  return { ...ownerAndGrant(input), sessionDigest: hash(input.session) };
}

function findDraft(input) {
  if (typeof input.id !== 'string' || !/^[0-9a-f]{12}$/i.test(input.id)) return null;
  return { _id: input.id.toUpperCase(), ...scope(input) };
}

function resultOf(doc, now) {
  if (!doc) return { status: 'unavailable' };
  if (doc.state === 'sent') return { status: 'sent', messageId: doc.messageId };
  if (doc.state === 'pending' && Number(doc.deadline) <= now) return { status: 'unavailable' };
  // If the process stopped during the API call we cannot tell whether Gmail accepted it.
  if (doc.state === 'sending' && now - Number(doc.claimedAt) >= 2 * minute) return { status: 'unknown' };
  return { status: doc.state };
}

function messageFields({ to, subject, body, thread }) {
  if (!isGmailAddress(to)) {
    throw new Error('Indica una sola dirección de correo válida.');
  }
  if (typeof subject !== 'string' || (!thread && !subject.trim()) || subject.length > (thread ? 900 : 200) || /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/u.test(subject)) {
    throw new Error('Escribe un asunto de hasta 200 caracteres en una línea.');
  }
  if (typeof body !== 'string' || !body.trim() || body.length > 12000 || /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(body)) {
    throw new Error('Escribe un mensaje de hasta 12 000 caracteres.');
  }
  return { ...(thread ? {thread:validatedThread(thread)} : {}), to, subject: subject.trim(), body: body.replace(/\r\n?/g, '\n').trim() };
}

export function createMailDrafts({ store = Store, key = Buffer.from(process.env.COACH_GMAIL_ENCRYPTION_KEY || '', 'base64'), now = Date.now } = {}) {
  const binding = doc => JSON.stringify([doc.owner, doc.sessionDigest, doc.grantDigest, doc._id]);
  const decode = doc => JSON.parse(openToken(doc.content, key, binding(doc)));
  const lookup = filter => store.findOne(filter).lean();

  // The original ID remains the encryption binding. Only the code of the displayed revision
  // may authorize a later send, including for drafts created before revision codes existed.
  async function locate(input) {
    const original = findDraft(input);
    if (!original) return null;
    const { _id:code, ...account } = original;
    const doc = await lookup(original) || await lookup({ ...account, revisionCode:code });
    return doc && (doc.revisionCode || doc._id) === code ? doc : null;
  }
  const current = doc => ({ _id:doc._id, state:'pending', content:doc.content, deadline:{ $gt:new Date(now()) } });
  async function finish(filter, state, messageId) {
    await store.updateOne({ ...filter, state: 'sending' }, {
      $set: { state, ...(messageId ? { messageId } : {}) }, $unset: { content: 1 }
    });
  }

  return {
    async prepare(input) {
      const account = scope(input);
      const message = messageFields(input);
      const time = now();
      // Codes are identifiers; authorization also requires the matching owner, session and grant.
      for (let attempt = 0; attempt < 3; attempt++) {
        const id = randomBytes(6).toString('hex').toUpperCase();
        const filter = { _id: id, ...account };
        try {
          await store.create({ ...filter, revisionCode:id, state: 'pending', content: sealToken(JSON.stringify(message), key, binding(filter)),
            deadline: new Date(time + draftLifetime), removeAfter: new Date(time + draftLifetime) });
          return { id, ...message };
        } catch (error) {
          if (error.code !== 11000 || attempt === 2) throw error;
        }
      }
    },
    async view(input) {
      const doc = await locate(input);
      const status = resultOf(doc, now());
      return status.status === 'pending' ? { ...status, id: doc.revisionCode || doc._id, ...decode(doc) } : status;
    },
    async revise(input) {
      const doc = await locate(input);
      const status = resultOf(doc, now());
      if (status.status !== 'pending') return status;
      const prior = decode(doc);
      const patch = input.changes || {};
      if (prior.thread && ((patch.to !== undefined && patch.to !== prior.to) || (patch.subject !== undefined && patch.subject !== prior.subject))) {
        throw new Error('Una respuesta conserva el destinatario y el asunto del hilo.');
      }
      const message = messageFields({ ...prior, to:patch.to ?? prior.to, subject:patch.subject ?? prior.subject, body:patch.body ?? prior.body });
      const time = now();
      for (let attempt = 0; attempt < 3; attempt++) {
        const id = randomBytes(6).toString('hex').toUpperCase();
        if (id === (doc.revisionCode || doc._id)) continue;
        try {
          const changed = await store.updateOne(current(doc), { $set:{
            revisionCode:id, content:sealToken(JSON.stringify(message), key, binding(doc)),
            deadline:new Date(time + draftLifetime), removeAfter:new Date(time + draftLifetime)
          } });
          return changed.matchedCount ? { status:'pending', id, ...message } : resultOf(await locate(input), now());
        } catch (error) { if (error.code !== 11000 || attempt === 2) throw error; }
      }
      throw new Error('Vuelve a intentar la corrección.');
    },
    async cancel(input) {
      const doc = await locate(input);
      const time = now();
      const status = resultOf(doc, time);
      if (status.status !== 'pending') return status;
      const result = await store.updateOne(current(doc), {
        $set: { state: 'cancelled', removeAfter: new Date(time + resultLifetime) }, $unset: { content: 1 }
      });
      return result.matchedCount ? { status: 'cancelled' } : resultOf(await locate(input), time);
    },
    async confirm(input) {
      const found = await locate(input);
      const time = now();
      const previous = resultOf(found, time);
      if (previous.status !== 'pending') return previous;
      if (typeof input.deliver !== 'function') throw new Error('Falta el envío de Gmail.');
      // Comparing the sealed content makes confirmation, cancellation and revision compete
      // for the exact version the seller reviewed.
      const doc = await store.findOneAndUpdate(current(found), {
        $set: { state: 'sending', claimedAt: new Date(time), removeAfter: new Date(time + resultLifetime) }, $unset: { content: 1 }
      }, { new: false }).lean();
      if (!doc) return resultOf(await locate(input), now());

      let status = 'failed';
      let messageId;
      try {
        const message = decode(doc);
        status = 'unknown';
        const receipt = await input.deliver(message);
        if (typeof receipt?.id === 'string' && receipt.id) {
          messageId = receipt.id;
          status = 'sent';
        }
      } catch (error) {
        if (error?.beforeSend === true || error?.providerRejected === true) status = 'failed';
      }
      // The receipt from Gmail still establishes delivery if persisting it temporarily fails.
      // The original claim blocks a second call even across restarts.
      await finish({ _id:doc._id }, status, messageId).catch(() => {});
      return messageId ? { status, messageId } : { status };
    },
    async purge(input) {
      await store.deleteMany(ownerAndGrant(input));
    }
  };
}
