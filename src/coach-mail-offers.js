import {createHmac, randomBytes} from 'node:crypto';
import mongoose from 'mongoose';
import {followupLimits} from './coach-followup-store.js';
import {isGmailAddress} from './coach-mail-address.js';
import {openToken, sealToken} from './coach-hycite-gmail.js';

// An offer exists only after Gmail accepted the reply with the two options. It holds
// identifiers and the offered times, never message bodies, and it reserves nothing.
const minute = 60000;
const day = 24 * 60 * minute;
const leaseMs = 2 * minute;
const schema = new mongoose.Schema({
  _id:String, owner:{type:String,required:true}, gmailGrant:String, calendarGrant:String, calendar:String,
  state:{type:String,required:true}, phase:String, content:String, ref:String,
  choice:{code:String,index:Number,replyId:String},
  lease:String, leaseUntil:Date, confirmedAt:Date, attempts:Number, result:String, current:String,
  deadline:Date, removeAfter:{type:Date,expires:0}
}, {versionKey:false, minimize:true, writeConcern:{w:'majority'}});
schema.index({owner:1,state:1,deadline:1});
schema.index({owner:1,'choice.code':1});
const Model = mongoose.models.CoachMailOffer || mongoose.model('CoachMailOffer', schema);

const iso = /^\d{4}-\d\d-\d\dT\d\d:\d\d(?::\d\d(?:\.\d{1,3})?)?Z$/;
// Offer IDs and choice codes carry a short keyed tag, so the chat can tell them apart from
// Calendar search IDs and proposal codes without a database lookup.
const tag = (key, kind, body, size) => createHmac('sha256', key).update('coach-offer-tag:' + kind + ':' + body).digest('hex').slice(0, size);
export const isOfferId = (id, key) => typeof id === 'string' && /^[a-f0-9]{32}$/.test(id) && (!key || tag(key, 'offer', id.slice(0, 26), 6) === id.slice(26));
export const newOfferId = key => { const body = randomBytes(13).toString('hex'); return body + tag(key, 'offer', body, 6); };
export const isOfferCode = (code, key) => typeof code === 'string' && /^[A-F0-9]{12}$/.test(code) && tag(key, 'code', code.slice(0, 9), 3).toUpperCase() === code.slice(9);
const newOfferCode = key => { const body = randomBytes(5).toString('hex').slice(0, 9).toUpperCase(); return body + tag(key, 'code', body, 3).toUpperCase(); };
export const offerDigest = (key, value) => createHmac('sha256', key).update('coach-offer:' + String(value)).digest('base64url');
// Derived from the offer, not from a login or tab: every retry and session writes the same Google event.
export const offerEventId = (key, id) => createHmac('sha256', key).update('coach-offer-event:' + id).digest('hex').slice(0, 32);
export const offerOperationId = id => 'offer' + id.slice(0, 24);

export function validatedOffer(offer) {
  const fail = () => { throw new Error('La oferta de horarios no es válida.'); };
  if (!offer || !isOfferId(offer.id)) fail();
  const slots = offer.slots;
  if (!Array.isArray(slots) || slots.length !== 2) fail();
  for (const slot of slots) {
    if (!iso.test(slot?.start || '') || !iso.test(slot?.end || '') || !(Date.parse(slot.start) < Date.parse(slot.end))) fail();
  }
  if (!Array.isArray(offer.labels) || offer.labels.length !== 2 || !offer.labels.every(l => typeof l === 'string' && l && l.length <= 120)) fail();
  if (typeof offer.timeZone !== 'string' || !offer.timeZone || offer.timeZone.length > 80) fail();
  new Intl.DateTimeFormat('en', {timeZone:offer.timeZone});
  if (typeof offer.sourceMessageId !== 'string' || !/^[a-z0-9_-]{1,128}$/i.test(offer.sourceMessageId)) fail();
  if (!isGmailAddress(offer.prospect) || Date.parse(slots[0].end) > Date.parse(slots[1].start)) fail();
  if (typeof offer.calendarGrant !== 'string' || !offer.calendarGrant || offer.calendarGrant.length > 100) fail();
  if (offer.leadId && !/^[a-f0-9]{24}$/i.test(offer.leadId)) fail();
  if (offer.leadName != null && (typeof offer.leadName !== 'string' || offer.leadName.length > 120)) fail();
  if (offer.expectedStart != null && !Number.isFinite(Date.parse(offer.expectedStart))) fail();
  if (!Number.isInteger(offer.durationMinutes) || offer.durationMinutes < 1 || offer.durationMinutes > 480) fail();
  return {id:offer.id, slots:slots.map(s => ({start:s.start, end:s.end})), labels:[...offer.labels], timeZone:offer.timeZone,
    sourceMessageId:offer.sourceMessageId, prospect:offer.prospect.toLowerCase(), calendarGrant:offer.calendarGrant,
    leadId:offer.leadId || '', leadName:offer.leadName || '', expectedStart:offer.expectedStart ?? null, durationMinutes:offer.durationMinutes};
}

export function createMailOffers({key, store = Model, now = Date.now, rememberEvent}) {
  const scope = b => ({owner:String(b.owner), gmailGrant:offerDigest(key, b.gmailGrant), calendarGrant:offerDigest(key, b.calendarGrant),
    calendar:offerDigest(key, b.calendarId)});
  const aad = (row, kind) => 'offer:' + row._id + ':' + row.owner + ':' + row.gmailGrant + ':' + row.calendarGrant + ':' + kind;
  const seal = (row, kind, data) => sealToken(JSON.stringify(data), key, aad(row, kind));
  const open = (row, kind, value) => JSON.parse(openToken(value, key, aad(row, kind)));
  const view = row => row && {...(row.content ? open(row, 'offer', row.content) : {}), id:row._id, state:row.state, phase:row.phase || '', ref:row.ref,
    choice:row.choice?.code ? {...row.choice} : null, deadline:row.deadline,
    leaseActive:Boolean(row.leaseUntil && Number(row.leaseUntil) > now()),
    result:row.result ? open(row, 'result', row.result) : null};
  const read = async q => {
    const row = await store.findOne(q).lean();
    try { return view(row); } catch { return null; }
  };

  return {
    newId: () => newOfferId(key),
    isId: id => isOfferId(id, key),
    isCode: code => isOfferCode(code, key),
    // Called once Gmail returned the sent message ID. The draft carries the durable offer ID,
    // so a repeated record after a lost write is idempotent.
    async record(b, offer, {threadId, sentMessageId}) {
      const clean = validatedOffer(offer);
      if (offerDigest(key, b.calendarGrant) !== clean.calendarGrant) throw new Error('La conexión de Calendar cambió.');
      if (!/^[a-z0-9_-]{1,128}$/i.test(threadId || '') || !/^[a-z0-9_-]{1,128}$/i.test(sentMessageId || '')) throw new Error('Falta el hilo.');
      const time = now();
      const last = Math.min(time + 7 * day, Math.max(...clean.slots.map(s => Date.parse(s.start))));
      const row = {_id:clean.id, ...scope(b), ref:clean.id.slice(0, 8).toUpperCase()};
      const {calendarGrant: _ignored, ...content} = clean;
      try {
        await store.create({...row, state:'offered', attempts:0, content:seal(row, 'offer', {...content, threadId, sentMessageId}),
          deadline:new Date(last), removeAfter:new Date(Math.max(last, time) + 2 * day)});
      } catch (error) {
        if (error.code !== 11000) throw error;
      }
      return {id:clean.id, ref:row.ref};
    },
    async active(b, {limit = 6,ref} = {}) {
      const rows = await store.find({...scope(b), ...(ref && {ref}),$or:[{state:'offered',deadline:{$gt:new Date(now())}},{state:{$in:['booking','booked']}}],removeAfter:{$gt:new Date(now())}})
        .sort({deadline:-1}).limit(limit).lean();
      return rows.map(row => { try { return view(row); } catch { return null; } }).filter(Boolean);
    },
    get: async (b, id) => isOfferId(id, key) ? read({_id:id, ...scope(b)}) : null,
    byCode: async (b, code) => isOfferCode(code, key) ? read({...scope(b), 'choice.code':code}) : null,
    // The seller's decision for one specific reply. The same decision keeps its code across tabs.
    async choose(b, id, {index, replyId}) {
      const offer = await read({_id:id, ...scope(b)});
      if (!offer || offer.state !== 'offered' || ![0, 1].includes(index) || !/^[a-z0-9_-]{1,128}$/i.test(replyId || '')) return null;
      if (offer.choice?.index === index && offer.choice.replyId === replyId) return {code:offer.choice.code};
      const code = newOfferCode(key);
      const changed = await store.updateOne({_id:id, ...scope(b), state:'offered', deadline:{$gt:new Date(now())}},
        {$set:{choice:{code, index, replyId}}});
      return changed.matchedCount ? {code} : null;
    },
    async discard(b, code) {
      const result = await store.updateOne({...scope(b), 'choice.code':code, state:'offered'}, {$unset:{choice:1}});
      return result.matchedCount ? 'discarded' : 'unavailable';
    },
    async book(b, code, {saveEvent, syncLead}) {
      const time = now();
      const base = {...scope(b), 'choice.code':code};
      const lease = randomBytes(12).toString('hex');
      const lock = {lease, leaseUntil:new Date(time + leaseMs)};
      let row = await store.findOneAndUpdate({...base, state:'offered', deadline:{$gt:new Date(time)}},
        {$set:{...lock, state:'booking', phase:'google', confirmedAt:new Date(time)}, $inc:{attempts:1}}, {new:false}).lean();
      if (!row) {
        // An interrupted confirmation can resume within a day; afterwards it is closed, not kept forever.
        row = await store.findOneAndUpdate({...base, state:'booking', leaseUntil:{$lte:new Date(time)}, deadline:{$gt:new Date(time - day)}},
          {$set:lock, $inc:{attempts:1}}, {new:false}).lean();
      }
      if (!row) {
        const prior = await read(base);
        if (prior?.state === 'booked' && prior.result) return {...prior.result, already:true};
        if (prior?.state === 'booking') {
          if (prior.leaseActive) return {status:'working'};
          await store.updateOne({...base, state:'booking'}, {$set:{state:'closed'}, $unset:{content:1, current:1}});
        }
        return {status:'unavailable'};
      }
      const offer = view({...row, result:undefined});
      const guard = {_id:row._id, lease};
      const startedAt = row.state === 'booking' ? Number(row.confirmedAt || 0) : time;
      const slot = offer.slots[row.choice.index];
      const eventId = offerEventId(key, row._id), operationId = offerOperationId(row._id);
      const proposal = {summary:'Demo · ' + (offer.leadName || offer.prospect), start:slot.start, end:slot.end, timeZone:offer.timeZone};
      let receipt = row.phase === 'crm' && row.current ? open(row, 'event', row.current) : null;
      try {
        if (!receipt) {
          const reconcileOnly = row.state === 'booking' && (now()-startedAt >= followupLimits.googleMs || Date.parse(slot.start) <= now());
          if (Date.parse(slot.start) <= now() && !reconcileOnly) {
            await store.updateOne(guard, {$set:{state:'offered'}, $unset:{choice:1, lease:1, leaseUntil:1, phase:1}});
            return {status:'past'};
          }
          const response = await saveEvent({...proposal, owner:b.owner, grant:b.calendarGrant, eventId, operationId, ...(reconcileOnly && {reconcileOnly:true}),
            mode:'create', checkAvailability:true, retryGoogle:(row.attempts || 0) > 0, name:offer.leadName || 'cliente', action:'demo', reminderMinutes:15});
          if (reconcileOnly && !response) {
            await store.updateOne(guard, {$set:{state:'offered'}, $unset:{choice:1,lease:1,leaseUntil:1,phase:1}});
            return {status:'unavailable'};
          }
          if (response?.id !== eventId) throw new Error('Respuesta incompleta de Calendar');
          receipt = {htmlLink:response.htmlLink || '',etag:response.etag || ''};
          const checkpoint = await store.updateOne(guard, {$set:{phase:'crm', current:seal(row, 'event', receipt)}});
          if (!checkpoint.matchedCount) return {status:'working'};
        }
        const reference = eventId.slice(0,12).toUpperCase();
        if (rememberEvent) await rememberEvent(b, {...proposal,...receipt,eventId,operationId,end:slot.end,action:'demo',
          name:offer.leadName || offer.prospect,leadId:offer.leadId,reminderMinutes:15,mode:'create',expectedStart:offer.expectedStart});
        let crm = 'none';
        if (offer.leadId && now()-startedAt >= followupLimits.crmMs) crm = 'expired';
        else if (offer.leadId) crm = await syncLead({owner:b.owner, leadId:offer.leadId, start:slot.start, action:'demo', expectedStart:offer.expectedStart,expectedEmail:offer.prospect}) ? 'updated' : 'conflict';
        const result = {status:'saved', crm, htmlLink:receipt.htmlLink, ...proposal, index:row.choice.index, reference};
        await store.updateOne(guard, {$set:{state:'booked', phase:'done', result:seal(row, 'result', result), removeAfter:new Date(now() + 30 * day)},
          $unset:{content:1, current:1, lease:1, leaseUntil:1}});
        return result;
      } catch (error) {
        if (!receipt && (error?.bookingBusy || (error?.status === 503 && /otra cita/i.test(error.message || '')))) {
          // The seller's calendar is confirming another appointment. Nothing was written to Google.
          await store.updateOne(guard, {$set:{state:'offered'}, $unset:{lease:1, leaseUntil:1, phase:1}});
          return {status:'calendarBusy'};
        }
        if (!receipt && error?.status && [400,401,403,404,409,410,412].includes(error.status)) {
          await store.updateOne(guard, {$set:{state:'offered'}, $unset:{choice:1, lease:1, leaseUntil:1, phase:1}});
          return {status:error.slotBusy ? 'slotBusy' : error.reauthorize ? 'reconnect' : 'changed'};
        }
        await store.updateOne(guard, {$set:{leaseUntil:new Date(now())}});
        return {status:receipt ? 'crmPending' : error?.reauthorize ? 'reconnect' : 'retry'};
      }
    },
    // A new Gmail or Calendar consent invalidates offers that have not started booking.
    async purge({owner, gmailGrant, calendarGrant}) {
      const q = {owner:String(owner), state:'offered'};
      if (gmailGrant) await store.deleteMany({...q, gmailGrant:offerDigest(key, gmailGrant)});
      if (calendarGrant) await store.deleteMany({...q, calendarGrant:offerDigest(key, calendarGrant)});
    }
  };
}
