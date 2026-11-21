import mongoose from 'mongoose';
import { purgeCalendarProposals } from './coach-followup-store.js';
import { createHash, randomBytes } from 'node:crypto';
import { calendarGoogleClient, calendarScope, calendarDescription, isSecondaryCalendarId } from './coach-calendar-google.js';
import { openToken, sealToken } from './coach-hycite-gmail.js';

const schema = new mongoose.Schema({
  _id: mongoose.Schema.Types.ObjectId, stateHash:String, sessionId:String, deadline:Date, verifier:String,
  exchangeId:String, accountSub:{type:String,unique:true,sparse:true}, mailbox:String, encryptedToken:String,
  grant:String, calendarId:String, marker:String, provisionState:String, provisionTime:Date
}, {versionKey:false, writeConcern:{w:'majority'}});
const Store = mongoose.models.CoachPersonalCalendar || mongoose.model('CoachPersonalCalendar', schema);
const random = () => randomBytes(32).toString('base64url');
const digest = text => createHash('sha256').update(text).digest('base64url');
const ownerKey = id => 'calendar-refresh:' + id;
const api = '/api/coach/conexiones/calendar/';

export function createCoachCalendarConnections({config,store=Store,google=calendarGoogleClient(config),purge=purgeCalendarProposals}) {
  const access = async doc => {
    const token = await google.refresh(openToken(doc.encryptedToken, config.key, ownerKey(doc._id)));
    if (!token.access_token) throw new Error('Google no devolvió acceso a Calendar.');
    return token.access_token;
  };

  async function provision(id, grant, token) {
    // Google assigns calendar IDs. Persist the claim before the provider write; an ambiguous
    // response can be recovered by validating the original calendar's private reference.
    const doc = await store.findOneAndUpdate({_id:id,grant,calendarId:{$exists:false},provisionState:{$exists:false}},
      {$set:{provisionState:'creating',provisionTime:new Date()}},{new:true}).lean();
    if (!doc) return 'calendarPending';
    let created;
    try {
      created = await google.createCalendar(token, doc.marker);
      if (!isSecondaryCalendarId(created?.id)) throw new Error('Google no devolvió el calendario.');
    } catch (error) {
      const certain = error.status >= 400 && error.status < 500 && error.status !== 408;
      await store.updateOne({_id:id,marker:doc.marker,provisionState:'creating'}, certain ?
        {$unset:{provisionState:1,provisionTime:1}} : {$set:{provisionState:'uncertain'}});
      throw error;
    }
    const saved = await store.updateOne({_id:id,marker:doc.marker},{$set:{calendarId:created.id},$unset:{provisionState:1,provisionTime:1}});
    return saved.matchedCount ? 'calendarConnected' : 'busy';
  }

  const service = {
    async getConnection(owner) {
      if (!config.calendarReady) return null;
      const doc = await store.findById(owner,'mailbox calendarId grant').lean();
      return doc?.grant && doc.calendarId ? {grant:doc.grant,mailbox:doc.mailbox,calendarId:doc.calendarId} : null;
    },
    async saveEvent(event) {
      const {owner,grant} = event;
      if (!owner || typeof grant !== 'string' || !grant) throw new Error('La conexión cambió.');
      const doc = await store.findOne({_id:owner,grant,calendarId:{$exists:true}}).lean();
      if (!doc?.encryptedToken || !config.calendarReady) throw new Error('Conecta Google Calendar.');
      const token = await access(doc);
      if (!await store.exists({_id:owner,grant,calendarId:doc.calendarId})) throw new Error('La conexión cambió.');
      return google.saveEvent(token,doc.calendarId,{...event,calendarId:doc.calendarId});
    }
  };

  return {
    service,
    async panel(id) {
      if (!config.calendarReady) return null;
      return store.findById(id,'mailbox grant calendarId provisionState provisionTime').lean();
    },
    register(app, own, back) {
      app.post(api + 'start', own(async (_req,res,{id,session,email}) => {
        if (!config.calendarReady) return back(res,'calendarDisabled');
        const state = 'cal.' + random(), verifier = random();
        await store.updateOne({_id:id},{$set:{stateHash:digest(state),verifier,sessionId:session,
          deadline:new Date(Date.now()+10*60000)},$unset:{exchangeId:1}},{upsert:true});
        return res.redirect(303,google.authorize(state,digest(verifier),email));
      }));
      app.post(api + 'disconnect', own(async (_req,res,{id}) => {
        // Google revokes the project's grants together, including this seller's Gmail.
        // Drop the usable credential while retaining the calendar ID for reconnection.
        const previous = await store.findOneAndUpdate({_id:id},{$unset:{grant:1,encryptedToken:1,stateHash:1,exchangeId:1,
          sessionId:1,deadline:1,verifier:1}},{new:false}).lean();
        if (previous?.grant) await purge({owner:id,grant:previous.grant},config.key);
        return back(res,'calendarDisconnected');
      }));
      app.post(api + 'prepare', own(async (req,res,{id}) => {
        if (!config.calendarReady) return back(res,'calendarDisabled');
        const doc = await store.findById(id).lean();
        if (!doc?.grant || doc.calendarId) return back(res,'busy');
        const token = await access(doc);
        if (doc.provisionState) {
          const calendarId = req.body?.calendarId;
          if (isSecondaryCalendarId(calendarId)) {
            const calendar = await google.getCalendar(token,calendarId);
            if (calendar.id !== calendarId || calendar.description !== calendarDescription(doc.marker)) return back(res,'calendarWrongId');
            const result = await store.updateOne({_id:id,grant:doc.grant,marker:doc.marker,calendarId:{$exists:false}},
              {$set:{calendarId},$unset:{provisionState:1,provisionTime:1}});
            return back(res,result.matchedCount ? 'calendarConnected' : 'busy');
          }
          if (req.body?.noCalendar !== 'yes' || Date.now()-new Date(doc.provisionTime).getTime()<120000) return back(res,'calendarPending');
          const cleared = await store.updateOne({_id:id,grant:doc.grant,provisionState:doc.provisionState,provisionTime:doc.provisionTime,
            calendarId:{$exists:false}},{$unset:{provisionState:1,provisionTime:1}});
          if (!cleared.matchedCount) return back(res,'busy');
        }
        return back(res,await provision(id,doc.grant,token));
      }));
    },
    async callback(req,res,{id,session,email},back) {
      if (!config.calendarReady) return back(res,'calendarDisabled');
      const exchangeId = random();
      const pending = await store.findOneAndUpdate({_id:id,stateHash:digest(req.query.state),sessionId:session,deadline:{$gt:new Date()}},
        {$set:{exchangeId},$unset:{stateHash:1,sessionId:1,verifier:1,deadline:1}},{new:false}).lean();
      if (!pending) return back(res,'expired');
      if (req.query.error || typeof req.query.code !== 'string') return back(res,'denied');
      const tokens = await google.exchange(req.query.code,pending.verifier);
      if (!tokens.access_token || !String(tokens.scope || '').split(/\s+/).includes(calendarScope)) return back(res,'denied');
      const user = await google.profile(tokens.access_token);
      if (user.email_verified !== true || !user.sub || !user.email || user.email.toLowerCase() !== email ||
          (pending.accountSub && pending.accountSub !== user.sub)) return back(res,'calendarWrongAccount');
      const encryptedToken = tokens.refresh_token ? sealToken(tokens.refresh_token,config.key,ownerKey(id)) : pending.encryptedToken;
      if (!encryptedToken) return back(res,'denied');
      let calendarId = pending.calendarId;
      if (calendarId) {
        try {
          const existing = await google.getCalendar(tokens.access_token,calendarId);
          if (existing?.id !== calendarId) return back(res,'calendarFailed');
        } catch (error) {
          if (![404,410].includes(error.status)) throw error;
          calendarId = null;
        }
      }
      const replaceMissing = Boolean(pending.calendarId && !calendarId);
      const grant = !replaceMissing && pending.grant || random();
      const marker = !replaceMissing && pending.marker || random();
      const unset = replaceMissing ? {exchangeId:1,calendarId:1,provisionState:1,provisionTime:1} : {exchangeId:1};
      try {
        const saved = await store.updateOne({_id:id,exchangeId},{$set:{accountSub:user.sub,mailbox:user.email,
          encryptedToken,grant,marker},$unset:unset});
        if (!saved.matchedCount) return back(res,'busy');
      } catch (error) {
        if (error.code === 11000) return back(res,'calendarWrongAccount');
        throw error;
      }
      return back(res,calendarId ? 'calendarConnected' : await provision(id,grant,tokens.access_token));
    }
  };
}
