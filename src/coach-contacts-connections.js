import mongoose from 'mongoose';
import { createHash, randomBytes } from 'node:crypto';
import { contactsGoogleClient, contactsScope, fail } from './coach-contacts-google.js';
import { openToken, sealToken } from './coach-hycite-gmail.js';

const schema = new mongoose.Schema({
  _id: mongoose.Schema.Types.ObjectId, stateHash:String, sessionId:String, deadline:Date, verifier:String,
  exchangeId:String, accountSub:{type:String,unique:true,sparse:true}, mailbox:String, encryptedToken:String, grant:String
}, {versionKey:false, writeConcern:{w:'majority'}});
const Store = mongoose.models.CoachPersonalContacts || mongoose.model('CoachPersonalContacts', schema);
const random = () => randomBytes(32).toString('base64url');
const digest = text => createHash('sha256').update(text).digest('base64url');
const ownerKey = id => 'contacts-refresh:' + id;
const api = '/api/coach/conexiones/contactos/';
// The root /api/coach/conexiones/gmail/callback dispatcher routes states with this prefix here.
export const contactsStatePrefix = 'people.';
const scopeList = value => String(value || '').split(/\s+/);

const reconnect = () => fail('Abre Mis conexiones y vuelve a autorizar Google Contactos.', 403, 'contactsReconnect', {reauthorize:true});
const changed = () => fail('La conexión de Google Contactos cambió. Vuelve a buscar el contacto.', 409, 'contactsChanged');

export function createCoachContactsConnections({config,store=Store,google=contactsGoogleClient(config),now=Date.now}) {
  async function authorized(owner, grant) {
    if (!config.contactsReady) throw fail('Google Contactos todavía no está configurado.', 503, 'contactsDisabled');
    if (!owner || typeof grant !== 'string' || !grant) throw changed();
    const doc = await store.findOne({_id:owner,grant}).lean();
    if (!doc?.encryptedToken) throw changed();
    let refresh;
    try { refresh = openToken(doc.encryptedToken, config.key, ownerKey(doc._id)); }
    catch { throw reconnect(); }
    const token = await google.refresh(refresh);
    if (!token?.access_token || (token.scope && !scopeList(token.scope).includes(contactsScope))) throw reconnect();
    // The seller may have disconnected or reconnected while the token was refreshed.
    if (!await store.exists({_id:doc._id,grant,encryptedToken:doc.encryptedToken})) throw changed();
    return token.access_token;
  }

  // Every provider error leaves with a stable code; none is reported as success.
  const run = async (owner, grant, action) => {
    try {
      return await action(await authorized(owner, grant));
    } catch (error) {
      if (error.reauthorize) Object.assign(error, {code:'contactsReconnect'});
      if (!error.code) error.code = 'contactsFailed';
      throw error;
    }
  };

  const service = {
    async profile(owner) {
      if (!config.contactsReady || !owner) return null;
      const doc = await store.findById(owner,'mailbox grant encryptedToken').lean();
      return doc?.grant && doc.encryptedToken ? {grant:doc.grant,mailbox:doc.mailbox} : null;
    },
    // Read-only: never creates or edits while searching.
    search: (owner, grant, person) => run(owner, grant, access => google.search(access, person)),
    get: (owner, grant, resourceName) => run(owner, grant, access => google.get(access, resourceName)),
    create: (owner, grant, person) => run(owner, grant, access => google.create(access, person)),
    // expected = {resourceName, etag} of the card the seller confirmed.
    update: (owner, grant, expected, person) => run(owner, grant, access => google.update(access, expected, person))
  };

  return {
    service,
    async panel(id) {
      if (!config.contactsReady) return null;
      const doc = await store.findById(id,'mailbox grant encryptedToken').lean();
      return doc ? {mailbox:doc.mailbox,grant:doc.grant,connected:Boolean(doc.grant && doc.encryptedToken)} : null;
    },
    register(app, own, back) {
      app.post(api + 'start', own(async (_req,res,{id,session,email}) => {
        if (!config.contactsReady) return back(res,'contactsDisabled');
        const state = contactsStatePrefix + random(), verifier = random();
        await store.updateOne({_id:id},{$set:{stateHash:digest(state),verifier,sessionId:session,
          deadline:new Date(now()+10*60000)},$unset:{exchangeId:1}},{upsert:true});
        return res.redirect(303,google.authorize(state,digest(verifier),email));
      }));
      app.post(api + 'disconnect', own(async (_req,res,{id}) => {
        // No Google revocation: it would revoke the project's other scopes (Gmail, Calendar).
        // Dropping the grant invalidates pending confirmations bound to it.
        await store.updateOne({_id:id},{$unset:{grant:1,encryptedToken:1,stateHash:1,exchangeId:1,
          sessionId:1,deadline:1,verifier:1}});
        return back(res,'contactsDisconnected');
      }));
    },
    async callback(req,res,{id,session,email},back) {
      if (!config.contactsReady) return back(res,'contactsDisabled');
      const state = typeof req.query?.state === 'string' ? req.query.state : '';
      if (!state.startsWith(contactsStatePrefix) || !session) return back(res,'expired');
      const exchangeId = random();
      const pending = await store.findOneAndUpdate({_id:id,stateHash:digest(state),sessionId:session,deadline:{$gt:new Date(now())}},
        {$set:{exchangeId},$unset:{stateHash:1,sessionId:1,verifier:1,deadline:1}},{new:false}).lean();
      if (!pending) return back(res,'expired');
      if (req.query.error || typeof req.query.code !== 'string' || !req.query.code) return back(res,'denied');
      let tokens, user;
      try {
        tokens = await google.exchange(req.query.code,pending.verifier);
        if (!tokens?.access_token || !scopeList(tokens.scope).includes(contactsScope)) return back(res,'contactsMissingScope');
        user = await google.profile(tokens.access_token);
      } catch {
        return back(res,'contactsFailed');
      }
      if (user?.email_verified !== true || !user.sub || typeof user.email !== 'string' ||
          user.email.toLowerCase() !== String(email || '').toLowerCase() ||
          (pending.accountSub && pending.accountSub !== user.sub)) return back(res,'contactsWrongAccount');
      const encryptedToken = tokens.refresh_token ? sealToken(tokens.refresh_token,config.key,ownerKey(id)) : pending.encryptedToken;
      if (!encryptedToken) return back(res,'contactsNoOffline');
      const grant = pending.grant && pending.encryptedToken ? pending.grant : random();
      try {
        const saved = await store.updateOne({_id:id,exchangeId},{$set:{accountSub:user.sub,mailbox:user.email.toLowerCase(),
          encryptedToken,grant},$unset:{exchangeId:1}});
        if (!saved.matchedCount) return back(res,'busy');
      } catch (error) {
        if (error.code === 11000) return back(res,'contactsWrongAccount');
        throw error;
      }
      return back(res,'contactsConnected');
    }
  };
}
