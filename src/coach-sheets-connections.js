import { createHash, randomBytes } from 'node:crypto';
import mongoose from 'mongoose';
import { openToken, sealToken } from './coach-hycite-gmail.js';
import { importPage, pickerPage, sheetMappingPage } from './coach-connections-page.js';
import { planImport, runImport } from './coach-sheets-import.js';
import { TEC_SHEET_IDS, tecSheetCreateBody, tecSheetRequests } from './coach-sheets-template.js';

export const sheetsApi = '/api/coach/conexiones/sheets/';
export const sheetsScope = 'https://www.googleapis.com/auth/drive.file';
const mime = 'application/vnd.google-apps.spreadsheet';
const schema = new mongoose.Schema({
  _id: mongoose.Schema.Types.ObjectId, accountSub: {type:String,unique:true,sparse:true}, mailbox:String,
  encryptedToken:String, grant:String, stateHash:String, sessionId:String, deadline:Date, verifier:String, exchangeId:String,
  spreadsheetId:String, spreadsheetTitle:String, ticketsSheetId:Number, referralsSheetId:Number, ticketsTitle:String, referralsTitle:String,
  tecSheet:Boolean, previousSpreadsheetId:String, previousSpreadsheetTitle:String, previousTicketsSheetId:Number, previousReferralsSheetId:Number
}, {versionKey:false, writeConcern:{w:'majority'}});
const Store = mongoose.models.CoachPersonalSheets || mongoose.model('CoachPersonalSheets', schema);
const random = () => randomBytes(32).toString('base64url');
const digest = value => createHash('sha256').update(value).digest('base64url');
const binding = owner => 'sheets-refresh:' + owner;
const fail = (message,status) => Object.assign(new Error(message),{status});
const knownScopes = new Set([sheetsScope, 'openid', 'email', 'https://www.googleapis.com/auth/userinfo.email']);
const allowedScopes = text => {
  const values = String(text || '').trim().split(/\s+/);
  return values.includes(sheetsScope) && values.every(value => knownScopes.has(value));
};
const panelFields = 'grant mailbox spreadsheetId spreadsheetTitle ticketsSheetId referralsSheetId ticketsTitle referralsTitle tecSheet previousSpreadsheetId previousSpreadsheetTitle';
const sheetIdValid = id => typeof id === 'string' && /^[\w-]{10,160}$/.test(id);

export function sheetsOAuthClient(config, fetcher = fetch) {
  const redirectUri = config.origin + '/api/coach/conexiones/gmail/callback';
  async function request(url,options) {
    const response = await fetcher(url,{...options,signal:AbortSignal.timeout(12000),redirect:'error'});
    if (response.ok) return response.json();
    const body = await response.json().catch(() => ({}));
    const invalidGrant = response.status === 401 || body.error === 'invalid_grant';
    throw Object.assign(fail(invalidGrant ? 'Vuelve a autorizar Google Sheets en Mis conexiones.' : 'Google no pudo abrir la hoja elegida.',
      invalidGrant ? 403 : response.status === 404 || response.status === 403 ? 422 : 503), {reauthorize:invalidGrant});
  }
  const token = fields => request('https://oauth2.googleapis.com/token',{method:'POST',body:new URLSearchParams({
    client_id:config.clientId, client_secret:config.secret, ...fields
  })});
  const read = (url,access) => request(url,{headers:{Authorization:'Bearer '+access}});
  return {
    authorize(state,challenge,email) {
      return 'https://accounts.google.com/o/oauth2/v2/auth?' + new URLSearchParams({
        client_id:config.clientId, redirect_uri:redirectUri, response_type:'code', access_type:'offline',
        prompt:'select_account consent', scope:'openid email '+sheetsScope, include_granted_scopes:'false',
        login_hint:email, state, code_challenge:challenge, code_challenge_method:'S256'
      });
    },
    exchange:(code,verifier) => token({grant_type:'authorization_code',code,code_verifier:verifier,redirect_uri:redirectUri}),
    refresh:refresh_token => token({grant_type:'refresh_token',refresh_token,scope:sheetsScope}),
    // Google Auth Library uses a POST with a bearer header for introspection.
    // The access token never becomes part of a query string, log or stored credential.
    async tokenScopes(access) {
      const info = await request('https://oauth2.googleapis.com/tokeninfo', {
        method: 'POST',
        headers: { Authorization: 'Bearer ' + access, 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }
      });
      if ([info.azp, info.aud, info.issued_to].some(aud => aud && aud !== config.clientId)) return '';
      return info.scope;
    },
    profile:access => read('https://openidconnect.googleapis.com/v1/userinfo',access),
    async metadata(access,id) {
      if (!sheetIdValid(id)) throw fail('Elige una hoja en Google Drive.',422);
      const path = encodeURIComponent(id);
      const [file,book] = await Promise.all([
        read('https://www.googleapis.com/drive/v3/files/'+path+'?supportsAllDrives=true&fields=id,mimeType,trashed,capabilities(canEdit)',access),
        read('https://sheets.googleapis.com/v4/spreadsheets/'+path+'?fields=spreadsheetId,properties(title),sheets(properties(sheetId,title,sheetType,hidden))',access)
      ]);
      if (file.id!==id || file.mimeType!==mime || file.trashed || file.capabilities?.canEdit!==true || book.spreadsheetId!==id) {
        throw fail('Elige una hoja de cálculo que puedas editar.',422);
      }
      const tabs=(book.sheets || []).map(entry=>entry.properties).filter(tab=>tab?.sheetType==='GRID' && !tab.hidden &&
        Number.isSafeInteger(tab.sheetId) && typeof tab.title==='string' && tab.title);
      if (tabs.length < 2 || typeof book.properties?.title!=='string') throw fail('La hoja debe tener dos pestañas distintas.',422);
      return {id,title:book.properties.title,tabs};
    },
    // Crea la Hoja TEC en el Drive del vendedor. Con el permiso drive.file el Coach solo puede abrir los archivos que él creó o
    // que el vendedor eligió, así que esta hoja queda a su alcance sin pedir permisos nuevos.
    async createTecSheet(access,{title,timeZone}) {
      const book = await request('https://sheets.googleapis.com/v4/spreadsheets?fields=spreadsheetId,properties(title)',{method:'POST',
        headers:{Authorization:'Bearer '+access,'Content-Type':'application/json'},body:JSON.stringify(tecSheetCreateBody({title,timeZone}))});
      if (!sheetIdValid(book?.spreadsheetId)) throw fail('Google no confirmó la hoja nueva.',503);
      await request('https://sheets.googleapis.com/v4/spreadsheets/'+encodeURIComponent(book.spreadsheetId)+':batchUpdate?fields=spreadsheetId',{method:'POST',
        headers:{Authorization:'Bearer '+access,'Content-Type':'application/json'},body:JSON.stringify({requests:tecSheetRequests()})});
      return {id:book.spreadsheetId,title:book.properties?.title || title};
    }
  };
}

export function createCoachSheetsConnections({config,store=Store,google=sheetsOAuthClient(config),reader=null}) {
  const connected = async (owner,grant) => {
    if (!config.sheetsReady) throw fail('La conexión de Google Sheets está pendiente de configuración.',503);
    const doc=await store.findOne({_id:owner,...(grant && {grant})}).lean();
    if (!doc?.grant || !doc.encryptedToken) throw fail('Conecta Google Sheets en Mis conexiones.',409);
    return doc;
  };
  const current = async (owner,grant) => Boolean(grant && await store.exists({_id:owner,grant}));
  async function accessToken(doc,forPicker=false) {
    const tokens = await google.refresh(openToken(doc.encryptedToken,config.key,binding(doc._id)));
    const granted = tokens?.access_token && (tokens.scope || (forPicker ? await google.tokenScopes(tokens.access_token) : ''));
    if (!tokens?.access_token || ((forPicker || granted) && !allowedScopes(granted))) {
      throw Object.assign(fail('Vuelve a autorizar Google Sheets en Mis conexiones.',403),{reauthorize:true});
    }
    if (!await current(doc._id,doc.grant)) throw fail('La conexión cambió. Abre Mis conexiones.',409);
    return tokens.access_token;
  }
  const panel = owner => config.sheetsReady ? store.findById(owner,panelFields).lean() : null;
  const destination = doc => doc?.grant && sheetIdValid(doc.spreadsheetId) && Number.isSafeInteger(doc.ticketsSheetId) &&
    Number.isSafeInteger(doc.referralsSheetId) && doc.referralsSheetId!==doc.ticketsSheetId ?
    {grant:doc.grant,spreadsheetId:doc.spreadsheetId,spreadsheetTitle:doc.spreadsheetTitle,ticketsSheetId:doc.ticketsSheetId,
     referralsSheetId:doc.referralsSheetId,ticketsTitle:doc.ticketsTitle,referralsTitle:doc.referralsTitle} : null;
  async function selectedDoc(owner,grant) {
    if (typeof grant!=='string' || !/^[\w-]{40,50}$/.test(grant)) throw fail('Vuelve a elegir la hoja.',409);
    return connected(owner,grant);
  }
  return {
    service:{
      async getConnection(owner) {return destination(await panel(owner));},
      async access(owner) {
        const doc=await connected(owner);
        const book=destination(doc);
        if (!book) throw fail('Elige la hoja y sus dos pestañas en Mis conexiones.',409);
        return {...book,token:await accessToken(doc)};
      },
      current
    },
    panel,
    async callback(req,res,{id,session,email,isSessionCurrent},back) {
      if (!config.sheetsReady) return back(res,'sheetsDisabled');
      const exchangeId=random();
      const pending=await store.findOneAndUpdate({_id:id,stateHash:digest(req.query.state),sessionId:session,deadline:{$gt:new Date()}},
        {$set:{exchangeId},$unset:{stateHash:1,sessionId:1,deadline:1,verifier:1}}, {new:false}).lean();
      if (!pending) return back(res,'expired');
      if (req.query.error || typeof req.query.code!=='string') return back(res,'denied');
      const tokens=await google.exchange(req.query.code,pending.verifier);
      if (!tokens.access_token || !allowedScopes(tokens.scope || await google.tokenScopes(tokens.access_token))) return back(res,'denied');
      const profile=await google.profile(tokens.access_token);
      if (profile?.email_verified!==true || typeof profile.sub!=='string' || !profile.sub || profile.email?.toLowerCase()!==email ||
        (pending.accountSub && pending.accountSub!==profile.sub)) return back(res,'sheetsWrongAccount');
      const encryptedToken=tokens.refresh_token ? sealToken(tokens.refresh_token,config.key,binding(id)) : pending.encryptedToken;
      if (!encryptedToken) return back(res,'denied');
      if (!await isSessionCurrent()) return back(res,'expired');
      try {
        const update=await store.updateOne({_id:id,exchangeId},{$set:{encryptedToken,accountSub:profile.sub,mailbox:profile.email,grant:random()},$unset:{exchangeId:1}});
        return back(res,update.matchedCount?'sheetsConnected':'busy');
      } catch (error) {
        if (error.code===11000) return back(res,'sheetsWrongAccount');
        throw error;
      }
    },
    register(app,own,back) {
      app.post(sheetsApi+'start',own(async (_req,res,{id,session,email})=>{
        if (!config.sheetsReady) return back(res,'sheetsDisabled');
        const state='sheets.'+random(), verifier=random();
        await store.updateOne({_id:id},{$set:{stateHash:digest(state),verifier,sessionId:session,deadline:new Date(Date.now()+600000)},
          $unset:{exchangeId:1}},{upsert:true});
        return res.redirect(303,google.authorize(state,digest(verifier),email));
      }));
      app.post(sheetsApi+'disconnect',own(async (_req,res,{id})=>{
        await store.deleteOne({_id:id});
        return back(res,'sheetsDisconnected');
      }));
      app.post(sheetsApi+'picker',own(async (_req,res,{id,session,email})=>{
        const doc=await connected(id);
        res.set('Referrer-Policy','strict-origin-when-cross-origin');
        res.set('Content-Security-Policy',"default-src 'none'; script-src 'self' https://apis.google.com https://*.gstatic.com; style-src 'self' 'unsafe-inline' https://*.googleapis.com; img-src 'self' data: https://*.gstatic.com https://*.google.com; connect-src 'self' https://*.googleapis.com https://*.google.com; frame-src https://*.google.com https://*.googleusercontent.com; base-uri 'none'; form-action 'self'; frame-ancestors 'none'");
        return res.send(pickerPage({csrf:session,grant:doc.grant,email}));
      }));
      app.post(sheetsApi+'picker-token',own(async (req,res,{id,isSessionCurrent})=>{
        const doc=await selectedDoc(id,req.body.grant);
        const token=await accessToken(doc,true);
        if (!await isSessionCurrent()) throw fail('Tu sesión cambió. Vuelve a abrir el Coach.',403);
        return res.json({accessToken:token,appId:config.sheetsAppId,pickerKey:config.sheetsPickerKey});
      }));
      app.post(sheetsApi+'create',own(async (req,res,{id,isSessionCurrent})=>{
        const doc=await selectedDoc(id,req.body.grant);
        // Los vendedores del piloto trabajan en el área de Chicago; el vendedor puede cambiar la zona en su hoja.
        const book=await google.createTecSheet(await accessToken(doc),{title:'Hoja TEC',timeZone:'America/Chicago'});
        if (!await isSessionCurrent()) return back(res,'expired');
        // Rifas van a Boletas y referidos a 4 en 14. Se cambia el permiso de la propuesta para que nada pendiente use la hoja anterior.
        // Se recuerda la hoja anterior (si no era otra Hoja TEC) para ofrecer pasar sus leads.
        const previous=doc.spreadsheetId && !doc.tecSheet && destination(doc) ? {previousSpreadsheetId:doc.spreadsheetId,previousSpreadsheetTitle:doc.spreadsheetTitle,
          previousTicketsSheetId:doc.ticketsSheetId,previousReferralsSheetId:doc.referralsSheetId} : {};
        const updated=await store.updateOne({_id:id,grant:doc.grant},{$set:{grant:random(),spreadsheetId:book.id,spreadsheetTitle:book.title,tecSheet:true,
          ticketsSheetId:TEC_SHEET_IDS.tickets,ticketsTitle:'Boletas',referralsSheetId:TEC_SHEET_IDS.referrals,referralsTitle:'4 en 14',...previous}});
        return back(res,updated.matchedCount?'sheetsCreated':'busy');
      }));
      // Pasar los leads de la hoja anterior a la Hoja TEC: primero se muestra cuántos son y luego se confirma.
      for (const action of ['import','import-confirm']) {
        app.post(sheetsApi+action,own(async (req,res,{id,session,sellerName,isSessionCurrent})=>{
          const doc=await selectedDoc(id,req.body.grant);
          if (!reader || !doc.tecSheet || !sheetIdValid(doc.previousSpreadsheetId) || !destination(doc)) return back(res,'sheetsImportNone');
          const access=await accessToken(doc);
          const newBook=destination(doc);
          const oldBook={spreadsheetId:doc.previousSpreadsheetId,ticketsSheetId:doc.previousTicketsSheetId,referralsSheetId:doc.previousReferralsSheetId};
          let planned;
          try { planned=await planImport(reader,access,{oldBook,newBook,sellerName}); }
          catch (error) { if (error.status===409) return back(res,'sheetsImportNotEmpty'); throw error; }
          if (!await isSessionCurrent()) return back(res,'expired');
          if (action==='import') {
            res.set('Referrer-Policy','same-origin');
            res.set('Content-Security-Policy',"default-src 'none'; style-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'");
            return res.send(importPage({csrf:session,grant:doc.grant,title:doc.previousSpreadsheetTitle,referrals:planned.referrals.length,tickets:planned.tickets.length,skipped:planned.skipped}));
          }
          if (!planned.referrals.length && !planned.tickets.length) return back(res,'sheetsImportNone');
          if (!await current(id,doc.grant)) return back(res,'busy');
          await runImport(reader,access,newBook,planned);
          await store.updateOne({_id:id,grant:doc.grant},{$set:{grant:random()},$unset:{previousSpreadsheetId:1,previousSpreadsheetTitle:1,previousTicketsSheetId:1,previousReferralsSheetId:1}});
          return back(res,'sheetsImported');
        }));
      }
      for (const action of ['choose','map']) {
        app.post(sheetsApi+action,own(async (req,res,{id,session,isSessionCurrent})=>{
          const doc=await selectedDoc(id,req.body.grant);
          const book=await google.metadata(await accessToken(doc),req.body.spreadsheetId);
          if (!await isSessionCurrent()) return back(res,'expired');
          if (action==='choose') {
            res.set('Referrer-Policy','same-origin');
            res.set('Content-Security-Policy',"default-src 'none'; style-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'");
            return res.send(sheetMappingPage({book,csrf:session,doc}));
          }
          const find=key=>book.tabs.find(tab=>String(tab.sheetId)===req.body[key]);
          const tickets=find('ticketsSheetId'), referrals=find('referralsSheetId');
          if (!tickets || !referrals || tickets.sheetId===referrals.sheetId) return back(res,'sheetsBadTabs');
          const updated=await store.updateOne({_id:id,grant:doc.grant},{$set:{grant:random(),spreadsheetId:book.id,spreadsheetTitle:book.title,
            ticketsSheetId:tickets.sheetId,ticketsTitle:tickets.title,referralsSheetId:referrals.sheetId,referralsTitle:referrals.title},
            $unset:{tecSheet:1,previousSpreadsheetId:1,previousSpreadsheetTitle:1,previousTicketsSheetId:1,previousReferralsSheetId:1}});
          return back(res,updated.matchedCount?'sheetsMapped':'busy');
        }));
      }
    }
  };
}
