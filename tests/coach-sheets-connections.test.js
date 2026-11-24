import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { createCoachSheetsConnections, sheetsApi, sheetsOAuthClient, sheetsScope } from '../src/coach-sheets-connections.js';
import { sheetMappingPage, pickerPage, connectionsPage } from '../src/coach-connections-page.js';

const exampleId='spreadsheet-12345678';
const tabs=[{sheetId:0,title:'BOLETAS NUEVAS'},{sheetId:9,title:'4-14 Agustín y Rigoberto'}];
function harness() {
  const records=new Map(),routes=new Map();
  const matches=(doc,f)=>doc&&Object.entries(f).every(([k,v])=>v?.$gt?doc[k]>v.$gt:doc[k]===v);
  const lean=v=>({lean:async()=>structuredClone(v)});
  const change=(doc,update)=>{Object.assign(doc,update.$set);for(const key of Object.keys(update.$unset||{}))delete doc[key]};
  const store={
    findById:id=>lean(records.get(id)),
    findOne:f=>lean(matches(records.get(f._id),f)?records.get(f._id):null),
    exists:async f=>Boolean(matches(records.get(f._id),f)),
    async updateOne(f,u,options) {
      if (!records.has(f._id) && options?.upsert) records.set(f._id,{_id:f._id});
      const doc=records.get(f._id);
      if (!matches(doc,f)) return {matchedCount:0};
      change(doc,u);return {matchedCount:1};
    },
    findOneAndUpdate(f,u) {
      const doc=records.get(f._id);
      if (!matches(doc,f))return lean(null);
      const previous=structuredClone(doc);change(doc,u);return lean(previous);
    },
    deleteOne:async f=>records.delete(f._id)
  };
  let verifier, active=true;
  const google={
    authorize:(state,challenge)=>({state,challenge}),
    async exchange(code,v){verifier=v;return {access_token:code,refresh_token:'refresh-'+code,scope:'openid email '+sheetsScope};},
    refresh:async token=>({access_token:token+'-current',scope:sheetsScope}),
    tokenScopes:async()=>'',
    profile:async token=>({sub:'google-'+token,email:token+'@example.com',email_verified:true}),
    async metadata(token,id){assert.equal(token,'refresh-a-current');assert.equal(id,exampleId);return {id,title:'Equipo',tabs};}
  };
  const cfg={sheetsReady:true,key:randomBytes(32),sheetsAppId:'123',sheetsPickerKey:'restricted-key'};
  const handler=createCoachSheetsConnections({config:cfg,google,store});
  const who=id=>({id,isSessionCurrent:async()=>active,session:'session-'+id,email:id+'@example.com'});
  const res={set(){return this},send:html=>html,redirect:(_status,to)=>to,json:body=>body};
  const back=(_res,notice)=>notice;
  handler.register({post:(path,h)=>routes.set(path,h)},h=>h,back);
  const post=(action,id,body={})=>routes.get(sheetsApi+action)({body},res,who(id));
  const callback=(id,state,code,other={})=>handler.callback({query:{state,code}},res,{...who(id),...other},back);
  return {records,post,callback,google,handler,setActive:v=>{active=v},verifier:()=>verifier};
}

test('Sheets autoriza con PKCE por sesión y vendedor, rechaza permisos sobrantes y cifra el token por propietario',async()=>{
  const h=harness();
  const request=await h.post('start','a');
  assert.match(request.state,/^sheets\./);
  assert.equal(await h.callback('b',request.state,'a'),'expired');
  assert.equal(await h.callback('a',request.state,'a',{session:'otro'}),'expired');
  assert.equal(await h.callback('a',request.state,'a'),'sheetsConnected');
  assert.equal(createHash('sha256').update(h.verifier()).digest('base64url'),request.challenge);
  assert.equal(await h.callback('a',request.state,'a'),'expired');
  assert.ok(!JSON.stringify(h.records.get('a')).includes('refresh-a'));
  const narrow=h.google.refresh;
  h.google.refresh=async()=>({access_token:'broad',scope:sheetsScope+' https://www.googleapis.com/auth/calendar'});
  await assert.rejects(h.post('picker-token','a',{grant:h.records.get('a').grant}),e=>e.status===403);
  h.google.refresh=async()=>({access_token:'unknown-scopes'});
  await assert.rejects(h.post('picker-token','a',{grant:h.records.get('a').grant}),e=>e.status===403);
  h.google.tokenScopes=async token=>{assert.equal(token,'unknown-scopes');return sheetsScope;};
  assert.equal((await h.post('picker-token','a',{grant:h.records.get('a').grant})).accessToken,'unknown-scopes');
  h.google.tokenScopes=async()=>sheetsScope+' https://www.googleapis.com/auth/calendar.readonly';
  await assert.rejects(h.post('picker-token','a',{grant:h.records.get('a').grant}),e=>e.status===403);
  h.google.refresh=narrow;
  h.setActive(false);
  await assert.rejects(h.post('picker-token','a',{grant:h.records.get('a').grant}),e=>e.status===403);
  h.setActive(true);
  assert.equal((await h.post('picker-token','a',{grant:h.records.get('a').grant})).accessToken,'refresh-a-current');
  const wrong=await h.post('start','b');
  assert.equal(await h.callback('b',wrong.state,'a'),'sheetsWrongAccount');
  const original=h.google.exchange;
  h.google.exchange=async(...args)=>({...await original(...args),scope:sheetsScope+' https://www.googleapis.com/auth/gmail.readonly'});
  assert.equal(await h.callback('b',(await h.post('start','b')).state,'b'),'denied');
  h.google.exchange=original;
  await h.callback('b',(await h.post('start','b')).state,'b');
  h.records.get('b').encryptedToken=h.records.get('a').encryptedToken;
  await assert.rejects(h.post('picker-token','b',{grant:h.records.get('b').grant}));
});

test('el mapeo verifica la hoja con el acceso propio, acepta sheetId cero e invalida las revisiones al cambiar el destino o desconectar',async()=>{
  const h=harness(),b=h.handler.service;
  await h.callback('a',(await h.post('start','a')).state,'a');
  await h.callback('b',(await h.post('start','b')).state,'b');
  assert.equal(await b.getConnection('a'),null);
  const grant=h.records.get('a').grant;
  await assert.rejects(h.post('picker-token','b',{grant}));
  const input={grant,spreadsheetId:exampleId,ticketsSheetId:'0',referralsSheetId:'9'};
  assert.equal(await h.post('map','a',{...input,referralsSheetId:'0'}),'sheetsBadTabs');
  assert.equal(await h.post('map','a',{...input,ticketsSheetId:'16'}),'sheetsBadTabs');
  assert.equal(await h.post('map','a',input),'sheetsMapped');
  const profile=await b.access('a');
  assert.equal(profile.ticketsSheetId,0);assert.equal(profile.referralsSheetId,9);assert.equal(profile.token,'refresh-a-current');
  assert.equal(await b.current('a',grant),false);assert.equal(await b.current('a',profile.grant),true);
  assert.equal(await b.current('b',profile.grant),false);assert.equal(await b.getConnection('b'),null);
  await assert.rejects(h.post('map','a',input));
  assert.equal(await h.post('disconnect','a'),'sheetsDisconnected');
  assert.equal(await b.getConnection('a'),null);assert.equal(await b.current('a',profile.grant),false);
  await assert.rejects(b.access('a'));
  assert.ok(h.records.get('b').encryptedToken);
});

test('la hoja anterior abre los destinos guardados sin Picker y solo se activa después de confirmar',async()=>{
  const h=harness();
  await h.callback('a',(await h.post('start','a')).state,'a');
  await h.callback('b',(await h.post('start','b')).state,'b');
  const a=h.records.get('a'), b=h.records.get('b');
  Object.assign(a,{spreadsheetId:'tec-current-123456',ticketsSheetId:10,referralsSheetId:11,tecSheet:true,
    previousSpreadsheetId:exampleId,previousSpreadsheetTitle:'Equipo',previousTicketsSheetId:0,previousReferralsSheetId:9});
  const grant=a.grant;
  const old=await h.post('choose','a',{grant,source:'previous',spreadsheetId:'ignored-spreadsheet'});
  assert.match(old,/value="0" selected>BOLETAS NUEVAS/);
  assert.match(old,/value="9" selected>4-14 Agustín y Rigoberto/);
  assert.equal(a.spreadsheetId,'tec-current-123456');
  assert.equal(await h.post('choose','b',{grant:b.grant,source:'previous',spreadsheetId:exampleId}),'sheetsBadLink');
  assert.equal(await h.post('choose','a',{grant,sheetUrl:'https://example.net/spreadsheets/d/'+exampleId}),'sheetsBadLink');
  assert.match(await h.post('choose','a',{grant,sheetUrl:'https://docs.google.com/spreadsheets/d/'+exampleId+'/edit?gid=9'}),/Destino de tus leads/);
  const metadata=h.google.metadata;
  h.google.metadata=async()=>{throw Object.assign(new Error('Google no autorizó ese archivo'),{status:422})};
  assert.equal(await h.post('choose','a',{grant,source:'previous'}),'sheetsPickFile');
  assert.equal(a.spreadsheetId,'tec-current-123456');
  h.google.metadata=metadata;
  assert.equal(await h.post('map','a',{grant,spreadsheetId:exampleId,ticketsSheetId:'0',referralsSheetId:'9'}),'sheetsMapped');
  const connected=await h.handler.service.access('a');
  assert.equal(connected.spreadsheetId,exampleId);
  assert.equal(connected.ticketsSheetId,0);
  assert.equal(connected.referralsSheetId,9);
  assert.equal(await h.handler.service.current('a',grant),false);
  assert.equal(await h.handler.service.getConnection('b'),null);
});

test('el cliente Google limita el OAuth al archivo elegido, verifica edición y excluye hojas ocultas',async()=>{
  const cfg={origin:'https://tec.example',clientId:'123-test',secret:'secret'};
  const fetches=[];
  let edit=true;
  const g=sheetsOAuthClient(cfg,async(url,opts)=>{
    fetches.push({url,opts});
    if (url.endsWith('/tokeninfo')) return Response.json({scope:sheetsScope,aud:cfg.clientId});
    if (url.includes('oauth2.googleapis')) return Response.json({access_token:'token'});
    if (url.includes('drive/v3')) return Response.json({id:exampleId,mimeType:'application/vnd.google-apps.spreadsheet',capabilities:{canEdit:edit}});
    return Response.json({spreadsheetId:exampleId,properties:{title:'Mi hoja'},sheets:[...tabs,{sheetId:3,title:'Privada',hidden:true}].map(properties=>({properties:{sheetType:'GRID',...properties}}))});
  });
  const auth=new URL(g.authorize('state','challenge','a@example.com'));
  assert.equal(auth.searchParams.get('scope'),'openid email '+sheetsScope);
  assert.equal(auth.searchParams.get('include_granted_scopes'),'false');
  assert.equal(auth.searchParams.get('code_challenge_method'),'S256');
  assert.equal(auth.searchParams.get('redirect_uri'),'https://tec.example/api/coach/conexiones/gmail/callback');
  await g.refresh('onlyserver');
  assert.equal(fetches[0].opts.body.get('scope'),sheetsScope);
  assert.equal((await g.metadata('private',exampleId)).tabs.length,2);
  assert.ok(fetches.slice(1).every(f=>f.opts.headers.Authorization==='Bearer private'&&new URL(f.url).protocol==='https:'));
  assert.equal(await g.tokenScopes('detached-token'),sheetsScope);
  const introspection=fetches.at(-1);
  assert.equal(introspection.url,'https://oauth2.googleapis.com/tokeninfo');
  assert.equal(introspection.opts.method,'POST');assert.equal(introspection.opts.headers.Authorization,'Bearer detached-token');
  assert.equal(introspection.opts.redirect,'error');assert.ok(!introspection.opts.body);
  edit=false;await assert.rejects(g.metadata('private',exampleId),e=>e.status===422);
  await assert.rejects(g.metadata('private','https://elsewhere.invalid'),e=>e.status===422);
});

test('la interfaz presenta solo al piloto el libro sugerido y permite otro archivo sin exponer credenciales',()=>{
  const base={pilot:false,sheetsReady:true,sheetsPilot:true,csrf:'csrf',coachEmail:'other@example.com'};
  assert.doesNotMatch(connectionsPage(base),/Young Dreamers/);
  assert.match(connectionsPage({...base,coachEmail:'agustincalderon286@gmail.com'}),/Young Dreamers/);
  assert.doesNotMatch(connectionsPage({...base,sheetsPilot:false}),/<h2>Google Sheets/);
  const html=sheetMappingPage({book:{id:exampleId,title:'A & B',tabs},csrf:'x',doc:{grant:'nonce'}});
  assert.match(html,/A &amp; B/);assert.match(html,/value="0" selected/);assert.match(html,/value="9" selected/);
  const picker=pickerPage({csrf:'x',grant:'nonce',email:'other@example.com'});
  assert.match(picker,/Abrir Google Drive/);assert.doesNotMatch(picker,/Young Dreamers|refresh_token|accessToken/);
});

test('el piloto de Sheets usa la sesión directa, CSRF y origen de su propia cuenta, también en el callback compartido',async()=>{
  const { registerCoachConnections,connectionConfig }=await import('../src/coach-connections.js');
  const routes=new Map(), app={get:(p,h)=>routes.set(p,h),post:(p,h)=>routes.set(p,h)};
  const cfg=connectionConfig({COACH_CONNECTIONS_PILOT_EMAILS:'gmailonly@example.com',COACH_SHEETS_PILOT_EMAILS:'a@example.com',
    COACH_GMAIL_CLIENT_ID:'123-example',COACH_GMAIL_CLIENT_SECRET:'secret',COACH_GMAIL_ENCRYPTION_KEY:Buffer.alloc(32,1).toString('base64'),
    COACH_SHEETS_PICKER_KEY:'restricted-key',APP_BASE_URL:'https://tec.example'});
  assert.equal(cfg.sheetsAppId,'123');
  const store={findById:()=>({lean:async()=>null}),updateOne:async()=>({matchedCount:1})};
  registerCoachConnections(app,{config:cfg,sheetsStore:store,switched:r=>Boolean(r.switched),requireActive:async r=>({
    user:{_id:r.id,email:r.id+'@example.com',accountType:r.accountType,name:'Seller '+r.id},session:{accessMode:r.mode,tokenHash:'session-'+r.id}
  })});
  async function invoke(path,{id='a',method='POST',body={},mode='direct',accountType='seat',site='same-origin',query={},switched=false}={}) {
    const res={code:200,set(){return this},vary(){return this},status(n){this.code=n;return this},send(v){this.body=v},json(v){this.body=v},redirect(n,to){this.code=n;this.to=to}};
    await routes.get(path)({path,id,switched,method,body,mode,accountType,query,get:key=>key==='Sec-Fetch-Site'?site:null},res);return res;
  }
  const page=await invoke('/mis-conexiones/',{method:'GET'});
  const csrf=page.body.match(/name="csrf" value="([^"]+)"/)[1];
  assert.match(page.body,/Conectar mi Google Sheets/);
  assert.equal((await invoke(sheetsApi+'start',{body:{csrf}})).code,303);
  for (const options of [{mode:'team_visit'},{mode:null,accountType:'seat'},{switched:true},{id:'b'},
    {site:'same-site'},{site:'cross-site'},{body:{csrf:'foreign'}}]) {
    assert.equal((await invoke(sheetsApi+'start',{body:{csrf},...options})).code,403);
  }
  const returnPath='/api/coach/conexiones/gmail/callback';
  assert.equal((await invoke(returnPath,{method:'GET',query:{state:'sheets.nonce'},mode:'team_visit'})).code,403);
  assert.equal((await invoke(returnPath,{method:'GET',query:{state:'sheets.nonce'},id:'other'})).code,403);
});

test('una autorización sin scope explícito usa tokeninfo y una sesión revocada durante Google no completa ni cambia el destino',async()=>{
  const h=harness();
  h.google.exchange=async code=>({access_token:code,refresh_token:'refresh-'+code});
  const lookedUp=[];
  h.google.tokenScopes=async access=>{lookedUp.push(access);return sheetsScope;};
  h.setActive(false);
  assert.equal(await h.callback('a',(await h.post('start','a')).state,'a'),'expired');
  assert.equal(h.records.get('a').encryptedToken,undefined);
  h.setActive(true);
  assert.equal(await h.callback('a',(await h.post('start','a')).state,'a'),'sheetsConnected');
  assert.deepEqual(lookedUp,['a','a']);
  const grant=h.records.get('a').grant;
  h.setActive(false);
  assert.equal(await h.post('map','a',{grant,spreadsheetId:exampleId,ticketsSheetId:'0',referralsSheetId:'9'}),'expired');
  assert.equal(await h.handler.service.getConnection('a'),null);
});

test('si tokeninfo identifica otro cliente de Google, el permiso de ese token no llega al selector',async()=>{
  const g=sheetsOAuthClient({origin:'https://example.com',clientId:'client'},async()=>Response.json({
    aud:'different-client',azp:'client',scope:sheetsScope
  }));
  assert.equal(await g.tokenScopes('only-for-test'),'');
});
