import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createLiveRecordReader } from '../src/coach-live-records.js';
import { sealToken, gmailScope } from '../src/coach-hycite-gmail.js';

const addresses=['agustincalderon286@gmail.com','calderonrigoberto51@gmail.com'];
const user=i=>({user:{_id:'seller-'+i,email:addresses[i],name:i?'Rigoberto Calderon':'Agustin Calderon'},session:{tokenHash:'test-login-'+i,accessMode:'direct'}});
const at=Date.parse('2026-11-21T18:00:00Z');
function fixture(overrides={}) {
  const key=Buffer.alloc(32,3), calls=[], book={grant:'grant',spreadsheetId:'selected-book',ticketsSheetId:1,referralsSheetId:2};
  const config={key,origin:'https://example.invalid',pilot:addresses,sheetsPilot:addresses,ready:true,sheetsReady:true,domains:['hycite.com']};
  const model=(name)=>({aggregate:pipeline=>{
    calls.push([name,pipeline[0].$match]);
    return {collation(){return this;},option:()=>[{totals:[{sheets:1,referrals:3}],latest:[{hostName:name==='native'?'Anfitrión Uno':'Anfitrión Dos',names:['A','B'],createdAt:new Date(at)}]}]};
  }});
  const person=owner=>({_id:owner,grant:'grant-'+owner,mailbox:'connected@example.invalid',encryptedToken:sealToken('refresh-'+owner,key,owner)});
  const deps={config,
    gmailStore:{findById:id=>({lean:async()=>{calls.push(['gmail connection',id]);return person(id);}}),exists:async q=>q.grant==='grant-'+q._id},
    gmail:{refresh:async token=>({access_token:token,scope:gmailScope}),inbox:async(token,options)=>{
      calls.push(['inbox',token,options]);return [{subject:'Cita de muestra',from:'Equipo',snippet:'El horario propuesto es por la tarde.'},{subject:'Cambios del guion',from:'Equipo',snippet:'El orden de la demo cambió.'}];
    },notices:async token=>{calls.push(['notices',token]);return {confirmed:[{order:'A-100',date:new Date(at).toISOString()}],review:[{subject:'Recibida',date:new Date(at).toISOString()}],limited:true};}},
    sheets:{access:async owner=>{calls.push(['sheets connection',owner]);return {...book,token:'access-'+owner};},getConnection:async()=>book,current:async()=>true},
    sheetsGoogle:{readFollowups:async token=>{calls.push(['sheets',token]);return {locale:'en_US',rows:[
      {kind:'referrals',fullName:'Contacto A',host:'Anfitrión Alfa',seller:'Agustin',followupDate:'11/21/2026'},
      {kind:'tickets',fullName:'Contacto B',seller:'',status:'pendiente'},
      {kind:'referrals',fullName:'Contacto C',seller:'Rigoberto',host:'Anfitrión Gamma'}
    ]};}},
    fetcher:async (url,init)=>{const u=new URL(url);calls.push(['google get',u.searchParams.get('q'),init.method]);
      assert.equal(init.method,'GET');assert.equal(u.hostname,'gmail.googleapis.com');
      if(u.pathname.endsWith('/messages'))return Response.json({messages:[{id:'a1'}],nextPageToken:'more'});
      return Response.json({id:'a1',snippet:'Invitación a un taller',payload:{headers:[{name:'Subject',value:'Novedades de la semana'},{name:'From',value:'Pruebas'}]}});
    },
    programSheets:model('native'),personalProgramSheets:model('private'),programQuery:u=>({ownerUserId:u._id,generatedByUserId:{$in:[u._id,null]}}),
    sessionCurrent:async()=>true,now:()=>at,...overrides};
  return {read:createLiveRecordReader(deps), calls,deps};
}
test('las frases del vendedor separan correo general, avisos de Hy Cite y confirmación de ventas',async()=>{
  const {read,calls}=fixture();
  const plain=await read(user(0),'Jorge, revisa mi correo, dime qué novedades hay');
  assert.deepEqual(plain.sources,['gmail']);assert.match(plain.text,/Cita de muestra/);
  assert.equal(calls.find(x=>x[0]==='inbox')[1],'refresh-seller-0');
  assert.ok(!calls.some(x=>x[0]==='notices' || x[0]==='sheets'));
  const company=await read(user(0),'Revisa mi correo y dime si tengo correos de Hy Cite');
  assert.match(company.text,/Novedades de la semana/);assert.doesNotMatch(company.text,/ventas confirmadas/);
  assert.ok(calls.some(x=>x[1]==='{from:(@hycite.com)}'));
  const result=await read(user(1),'¿Cuántas ventas confirmadas de Hy Cite tengo hoy?');
  assert.match(result.text,/1 venta confirmada y 1 aviso por revisar/); assert.match(result.text,/solo los 100/);
  assert.deepEqual(calls.filter(x=>x[0]==='notices'),[['notices','refresh-seller-1']]);
});
test('las capturas de los dos modelos y la hoja compartida se consultan con alcance de vendedor y en una sola lectura de Sheets',async()=>{
  for(const phrase of ['Revisa mi cuatro en catorce','¿Cómo va mi 4 en 14?','Revisa mi correo y mi 4 en 14']) {
    const {read,calls}=fixture(),out=await read(user(0),phrase);
    assert.match(out.text,/2 hojas guardadas y 6 referidos/);assert.match(out.text,/En Sheets, 4 en 14: 1 registro/);
    assert.doesNotMatch(out.text,/Contacto C/);assert.equal(out.sources.includes('gmail'),phrase.includes('correo'));
    const native=calls.find(c=>c[0]==='native')[1],personal=calls.find(c=>c[0]==='private')[1];
    assert.deepEqual(native.generatedByUserId,{$in:['seller-0',null]});assert.equal(personal.ownerUserId,'seller-0');
    assert.equal(calls.filter(c=>c[0]==='sheets').length,1);assert.ok(out.text.length<=500);
  }
});
test('boletas, siguientes preguntas y novedades no confunden a los vendedores ni hacen lecturas irrelevantes',async()=>{
  const {read,calls}=fixture();
  const q='Revisa mi correo',prior=await read(user(0),q);
  const previous=[{role:'user',content:q},{role:'assistant',content:prior.text,sources:prior.sources}];
  const next=await read(user(0),'¿Y qué dice el segundo?',previous);
  assert.match(next.text,/El orden de la demo cambió/);
  const more=await read(user(1),'¿Y mis boletas?',previous);
  assert.deepEqual(more.sources,['sheetsTickets']);assert.match(more.text,/Contacto B/);assert.match(more.text,/sin vendedor asignado/);
  const refs=await read(user(1),'Lee mis referidos en Sheets');
  assert.match(refs.text,/Contacto C/);assert.doesNotMatch(refs.text,/Contacto A/);
  assert.equal(await read(user(0),'¿Cómo se llena el 4 en 14?'),null);
  assert.equal(await read(user(0),'Redacta un correo a mi equipo'),null);
  assert.equal(await read(user(0),'Necesito practicar el cierre'),null);
  assert.equal(await read(user(0),'¿Qué dice el segundo?',[{role:'assistant',content:'viernes',sources:['calendar']}]),null);
  assert.ok(!calls.some(c=>c[0]==='native'));
});
test('distingue 0, acceso pendiente y lectura fallida; descarta datos si la concesión, el archivo o la sesión cambian',async()=>{
  let f=fixture({gmailStore:{findById:()=>({lean:async()=>null})}});
  assert.match((await f.read(user(0),'Revisa mi correo')).text,/Conecta tu Gmail/);
  f=fixture({gmail:{refresh:async()=>({access_token:'test'}),inbox:async()=>[]}});
  assert.match((await f.read(user(0),'Revisa mi correo')).text,/no encontré/);
  f=fixture({gmail:{refresh:async()=>{throw Error('private provider error')}}});
  assert.match((await f.read(user(0),'Revisa mi correo')).text,/No pude completar/);
  f=fixture({sheets:{access:async()=>({grant:'abc',token:'a'}),getConnection:async()=>({grant:'abc',spreadsheetId:'changed'}),current:async()=>true}});
  assert.match((await f.read(user(0),'Revisa mis boletas')).text,/Cambió la hoja/);
  let checks=0;f=fixture({sessionCurrent:async()=>++checks===1});
  assert.deepEqual(await f.read(user(0),'Revisa mi correo'),{text:'Tu sesión cambió. Vuelve a abrir Jorge desde tu propia cuenta.',sources:[]});
  f=fixture();const delegated=user(0);delegated.session.accessMode='switched';
  assert.deepEqual((await f.read(delegated,'Revisa mi correo')).sources,[]);assert.equal(f.calls.length,0);
});
test('solo enumera fuentes que pudo leer y nunca atribuye ventas fuera de los 90 días',async()=>{
  const missing=fixture({gmailStore:{findById:()=>({lean:async()=>null})}});
  const combined=await missing.read(user(0),'Revisa mi correo y mi 4 en 14');
  assert.match(combined.text,/Conecta tu Gmail/);
  assert.deepEqual(combined.sources,['tec414','sheets414']);
  const {read,calls}=fixture();
  const rejected=await read(user(0),'¿Cuántas ventas confirmadas hubo en 2024?');
  assert.deepEqual(rejected.sources,[]);assert.match(rejected.text,/90 días/);assert.ok(!calls.some(c=>c[0]==='notices'));
  const old=new Date(at-91*86_400_000).toISOString(),future=new Date(at+86_400_000).toISOString();
  const bounded=fixture({gmail:{refresh:async()=>({access_token:'m'}),notices:async()=>({confirmed:[{date:old},{date:future}],review:[]})}});
  const result=await bounded.read(user(0),'Revisa mis ventas confirmadas de Hy Cite');
  assert.match(result.text,/0 ventas confirmadas/);assert.deepEqual(result.sources,['gmail']);
});
