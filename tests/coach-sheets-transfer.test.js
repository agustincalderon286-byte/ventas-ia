import test from 'node:test';
import assert from 'node:assert/strict';
import { createSheetsTransfer } from '../src/coach-sheets-transfer.js';
import { createSheetsGoogle } from '../src/coach-sheets-google.js';

const book = { spreadsheetId:'file_for_test_only', ticketsSheetId:0, referralsSheetId:91 };
const alice = {owner:'one',session:'s1',grant:'g1',access:'access',sellerName:'Prueba',workbook:book,isCurrent:async()=>true};
const bob = {...alice,owner:'two',session:'s2',grant:'g2'};
const contact = (fullName='Julián Ejemplo',phone='(312) 555-0101') => ({fullName,phone});
const draft = (...referrals) => ({kind:'tickets',referrals});
function fixture() {
  const docs = new Map(), locks = new Map();
  const same = (d,f) => d && Object.entries(f).every(([k,v])=>d[k]===v);
  const store = {
    async create(d){docs.set(d._id,{...d});}, async get(f){ const d=docs.get(f._id);return same(d,f)?{...d}:null; },
    async bind(f,c){const d=docs.get(f._id);if (!same(d,f)|| d.selection!=null&&d.selection!==c) return false; d.selection=c;return true;},
    async finish(f,n){const d=docs.get(f._id);d.written=n;delete d.payload;},
    async lock(f,n,t,j){ if ((locks.get(f)?.until||0)>t) return false;locks.set(f,{nonce:n,job:j,until:t+300000});return true; },
    async unlockJob(f,j){if(locks.get(f)?.job===j)locks.delete(f);},
    async owns(f,n){return locks.get(f)?.nonce===n;},async unlock(f,n){if(locks.get(f)?.nonce===n)locks.delete(f);}
  };
  const state={title:'Piloto',tickets:{title:'Boletas',sheetId:0},referrals:{title:'4-14',sheetId:91},contacts:[],notes:new Set()};
  const calls=[];
  const google={read:async()=>state, async append(_token,_book,target,input,selected,marker){
    calls.push(selected); if (state.notes.has(marker)) throw Error('escritura repetida');
    state.notes.add(marker);
    for(const i of selected)state.contacts.push({...input.referrals[i],row:state.contacts.length+2,sheetTitle:target.title});
  }};
  const clock={value:123456789};
  return {docs,locks,state,calls,google,store,clock,service:createSheetsTransfer({google,store,key:Buffer.alloc(32,17),now:()=>clock.value})};
}

test('preview distinguishes exact matches, either single-field match, and a repeated row in the same batch across both tabs', async()=>{
  const f=fixture();
  f.state.contacts.push({sheetTitle:'4-14',row:8,fullName:'JULIAN  EJEMPLO',phone:'3125550111 / +1 312-555-0101'});
  const p=await f.service.preview(alice,draft(contact(),contact('Otra Persona'),contact('Alma Muestra','3125550199'),contact('Alma Muestra','3125550199')));
  assert.deepEqual(p.rows.map(x=>x.status),['duplicate','possible','new','duplicate']);
  assert.deepEqual(p.rows[0].matches,[{sheetTitle:'4-14',row:8,reason:'name_phone',fullName:'JULIAN  EJEMPLO'}]);
  assert.equal(p.rows[3].matches[0].batch,true);
  assert.equal(f.calls.length,0);
  assert.ok(!JSON.stringify([...f.docs.values()]).includes('Julián'));
  await assert.rejects(f.service.confirm(alice,{previewId:p.previewId,selected:[0]}),e=>e.status===422);
  const result=await f.service.confirm(alice,{previewId:p.previewId,selected:[1,2]});
  assert.equal(result.addedCount,2);assert.equal(result.sheetTitle,'Boletas');
  assert.deepEqual(await f.service.confirm(alice,{previewId:p.previewId,selected:[2,1]}),result);
  await assert.rejects(f.service.confirm(alice,{previewId:p.previewId,selected:[2]}),e=>e.status===409);
  assert.equal(f.calls.length,1);
});

test('another seller writing the shared file makes stale duplicate previews require a fresh decision', async()=>{
  const f=fixture();
  const a=await f.service.preview(alice,draft(contact()));
  const b=await f.service.preview(bob,draft(contact()));
  const settled=await Promise.allSettled([f.service.confirm(alice,{previewId:a.previewId,selected:[0]}),f.service.confirm(bob,{previewId:b.previewId,selected:[0]})]);
  assert.equal(settled.filter(item=>item.status==='fulfilled').length,1);
  const [who,id]=settled[0].status==='rejected'?[alice,a.previewId]:[bob,b.previewId];
  await assert.rejects(f.service.confirm(who,{previewId:id,selected:[0]}),e=>e.status===409&&e.preview.rows[0].status==='duplicate');
  assert.equal(f.calls.length,1);
});

test('ambiguous upstream result preserves claim, and later retry retrieves the committed batch instead of appending', async()=>{
  const f=fixture(), p=await f.service.preview(alice,draft(contact()));
  const body={previewId:p.previewId,selected:[0]}, append=f.google.append;
  f.google.append=async(...args)=>{await append(...args);throw Object.assign(Error('Red temporal'),{status:503});};
  await assert.rejects(f.service.confirm(alice,body),e=>e.status===503);
  assert.equal((await f.service.confirm(alice,body)).addedCount,1);
  assert.equal(f.locks.size,0);
  f.clock.value+=300001;
  assert.equal((await f.service.confirm(alice,body)).addedCount,1);
  assert.equal(f.calls.length,1);
});

test('session, seller, grant and destination bind the review. A disconnected grant or expired review cannot write',async()=>{
  const f=fixture(), p=await f.service.preview(alice,draft(contact())), b={previewId:p.previewId,selected:[0]};
  for(const other of [bob,{...alice,session:'other'},{...alice,grant:'other'},{...alice,workbook:{...book,spreadsheetId:'anotherFile'}}]) {
    await assert.rejects(f.service.confirm(other,b),e=>e.status===409);
  }
  await assert.rejects(f.service.confirm({...alice,isCurrent:async()=>false},b),e=>e.status===409);
  await assert.rejects(f.service.preview(alice,{kind:'referrals',referrals:[contact()]}),e=>e.status===422);
  await assert.rejects(f.service.confirm(alice,{...b,selected:[8]}),e=>e.status===422);
  f.clock.value+=31*60000;
  await assert.rejects(f.service.confirm(alice,b),e=>e.status===409);
  assert.equal(f.calls.length,0);
});

const toRow = cells => ({values:cells.map(v=>v==null?{}:{formattedValue:v})});
const ticketHeaders=['','NOMBRE DEL PROSPECTO','ESPOSO (A)','TELEFONOS','LUGAR O TIENDA DONDE SE PROSPECTO','CIUDAD O ZONA DE RESIDENCIA / ZIP CODE','OBSERVACIONES DEL PROSPECTADOR','COMENTARIOS DEL TELEMARKETING','RESULTADO DE LA CITA'];
const refHeaders=['','NRO','NOMBRES','TELEFONOS','CIUDAD O ZONA','FECHA DEL 4/14','CONOCE RP','RELACION CON EL ANFITRION','OBSERVACIONES DEL VENDEDOR','VENDEDOR QUE PROSPECTÓ','COMENTARIOS DEL TELEMARKETING','RESULTADO DE LA CITA'];
function provider(custom) {
  const requests=[];
  const fake=async(url,init)=>{
    const body=init.body&&JSON.parse(init.body);
    requests.push({url,body,init});
    let payload;
    if(!body)payload={properties:{title:'Libro Chicago'},sheets:[0,91].map(id=>({properties:{sheetId:id,sheetType:'GRID',title:'Tab '+id,gridProperties:{columnCount:16,rowCount:1000}}}))};
    else if(url.includes(':batchUpdate'))payload={spreadsheetId:book.spreadsheetId,replies:[{}]};
    else if(body.dataFilters[0].gridRange.startRowIndex===0){
      const headers=custom||[[ticketHeaders],[['REFERENCIA','REFERIDOS'],refHeaders]];
      payload={sheets:headers.map((rows,i)=>({properties:{sheetId:i?91:0},data:[{rowData:rows.map(toRow)}]}))};
    } else {
      const sheets=new Map();
      for(const {gridRange:r} of body.dataFilters){
        if(!sheets.has(r.sheetId))sheets.set(r.sheetId,{properties:{sheetId:r.sheetId},data:[]});
        const d={startRow:r.startRowIndex,startColumn:r.startColumnIndex};
        if(r.sheetId===0&&r.startColumnIndex===1)d.rowData=[{values:[{formattedValue:'Solo Prueba',note:'TEC:demo'}]}];
        if(r.sheetId===0&&r.startColumnIndex===3)d.rowData=[{values:[{formattedValue:'3.13E+09',effectiveValue:{numberValue:3125550192}}]}];
        sheets.get(r.sheetId).data.push(d);
      }
      payload={sheets:[...sheets.values()]};
    }
    return Response.json(payload);
  };
  return {google:createSheetsGoogle(fake),requests};
}

test('Chicago layout preserves host blocks, telephone strings, vendor/date, and never writes history/result or turns contact data into formulas',async()=>{
  const f=provider(), result=await f.google.read('token',book);
  assert.deepEqual(result.contacts,[{sheetId:0,sheetTitle:'Tab 0',row:2,fullName:'Solo Prueba',phone:'3125550192'}]);
  assert.ok(result.notes.has('TEC:demo'));
  const input={kind:'referrals',hostName:'Anfitriona Ejemplo',hostPhone:'3125550110',startDate:'2026-01-22',sellerName:'Vendedor Ejemplo',
    referrals:[{...contact('=Texto','3125550101'),city:'Chicago',relationship:'Amiga'},{...contact('Vecina','3125550102'),city:'Elgin'}]};
  await f.google.append('token',book,result.referrals,input,[0,1],'TEC:lote:0,1');
  const append=f.requests.at(-1).body.requests[0].appendCells;
  assert.equal(append.sheetId,91);assert.equal(append.rows.length,2);
  const [first,second]=append.rows.map(r=>r.values);
  assert.deepEqual(first[0].userEnteredValue,{stringValue:'Anfitriona Ejemplo\n3125550110'});
  assert.deepEqual(first[2],{userEnteredValue:{stringValue:'=Texto'},note:'TEC:lote:0,1'});
  assert.deepEqual(first[3].userEnteredValue,{stringValue:'3125550101'});
  assert.equal(first[5].userEnteredValue.stringValue,'2026-01-22');
  assert.equal(first[7].userEnteredValue.stringValue,'Amiga');assert.equal(first[9].userEnteredValue.stringValue,'Vendedor Ejemplo');
  assert.equal(second[1].userEnteredValue.numberValue,2);assert.deepEqual(second[0],{});
  assert.equal(first[10],undefined);assert.equal(first[11],undefined);
  await f.google.append('token',book,result.tickets,{...input,kind:'tickets'},[0],'TEC:otro:0');
  const ticket=f.requests.at(-1).body.requests[0].appendCells.rows[0].values;
  assert.equal(ticket[1].userEnteredValue.stringValue,'=Texto');assert.equal(ticket[3].userEnteredValue.stringValue,'3125550101');assert.equal(ticket[5].userEnteredValue.stringValue,'Chicago');
  assert.equal(ticket[7],undefined);assert.equal(ticket[8],undefined);
});

test('custom personal sheets detect columns by heading instead of assuming Chicago letters; unknown headers fail without writes',async()=>{
  const f=provider([[['Teléfono','Ciudad','Nombre']],[['Nombre','Anfitrión','Teléfono','Dirección']]]);
  const state=await f.google.read('token',book);
  assert.equal(state.tickets.cols.fullName,2);assert.equal(state.referrals.cols.host,1);assert.equal(state.referrals.cols.phone,2);
  await assert.rejects(provider([[['Monto','Ubicación']],[['Nombre','Anfitrión','Teléfono']]]).google.read('token',book), e=>e.status===422);
});
