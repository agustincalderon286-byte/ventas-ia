import test from 'node:test';
import assert from 'node:assert/strict';
import {createCoachSheetTools} from '../src/coach-sheets-tools.js';
import {createSheetsGoogle} from '../src/coach-sheets-google.js';

const today='2026-11-21';
const ctx=id=>({owner:id,session:'sesion-'+id,grant:'permiso-'+id,access:'token-'+id,sellerName:id+' Ejemplo',
  workbook:{spreadsheetId:'hoja-de-equipo',ticketsSheetId:0,referralsSheetId:9},isCurrent:async()=>true});
const cell=(value,column=7)=>({column,displayed:value,formula:false});
function row(name,host,seller,rowNum,old='2026-11-17: llamar el 2026-11-20.') {
  return {kind:'referrals',sheetId:9,sheetTitle:'4 en 14',row:rowNum,fullName:name,host,seller,sellerColumn:true,
    phone:'31255501'+String(rowNum).padStart(2,'0'),email:'correo'+rowNum+'@example.invalid',history:old,result:'',status:'',followupDate:'',
    editable:{history:cell(old)}};
}
function fixture() {
  const rows=[row('Ana López','Rosa Silva','Agustín',4),row('Ana Luna','Rosa Silva','Rigoberto',5),row('Luis Soto','Rosa Jiménez','',6)];
  const writes=[],calls=[];
  const google={
    async readFollowups(access,book,opts) {
      assert.ok(opts.forPlan);
      calls.push({access,book:book.spreadsheetId});
      return {rows:structuredClone(rows),locale:'en_US'};
    },
    async replaceFollowupCell(access,book,{row,field,column,before,after}) {
      const lead=rows.find(item=>item.row===row);
      field=Object.keys(lead.editable).find(key=>lead.editable[key].column===column);
      if (lead.editable[field].displayed!==before || lead.editable[field].formula) return false;
      lead.editable[field].displayed=after; lead[field]=after;
      writes.push({access,row,column});return true;
    }
  };
  const transfer={
    preview:async(c,input)=>({previewId:'agregar',rows:[{status:input.referrals[0].phone==='duplicado'?'duplicate':'new',...input.referrals[0]}],sheetTitle:'Boletas'}),
    confirm:async(c,b)=>{assert.equal(b.previewId,'agregar');writes.push({owner:c.owner,add:true});return {written:1,sheetTitle:'Boletas'};}
  };
  return {rows,writes,calls,google,tools:createCoachSheetTools({google,transfer})};
}
test('la búsqueda de Rosa respeta vendedor y anfitrión, permite elegir homónimos y mantiene las notas privadas',async()=>{
  const {tools,rows}=fixture();
  const a=ctx('Agustín'), r=ctx('Rigoberto');
  let ra=await tools.search(a,{host:'Rósa Jimenez',today});
  assert.deepEqual(ra.contacts.map(x=>x.fullName),['Luis Soto']);
  ra=await tools.search(a,{host:'Rosa',bucket:'overdue',today});
  const rr=await tools.search(r,{host:'Rosa',today});
  assert.deepEqual(ra.contacts.map(x=>x.fullName),['Ana López','Luis Soto']);
  assert.deepEqual(rr.contacts.map(x=>x.fullName),['Ana Luna','Luis Soto']);
  assert.equal(ra.contacts[0].email,rows[0].email);
  assert.equal(ra.contacts[0].history,undefined);
  assert.notEqual(ra.contacts[1].identityKey,rr.contacts[1].identityKey);
  await assert.rejects(tools.revalidate(r,ra.contacts[0].identityKey),/fila/);
});

test('nota en Chicago y nueva fecha, solo bajo confirmación propia; conserva historial y reintenta sin duplicar',async()=>{
  const {tools,rows,writes}=fixture(),a=ctx('Agustín');
  const contact=(await tools.search(a,{name:'Ana Lopez',today})).contacts[0];
  const draft=await tools.prepareNote(a,{key:contact.key,note:'Pidió información sobre la demo.',today,followupDate:'2026-11-27'});
  assert.match(draft.preview,/Ana López.+fila 4/);assert.equal(writes.length,0);
  await assert.rejects(tools.confirmNote(ctx('Rigoberto'),draft.proposal),/fila/);
  await assert.rejects(tools.confirmNote({...a,grant:'permiso-nuevo'},draft.proposal),/fila/);
  assert.equal((await tools.confirmNote(a,draft.proposal)).status,'saved');
  assert.equal((await tools.confirmNote(a,draft.proposal)).status,'saved');
  assert.equal(writes.length,1);
  assert.match(rows[0].history,/2026-11-17:.+ \/ 2026-11-21: Pidió información sobre la demo\. Seguimiento el 2026-11-27\./);
  assert.equal((await tools.search(a,{name:'López',today:'2026-11-27',bucket:'today'})).total,1);
  assert.equal(await tools.revalidate(a,contact.identityKey),true);
  await assert.rejects(tools.prepareNote(a,{key:contact.key,note:'Tras cambio',today}),/fila/);
});

test('cambios por telemarketing, permisos revocados, celdas de fórmula y cambios de columna detienen escrituras',async()=>{
  for (const mutate of [
    r=>{r.status='No llamar'},r=>{r.phone='3125550199'},r=>{r.seller='Rigoberto'},
    r=>{r.editable.history.column++},r=>{r.editable.history.formula=true},
    r=>{r.history='Habló con Rosa';r.editable.history.displayed=r.history;}
  ]) {
    const {tools,rows,writes}=fixture(),a=ctx('Agustín');
    const {contacts}=await tools.search(a,{name:'Ana',today});
    const {proposal}=await tools.prepareNote(a,{key:contacts[0].key,note:'Pidió la demo',today});
    mutate(rows[0]);
    await assert.rejects(tools.confirmNote(a,proposal),/fila/);
    assert.equal(writes.length,0);
  }
});

test('reconcilia una confirmación Google perdida y una fecha en su columna dedicada',async()=>{
  const {tools,rows,writes,google}=fixture(),a=ctx('Agustín');
  rows[0].followupDate='2026-11-20'; rows[0].editable.followupDate=cell('11/20/26',8);
  const {contacts}=await tools.search(a,{name:'Ana',today});
  const draft=await tools.prepareNote(a,{key:contacts[0].key,note:'Llamar de nuevo',followupDate:'2026-11-24',today});
  const real=google.replaceFollowupCell;
  let timeout=true;google.replaceFollowupCell=async(...args)=>{await real(...args);if(timeout){timeout=false;throw Error('Respuesta perdida');}return true;};
  await assert.rejects(tools.confirmNote(a,draft.proposal),/perdida/);
  assert.equal((await tools.confirmNote(a,draft.proposal)).status,'saved');
  assert.equal((await tools.confirmNote(a,draft.proposal)).status,'saved');
  assert.equal(rows[0].followupDate,'2026-11-24');assert.equal(writes.length,2);
});

test('un alta reutiliza el control de duplicados y requiere confirmar con su permiso y libro',async()=>{
  const {tools,writes}=fixture(),a=ctx('Agustín');
  const add={fullName:'Tania Ejemplo',phone:'3125550166',kind:'tickets'};
  await assert.rejects(tools.prepareAdd(a,{...add,phone:'duplicado'}),/ya figuran/);
  const draft=await tools.prepareAdd(a,add);
  assert.match(draft.preview,/Tania Ejemplo.+3125550166.+Boletas/);assert.equal(writes.length,0);
  await assert.rejects(tools.confirmAdd({...a,session:'otra'},draft.proposal),/fila/);
  assert.equal((await tools.confirmAdd(a,draft.proposal)).status,'saved');assert.equal(writes.length,1);
});

test('el proveedor limita el reemplazo al valor anterior de una sola celda y excluye fórmulas',async()=>{
  const seen=[];
  const google=createSheetsGoogle(async(_url,opts)=>{
    const op=JSON.parse(opts.body).requests[0].findReplace;
    seen.push(op);
    return Response.json({replies:[{findReplace:{occurrencesChanged:seen.length===1?1:0}}]});
  });
  const base={sheetId:9,row:4,column:7,before:'Nota',after:'Nota / Gestión'};
  assert.equal(await google.replaceFollowupCell('actor',ctx('a').workbook,base),true);
  assert.equal(await google.replaceFollowupCell('actor',ctx('a').workbook,{...base,before:'',after:'2026-11-21: cuesta $70'}),false);
  assert.deepEqual(seen[0].range,{sheetId:9,startRowIndex:3,endRowIndex:4,startColumnIndex:7,endColumnIndex:8});
  assert.ok(seen[0].matchEntireCell && seen[0].matchCase);assert.equal(seen[0].includeFormulas,false);
  assert.equal(seen[1].find,'^$');assert.equal(seen[1].replacement,'2026-11-21: cuesta \\$70');
});

test('la caché de consulta pertenece a una petición; revalidar revisa la identidad actual y no reutiliza datos',async()=>{
  const {tools,rows,calls}=fixture(),a=ctx('Agustín');
  const [first]=await Promise.all(['overdue','today','undated'].map(bucket=>tools.search(a,{bucket,today})));
  assert.equal(calls.length,1);
  const selected=first.contacts[0];
  rows[0].email='otro@example.invalid';
  await assert.rejects(tools.revalidate(a,selected.identityKey),/fila/);
  assert.equal(calls.length,2);
  const changed=(await tools.search(ctx('Agustín'),{name:'Ana',today})).contacts[0];
  assert.equal(changed.email,rows[0].email);assert.notEqual(changed.identityKey,selected.identityKey);
  rows[0].seller='Rigoberto';
  await assert.rejects(tools.revalidate(a,changed.identityKey),/fila/);
});

test('la búsqueda con detalle pide la dirección y entrega los datos de contacto, salvo el teléfono de quien pidió no ser llamado',async()=>{
  const seen=[];
  const base={kind:'referrals',sheetId:9,sheetTitle:'4 en 14',host:'Rosa Silva',seller:'',sellerColumn:false,result:'',status:'',followupDate:'',editable:{}};
  const google={async readFollowups(access,book,opts) {
    seen.push(opts);
    return {locale:'en_US',rows:[
      {...base,row:4,fullName:'Lidia Ejemplo',phone:'(312) 555-0131',email:'lidia@example.invalid',address:'12 Calle Ficticia',city:'Chicago',history:'Quiere la olla grande.'},
      {...base,row:5,fullName:'Lidia Prueba',phone:'312-555-0132',email:'',history:'Pidió que no la llamen.'}]};
  }};
  const tools=createCoachSheetTools({google});
  const detailed=await tools.search(ctx('agustin'),{name:'Lidia',today,detail:true});
  assert.equal(seen[0].withAddress,true);
  assert.deepEqual(detailed.contacts.map(c=>[c.fullName,c.rawPhone,c.address,c.rawEmail,c.noCalls,c.history]),[
    ['Lidia Ejemplo','(312) 555-0131','12 Calle Ficticia, Chicago','lidia@example.invalid',false,'Quiere la olla grande.'],
    ['Lidia Prueba','','','',true,'Pidió que no la llamen.']]);
  const plain=await tools.search(ctx('rigoberto'),{name:'Lidia',today});
  assert.equal(seen[1].withAddress,undefined);
  assert.equal('address' in plain.contacts[0],false);
  assert.equal('rawPhone' in plain.contacts[0],false);
});

test('la lectura con detalle tolera nombres incompletos o mal escritos, busca en las notas y ordena por zona',async()=>{
  const base={kind:'referrals',sheetId:9,sheetTitle:'4 en 14',host:'Rosa Silva',seller:'',sellerColumn:false,result:'',status:'',followupDate:'',editable:{},email:''};
  const sheet=[
    {...base,row:4,fullName:'Lidia Ejemplo',phone:'312-555-0131',address:'12 Calle Ficticia',city:'Blue Island, IL 60406',history:'2026-11-17: quiere la olla grande, llamar el 2026-11-20.'},
    {...base,row:5,fullName:'Pedro Ficticio',phone:'312-555-0132',address:'9 Av. Prueba',city:'Chicago Heights',history:''},
    {...base,row:6,fullName:'Ana Demostración',phone:'312-555-0133',address:'3 Calle Ejemplo, Chicago, IL 60617',city:'',history:''},
    {...base,row:7,fullName:'Luis Ejemplo',phone:'312-555-0134',address:'',city:'Blue Island',history:'Pidió que no la llamen.'},
    {...base,row:8,fullName:'Marta Prueba',phone:'312-555-0135',address:'',city:'',history:''},
    {...base,kind:'tickets',sheetId:0,sheetTitle:'Rifa',row:3,fullName:'Jorge Prueba',phone:'312-555-0136',address:'',city:'Blue Island',history:''}];
  const tools=createCoachSheetTools({google:{async readFollowups() { return {locale:'en_US',rows:structuredClone(sheet)}; }}});
  const names=async(args,id='agustin')=>{const r=await tools.search(ctx(id),{today,detail:true,...args});return [r.contacts.map(c=>c.fullName),Boolean(r.approximate)];};

  assert.deepEqual(await names({name:'Lidia'}),[['Lidia Ejemplo'],false]);
  assert.deepEqual(await names({name:'Lidya'},'b'),[['Lidia Ejemplo'],true]);
  assert.deepEqual(await names({name:'Lid'},'c'),[['Lidia Ejemplo'],true]);
  assert.deepEqual(await names({name:'Lidia Ejemlo'},'d'),[['Lidia Ejemplo'],true]);
  assert.deepEqual(await names({name:'Zoila'},'e'),[[],false]);
  assert.deepEqual(await names({words:'OLLA grande'},'f'),[['Lidia Ejemplo'],false]);
  assert.deepEqual(await names({words:'sartén'},'g'),[[],false]);
  // Fuera del modo detalle el nombre sigue exigiendo palabras exactas.
  assert.equal((await tools.search(ctx('h'),{name:'Lidya',today})).total,0);

  const catalog=await tools.byZone(ctx('i'),{today});
  assert.deepEqual(catalog.cities.map(c=>[c.name,c.count]),[['Blue Island',3],['Chicago Heights',1]]);
  assert.deepEqual(catalog.zips,[{code:'60406',count:1},{code:'60617',count:1}]);
  assert.equal(catalog.unplaced,1);
  assert.equal(catalog.total,6);

  const near=await tools.byZone(ctx('j'),{zones:['Blue Island','Chicago','Chicago Heights'],today,limit:10});
  assert.deepEqual(near.contacts.map(c=>[c.fullName,c.zoneIndex]),[['Lidia Ejemplo',0],['Jorge Prueba',0],['Luis Ejemplo',0],['Ana Demostración',1],['Pedro Ficticio',2]]);
  assert.deepEqual(near.perZone,[3,1,1]);
  assert.equal(near.contacts[0].bucket,'overdue');
  assert.equal(near.contacts[0].address,'12 Calle Ficticia, Blue Island, IL 60406');
  assert.equal(near.contacts[2].noCalls,true);
  assert.equal(near.contacts[2].rawPhone,'');
  const byZip=await tools.byZone(ctx('k'),{zones:['60617','60406'],today,kind:'referrals',limit:1});
  assert.deepEqual([byZip.contacts.map(c=>c.fullName),byZip.total,byZip.perZone],[['Ana Demostración'],2,[1,1]]);
  assert.equal((await tools.byZone(ctx('l'),{zones:['Harvey'],today})).total,0);
});

test('la ficha por identidad entrega la versión vigente de la fila y el teléfono marcable, solo al vendedor que la leyó',async()=>{
  const cellAt=value=>({column:7,displayed:value,formula:false});
  const base={kind:'referrals',sheetId:9,sheetTitle:'4 en 14',host:'Rosa Silva',seller:'',sellerColumn:false,result:'',status:'',followupDate:''};
  const rows=[
    {...base,row:4,fullName:'Lidia Ejemplo',phone:'(312) 555-0131',email:'lidia@example.invalid',address:'12 Calle Ficticia',city:'Chicago',history:'Quiere la olla grande.',editable:{history:cellAt('Quiere la olla grande.')}},
    {...base,row:5,fullName:'Lidia Prueba',phone:'312-555-0132',email:'',history:'Pidió que no la llamen.',editable:{history:cellAt('Pidió que no la llamen.')}}];
  const tools=createCoachSheetTools({google:{async readFollowups() { return {locale:'en_US',rows:structuredClone(rows)}; }}});
  const seller=ctx('agustin');
  const [first,second]=(await tools.search(seller,{name:'Lidia',today,detail:true})).contacts;
  const card=await tools.card(seller,first.identityKey);
  assert.deepEqual([card.fullName,card.row,card.sheetTitle,card.phone,card.email,card.noCalls],['Lidia Ejemplo',4,'4 en 14','+13125550131','lidia@example.invalid',false]);
  assert.equal('address' in card,false);
  // La versión sirve para preparar la nota aunque la búsqueda se hiciera con dirección.
  assert.match((await tools.prepareNote(seller,{key:card.key,note:'No contestó.',today})).preview,/Lidia Ejemplo — 4 en 14, fila 4\./);
  const quiet=await tools.card(seller,second.identityKey);
  assert.equal(quiet.noCalls,true);
  assert.equal('phone' in quiet,false);
  await assert.rejects(tools.card(ctx('rigoberto'),first.identityKey),/fila/);
  rows[0].fullName='Lidia Otra';
  await assert.rejects(tools.card(seller,first.identityKey),/fila/);
});
