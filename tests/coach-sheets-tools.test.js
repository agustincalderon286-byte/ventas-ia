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
