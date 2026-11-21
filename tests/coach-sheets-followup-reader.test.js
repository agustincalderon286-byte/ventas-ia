import test from 'node:test';
import assert from 'node:assert/strict';
import {createSheetsGoogle} from '../src/coach-sheets-google.js';

const sheetBook = id => ({spreadsheetId:id,ticketsSheetId:0,referralsSheetId:7});
const tabs = {
  0:[['Nombre del prospecto','Teléfonos','Comentarios del telemarketing','Resultado de la cita','Fecha de seguimiento','Estado del prospecto']],
  7:[['REFERENCIA','REFERIDOS'],['','NRO','Nombres','Teléfonos','Fecha del 4/14','Vendedor que prospectó','Comentarios del telemarketing','Resultado de la cita']]
};

function provider({headers=tabs,records,omitColumn,extraLength=0}) {
  const calls = [];
  const request = async (url, init) => {
    const id = new URL(url).pathname.split('/')[3].split(':')[0];
    const body = init.body && JSON.parse(init.body);
    const ranges = body?.dataFilters?.map(f => f.gridRange);
    calls.push({url,token:init.headers.Authorization,ranges});
    const data = records[id];
    const meta = [0,7,99].map(sheetId => ({properties:{sheetId, title:'Pestaña '+sheetId, sheetType:'GRID',gridProperties:{columnCount:15,rowCount:90000}}}));
    if (!body) return Response.json({properties:{title:'Prueba de integración',locale:'es_MX'},sheets:meta});
    if (url.includes('/values:batchGetByDataFilter')) {
      return Response.json({valueRanges:ranges.map(r => {
        const rows = data[r.sheetId];
        const usedRows = Object.keys(rows).filter(i => rows[i]?.[r.startColumnIndex]).map(Number);
        const length = extraLength || Math.max(0,Math.max(-1,...usedRows)+1-r.startRowIndex);
        const values = Array.from({length},(_,i) => {
          const cell = rows[r.startRowIndex+i]?.[r.startColumnIndex];
          return cell ? [cell] : [];
        });
        return {dataFilters:[{gridRange:r}],valueRange:{range:'Tab',...(values.length && {values})}};
      })});
    }
    assert.ok(url.includes(':getByDataFilter'));
    if (ranges[0].startRowIndex === 0) return Response.json({sheets:ranges.map(r=>({
      properties:{sheetId:r.sheetId},data:[{rowData:headers[r.sheetId].map(row => ({values:row.map(value=>({formattedValue:value}))}))}]
    }))});
    const sheetId = ranges[0].sheetId;
    assert.ok(ranges.every(r=>r.sheetId===sheetId && r.endRowIndex-r.startRowIndex<=1000));
    const blocks = ranges.filter(r=>r.startColumnIndex!==omitColumn).map(r=>{
      const rowData = Array.from({length:r.endRowIndex-r.startRowIndex},(_,i)=>{
        const cell = data[sheetId][r.startRowIndex+i]?.[r.startColumnIndex];
        return cell == null ? {} : {values:[typeof cell==='object' ? cell : {formattedValue:cell}]};
      });
      return {startRow:r.startRowIndex,startColumn:r.startColumnIndex,rowData};
    });
    return Response.json({sheets:[{properties:{sheetId},data:blocks}]});
  };
  return {reader:createSheetsGoogle(request),calls};
}
const bookA=sheetBook('spreadsheet-alpha'), bookB=sheetBook('spreadsheet-bravo');
const serial=(Date.UTC(2026,10,21)-Date.UTC(1899,11,30))/86400000;
const records={
  [bookA.spreadsheetId]:{
    0:{1:['Prueba A','(555) 000-0000','Llamar','', {formattedValue:'21/11/26',effectiveValue:{numberValue:serial+0.8},effectiveFormat:{numberFormat:{type:'DATE_TIME'}}},'Pendiente']},
    7:{
      1001:['Anfitrión A\n(312) 555-0191','1','Contacto Uno','555','1/1/21','Vendedora A','Pidió una llamada'],
      1002:['(312) 555-0191','2','Contacto Dos','555','1/1/21','','','Pendiente'],
      1003:tabs[7][0], 1004:tabs[7][1],
      1005:['','1','Contacto Tres','','','','Llamar de nuevo']
    }
  },
  [bookB.spreadsheetId]:{0:{1:['Prueba B','','Contactar']},7:{}}
};

test('reads only mapped follow-up fields, finds leads after sparse sections and preserves groups across pages',async()=>{
  const {reader,calls}=provider({records});
  const {rows,locale}=await reader.readFollowups('alpha-token',bookA);
  assert.equal(locale,'es_MX');
  assert.deepEqual(rows.map(row=>row.fullName),['Prueba A','Contacto Uno','Contacto Dos','Contacto Tres']);
  assert.deepEqual(rows[0],{kind:'tickets',sheetTitle:'Pestaña 0',sheetId:0,row:2,
    fullName:'Prueba A',host:'',seller:'',sellerColumn:false,history:'Llamar',result:'',followupDate:'2026-11-21',status:'Pendiente'});
  assert.deepEqual(rows.slice(1).map(r=>[r.host,r.seller,r.sellerColumn,r.followupDate,r.result,r.row]),[
    ['Anfitrión A','Vendedora A',true,'','',1002], ['Anfitrión A','',true,'','Pendiente',1003],['','',true,'','',1006]]);
  assert.ok(calls.every(call=>call.token==='Bearer alpha-token' && call.url.includes(bookA.spreadsheetId) && !call.url.includes(':batchUpdate')));
  const fieldRanges=calls.flatMap(call=>call.ranges||[]).filter(r=>r.startRowIndex>0);
  assert.ok(fieldRanges.every(r=> r.sheetId===0 ? r.startColumnIndex!==1 : r.sheetId===7 && ![3,4].includes(r.startColumnIndex)));
});

test('separate concurrent reads use each supplied workbook and bearer exclusively', async()=>{
  const {reader,calls}=provider({records});
  const [a,b]=await Promise.all([reader.readFollowups('alpha-token',bookA),reader.readFollowups('bravo-token',bookB)]);
  assert.ok(a.rows.some(row=>row.fullName==='Prueba A'));
  assert.deepEqual(b.rows.map(row=>row.fullName),['Prueba B']);
  assert.ok(calls.every(call=>call.token==='Bearer '+(call.url.includes(bookA.spreadsheetId)?'alpha':'bravo')+'-token'));
});

test('Chicago host captions retain their prospect and backfill from the next row across a page boundary', async()=>{
  const referrals = {
    999:tabs[7][0],1000:tabs[7][1],
    1001:['NOMBRE DEL ANFITRIÓN','1','Uno'],
    1002:['Clara Ejemplo','2','Dos'],
    1007:['TELÉFONO DEL ANFITRIÓN','7','Siete'],
    1008:['312-555-0100','8','Ocho'],
    1013:['El ANFITRIÓN debe completar la lista de referidos y revisar la información.','13','Trece'],
    1018:['Martes. 19/10/21. 1:00pm. Historial del anfitrión.'],
    1021:['','14','Prospecto sin bloque verificable'],
    1022:tabs[7][0],1023:tabs[7][1],
    1025:['NOMBRE DEL ANFITRIÓN','1','Sin nombre confirmado'],
    1026:['TELÉFONO DEL ANFITRIÓN','2','Otro contacto'],
    1027:['3125550111','3','Último del bloque'],
    1030:['Luna Prueba\n(312) 555-0163','1','Carga digital'],
    1031:['','2','Segundo digital'],
    1032:['Historial viejo', '', 'Contacto sin grupo comprobado'],
    1033:['NOMBRE DEL ANFITRIÓN','1','Único referido del bloque nuevo'],
    1034:['Luz Ejemplo']
  };
  const source = {[bookA.spreadsheetId]:{0:{},7:referrals}};
  const {rows} = await provider({records:source}).reader.readFollowups('alpha-token',bookA);
  assert.deepEqual(rows.map(({fullName,host})=>[fullName,host]),[
    ['Uno','Clara Ejemplo'],['Dos','Clara Ejemplo'],['Siete','Clara Ejemplo'],['Ocho','Clara Ejemplo'],['Trece','Clara Ejemplo'],
    ['Prospecto sin bloque verificable',''],['Sin nombre confirmado',''],['Otro contacto',''],['Último del bloque',''],
    ['Carga digital','Luna Prueba'],['Segundo digital','Luna Prueba'],['Contacto sin grupo comprobado',''],
    ['Único referido del bloque nuevo','Luz Ejemplo']
  ]);
});

test('a flat referral sheet takes the host from the same row without inheriting a previous value', async()=>{
  const headers={...tabs,7:[['Anfitrión','Nombre','Teléfono','Comentarios del telemarketing']]};
  const source={[bookA.spreadsheetId]:{0:{},7:{
    1:['Daniela Ejemplo','Uno','555','Pendiente'],2:['','Dos','555','Pendiente'],
    3:['Alma Prueba','Tres','555','Pendiente']
  }}};
  const {rows}=await provider({headers,records:source}).reader.readFollowups('alpha-token',bookA);
  assert.deepEqual(rows.map(row=>row.host),['Daniela Ejemplo','','Alma Prueba']);
});

test('unknown headings, omitted Google ranges, and oversized sheets fail without returning an empty or partial review',async()=>{
  const badHeaders={...tabs,0:[['Nombre','Teléfono','Fecha de venta']]};
  await assert.rejects(provider({records,headers:badHeaders}).reader.readFollowups('a',bookA),e=>e.status===422);
  await assert.rejects(provider({records,omitColumn:2}).reader.readFollowups('a',bookA),e=>e.status===503);
  await assert.rejects(provider({records,extraLength:16001}).reader.readFollowups('a',bookA),e=>e.status===422);
});
