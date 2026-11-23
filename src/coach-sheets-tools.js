import { createHash } from 'node:crypto';
import { saleScope } from './coach-sheets-agent.js';
import { dialNumber, reviewConfirmedSales } from './coach-postsale.js';
import { classifySheetsFollowup, excludesPhoneContact } from './coach-sheets-followup-input.js';
import { createSheetsGoogle } from './coach-sheets-google.js';
import { createSheetsTransfer } from './coach-sheets-transfer.js';
import { AGENDA_HEADERS, RESULTS } from './coach-sheets-template.js';

const hash = parts => createHash('sha256').update(JSON.stringify(parts)).digest('base64url');
const fail = (status,message) => Object.assign(new Error(message),{status});
const text = value => String(value ?? '').trim();
const fold = value => text(value).normalize('NFD').replace(/\p{M}/gu,'').toLowerCase().replace(/[^\p{L}\p{N}]+/gu,' ').trim();
const conflict = () => fail(409,'La fila, su asignación o el historial cambió. Revisa el contacto y prepara una vista previa nueva.');

function validDate(value) {
  return typeof value==='string' && /^\d{4}-\d{2}-\d{2}$/.test(value) &&
    Number.isFinite(Date.parse(value)) && new Date(value).toISOString().slice(0,10)===value;
}

function scope(ctx) {
  if (!ctx?.owner || !ctx.session || !ctx.access || !ctx.grant || !ctx.workbook?.spreadsheetId) {
    throw fail(403,'Conecta tu propia cuenta de Google Sheets y elige las dos pestañas.');
  }
  return hash([String(ctx.owner),String(ctx.session),ctx.grant,ctx.workbook.spreadsheetId,
    ctx.workbook.ticketsSheetId,ctx.workbook.referralsSheetId]);
}

async function check(ctx) {
  if (typeof ctx.isCurrent!=='function' || !await ctx.isCurrent()) throw fail(409,'Tu sesión o el acceso al archivo cambió. Vuelve a abrir tu Coach.');
}

function identity(binding,row) {
  return hash([binding,row.sheetId,row.row,row.fullName,row.phone,row.email,row.host,row.seller,row.sellerColumn]);
}

function version(id,row) {
  return hash([id,row.history,row.followupDate,row.status,row.result,row.editable]);
}

function matchWords(label,query) {
  if (!query) return true;
  const words=new Set(fold(label).split(' '));
  return query.split(' ').every(word=>words.has(word));
}

// Coincidencia tolerante para lecturas: prefijos («Lidi») y una letra de diferencia («Lidya»).
// Nunca se usa para elegir la ficha de un cambio; ahí el nombre debe coincidir exacto.
function near(a,b) {
  if (a===b) return true;
  if (Math.min(a.length,b.length)<4 || Math.abs(a.length-b.length)>1) return false;
  let i=0, j=0, edits=0;
  while (i<a.length && j<b.length) {
    if (a[i]===b[j]) { i++; j++; continue; }
    if (++edits>1) return false;
    if (a.length>b.length) i++; else if (b.length>a.length) j++; else { i++; j++; }
  }
  return edits+(a.length-i)+(b.length-j)<=1;
}
function matchLoose(label,query) {
  if (!query) return true;
  const words=fold(label).split(' ');
  return query.split(' ').every(q=>words.some(word=>(q.length>=3 && word.startsWith(q)) || near(word,q)));
}

// Ciudad o zona de una ficha sin código postal ni estado, para comparar nombres de lugar.
const zipOf = value => /\b(\d{5})(?:\s\d{4})?\b/.exec(fold(value))?.[1] || '';
const cityCore = value => fold(value).replace(/\b\d{5}(?: \d{4})?\b/g,' ').replace(/\b(?:il|illinois|in|indiana|wi|wisconsin|usa|eeuu)\b/g,' ').replace(/\s+/g,' ').trim();

function detailOf(hit,followup) {
  const {row}=hit;
  const noCalls=excludesPhoneContact(row);
  return {...contactOf(hit,followup),status:text(row.status),result:text(row.result),followupDate:text(row.followupDate),
    history:text(row.history).slice(-600),noCalls,rawPhone:noCalls ? '' : text(row.phone).slice(0,40),
    rawEmail:text(row.email).slice(0,254),address:[text(row.address),text(row.city)].filter(Boolean).join(', ').slice(0,240)};
}

function noteValue(before,entry) {
  const value=before ? before+' / '+entry : entry;
  if (value.length>50000) throw fail(422,'La nota excede el espacio disponible en la celda del historial.');
  return value;
}

function contactOf({row,key,id},followup,withPhone=false) {
  const email=text(row.email);
  const phone=withPhone && !excludesPhoneContact(row) ? dialNumber(row.phone) : '';
  return {key,identityKey:id,fullName:row.fullName,host:row.host,kind:row.kind,row:row.row,sheetTitle:row.sheetTitle,
    ...(phone && {phone}),
    ...(email.length<=254 && /^[^\s@,;:<>]+@[^\s@,;:<>]+\.[^\s@,;:<>]+$/.test(email) && {email}),
    ...(followup && {date:followup.date || '',reason:followup.reason,bucket:followup.bucket})};
}

// Fecha y hora de la Agenda: Sheets entrega días desde el 30/12/1899 y horas como fracción del día.
function agendaDate(value) {
  if (typeof value==='number' && value>=1 && value<2958466) return new Date(Date.UTC(1899,11,30)+Math.floor(value)*86400000).toISOString().slice(0,10);
  const m=/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(text(value));
  return m ? m[3]+'-'+m[2].padStart(2,'0')+'-'+m[1].padStart(2,'0') : '';
}
function agendaTime(value) {
  if (typeof value==='number' && value>=0) {
    const minutes=Math.round((value%1)*1440)%1440;
    return String(Math.floor(minutes/60)).padStart(2,'0')+':'+String(minutes%60).padStart(2,'0');
  }
  const m=/^(\d{1,2})(?::(\d{2}))?\s*(am|pm|a\.m\.|p\.m\.)?$/i.exec(text(value));
  if (!m) return text(value).slice(0,20);
  let h=Number(m[1])%12+(/^p/i.test(m[3] || '') ? 12 : 0); if (!m[3]) h=Number(m[1]);
  return String(h).padStart(2,'0')+':'+(m[2] || '00');
}

export function createCoachSheetTools({google=createSheetsGoogle(),transfer=createSheetsTransfer({google})}={}) {
  // ctx belongs to one authenticated chat request. Only list/search shares a snapshot.
  const searchReads=new WeakMap();
  async function load(ctx, referralsOnly=false, withAddress=false) {
    const binding=scope(ctx);
    await check(ctx);
    const data=await google.readFollowups(ctx.access,ctx.workbook,{forPlan:true, ...(referralsOnly && {referralsOnly:true}), ...(withAddress && {withAddress:true})});
    await check(ctx);
    return {locale:data.locale,rows:data.rows.filter(row=>saleScope(row,ctx.sellerName)).map(row=>{
      const id=identity(binding,row);
      return {id,key:version(id,row),row};
    })};
  }

  return {
    // detail añade el estado, el final del historial y los datos de contacto tal como están en la hoja,
    // para lecturas del asistente. El teléfono se omite si la ficha pide no ser contactada.
    // Con detail, el nombre y el anfitrión admiten prefijos y una letra de diferencia cuando no hay coincidencia exacta,
    // y `words` busca dentro del historial, el resultado y el estado.
    async search(ctx,{name,host,bucket,today,offset=0,withPhone=false,kind,detail=false,words=''}={}) {
      const binding=scope(ctx);
      await check(ctx);
      let cached=searchReads.get(ctx);
      if (cached?.binding!==binding) {
        cached={binding,pending:load(ctx,false,detail)};
        searchReads.set(ctx,cached);
      }
      let data;
      try { data=await cached.pending; } catch (e) { searchReads.delete(ctx);throw e; }
      await check(ctx);
      const {rows,locale}=data;
      const nameKey=fold(name),hostKey=fold(host);
      if (name && !nameKey || host && !hostKey || (bucket && !['today','overdue','undated','future'].includes(bucket)) ||
          (bucket && !validDate(today))) throw fail(422,'Revisa el nombre, el anfitrión o el día de los seguimientos.');
      if (!Number.isSafeInteger(offset) || offset<0) throw fail(422,'Revisa la página solicitada.');
      const wanted=detail ? fold(words).split(' ').filter(Boolean) : [];
      const collect=match=>{
        const found=[];
        for (const hit of rows) {
          const {row}=hit;
          if (kind && row.kind!==kind) continue;
          if (!match(row.fullName,nameKey) || !match(row.host,hostKey)) continue;
          if (wanted.length) {
            const notes=fold([row.history,row.result,row.status].join(' '));
            if (!wanted.every(word=>notes.includes(word))) continue;
          }
          const followup=classifySheetsFollowup(row,{today,locale});
          if (bucket && followup?.bucket!==bucket) continue;
          found.push(detail ? detailOf(hit,followup) : contactOf(hit,followup,withPhone));
        }
        return found;
      };
      let matches=collect(matchWords), approximate=false;
      if (detail && !matches.length && (nameKey || hostKey)) { matches=collect(matchLoose); approximate=matches.length>0; }
      return {contacts:matches.slice(offset,offset+12),total:matches.length,...(approximate && {approximate})};
    },

    // Fichas por cercanía aproximada. `zones` son ciudades o códigos postales en el orden deseado, de la más
    // cercana a la más lejana. Sin zonas devuelve el catálogo de ciudades y códigos postales de la hoja.
    async byZone(ctx,{zones=[],today,kind,limit=5}={}) {
      const {rows,locale}=await load(ctx,false,true);
      const pool=rows.filter(({row})=>!kind || row.kind===kind);
      const place=row=>({core:cityCore(row.city),all:fold(text(row.city)+' '+text(row.address)),zip:zipOf(text(row.address)+' '+text(row.city))});
      if (!zones.length) {
        const cities=new Map(), zips=new Map();
        let unplaced=0;
        for (const {row} of pool) {
          const where=place(row);
          if (!where.core && !where.zip) { unplaced++; continue; }
          if (where.core) {
            // Se muestra la forma más corta escrita en la hoja: «Blue Island» antes que «Blue Island, IL 60406».
            const written=text(row.city).replace(/\s+/g,' ').slice(0,60);
            const entry=cities.get(where.core) || {name:written,count:0};
            if (written.length<entry.name.length) entry.name=written;
            entry.count++; cities.set(where.core,entry);
          }
          if (where.zip) zips.set(where.zip,(zips.get(where.zip) || 0)+1);
        }
        const top=list=>list.sort((a,b)=>b.count-a.count).slice(0,60);
        return {cities:top([...cities.values()]),zips:top([...zips].map(([code,count])=>({code,count}))),unplaced,total:pool.length};
      }
      const keys=zones.map(zone=>fold(zone)).map(key=>({key,zip:/^\d{5}$/.test(key),core:cityCore(key)}));
      const zoneOf=where=>{
        let best=-1;
        keys.forEach((zone,index)=>{
          const hit=zone.zip ? where.zip===zone.key || new RegExp('(?:^| )'+zone.key+'(?: |$)').test(where.all)
            : zone.core && (where.core ? where.core===zone.core : new RegExp('(?:^| )'+zone.core+'(?: |$)').test(where.all));
          // Entre varias zonas que coinciden, gana el nombre más largo: «Chicago Heights» antes que «Chicago».
          if (hit && (best<0 || zone.key.length>keys[best].key.length)) best=index;
        });
        return best;
      };
      const priority=['overdue','today','undated','new','future','other'];
      const found=[], perZone=zones.map(()=>0);
      for (const hit of pool) {
        const index=zoneOf(place(hit.row));
        if (index<0) continue;
        let followup=classifySheetsFollowup(hit.row,{today,locale});
        if (!followup && ['history','result','followupDate','status'].every(field=>!text(hit.row[field]))) {
          followup={bucket:'new',date:'',reason:'La ficha todavía no tiene una gestión registrada.'};
        }
        perZone[index]++;
        found.push({...detailOf(hit,followup),zoneIndex:index,rank:priority.indexOf(followup?.bucket || 'other')});
      }
      found.sort((a,b)=>a.zoneIndex-b.zoneIndex || a.rank-b.rank || (a.date || '').localeCompare(b.date || '') || a.row-b.row);
      return {contacts:found.slice(0,limit),total:found.length,perZone};
    },

    // pool: la lista completa ordenada, con el estado de cada ficha, para los bloques de llamadas de «Mi día».
    async nextCall(ctx,{today,seen=[],name='',host='',bucket='',kind,shortlist=false,pool=false}={}) {
      const {rows,locale}=await load(ctx,shortlist && kind==='referrals');
      const priority=['overdue','today','undated','new'];
      const ranked=[];
      for (const hit of rows) {
        if (kind && hit.row.kind!==kind) continue;
        if (!matchWords(hit.row.fullName,fold(name)) || !matchWords(hit.row.host,fold(host))) continue;
        let followup=classifySheetsFollowup(hit.row,{today,locale});
        if ((shortlist || pool) && !followup && ['history','result','followupDate','status'].every(k=>!text(hit.row[k])))
          followup={bucket:'new',date:'',reason:'La ficha todavía no tiene una gestión registrada.'};
        if (!followup || !priority.includes(followup.bucket) || bucket && bucket!==followup.bucket) continue;
        const contact=contactOf(hit,followup,true);
        if (contact.phone) ranked.push(pool ? {...contact,status:text(hit.row.status).slice(0,80)} : contact);
      }
      ranked.sort((a,b)=>priority.indexOf(a.bucket)-priority.indexOf(b.bucket) || a.date.localeCompare(b.date) || a.row-b.row);
      if (pool) return {contacts:ranked,total:ranked.length};
      if (name && ranked.length>1) throw fail(422,'Hay varias fichas pendientes con ese nombre. Dime el nombre completo y el anfitrión antes de llamar.');
      if (shortlist) return {contacts:ranked.slice(0,5),total:ranked.length};
      return {contact:ranked.find(c=>!seen.includes(c.identityKey)),total:ranked.length};
    },

    async postSale(ctx,snapshot,range) {
      const {rows,locale}=await load(ctx);
      const review=reviewConfirmedSales(snapshot,rows,{...range,locale});
      return {...review,pending:review.pending.map(({hit,order,reason})=>({...contactOf(hit,null,true),order,reason}))};
    },

    // Ficha vigente por su identidad, con la versión de la fila y el teléfono marcable,
    // para que el asistente prepare una nota, una cita o un mensaje sobre la persona que ya leyó.
    async card(ctx,identityKey) {
      const hit=(await load(ctx)).rows.find(({id})=>id===identityKey);
      if (!hit) throw conflict();
      const contact=contactOf(hit,null,true);
      return {...contact,noCalls:excludesPhoneContact(hit.row)};
    },

    async revalidate(ctx,identityKey) {
      const hit=(await load(ctx)).rows.find(({id})=>id===identityKey);
      if (!hit) throw conflict();
      return true;
    },

    async prepareNote(ctx,{key,note='',followupDate='',today}={}) {
      if (typeof note!=='string' || note.length>1200 || typeof followupDate!=='string' ||
          !validDate(today) || followupDate && !validDate(followupDate)) throw fail(422,'Revisa el texto y las fechas de la nota.');
      note=note.trim().replace(/[\r\n\t]+/g,' ');
      if (/[\p{Cc}\p{Cf}]/u.test(note) || (!note && !followupDate)) throw fail(422,'Indica la nota o la nueva fecha que quieres guardar.');
      const hit=(await load(ctx)).rows.find(row=>row.key===key);
      if (!hit) throw conflict();
      const {row,id}=hit, history=row.editable?.history, dateCell=row.editable?.followupDate;
      if (!history && (note || !dateCell) || history?.formula && note) throw fail(422,'Esta pestaña necesita una columna de notas editable para registrar el texto.');
      const fields=[];
      const entry=today+': '+[note,followupDate && 'Seguimiento el '+followupDate+'.'].filter(Boolean).join(' ');
      const useHistory=Boolean(note || followupDate && (!dateCell || (!dateCell.displayed && history)));
      if (useHistory) {
        if (!history || history.formula) throw fail(422,'La columna de notas contiene una fórmula. Revisa el destino en la hoja.');
        fields.push({field:'history',column:history.column,before:hash(history.displayed),after:hash(noteValue(history.displayed,entry))});
      }
      // Chicago stores the date in its history; in custom workbooks preserve an existing dedicated date.
      if (followupDate && dateCell && (dateCell.displayed || !useHistory) && row.followupDate!==followupDate) {
        if (dateCell.formula) throw fail(422,'La fecha actual proviene de una fórmula. Revisa el destino en la hoja.');
        fields.push({field:'followupDate',column:dateCell.column,before:hash(dateCell.displayed),after:hash(followupDate)});
      }
      if (!fields.length) throw fail(422,'La fecha solicitada ya figura en esta fila.');
      const changed=new Set(fields.map(field=>field.field));
      const unchanged=hash([row.status,row.result,...['history','followupDate'].filter(field=>!changed.has(field)).map(field=>row.editable?.[field])]);
      const preview=row.fullName+' — '+row.sheetTitle+', fila '+row.row+'. '+
        [useHistory && 'Agregar al historial: «'+entry+'».',changed.has('followupDate') &&
          'Fecha de seguimiento: '+(row.followupDate || 'sin fecha')+' → '+followupDate+'.'].filter(Boolean).join(' ');
      return {preview,proposal:{binding:scope(ctx),id,fields,unchanged,entry,followupDate}};
    },

    async confirmNote(ctx,proposal) {
      searchReads.delete(ctx);
      if (proposal?.binding!==scope(ctx) || !Array.isArray(proposal.fields) || !proposal.fields.length) throw conflict();
      const fields=proposal.fields;
      const untouched=['history','followupDate'].filter(field=>!fields.some(f=>f.field===field));
      async function inspect() {
        const hit=(await load(ctx)).rows.find(({id})=>id===proposal.id);
        if (!hit || hash([hit.row.status,hit.row.result,...untouched.map(field=>hit.row.editable?.[field])])!==proposal.unchanged) throw conflict();
        const states=fields.map(f=>{
          const cell=hit.row.editable?.[f.field];
          const sameColumn=cell && !cell.formula && cell.column===f.column;
          const done=sameColumn && (hash(cell.displayed)===f.after || f.field==='followupDate' && hit.row.followupDate===proposal.followupDate);
          return {f,cell,done,conflict:!done && (!sameColumn || hash(cell.displayed)!==f.before)};
        });
        if (states.some(s=>s.conflict)) {
          const saved=states.filter(s=>s.done).map(s=>s.f.field==='history'?'el historial':'la fecha');
          if (saved.length) throw fail(409,'Ya se guardó '+saved.join(' y ')+'. Otra celda cambió. Revisa la fila antes de guardar el resto.');
          throw conflict();
        }
        return {row:hit.row,states};
      }
      let snapshot=await inspect();
      for (let index=0;index<fields.length;index++) {
        const {f,cell,done}=snapshot.states[index];
        if (done) continue;
        const after=f.field==='history' ? noteValue(cell.displayed,proposal.entry) : proposal.followupDate;
        if (hash(after)!==f.after) throw conflict();
        await check(ctx);
        const changed=await google.replaceFollowupCell(ctx.access,ctx.workbook,{sheetId:snapshot.row.sheetId,row:snapshot.row.row,
          column:f.column,before:cell.displayed,after});
        // A retry also recognizes a previous successful replacement from its verified contents.
        snapshot=await inspect();
        if (!snapshot.states[index].done) {
          if (!changed && !cell.displayed) throw fail(409,'Google no pudo completar la edición condicional de la celda vacía. Revisa el historial en la hoja.');
          throw conflict();
        }
      }
      return {status:'saved',message:'Google Sheets confirmó el cambio de '+snapshot.row.fullName+' en '+snapshot.row.sheetTitle+', fila '+snapshot.row.row+'.'};
    },

    async prepareAdd(ctx,input) {
      scope(ctx);
      await check(ctx);
      const {fullName,phone,hostName,hostPhone,city,address,relationship,kind}=input || {};
      const draft=await transfer.preview(ctx,{kind,hostName,hostPhone,referrals:[{fullName,phone,city,address,relationship}]});
      await check(ctx);
      const row=draft.rows[0];
      let duplicates='';
      if (row.status!=='new') {
        const candidates=(await load(ctx)).rows.map(hit=>hit.row);
        const visible=(row.matches || []).map(match=>candidates.find(c=>c.sheetId===match.sheetId && c.row===match.row && fold(c.fullName)===fold(match.fullName))).filter(Boolean);
        duplicates=visible.length ? ' Coincidencias: '+visible.map(c=>c.fullName+' ('+c.sheetTitle+', fila '+c.row+')').join(', ')+'.' : '';
        if (row.status==='duplicate') throw fail(409,'El mismo nombre y teléfono ya figuran en la hoja.'+duplicates+' Revisa el contacto existente.');
      }
      return {proposal:{binding:scope(ctx),previewId:draft.previewId},preview:'Agregar '+row.fullName+' · '+row.phone+' a '+draft.sheetTitle+
        (hostName?' con anfitrión '+hostName:'')+'.'+(city?' Ciudad: '+city+'.':'')+(address?' Dirección: '+address+'.':'')+
        (relationship?' Relación: '+relationship+'.':'')+(hostPhone?' Teléfono del anfitrión: '+hostPhone+'.':'')+
        (row.status==='possible'?' Posible duplicado de nombre o teléfono.'+duplicates+' Revisa la hoja antes de confirmar.':'')};
    },

    // Agenda de la Hoja TEC. Devuelve null si el archivo conectado no tiene la pestaña Agenda con sus encabezados.
    async agenda(ctx,{from,to}={}) {
      scope(ctx);
      await check(ctx);
      const tabs=await google.tecTabs(ctx.access,ctx.workbook);
      if (!tabs.agenda) return null;
      const values=await google.rangeValues(ctx.access,ctx.workbook,"'Agenda'!A1:M"+Math.min(tabs.agenda.gridProperties?.rowCount || 2000,5000),true);
      await check(ctx);
      if (AGENDA_HEADERS.some((h,i)=>fold(values[0]?.[i])!==fold(h))) return null;
      const rows=[];
      for (const [i,raw] of values.slice(1).entries()) {
        const cells=Array.from({length:13},(_,c)=>raw?.[c] ?? '');
        if (!text(cells[4])) continue;
        const date=agendaDate(cells[0]), time=agendaTime(cells[1]);
        if ((from && (!date || date<from)) || (to && (!date || date>to))) continue;
        const row={row:i+2,date,time,seller:text(cells[2]),telemarketer:text(cells[3]),client:text(cells[4]),phone:text(cells[5]),
          address:text(cells[6]),origin:text(cells[7]),notes:text(cells[8]).slice(0,400),result:text(cells[9]),
          sale:typeof cells[10]==='number' ? cells[10] : text(cells[10]),referrals:typeof cells[11]==='number' ? cells[11] : text(cells[11]),sellerNotes:text(cells[12]).slice(0,400)};
        row.key=hash([scope(ctx),row.row,row.client,row.date,row.time,row.result,row.sale,row.referrals,row.sellerNotes]);
        rows.push(row);
      }
      return {sheetId:tabs.agenda.sheetId,rows};
    },

    // Prepara, sin escribir, el resultado de una cita de la Agenda: resultado, venta, referidos y una nota del vendedor.
    async prepareResult(ctx,{row,key,result,sale=null,referrals=null,note=''}) {
      if (!RESULTS.some(([r])=>r===result)) throw fail(422,'Elige un resultado de la lista: '+RESULTS.map(([r])=>r).join(', ')+'.');
      if (sale!=null && (!Number.isFinite(sale) || sale<0 || sale>100000)) throw fail(422,'Revisa el monto de la venta.');
      if (referrals!=null && (!Number.isInteger(referrals) || referrals<0 || referrals>200)) throw fail(422,'Revisa el número de referidos.');
      if (typeof note!=='string' || note.length>600 || /[\p{Cc}\p{Cf}]/u.test(note)) throw fail(422,'Revisa la nota.');
      const data=await this.agenda(ctx);
      const hit=data?.rows.find(r=>r.row===row);
      if (!hit || hit.key!==key) throw conflict();
      const after={result,sale:result==='Venta' ? sale : null,referrals,sellerNotes:note.trim() ? (hit.sellerNotes ? hit.sellerNotes+' / ' : '')+note.trim() : hit.sellerNotes};
      const money=v=>v==null || v==='' ? 'sin venta' : '$'+Number(v).toLocaleString('en-US');
      const preview='Agenda, fila '+hit.row+' · '+hit.client+(hit.date ? ' · '+hit.date+(hit.time ? ' '+hit.time : '') : '')+'. Resultado: '+(hit.result || 'sin resultado')+' → '+result+
        '. Venta: '+money(after.sale)+'. Referidos: '+(referrals==null ? 'sin anotar' : referrals)+'.'+(note.trim() ? ' Nota: «'+note.trim()+'».' : '');
      return {preview,proposal:{binding:scope(ctx),sheetId:data.sheetId,row:hit.row,key:hit.key,client:hit.client,after}};
    },

    async confirmResult(ctx,proposal) {
      if (proposal?.binding!==scope(ctx)) throw conflict();
      await check(ctx);
      const data=await this.agenda(ctx);
      const hit=data?.rows.find(r=>r.row===proposal.row);
      if (!hit || hit.key!==proposal.key || data.sheetId!==proposal.sheetId) throw conflict();
      const {after}=proposal;
      await google.writeCells(ctx.access,ctx.workbook,{sheetId:data.sheetId,row:hit.row,column:9,cells:[after.result,after.sale,after.referrals,after.sellerNotes]});
      await check(ctx);
      return {status:'saved',message:'Google Sheets guardó el resultado de '+hit.client+' en la Agenda, fila '+hit.row+'.'};
    },

    // Números del Resumen de la Hoja TEC tal como los calcula la hoja. null si el archivo no tiene esa pestaña.
    async summary(ctx) {
      scope(ctx);
      await check(ctx);
      const tabs=await google.tecTabs(ctx.access,ctx.workbook);
      if (!tabs.summary) return null;
      const values=await google.rangeValues(ctx.access,ctx.workbook,"'Resumen'!A1:I38");
      await check(ctx);
      if (fold(values[0]?.[0])!==fold('Resumen de resultados')) return null;
      const periods=(values[3] || []).slice(1,4).map(text);
      const metrics=values.slice(8,29).filter(r=>text(r?.[0]) && r.length>1).map(r=>({nombre:text(r[0]),valores:r.slice(1,4).map(text)}));
      const head=(values[31] || []).map(text);
      const telemarketers=values.slice(32,38).filter(r=>text(r?.[0])).map(r=>Object.fromEntries(head.map((h,i)=>[h,text(r[i])])));
      return {periods,metrics,telemarketers,range:{desde:(values[4] || []).slice(1,4).map(text),hasta:(values[5] || []).slice(1,4).map(text)}};
    },

    async confirmAdd(ctx,proposal) {
      searchReads.delete(ctx);
      if (proposal?.binding!==scope(ctx)) throw conflict();
      await check(ctx);
      const result=await transfer.confirm(ctx,{previewId:proposal.previewId,selected:[0]});
      return {status:'saved',message:'Google Sheets confirmó '+result.written+' contacto en '+result.sheetTitle+'.'};
    }
  };
}
