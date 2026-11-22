import { createHash } from 'node:crypto';
import { saleScope } from './coach-sheets-agent.js';
import { dialNumber, reviewConfirmedSales } from './coach-postsale.js';
import { classifySheetsFollowup, excludesPhoneContact } from './coach-sheets-followup-input.js';
import { createSheetsGoogle } from './coach-sheets-google.js';
import { createSheetsTransfer } from './coach-sheets-transfer.js';

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

export function createCoachSheetTools({google=createSheetsGoogle(),transfer=createSheetsTransfer({google})}={}) {
  // ctx belongs to one authenticated chat request. Only list/search shares a snapshot.
  const searchReads=new WeakMap();
  async function load(ctx) {
    const binding=scope(ctx);
    await check(ctx);
    const data=await google.readFollowups(ctx.access,ctx.workbook,{forPlan:true});
    await check(ctx);
    return {locale:data.locale,rows:data.rows.filter(row=>saleScope(row,ctx.sellerName)).map(row=>{
      const id=identity(binding,row);
      return {id,key:version(id,row),row};
    })};
  }

  return {
    async search(ctx,{name,host,bucket,today,offset=0,withPhone=false}={}) {
      const binding=scope(ctx);
      await check(ctx);
      let cached=searchReads.get(ctx);
      if (cached?.binding!==binding) {
        cached={binding,pending:load(ctx)};
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
      const matches=[];
      for (const {row,key,id} of rows) {
        if (!matchWords(row.fullName,nameKey) || !matchWords(row.host,hostKey)) continue;
        const followup=classifySheetsFollowup(row,{today,locale});
        if (bucket && followup?.bucket!==bucket) continue;
        matches.push(contactOf({row,key,id},followup,withPhone));
      }
      return {contacts:matches.slice(offset,offset+12),total:matches.length};
    },

    async nextCall(ctx,{today,seen=[],name='',host='',bucket=''}={}) {
      const {rows,locale}=await load(ctx);
      const priority=['overdue','today','undated'];
      const ranked=[];
      for (const hit of rows) {
        if (!matchWords(hit.row.fullName,fold(name)) || !matchWords(hit.row.host,fold(host))) continue;
        const followup=classifySheetsFollowup(hit.row,{today,locale});
        if (!followup || !priority.includes(followup.bucket) || bucket && bucket!==followup.bucket) continue;
        const contact=contactOf(hit,followup,true);
        if (contact.phone) ranked.push(contact);
      }
      ranked.sort((a,b)=>priority.indexOf(a.bucket)-priority.indexOf(b.bucket) || a.date.localeCompare(b.date) || a.row-b.row);
      if (name && ranked.length>1) throw fail(422,'Hay varias fichas pendientes con ese nombre. Dime el nombre completo y el anfitrión antes de llamar.');
      return {contact:ranked.find(c=>!seen.includes(c.identityKey)),total:ranked.length};
    },

    async postSale(ctx,snapshot,range) {
      const {rows,locale}=await load(ctx);
      const review=reviewConfirmedSales(snapshot,rows,{...range,locale});
      return {...review,pending:review.pending.map(({hit,order,reason})=>({...contactOf(hit,null,true),order,reason}))};
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

    async confirmAdd(ctx,proposal) {
      searchReads.delete(ctx);
      if (proposal?.binding!==scope(ctx)) throw conflict();
      await check(ctx);
      const result=await transfer.confirm(ctx,{previewId:proposal.previewId,selected:[0]});
      return {status:'saved',message:'Google Sheets confirmó '+result.written+' contacto en '+result.sheetTitle+'.'};
    }
  };
}
