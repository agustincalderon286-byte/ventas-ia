import {openToken} from './coach-hycite-gmail.js';
import {isCompletePhone} from './coach-referral-photo.js';
import {sheetsToday, excludesPhoneContact, sheetHistoryEntries} from './coach-sheets-followup-input.js';

const fail=(status,message)=>Object.assign(new Error(message),{status});
const fold=v=>String(v??'').normalize('NFD').replace(/\p{M}/gu,'').trim().toLowerCase();
const oneDay=86400000;
export function dialNumber(value) {
  const raw=String(value??'').trim();
  if (!isCompletePhone(raw)) return '';
  const digits=raw.replace(/\D/g,'');
  return raw.startsWith('+')?'+'+digits:'+'+(digits.length===10?'1':'')+digits;
}

export function createPostSaleReader({config,store,gmail}) {
  async function connection(actor) {
    if (!config.ready || !config.pilot?.some(e=>e==='*' || e===fold(actor.email)) || !await actor.current()) {
      throw fail(403,'Abre Gmail desde tu propia sesión del piloto.');
    }
    const doc=await store.findById(actor.owner,'mailbox encryptedToken grant').lean();
    if (!doc?.grant || !doc.encryptedToken) throw fail(422,'Conecta tu Gmail en Mis conexiones para revisar la postventa.');
    return doc;
  }
  return async (actor, {knownGrant='',verifyOnly=false}={})=>{
    const doc=await connection(actor);
    if (knownGrant && doc.grant!==knownGrant) throw fail(409,'Tu conexión de Gmail cambió. Pide una revisión nueva de postventa.');
    if (verifyOnly) return;
    const token=await gmail.refresh(openToken(doc.encryptedToken,config.key,actor.owner));
    const snapshot=await gmail.notices(token.access_token,{forPostSale:true});
    if ((await connection(actor)).grant!==doc.grant) throw fail(409,'Cambió la cuenta de Gmail durante la consulta. Pídela de nuevo.');
    return {...snapshot,grant:doc.grant};
  };
}

export function salePeriod(period,today) {
  const day=Date.parse(today+'T00:00:00Z');
  const monday=day-((new Date(day).getUTCDay()+6)%7)*oneDay;
  const bounds={
    week:[monday,day],lastWeek:[monday-7*oneDay,monday-oneDay],today:[day,day],
    yesterday:[day-oneDay,day-oneDay],last7:[day-6*oneDay,day],last30:[day-29*oneDay,day],
    month:[Date.parse(today.slice(0,7)+'-01T00:00:00Z'),day]
  };
  if (!Object.hasOwn(bounds,period||'week')) throw fail(422,'¿Quieres revisar hoy, ayer, esta semana, la semana pasada, este mes o los últimos 7 o 30 días?');
  const [from,to]=bounds[period||'week'].map(d=>new Date(d).toISOString().slice(0,10));
  return {from,to};
}

function rowOrders(row) {
  const explicit=String(row.order||'').trim().toUpperCase();
  const ids=new Set(explicit && /^[A-Z0-9]+(?:-[A-Z0-9]+)*$/.test(explicit)?[explicit]:[]);
  const tagged=/\b(?:order|orden|pedido)(?:\s+(?:n[uú]mero|no\.?|number))?\s*[:#]\s*([\da-z]+(?:-[\da-z]+)*)(?![\da-z_/-])/gi;
  for (const m of (row.history+' '+row.result).matchAll(tagged)) if (/\d/.test(m[1])) ids.add(m[1].toUpperCase());
  return ids;
}

function postSaleHistory(row,order,day,today,requireOrder,locale) {
  // A dated, completed management note is evidence. A planned call, missed attempt or an old
  // entry never establishes that the seller actually offered this buyer the demonstration.
  const entries=[row.history,row.result].flatMap(value=>sheetHistoryEntries(value,locale));
  return entries.some(({text:value,anchor:date})=>{
    if (!date || date<day || date>today || /\b(?:no|sin|pendiente|intentar|intent[eo]|buzon|falta)\b/.test(value)) return false;
    const ids=rowOrders({history:value});
    if ((requireOrder || ids.size) && !ids.has(order)) return false;
    return /\b(?:ofreci(?:mos)?|ofrecida|ofrecido|agend[ea]|confirm[ea]|realic[eé]|realizada|completada|acepto|rechazo)\b.{0,42}\b(?:demo(?:stracion)? de uso|pos(?:t)?venta)\b|\b(?:demo(?:stracion)? de uso|pos(?:t)?venta)\s+(?:ya\s+)?(?:ofrecida|agendada|realizada|completada|confirmada|rechazada)\b/.test(value);
  });
}

export function reviewConfirmedSales(snapshot,hits,{from,to,today,zone,locale}) {
  const pending=[],linked=[],unmatched=[],ambiguous=[];
  const byRow=new Map();
  let completed=0,excluded=0,count=0;
  const rows=hits.map(hit=>({hit,orders:rowOrders(hit.row),phone:dialNumber(hit.row.phone),email:fold(hit.row.email)}));
  for (const sale of snapshot.confirmed || []) {
    const when=Date.parse(sale.date);
    if (!sale.confirmed || !sale.order || !Number.isFinite(when)) continue;
    const day=sheetsToday(when,zone);
    if (day<from || day>to) continue;
    count++;
    const order=sale.order,phone=dialNumber(sale.customer?.phone),email=fold(sale.customer?.email);
    const matches=rows.filter(r=>r.orders.has(order) || !!phone && phone===r.phone || !!email && email===r.email);
    if (!matches.length) {unmatched.push(order);continue;}
    if (matches.length!==1) {ambiguous.push(order);continue;}
    const [{hit,orders,phone:listedPhone,email:listedEmail}]=matches;
    if (phone && listedPhone && phone!==listedPhone || email && listedEmail && email!==listedEmail) {
      ambiguous.push(order);continue;
    }
    if (excludesPhoneContact(hit.row)) {excluded++;continue;}
    const items=byRow.get(hit.id) || [];
    items.push({hit,orders,order,day,phone,listedPhone});
    byRow.set(hit.id,items);
  }
  for (const items of byRow.values()) for (const {hit,orders,order,day,phone,listedPhone} of items) {
    linked.push({identityKey:hit.id,order});
    if (postSaleHistory(hit.row,order,day,today,items.length>1 || orders.size>1,locale)) {completed++;continue;}
    pending.push({hit,order,day,reason:'Pedido '+order+' confirmado el '+day+
      '. Coincide el '+(orders.has(order)?'número de pedido':phone&&phone===listedPhone?'teléfono del comprador':'correo del comprador')+
      '. No encontré una oferta de demo de uso registrada con fecha desde esa confirmación.'});
  }
  return {pending,linked,unmatched,ambiguous,count,completed,excluded,limited:!!snapshot.limited};
}
