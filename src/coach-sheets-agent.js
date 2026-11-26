import { createHash } from 'node:crypto';
import { openToken, sealToken } from './coach-hycite-gmail.js';
import { calendarIntent, validTimeZone } from './coach-followup-input.js';
import { classifySheetsFollowup, sheetsToday } from './coach-sheets-followup-input.js';

const fold = value => String(value || '').normalize('NFD').replace(/\p{M}/gu,'').toLowerCase().trim();
const display = (value,limit=100) => String(value || '').replace(/[\p{Cc}\p{Cf}\s]+/gu,' ').trim().slice(0,limit);
const reply = (respuesta, pending='', actions=[]) => ({mode:'coach',coachMailAgent:true,coachSheetsAgent:true,
  coachSheetsPending:pending,coachHelpActions:actions,respuesta});
const connections = [{type:'connections',label:'Mis conexiones'}];
const pageSize = 5;
const headings = {today:'Para hoy',undated:'Pendientes sin fecha',overdue:'Vencidos'};

export function sheetsChatIntent(input) {
  const text = fold(input);
  if (/\b(?:correo|gmail|whatsapp|borrador|redacta|envia|manda)\b/.test(text)) return null;
  const choice = /\b(?:lead|prospecto|referido|boleta|seguimiento)\s+(?:numero\s+|n(?:ro\.?|o\.|\.?[º°])\s*)?#?(\d{1,3})\b/.exec(text);
  if (choice && /^(?:[¿¡]\s*)?(?:(?:por favor|coach)[, ]+)?(?:busca(?:me)?|dame|propon(?:me)?|agenda(?:me)?|programa(?:me)?)\b/.test(text)) {
    const cal=calendarIntent(text);
    if (['invitation','recurring','discard','confirm','move'].includes(cal?.kind) ||
        /\b(?:sin (?:agendar|reservar)|no (?:quiero|agendes|reserves))\b/.test(text)) return null;
    const slots=/\b(?:horarios?|opciones|disponibilidad)\b/.test(text);
    if (!slots && !/\b(?:agenda(?:me)?|programa(?:me)?)\b/.test(text)) return null;
    // Parse dates/times from the seller's words. The selection number is never a time.
    return {kind:'select', index:Number(choice[1])-1, slots,
      schedule:text.replace(choice[0],'').replace(/\b(?:para|con|al?)\s+(?:el|la)\s*(?=,|$)/g,'')};
  }
  const source = /\b(?:leads?|prospectos?|referencias?|referidos?|boletas?|rifas?|sheets|hojas?|4\s*(?:en|-)\s*14)\b/.test(text);
  const period = /\b(?:hoy|sin fecha|vencid[oa]s?|atrasad[oa]s?)\b/.test(text);
  const task = /\b(?:seguimientos?|pendientes?|llamar|llamadas?|toca|contactar)\b/.test(text);
  const lookup=source && /\b(?:revisa(?:me)?|consulta|muestra(?:me)?|lista(?:me)?|dime)\b/.test(text);
  if (!(task || source && (period || lookup)) || !(source || (period && /\b(?:a quien|quienes|sin fecha|le toca|les toca)\b/.test(text)))) return null;
  if (/\b(?:calendario|google calendar|redacta|escribe un mensaje)\b/.test(text) && !source) return null;
  if (/\b(?:agend\w*|program\w*|mueve|cancela|confirma|descarta)\b/.test(text)) return null;
  if (/\b(?:manana|ayer|semana|mes)\b/.test(text) && !period) return {kind:'unsupported'};
  const page=Number(/\bpagina\s+(\d{1,6})\b/.exec(text)?.[1] || 1);
  const only=page>1 && /\bhoy\b/.test(text) && !/\bsin fecha\b/.test(text) ? 'today' : /\b(?:sin fecha)\b/.test(text) && !/\bhoy\b/.test(text) ? 'undated'
    : /\b(?:vencid[oa]s?|atrasad[oa]s?)\b/.test(text) && !/\bhoy\b/.test(text) ? 'overdue' : '';
  return {kind:'list',page,only};
}

export function saleScope(row, userName) {
  const assigned=fold(row.seller);
  if (!assigned) return 'compartido, sin vendedor indicado';
  const name=fold(userName).replace(/\s+/g,' ');
  const first=name.split(' ')[0];
  return assigned.split(/\s+(?:y|and)\s+|[,;&/]+/).some(label=>{
    const part=label.trim();
    return name && (part===name || part===first);
  }) ? 'asignado a ti' : '';
}

function selectionKey(row) {
  return createHash('sha256').update(JSON.stringify([row.sheetId,row.row,row.fullName,row.host,row.seller,
    row.history,row.result,row.followupDate,row.status])).digest('base64url');
}

export function createSheetsChatAgent({config,sheets,google,switched,readSession,calendar,now=Date.now}) {
  return async args => {
    const {req,auth,sessionId,text}=args;
    const intent=sheetsChatIntent(text);
    if (!intent) return null;
    const user=auth?.user, session=auth?.session;
    const own = identity => identity?.session?.accessMode==='direct' || (identity?.session && !identity.session.accessMode && identity.user?.accountType!=='seat');
    if (!user?._id || !session?.tokenHash || req.body?.accountId!==String(user._id) || switched(req) || !own(auth)) return reply('Abre tu propia sesión directa del Coach para revisar Google Sheets.');
    if (!config.sheetsPilot?.some(value=>value==='*' || value===fold(user.email))) return reply('Tu cuenta todavía no participa en el piloto de Google Sheets.');
    if (intent.kind==='unsupported') return reply('De tus boletas y tus referidos te puedo enseñar los seguimientos de hoy, los vencidos y los que no tienen fecha; de otras fechas todavía no. ¿Cuál de esos quieres ver?');
    const zone=validTimeZone(req.body?.coachTimeZone);
    if (!zone) return reply('Abre el chat con la zona horaria de tu dispositivo para identificar qué seguimientos corresponden a hoy.');
    try {
      const {token,grant,...book}=await sheets.access(user._id);
      const aad=JSON.stringify(['sheets-followups',String(user._id),session.tokenHash,sessionId,grant]);
      let selectedKey;
      if (intent.kind==='select') {
        try {
          const sealed=req.body?.coachSheetsPending;
          if (typeof sealed!=='string' || sealed.length>6000) throw Error();
          const selection=JSON.parse(openToken(sealed,config.key,aad));
          if (selection.until<=now() || !(selectedKey=selection.keys[intent.index])) throw Error();
        } catch { return reply('Pídeme «revisa mis leads de hoy» y elige el número de un prospecto de esa lista.'); }
      }
      const data=await google.readFollowups(token,book);
      const fresh=await readSession(req);
      if (!own(fresh) || String(fresh?.user?._id)!==String(user._id) || switched(req) ||
          fresh.session.tokenHash!==session.tokenHash || !await sheets.current(user._id,grant)) return reply('Tu sesión o tu hoja cambió. Abre tu cuenta y vuelve a pedir los seguimientos.');
      const today=sheetsToday(now(),zone);
      const groups={today:[],undated:[],overdue:[]};
      for (const row of data.rows) {
        const scope=saleScope(row,user.name);
        if (!scope) continue;
        const item=classifySheetsFollowup(row,{today,locale:data.locale});
        if (!item || !groups[item.bucket]) continue;
        groups[item.bucket].push({row,item,scope});
      }
      if (intent.kind==='select') {
        const match=Object.values(groups).flat().find(({row})=>selectionKey(row)===selectedKey);
        if (!match) return reply('La fila o su seguimiento cambió. Pídeme la lista actualizada antes de preparar la cita.');
        return calendar({...args,text:intent.schedule,sheetsContact:{name:display(match.row.fullName,80),slots:intent.slots}});
      }
      const lines=['Google Sheets · '+today+' ('+zone+')',display(book.ticketsTitle)+' y '+display(book.referralsTitle)];
      const keys=[];
      for (const bucket of intent.only?[intent.only]:['today','undated']) {
        const rows=groups[bucket], start=(Math.max(1,intent.page)-1)*pageSize;
        lines.push('',headings[bucket]+' ('+rows.length+')');
        if (!rows.length) {lines.push(bucket==='today'?'Ninguna fila con seguimiento fechado para hoy.':'Ninguna fila en este grupo.');continue;}
        if (start>=rows.length) {lines.push('No hay más filas en esta página.');continue;}
        for (const {row,item,scope} of rows.slice(start,start+pageSize)) {
          keys.push(selectionKey(row));
          const host=row.host?' · Anfitrión '+display(row.host):'';
          lines.push(keys.length+'. '+display(row.fullName)+' — '+display(item.reason,175)+(item.date?' Fecha de seguimiento '+item.date+'.':'')+' ('+display(row.sheetTitle)+', fila '+row.row+host+' · '+scope+').');
        }
        if (start+pageSize<rows.length) lines.push('Para continuar: «'+(bucket==='today'?'Leads de seguimiento de hoy':bucket==='overdue'?'Leads vencidos':headings[bucket])+' página '+(intent.page+1)+'».');
      }
      if (!intent.only && groups.overdue.length) lines.push('',groups.overdue.length+' seguimientos con fecha anterior. Puedes pedir «Leads vencidos».');
      if (keys.length) lines.push('Para preparar una cita, escribe «Busca dos horarios para el lead 1 mañana» o «Agenda al lead 1 mañana a las 4 pm». Usa el número de esta lista.');
      const sealed=keys.length?sealToken(JSON.stringify({keys,until:now()+15*60000}),config.key,aad):'';
      return reply(lines.join('\n'),sealed);
    } catch (error) {
      if ([403,409,422].includes(error.status) || error.reauthorize) return reply(display(error.message,250), '',connections);
      return reply('No pude terminar la lectura de las dos pestañas de Google Sheets. Vuelve a intentar la consulta.', '',connections);
    }
  };
}
