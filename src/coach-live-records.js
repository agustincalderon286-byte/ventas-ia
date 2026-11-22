import { inDemoClosePilot } from './coach-demo-close.js';
import { gmailClient, gmailScope, openToken } from './coach-hycite-gmail.js';
import { mailReadOptions, mailReadClarification } from './coach-mail-intent.js';
import { mailPeriod } from './coach-mail-period.js';
import { createCoachSheetsConnections } from './coach-sheets-connections.js';
import { createSheetsGoogle } from './coach-sheets-google.js';
import { saleScope } from './coach-sheets-agent.js';
import { classifySheetsFollowup, sheetsToday } from './coach-sheets-followup-input.js';

const fold = text => String(text ?? '').normalize('NFD').replace(/\p{M}/gu,'').toLowerCase().replace(/\s+/g,' ').trim();
const liveSources = new Set(['gmail','sheets414','sheetsTickets','tec414']);
const labels = {sheets414:'En Sheets, 4 en 14',sheetsTickets:'En Sheets, boletas',tec414:'En TEC, 4 en 14',gmail:'Gmail'};
const readCue = /\b(?:mi|mis|me|tenemos|tengo|revisa(?:me)?|chec[ak]a(?:me)?|checa(?:me)?|busca(?:me)?|mira|muestra(?:me)?|dime|lee(?:me)?|consulta|cuant[oa]s?|cuales|quienes|hay|llego|llegaron|nuev[oa]s?|novedades|a quien|como va|que tal|estado|pendientes?|vencidos?)\b/;
const teaching = /\b(?:como (?:se |puedo |debo |puede |podria |tengo que )?(?:hago|hacer|conect\w*|llen\w*|escrib\w*|envi\w*|uso|us[ao]r|funciona|trabaj\w*|mejor\w*|consig\w*|capt\w*|pid[oa]|pido|pedir|revis\w*)|que (?:es|son|significa)|como (?:le |les )?digo|practiquemos|ensename|dame (?:un )?(?:ejemplo|guion|consejo)|simul\w*|redact\w*|preparame un correo)\b/;
const writeCue = /\b(?:envia(?:le|me)?|mandale|manda(?:me)? (?:un |el )?(?:correo|mensaje)|responde(?:le)? (?:a|al)|borra|elimina|actualiza|guarda|registra|agenda(?:me)? (?:una|la)|agrega|anade|modifica|marca como)\b/;
const hycite = /\b(?:hy[\s-]?cite|hi[\s-]?cite|jai\s?sait)\b/;
const four = /\b(?:4|cuatro)\s*(?:en|por|de|[-/x])\s*(?:14|catorce)\b/;
const tickets = /\b(?:boletas?|rifas?|boletos?)\b/;
const readMail = /\b(?:correos?|e[\s-]?mails?|gmail|bandeja|inbox)\b/;
const sales = /\b(?:ventas? (?:confirmad[ao]s?|aprobad[ao]s?)|(?:mis|cuantas|que|tengo|hay) ventas|(?:ventas?|ordenes?|pedidos?) (?:de )?(?:hy[\s-]?cite)|(?:orden|pedido) (?:confirmad[oa]|aprobad[oa]))\b/;
const ordinalWords = ['primero','segundo','tercero','cuarto','quinto'];
const dateFormatter = new Intl.DateTimeFormat('es-US',{month:'short',day:'numeric',timeZone:'America/Chicago'});
const sessionChanged = () => ({ text:'Tu sesión cambió. Vuelve a abrir Jorge desde tu propia cuenta.',sources:[] });
const unavailable = text => ({text,verified:false});
const permitted = (list,email) => Array.isArray(list) && (list.includes('*') || list.includes(email));

function clean(value, max=60) {
  return String(value ?? '').replace(/https?:\/\/\S+/gi,'').replace(/[\p{Cc}\p{Cf}\s]+/gu,' ').trim().slice(0,max);
}
function subject(value) {
  const raw = clean(value,70);
  if (/\b(?:passcode|one.time|verification code|codigo de (?:seguridad|verificacion|acceso)|password reset)\b/.test(fold(raw))) return 'aviso de seguridad';
  return raw || 'Sin asunto';
}
function ordinal(text) {
  const word=fold(text).match(/\b(primer[oa]?|segund[oa]|tercer[oa]?|cuart[oa]|quint[oa])\b/);
  if (word) return ordinalWords.findIndex(val=>val.slice(0,4)===word[0].slice(0,4));
  const num=fold(text).match(/\b(?:correo|mensaje|numero|referido|boleta) (?:numero )?([1-5])\b/);
  return num?Number(num[1])-1:-1;
}
function directSources(q) {
  const sources=[];
  if (readMail.test(q) || sales.test(q) || (hycite.test(q) && /\b(?:aviso|avisos|confirmaciones|mensajes?|mando|envio|escribio)\b/.test(q))) sources.push('gmail');
  const sheet=/\b(?:sheets?|hoja de calculo|google sheets)\b/.test(q);
  const explicitTec=/\b(?:en|de|del|desde) (?:mi )?tec\b/.test(q);
  const refs=four.test(q) || /\b(?:mis |las |los )?(?:referencias|referidos|anfitrion(?:es)?)\b/.test(q);
  if (refs && (!sheet || explicitTec)) sources.push('tec414');
  if (refs && (!explicitTec || sheet)) sources.push('sheets414');
  if (tickets.test(q)) sources.push('sheetsTickets');
  if (!refs && !tickets.test(q) && (sheet || /\b(?:mis leads|mis prospectos|a quien (?:le )?(?:toca|llamo)|le toca seguimiento)\b/.test(q))) sources.push('sheetsTickets','sheets414');
  return sources;
}
function selectIntent(input,recent) {
  const q=fold(input);
  if (!q || teaching.test(q) || writeCue.test(q)) return null;
  const last=recent?.at(-1);
  const previous=last?.role==='assistant' && Array.isArray(last.sources) ? last.sources.filter(v=>liveSources.has(v)) : [];
  const priorUser=recent?.at(-2)?.role==='user' ? recent.at(-2).content : '';
  let sources=directSources(q), filters=q;
  const followup = ordinal(q)>=0 || /^(?:[¿¡]?\s*(?:y\s+)?(?:hoy|ayer|esta semana|este mes|cuantos|cuales|quienes|los sin leer|sin leer|los vencidos|sin fecha|dame (?:los )?nombres|de quien|de quienes|que dice|que dicen|quien(?:es)? (?:es|son)))\b/.test(q);
  if (!sources.length) {
    if (!followup || !previous.length) return null;
    sources=[...new Set(previous)];
    if (sources.includes('gmail') && sources.length>1) return { clarification:'¿Quieres que revise el correo, las boletas o tu 4 en 14?',sources };
    filters=fold(priorUser)+' '+q;
  } else if (ordinal(q)>=0 && previous.length && sources.every(src=>previous.includes(src))) {
    filters=fold(priorUser)+' '+q;
  }
  if (!followup && !readCue.test(q)) return null;
  return {sources, q, filters, index:ordinal(q),previousText:previous.length ? last.content : '',priorUser};
}

const gmailRoot='https://gmail.googleapis.com/gmail/v1/users/me/messages';
// The normal inbox reader supports exact sender addresses. Use the same Gmail API for
// searches by sender domain or subject, and fetch only the fields spoken to the seller.
async function searchInbox(token,search,{fetcher,signal}) {
  const get=async suffix=>{
    const timeout=AbortSignal.timeout(9000);
    const response=await fetcher(gmailRoot+suffix,{method:'GET',headers:{Authorization:'Bearer '+token},redirect:'error',
      signal:signal?AbortSignal.any([signal,timeout]):timeout});
    if (!response.ok) {await response.body?.cancel();throw Object.assign(new Error('Gmail read failed'),{reauthorize:[401,403].includes(response.status)});}
    return response.json();
  };
  const params=new URLSearchParams({labelIds:'INBOX',maxResults:'5',fields:'messages(id),nextPageToken'});
  if(search.unread)params.append('labelIds','UNREAD');
  const words=[];
  if(search.from)words.push('from:"'+search.from+'"');
  if(search.domains?.length)words.push('{'+search.domains.map(domain=>'from:(@'+domain+')').join(' ')+(search.topic?' "Hy Cite" HyCite':'')+'}');
  if(search.phrase)words.push('"'+search.phrase+'"');
  if(search.period)words.push('after:'+search.period.since,'before:'+search.period.before);
  else if(search.days)words.push('newer_than:'+search.days+'d');
  if(words.length)params.set('q',words.join(' '));
  const list=await get('?'+params);
  const metadata=new URLSearchParams({format:'metadata',fields:'id,internalDate,snippet,payload/headers'});
  for(const key of ['From','Subject'])metadata.append('metadataHeaders',key);
  const rows=await Promise.all((list.messages||[]).slice(0,5).map(async ({id})=>{
    if(typeof id!=='string' || !/^[a-z\d_-]{1,128}$/i.test(id))throw Error('Invalid Gmail ID');
    const msg=await get('/'+encodeURIComponent(id)+'?'+metadata);
    const header=name=>msg.payload?.headers?.find(h=>h.name?.toLowerCase()===name)?.value;
    return {subject:header('subject'),from:header('from'),snippet:msg.snippet};
  }));
  return {rows,more:!!list.nextPageToken};
}
function inboxSummary(rows,more,index,previousText,where) {
  if (!rows.length) return 'Gmail: no encontré correos'+where+'.';
  if (index>=0) {
    const message=rows[index];
    if(!message)return 'Gmail: no hay un correo en esa posición entre los resultados.';
    const title=subject(message.subject);
    if(previousText && !previousText.includes(title) || rows.filter(row=>subject(row.subject)===title).length>1) return 'La lista de Gmail cambió o el asunto se repite. Pídeme la lista actualizada antes de elegir.';
    const summary=title==='aviso de seguridad'?'':clean(message.snippet,140);
    return 'Gmail, «'+title+'». '+(summary?'Vista previa: '+summary:'El remitente es '+clean(message.from?.split('<')[0] || message.from,55)+'.');
  }
  return 'Gmail'+where+': '+rows.length+(more?' o más':'')+' '+(rows.length===1?'correo':'correos')+'. '+rows.slice(0,3).map((m,i)=>(i+1)+'. «'+subject(m.subject)+'»').join(' ')+'.';
}
function salesSummary(snapshot,period,days,at) {
  const floor=at-90*86_400_000;
  const from=Math.max(floor,period?period.since*1000:at-days*86_400_000), until=Math.min(at+1,period?period.before*1000:Infinity);
  const inRange=r=>Date.parse(r.date)>=from&&Date.parse(r.date)<until;
  const confirmed=snapshot.confirmed.filter(inRange),review=snapshot.review.filter(inRange);
  const window=period?period.label:'últimos '+days+' días';
  return 'Gmail, avisos de Hy Cite, '+window+': '+confirmed.length+' '+(confirmed.length===1?'venta confirmada':'ventas confirmadas')+' y '+review.length+' '+(review.length===1?'aviso por revisar':'avisos por revisar')+'.'+
    (confirmed.length?' Última orden confirmada: '+clean(confirmed[0].order,28)+', '+dateFormatter.format(new Date(confirmed[0].date))+'.':'')+
    (snapshot.limited?' El conteo cubre solo los 100 avisos más recientes.':'');
}
function targetHost(q) {
  const match=q.match(/\b(?:anfitrion(?:a)?|referidos de|referencias de)\s+([\p{L}][\p{L}'’-]*(?:\s+[\p{L}][\p{L}'’-]*){0,3})/u);
  return match?.[1]?.split(/\s+(?:y|en|para|del|hoy|ayer)\b/)[0]?.trim() || '';
}
function stayBrief(parts) {
  if(parts.length===1)return parts[0];
  const limit= Math.floor(465/parts.length);
  return parts.map(p=>{
    if(p.length<=limit)return p;
    const pieces=p.match(/.{1,}?[.!?](?=\s|$)/gu)||[];
    let brief='';
    for(const piece of pieces){if((brief+piece).length>limit)break;brief+=piece;}
    return brief.trim() || p.slice(0,limit-1).replace(/\s+\S*$/u,'')+'…';
  }).join(' ');
}

function bucketFor(q) {
  if(/\bsin fecha\b/.test(q))return 'undated';
  if(/\b(?:vencid[oa]s?|atrasad[oa]s?)\b/.test(q))return 'overdue';
  if(/\b(?:hoy|para hoy)\b/.test(q) && /\b(?:leads?|seguimientos?|llam[oa]r?|toca|pendientes?)\b/.test(q))return 'today';
  return '';
}
function sheetSummary(source,data,user,intent,at) {
  const kind=source==='sheets414'?'referrals':'tickets', label=labels[source];
  const today=sheetsToday(at,'America/Chicago'),bucket=bucketFor(intent.filters),host=targetHost(intent.filters);
  const rows=data.rows.filter(row=>row.kind===kind && saleScope(row,user.name) && (!host || fold(row.host).includes(host)));
  const classified=rows.map(row=>({row,follow:classifySheetsFollowup(row,{today,locale:data.locale})}));
  const entries=bucket?classified.filter(item=>item.follow?.bucket===bucket):classified;
  const shared=entries.filter(({row})=>!clean(row.seller)).length;
  const filtered=bucket?({today:' con seguimiento para hoy',overdue:' con seguimiento vencido',undated:' pendientes sin fecha'})[bucket]:'';
  const what=' registros'+filtered+(host?' del anfitrión '+host:'');
  if(!entries.length)return label+': encontré 0'+what+'.';
  const list=entries.slice(0,3).map(({row})=>clean(row.fullName,48));
  if(intent.index>=0) {
    const selected=entries[intent.index];
    if(!selected || !intent.previousText?.includes(clean(selected.row.fullName,48)))return label+': vuelve a pedirme la lista para verificar esa posición.';
    return label+': '+clean(selected.row.fullName,48)+(selected.row.host?', anfitrión '+clean(selected.row.host,45):'')+
      (selected.follow?'. '+selected.follow.reason+(selected.follow.date?' Fecha: '+selected.follow.date:''):'')+'.';
  }
  return label+': '+entries.length+what+(shared?' ('+shared+' sin vendedor asignado)':'')+'. '+list.join(', ')+'.';
}
async function nativeSheets({programSheets,personalProgramSheets,programQuery},user,intent) {
  const legacy=programQuery(user);
  if(!legacy?.ownerUserId || !Object.hasOwn(legacy,'generatedByUserId'))throw Error('Missing personal capture scope');
  const host=targetHost(intent.filters);
  const collation={locale:'es',strength:1};
  const aggregate=(model,scope)=>model.aggregate([
    {$match:{...scope,programType:'4_en_14',...(host && {hostName:host})}},
    {$facet:{
      totals:[{$group:{_id:null,sheets:{$sum:1},referrals:{$sum:{$size:{$ifNull:['$referrals',[]]}}}}}],
      latest:[{$sort:{updatedAt:-1,createdAt:-1,_id:1}},{$limit:3},{$project:{_id:0,hostName:1,updatedAt:1,createdAt:1,
        names:{$slice:[{$ifNull:['$referrals.fullName',[]]},3]}}}]
    }}
  ]).collation(collation).option({maxTimeMS:5000});
  const results=await Promise.all([aggregate(programSheets,legacy),aggregate(personalProgramSheets,{ownerUserId:user._id})]);
  let count=0,refs=0;const rows=[];
  for(const [result] of results) {
    count+=result?.totals[0]?.sheets||0;refs+=result?.totals[0]?.referrals||0;
    rows.push(...(result?.latest||[]));
  }
  rows.sort((a,b)=>Number(new Date(b.updatedAt||b.createdAt||0))-Number(new Date(a.updatedAt||a.createdAt||0)));
  const prefix='En TEC, 4 en 14'+(host?', anfitrión '+host:'');
  if(!count)return prefix+': no encontré hojas tuyas guardadas.';
  const latest=rows[0],names=(latest?.names||[]).map(n=>clean(n,38)).filter(Boolean);
  return prefix+': '+count+' '+(count===1?'hoja guardada':'hojas guardadas')+' y '+refs+' '+(refs===1?'referido':'referidos')+'.'+
    (latest?.hostName?' La más reciente es de '+clean(latest.hostName,45)+'.':'')+
    (names.length?' En esa hoja: '+names.join(', ')+'.':'');
}

export function createLiveRecordReader({config,gmailStore,programSheets,personalProgramSheets,programQuery,sessionCurrent,
  gmail=gmailClient(config),sheets=createCoachSheetsConnections({config}).service,sheetsGoogle=createSheetsGoogle(),fetcher=fetch,now=Date.now}) {
  async function mailbox(auth,intent,signal) {
    const user=auth.user,q=intent.filters;
    if(!config.ready || !permitted(config.pilot,fold(user.email)))return unavailable('Gmail no está disponible para esta cuenta en Mis conexiones.');
    const doc=await gmailStore.findById(user._id,'mailbox encryptedToken grant').lean();
    if(!doc?.encryptedToken || !doc.grant || !doc.mailbox || String(doc._id)!==String(user._id))return unavailable('Conecta tu Gmail en Mis conexiones para que pueda revisarlo.');
    const asSales=sales.test(q);
    if(asSales && /\b(?:20\d\d|hace (?:un|dos|tres|\d+) anos?|ultimo ano|pasado ano)\b/.test(q)) {
      return unavailable('Los avisos de ventas de Hy Cite cubren los últimos 90 días. Dime un período dentro de esa ventana.');
    }
    const opts=mailReadOptions(q);
    const period=mailPeriod(intent.q,now()) || mailPeriod(q,now());
    const isHy=hycite.test(q);
    const clarificationInput=mailPeriod(intent.q,now()) && intent.q!==q ? intent.q : q;
    const topicText=clarificationInput.replace(/\b(?:sobre el tema|acerca de|con asunto|asunto)\b/g,'sobre');
    const clarification=mailReadClarification(topicText,asSales?'sales':'inbox');
    if(clarification && !(isHy && /remitente|direccion/i.test(fold(clarification))))return unavailable(clarification);
    if(asSales && (opts.from || opts.unread))return unavailable('¿Quieres contar ventas confirmadas de Hy Cite o revisar correos por remitente o sin leer?');
    const tokens=await gmail.refresh(openToken(doc.encryptedToken,config.key,user._id));
    if(!tokens?.access_token || (tokens.scope && !String(tokens.scope).split(/\s+/).includes(gmailScope)))return unavailable('Vuelve a autorizar la lectura de Gmail en Mis conexiones.');
    signal?.throwIfAborted();
    let summary;
    if(asSales) {
      const snapshot=await gmail.notices(tokens.access_token);
      summary=salesSummary(snapshot,period,opts.days||90,now());
    } else {
      const topic=/\b(?:sobre|acerca de|mencion\w*)\b/.test(q);
      const otherTopic=!isHy && /(?:sobre|asunto|acerca de)\s+([\p{L}\p{N}][\p{L}\p{N} ]{0,58})/u.exec(q)?.[1]?.split(/\s+(?:y|hoy|ayer)\b/)[0]?.trim();
      const domains=isHy?(config.domains||[]).filter(d=>/^(?:[a-z\d-]+\.)+[a-z]{2,}$/.test(d)):undefined;
      if(isHy && !domains.length)return unavailable('Falta configurar los dominios de Hy Cite para buscar esos correos.');
      const query={...opts,period,domains,topic,phrase:otherTopic};
      // Reuse the regular inbox tool for general reads. Its result is always capped at five.
      const result=isHy || otherTopic ? await searchInbox(tokens.access_token,query,{fetcher,signal}):
        await gmail.inbox(tokens.access_token,{...opts,...(period&&{days:undefined,since:period.since,before:period.before})})
          .then(rows=>({rows,more:rows.length===5}));
      const where=(opts.unread?' sin leer':'')+(isHy?(topic?' sobre Hy Cite':' de Hy Cite'):otherTopic?' sobre '+otherTopic:opts.from?' de '+opts.from:'')+
        (period?' de '+period.label:opts.days?' de los últimos '+opts.days+' días':'');
      summary=inboxSummary(result.rows,result.more,intent.index,intent.previousText,where);
    }
    signal?.throwIfAborted();
    if(!await gmailStore.exists({_id:user._id,grant:doc.grant,mailbox:doc.mailbox}))return unavailable('La conexión de Gmail cambió. Vuelve a pedir la consulta.');
    return summary;
  }
  async function sheetData(user,signal) {
    if(!config.sheetsReady || !permitted(config.sheetsPilot,fold(user.email)))throw new Error('Sheets no está disponible en Mis conexiones.',{cause:'missing'});
    const {token,...book}=await sheets.access(user._id);
    signal?.throwIfAborted();
    const data=await sheetsGoogle.readFollowups(token,book);
    signal?.throwIfAborted();
    const fresh=await sheets.getConnection(user._id);
    if(!fresh || !['grant','spreadsheetId','ticketsSheetId','referralsSheetId'].every(k=>fresh[k]===book[k]) || !await sheets.current(user._id,book.grant))throw new Error('Cambió la hoja elegida. Vuelve a pedir la consulta.',{cause:'changed'});
    return data;
  }
  return async (auth,utterance,recent=[],{signal}={})=>{
    const intent=selectIntent(utterance,recent);
    if(!intent)return null;
    if(!inDemoClosePilot(auth,false) || !await sessionCurrent(auth))return sessionChanged();
    if(intent.clarification)return {text:intent.clarification,sources:[]};
    signal?.throwIfAborted();
    let sheetRead;
    const parts=await Promise.all(intent.sources.map(async source=>{
      try {
        if(source==='gmail')return await mailbox(auth,intent,signal);
        if(source==='tec414')return await nativeSheets({programSheets,personalProgramSheets,programQuery},auth.user,intent);
        sheetRead ??= sheetData(auth.user,signal);
        return sheetSummary(source,await sheetRead,auth.user,intent,now());
      } catch(e) {
        signal?.throwIfAborted();
        if(source==='gmail')return unavailable(e.reauthorize?'Vuelve a autorizar Gmail en Mis conexiones.':'No pude completar la lectura de Gmail. Intenta de nuevo.');
        if(source==='tec414')return unavailable('No pude consultar tus hojas 4 en 14 guardadas en TEC.');
        if(e.cause==='changed')return unavailable('Cambió la hoja de Google Sheets. Repite la consulta.');
        if(e.status===409 || e.cause==='missing')return unavailable('Conecta Google Sheets y elige tus pestañas en Mis conexiones.');
        if(e.status===403|| e.reauthorize)return unavailable('Vuelve a autorizar Google Sheets en Mis conexiones.');
        return unavailable('No pude leer las pestañas seleccionadas en Google Sheets. Revisa la conexión y sus encabezados.');
      }
    }));
    signal?.throwIfAborted();
    if(!await sessionCurrent(auth))return sessionChanged();
    return {text:stayBrief([...new Set(parts.map(part=>typeof part==='string'?part:part.text))]),
      sources:intent.sources.filter((_,i)=>typeof parts[i]==='string')};
  };
}
