import {readFollowup} from './coach-followup-input.js';

export const fold = text => String(text ?? '').normalize('NFD').replace(/\p{M}/gu,'').toLowerCase().replace(/\s+/g,' ').trim();
const stopWords = /\b(?:pasar|paso|visitar|visita(?:r)?|parar|hacer una parada|recoger|de camino)\b/;
const routeWords = /\b(?:rutas?|trayectos?|trafico|maps|traslado|(?:llego|llegare|llegaria) a tiempo|a que hora (?:debo |deberia |tengo que )?sal(?:go|ir)|cuanto (?:me )?tard(?:o|are|aria)|alcanzo a llegar|tiempo de llegar)\b/;
const visitWindow = /\b(?:cabe|cabria|antes|chance|alcanzo|da tiempo|puedo pasar|puedo visitar|entre mis)\b/;
export function routeIntent(text) {
  const t=fold(text);
  if (/^(?:¿\s*)?(?:confirmar|descartar|enviar|responde|escribe|redacta|mandale|dile|prepara(?:me)? un (?:mensaje|correo)|como (?:puedo |se )?usar)\b/.test(t)) return false;
  return (routeWords.test(t) && /\b(?:demo|visita|cita|cliente|llegar|calendario|agenda|salgo|salir|destino)\b/.test(t))
    || (stopWords.test(t) && visitWindow.test(t) && /\b(?:demo|cita|la de las?|antes de|cliente|señora|senora)\b/.test(t));
}

export function otherRequest(text) {
  return /\b(?:gmail|whatsapp|boletas nuevas|mis contactos|postventa|hy\s?cite|ventas? confirmadas?|demo de uso|siguiente pendiente|borrador|correos?|google contacts)\b/.test(fold(text)) || /^(?:tecplan|teccontacts?|confirmar |descartar (?:cita|correo)|guardar |guarda |agendame|agenda |programa |reagenda|mueve |revisa |dame |muestra|prepara |responde|escribe |manda|busca mis|quiero (?:agendar|mandar|enviar)|corrige)\b/.test(fold(text));
}
// A pending turn belongs to a field. Hand other sales requests back to the
// dispatcher even when a route was the last conversation on screen.
export function routeAnswer(text,field,context) {
  const t=fold(text).replace(/^[¿¡]|[?!.]$/g,'').trim();
  if (/^(?:no se|no tengo (?:la direccion|el tiempo)|no recuerdo)$/.test(t)) return true;
  if (['origin','stop','destination'].includes(field)) {
    if (/(?:^|\s)(?:necesito que|quiero que|hay que|me puedes|puedes|dime|dame|pendientes|seguimientos)\b/.test(t)) return false;
    return usablePlace(text) || /^(?:salgo|saldre|parto|estoy|desde|en|la direccion|la calle|origen|parada|destino)\b/.test(t) ||
      /^[\p{L}][\p{L}. '-]{0,45}$/u.test(t) && t.split(' ').length<=3;
  }
  if (field==='duration' || field==='manual') {
    return /\b(?:minutos?|mins?|horas?|hrs?|maps)\b/.test(t) || /^\d+(?:\s*(?:y|,|;)\s*\d+)?$/.test(t);
  }
  if (field==='choose' && /^(?:(?:la |opcion )?[1-5]|(?:la )?(?:primera|segunda|tercera|cuarta|quinta)|(?:otra|ninguna)(?: cita| demo)?)$/.test(t)) return true;
  return /^(?:(?:de|por|en) la )?(?:manana|tarde|noche|am|pm)$/.test(t) ||
    Boolean(clockFragment(t) || readFollowup(t,{kind:'availability'},context).state.day!=null);
}
const place = raw => String(raw || '').trim().replace(/^[¿¡\s:]+|[.?!\s;]+$/gu,'').slice(0,240);
export function usablePlace(value) {
  const t=fold(value);
  return typeof value==='string' && value.length>=5 && value.length<=240 &&
    !/[\p{Cc}<>]|https?:\/\/|@/iu.test(value) && !/\b(?:mi casa|mi ubicacion|casa del cliente|su casa|no (?:se|tengo|recuerdo))\b/.test(t) &&
    (/\p{L}[^,]+,\s*\p{L}/u.test(t) || /\d.+\b(?:st(?:reet)?|ave(?:nue)?|dr(?:ive)?|rd|road|pl(?:ace)?|ln|lane|blvd|bl|way|ct|court|calle|avenida)\.?\s+\p{L}/u.test(t) || /\d.+\p{L}.+\b\d{5}(-\d{4})?$/u.test(t));
}

const boundary = /\s+(?:a las?\b|antes de\b|y (?:luego|despu[eé]s|paso|salgo|visito|la demo|la cita)|para (?:la|mi) (?:demo|cita)|durante\b|por \d+ min|hoy\b|ma[ñn]ana\b)|[;?¿!]|\.(?:\s|$)/iu;
function atMarker(text, marker) {
  for (const match of text.matchAll(new RegExp(marker,marker.flags+'g'))) {
    const result=place(text.slice(match.index+match[0].length).split(boundary)[0]);
    if (usablePlace(result)) return result;
  }
  return '';
}

function clockFragment(text) {
  const m=/(?:\ba (?:las?\s+)?|\b(?:la|el) de las?\s+|^)(\d{1,2}|una?|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce)(?::\d{2}| y (?:media|cuarto))?(?:\s*(?:[ap]\.?\s*m\.?|de la (?:tarde|noche|ma[ñn]ana)))?(?![\d:])/iu.exec(text);
  if (!m) return '';
  return m[0].replace(/^.*?de las?\s+(?=\d|[a-z])/i,'a las ');
}
const clockFrom=(text,ctx)=>readFollowup(clockFragment(text),{},ctx).state.clock;
export function routeDetails(text, state, context) {
  const clean=text.trim(), t=fold(clean), waiting=state.waiting;
  const stopName = /\b(?:pas(?:ar|o) (?:a ver a |con )|visit(?:ar|o) a |(?:parar|parada) con )(?:(?:la |el )?(?:se[ñn]or(?:a)?|sr[a]?\.?)\s+)?([\p{L}][\p{L} ’'-]{0,65}?)(?=\s+(?:antes|en|a las?|ma[ñn]ana|hoy|por|durante)\b|[,.;?!¿]|$)/iu.exec(clean)?.[1]?.trim();
  if (stopName && stopName!==state.stopName && !/\b(?:otro|un|una|cliente|alguien)\b/.test(fold(stopName))) {
    state.stopName=stopName;
    delete state.stop; delete state.lookedUp; delete state.sheetStop; delete state.sheetGrant;
  }
  if (stopWords.test(t) && visitWindow.test(t) && !/\b(?:sin (?:hacer )?parada|sin visitar)\b/.test(t)) state.visit=true;
  const when=readFollowup(clean,{kind:'availability'},context).state;
  if (when.day!=null) {
    if (state.day!==when.day) {delete state.eventId;delete state.choices;}
    state.day=when.day;
    delete state.next;
  } else if (/\b(?:proxima|siguiente) (?:cita|demo|visita)\b/.test(t)) state.next=true;
  const departureClause=/\b(?:salgo|saldr[eé]|saliendo|puedo salir|a partir de|desde las?|salida)\b([^;?!¿.]+)/iu.exec(clean);
  const targetClause=/\b(?:demo(?:straci[oó]n)?|cita|la de las?)\b([^;?!¿.]{0,155})/iu.exec(clean);
  if (departureClause) {
    const clock=clockFrom(departureClause[0].replace(/^(?:desde|a partir de) las? /i,'a las '),context);
    if (clock) state.startClock=clock;
  }
  if (targetClause) {
    const clock=clockFrom(targetClause[0].split(/\b(?:salgo|saldr[eé]|puedo salir|a partir de|desde las?)\b/iu)[0],context);
    if (clock) state.demoClock=clock;
  }
  if (waiting==='demoTime' || waiting==='departure') {
    const fields=readFollowup(clean,{clock:waiting==='departure'?state.startClock:state.demoClock},context).state;
    if (fields.clock) state[waiting==='departure'?'startClock':'demoClock']=fields.clock;
  }
  for (const [key,marker] of [
    ['origin',/\b(?:salgo de|saldr[eé] de|saliendo de|origen\s*[:=]|parto de|estoy en)\s*/iu],
    ['stop',/\b(?:parada\s*[:=]|pas(?:ar|o) (?:por|a)|visit(?:ar|o) (?:a un cliente )?en|visita (?:en|a)|cliente en)\s*/iu],
    ['destination',/\b(?:(?:demo(?:straci[oó]n)?|cita) (?:es |ser[aá] )?en|destino\s*[:=])\s*/iu]
  ]) {
    const val=atMarker(clean,marker);
    if (val) {state[key]=val;if (key==='stop') delete state.sheetStop;}
  }
  if (['origin','stop','destination'].includes(waiting)) {
    const answer=place(clean.replace(/^(?:estoy en|salgo de|es en|es|la direcci[oó]n es|en)\s+/iu,''));
    if (usablePlace(answer) && !/[;?¿!]|\b(?:origen|parada|destino)\s*:/iu.test(answer)) state[waiting]=answer;
  }
  if (waiting==='duration' && /^\d{1,3}$/.test(t) && Number(t)>0 && Number(t)<=720) state.visitMinutes=Number(t);
  const dur=/\b(\d{1,3}|media|una?|dos|quince|veinte|treinta|cuarenta y cinco)\s*(min(?:utos?)?s?|horas?|hrs?)\b/i.exec(t);
  if (dur && waiting!=='manual') {
    const n=Number(dur[1]) || ({media:.5,un:1,una:1,dos:2,quince:15,veinte:20,treinta:30,'cuarenta y cinco':45})[dur[1]];
    const min=n*(dur[2][0]==='h'?60:1);
    if (Number.isInteger(min) && min>0 && min<=720) state.visitMinutes=min;
  }
  return state;
}
