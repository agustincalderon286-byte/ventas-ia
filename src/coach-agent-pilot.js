import {calendarIntent} from './coach-followup-input.js';
import {classifyMailIntent} from './coach-mail-intent.js';
import {excludedReads} from './coach-plan-responses.js';

const sellers = new Set(['agustincalderon286@gmail.com','calderonrigoberto51@gmail.com']);
const models = new Set(['gpt-6-luna','gpt-6.2-sol']);
const fold = value => String(value || '').toLowerCase().normalize('NFD').replace(/\p{M}/gu,'').trim();

export function sellerAgentModel(email, configured = 'gpt-6.2-sol') {
  return sellers.has(fold(email)) && models.has(configured) ? configured : null;
}

export function agentConnectionReader({config,store,sheets,calendar}) {
  return async actor => {
    const inList = list => list?.some(item=>item==='*' || item===fold(actor.email));
    const personal = inList(config.pilot);
    const [gmail,cal,sheet] = await Promise.allSettled([
      personal && config.ready ? store.findById(actor.owner,'grant mailbox sendEnabled').lean() : null,
      inList(config.calendarPilot ?? config.pilot) && config.calendarReady ? calendar.getConnection(actor.owner) : null,
      inList(config.sheetsPilot) && config.sheetsReady ? sheets.getConnection(actor.owner) : null
    ]);
    const state = (result, usable) => result.status==='rejected' ? 'unknown' : usable(result.value) ? 'connected' : 'missing';
    return {
      gmail:state(gmail,doc=>doc?.grant && doc.mailbox),send:state(gmail,doc=>doc?.grant && doc.sendEnabled),
      calendar:state(cal,doc=>doc?.calendarId && doc.grant),availability:state(cal,doc=>doc?.availabilityEnabled),
      sheets:state(sheet,doc=>doc?.spreadsheetId && doc.grant)
    };
  };
}

export function describeAgentConnections(state) {
  const status=(key,ready,missing)=>state?.[key]==='connected' ? ready : state?.[key]==='missing' ? missing : 'No pude verificar el estado.';
  return [
    'Google Calendar · '+status('calendar','Puedo leer las citas de Seguimientos TEC.','Falta conectar tu calendario.'),
    'Disponibilidad · '+status('availability','Puedo consultar los bloques ocupados de tu calendario principal.','Falta activar los horarios libres.'),
    'Google Sheets · '+status('sheets','Puedo revisar tus boletas y referidos y preparar cambios.','Falta conectar y elegir tu hoja.'),
    'Gmail · '+status('gmail','Puedo revisar el correo y las confirmaciones de Hy Cite.','Falta conectar tu buzón.'),
    'Envíos · '+status('send','Puedes revisar y confirmar un envío.','Falta autorizar el envío de correos.')
  ].join('\n');
}

// A few unambiguous single-source reads remain available during a model outage.
// Any mixed or qualified request stays with the tool planner.
export function offlineRead(text,lastRead) {
  if (excludedReads(text).size) return null;
  const t=fold(text).replace(/[¿?¡!.]/g,'').trim();
  if (/\b(?:no|ni|sin consultar|evita|nunca|ejemplo|como puedo|imagina|si pudieras|supongamos|si tuviera|no quiero)\b/.test(t)) return null;
  if (/\b(?:guarda|agendame|agenda una|programa|agrega|prepara|borrador|manda|envia|responde|mueve|cambia|borra|cancela|dos (?:horarios|huecos)|busca.{0,10}libres)\b/.test(t)) return null;
  const day='(?:hoy|manana|pasado manana|(?:el )?(?:lunes|martes|miercoles|jueves|viernes|sabado|domingo))';
  const dayOnly=new RegExp('^(?:y )?'+day+'$');
  if(lastRead==='agenda' && dayOnly.test(t)) return 'agenda';
  const other=/\b(?:correo|correos|gmail|hy cite|ventas|hoja|referidos|boletas|leads|contactos|whatsapp|ruta|trafico)\b/;
  if(!other.test(t) && (calendarIntent(t)?.kind==='list' ||
    new RegExp('^(?:(?:oye |jorge |coach |por favor )|[, ])*(?:que tengo|tengo (?:alguna |alguna otra )?(?:citas?|demos?)|a que hora (?:es|tengo) mi (?:primera |siguiente )?(?:demo|cita)) (?:para )?'+day+'$').test(t))) return 'agenda';
  if (/\b(?:calendario|agenda|demo|sheets|hoja|referidos|boletas|leads)\b/.test(t)) return null;
  const mail=classifyMailIntent(text)?.kind;
  return ['inbox','sales'].includes(mail)?mail:null;
}
