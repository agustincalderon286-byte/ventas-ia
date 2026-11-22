// Catálogo de lecturas del asistente del vendedor. Cada herramienta usa los servicios
// ya existentes con la cuenta del vendedor y devuelve datos, nunca texto para mostrar.
// Ninguna escribe. Los identificadores de Google no salen de aquí: el modelo recibe
// referencias cortas («m1») válidas solo para este vendedor y esta conversación.
// El contenido de correos, hojas y contactos es de terceros: se entrega como datos.
import {resolveInstant} from './coach-followup-input.js';
import {availableSlots} from './coach-calendar-slots.js';
import {sheetsToday} from './coach-sheets-followup-input.js';
import {openToken} from './coach-hycite-gmail.js';
import {isGmailAddress} from './coach-mail-address.js';

const day = {type:'string',maxLength:10,description:'Fecha AAAA-MM-DD en la zona del vendedor.'};
const tool = (name,source,description,properties={}) => ({name,source,description,
  parameters:{type:'object',additionalProperties:false,properties,required:Object.keys(properties)}});

export const agentTools = Object.freeze([
  tool('consultar_agenda','agenda','Lee las citas de «Seguimientos TEC» y los bloques ocupados del calendario principal entre dos fechas, ambas incluidas. Máximo 31 días.',
    {desde:day,hasta:day}),
  tool('buscar_horarios_libres','slots','Busca hasta dos horarios libres en un día y rango de horas. Son opciones: no reserva nada.',
    {dia:day,desdeHora:{type:'string',maxLength:5,description:'HH:MM en 24 horas.'},hastaHora:{type:'string',maxLength:5,description:'HH:MM en 24 horas.'},
      minutos:{type:'integer',minimum:15,maximum:240,description:'Duración de la cita.'}}),
  tool('leer_bandeja','inbox','Lee los 5 correos más recientes de la bandeja de entrada que cumplan los filtros: remitente, asunto, fecha y un extracto corto.',
    {soloNoLeidos:{type:'boolean'},remitente:{type:'string',maxLength:254,description:'Dirección exacta del remitente, o vacío.'},
      asunto:{type:'string',maxLength:160,description:'Palabras del asunto, o vacío.'},soloHyCite:{type:'boolean'},
      ultimosDias:{type:'integer',minimum:0,maximum:365,description:'0 para no filtrar por fecha.'}}),
  tool('leer_hilo','thread','Lee los mensajes de la conversación de un correo ya listado, del más antiguo al más reciente.',
    {ref:{type:'string',maxLength:8,description:'Referencia del correo, por ejemplo m2.'}}),
  tool('revisar_ventas_hycite','sales','Lee los avisos de Hy Cite del buzón y separa ventas confirmadas de avisos por revisar.',
    {ultimosDias:{type:'integer',minimum:1,maximum:90}}),
  tool('buscar_referidos','referrals','Busca fichas en la hoja personal del vendedor (referidos del 4 en 14 y boletas de rifa). Devuelve estado, resultado, próxima fecha y el final del historial. 12 por página.',
    {nombre:{type:'string',maxLength:80,description:'Palabras del nombre, o vacío.'},anfitrion:{type:'string',maxLength:80,description:'Palabras del nombre del anfitrión, o vacío.'},
      seguimiento:{type:'string',enum:['','hoy','vencidos','sin_fecha','futuros']},tipo:{type:'string',enum:['','referidos','boletas']},
      pagina:{type:'integer',minimum:1,maximum:50}}),
  tool('priorizar_llamadas','calls','Ordena hasta cinco personas de la hoja a quienes conviene llamar ahora, con el motivo: vencidos, de hoy, sin fecha y primeros contactos.',
    {tipo:{type:'string',enum:['','referidos','boletas']}}),
  tool('buscar_contactos','contacts','Busca una persona en los Contactos de Google del vendedor. Indica si existe y cuántos teléfonos y correos tiene guardados.',
    {nombre:{type:'string',maxLength:80}}),
  tool('estado_conexiones','connections','Indica qué cuentas del vendedor están conectadas al Coach y cuáles faltan.'),
  tool('pasar_al_coach',null,'Entrega el mensaje al flujo existente del Coach cuando el vendedor pide consejo o entrenamiento de ventas, o pide preparar, cambiar, guardar o enviar algo (cita, correo, WhatsApp, nota, alta, contacto), un trayecto entre visitas o la postventa enlazada a su hoja.',
    {motivo:{type:'string',enum:['consejo','gestion','trayecto','postventa','otro']}})
]);

const buckets = {hoy:'today',vencidos:'overdue',sin_fecha:'undated',futuros:'future'};
const kinds = {referidos:'referrals',boletas:'tickets'};
const civil = value => {
  const match=/^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) return null;
  const at=Date.UTC(Number(match[1]),Number(match[2])-1,Number(match[3]));
  return new Date(at).toISOString().slice(0,10)===value ? at : null;
};
const clock = value => {
  const match=/^([01]\d|2[0-3]):([0-5]\d)$/.exec(value);
  return match ? {hour:Number(match[1]),minute:Number(match[2])} : null;
};
const clean = (value,max=240) => String(value ?? '').replace(/[\p{Cc}\p{Cf}]+/gu,' ').replace(/\s+/g,' ').trim().slice(0,max);
const invalid = nota => ({estado:'peticion_invalida',nota});
const missing = (conexion,nota) => ({estado:'sin_conexion',conexion,nota});

export function localStamp(value, zone) {
  const parts=new Intl.DateTimeFormat('en-CA',{timeZone:zone,year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date(value));
  const part=key=>parts.find(p=>p.type===key).value;
  return part('year')+'-'+part('month')+'-'+part('day')+' '+part('hour')+':'+part('minute');
}

function failure(error, conexion) {
  if (error?.reauthorize || error?.reconnect) return {estado:'renovar_permiso',conexion,nota:'Google pide volver a autorizar esta cuenta en Mis conexiones.'};
  if (error?.status===403) return missing(conexion,clean(error.message) || 'Esta cuenta no tiene acceso.');
  if (error?.status===422 || error?.status===409 || error?.status===400) return invalid(clean(error.message) || 'Google rechazó la consulta.');
  if (error?.status===429) return {estado:'fallo',nota:'Google alcanzó temporalmente su límite. No se leyó nada.'};
  return {estado:'fallo',nota:'La consulta no se completó. No se leyó nada.'};
}

export function createAgentTools({config,calendar,store,gmail,syncSales,sheets,sheetTools,contacts,readConnections}) {
  const inList = (list,email) => Boolean(list?.some(item=>item==='*' || item===email));

  // actor: {owner,session,email,name,zone,current,now}. refs conserva las referencias entre turnos.
  // turn recoge lo leído en este mensaje que el chat necesita para sus botones.
  return function bind(actor, refs={}, turn={}) {
    const email=String(actor.email || '').toLowerCase();
    const now=()=>typeof actor.now==='function' ? actor.now() : Date.now();
    const stamp=value=>localStamp(value,actor.zone);
    const guard=async()=>{ if (!await actor.current()) throw Object.assign(Error('La sesión cambió.'),{sessionChanged:true}); };

    async function calendarAccess() {
      if (!inList(config.pilot,email)) return {problem:missing('calendar','Esta cuenta no participa en el piloto de Calendar.')};
      if (!config.calendarReady) return {problem:{estado:'fallo',nota:'Calendar no está disponible en TEC en este momento.'}};
      const connection=await calendar.getConnection(actor.owner);
      if (!connection?.grant || !connection.calendarId) return {problem:missing('calendar','El vendedor no ha conectado Google Calendar en Mis conexiones.')};
      return {connection};
    }
    async function mailAccess() {
      if (!inList(config.pilot,email)) return {problem:missing('gmail','Esta cuenta no participa en el piloto de Gmail.')};
      if (!config.ready) return {problem:{estado:'fallo',nota:'Gmail no está disponible en TEC en este momento.'}};
      const doc=await store.findById(actor.owner,'mailbox encryptedToken grant').lean();
      if (!doc?.grant || !doc.encryptedToken || !doc.mailbox) return {problem:missing('gmail','El vendedor no ha conectado Gmail en Mis conexiones.')};
      let credential;
      try { credential=openToken(doc.encryptedToken,config.key,actor.owner); }
      catch { return {problem:{estado:'renovar_permiso',conexion:'gmail',nota:'Hay que volver a vincular Gmail en Mis conexiones.'}}; }
      const token=await gmail.refresh(credential);
      return {doc,access:token.access_token};
    }
    async function mailUnchanged(doc) {
      return (await store.findById(actor.owner,'grant').lean())?.grant===doc.grant;
    }
    let sheetContext;
    async function sheetAccess() {
      if (!inList(config.sheetsPilot,email)) return {problem:missing('sheets','Esta cuenta no participa en el piloto de Google Sheets.')};
      if (!config.sheetsReady) return {problem:{estado:'fallo',nota:'Google Sheets no está disponible en TEC en este momento.'}};
      if (!sheetContext) {
        let book;
        try { book=await sheets.access(actor.owner); }
        catch (error) {
          if (error?.reauthorize) return {problem:failure(error,'sheets')};
          return {problem:missing('sheets',clean(error?.message) || 'El vendedor no ha conectado su hoja en Mis conexiones.')};
        }
        const {token,grant,...workbook}=book;
        sheetContext={owner:actor.owner,session:actor.session,grant,workbook,access:token,sellerName:actor.name,
          isCurrent:async()=>await actor.current() && await sheets.current(actor.owner,grant)};
      }
      return {ctx:sheetContext};
    }
    const card=(c,index)=>({ref:'r'+(index+1),nombre:clean(c.fullName,130),anfitrion:clean(c.host,130),tipo:c.kind==='tickets'?'boleta':'referido',
      hoja:clean(c.sheetTitle,80),fila:c.row,
      ...(c.bucket && {seguimiento:{estado:({today:'hoy',overdue:'vencido',undated:'sin fecha',future:'futuro',new:'primer contacto'})[c.bucket] || c.bucket,
        fecha:c.date || '',motivo:clean(c.reason)}})});

    const handlers = {
      async consultar_agenda({desde,hasta}) {
        const first=civil(desde), last=civil(hasta);
        if (first==null || last==null || last<first || last-first>30*86400000) return invalid('Usa fechas AAAA-MM-DD, con «hasta» igual o posterior a «desde» y un máximo de 31 días.');
        const start=resolveInstant(first,0,0,actor.zone), end=resolveInstant(last+86400000,0,0,actor.zone);
        if (start==null || end==null) return invalid('Ese día coincide con un cambio de horario. Prueba con el día siguiente.');
        const {connection,problem}=await calendarAccess();
        if (problem) return problem;
        const {events,busy,availabilityEnabled}=await calendar.readAgenda({owner:actor.owner,grant:connection.grant,zone:actor.zone,
          includeEvents:true,includePrimary:true,start:new Date(start).toISOString(),end:new Date(end).toISOString()});
        const avisos=[];
        if (events.status==='rejected') avisos.push('No se pudieron leer las citas de Seguimientos TEC.');
        if (!availabilityEnabled) avisos.push('El calendario principal no se consultó: falta activar «Horarios libres» en Mis conexiones.');
        else if (busy.status==='rejected') avisos.push('No se pudieron leer los bloques ocupados del calendario principal.');
        const rows=events.status==='fulfilled' ? events.value : [];
        return {estado:events.status==='fulfilled' || (availabilityEnabled && busy.status==='fulfilled') ? 'ok' : 'fallo',zona:actor.zone,desde,hasta,
          citasSeguimientosTEC:rows.slice(0,40).map(item=>({titulo:clean(item.title,130),
            ...(item.allDay ? {todoElDia:true,dia:item.start} : {inicio:stamp(item.start),fin:item.openEnd?'sin especificar':stamp(item.end)}),
            ...(item.attendance==='declined' && {invitacion:'rechazada'}),...(item.attendance==='needsAction' && {invitacion:'pendiente'}),
            ...(item.available && {marcadoLibre:true})})),
          ...(rows.length>40 && {citasOmitidas:rows.length-40}),
          ...(availabilityEnabled && busy.status==='fulfilled' && {ocupadoCalendarioPrincipal:busy.value.slice(0,40).map(b=>({inicio:stamp(b.start),fin:stamp(b.end)})),
            notaPrincipal:'Del calendario principal solo se conocen los bloques ocupados, sin títulos.'}),
          ...(avisos.length && {avisos})};
      },

      async buscar_horarios_libres({dia,desdeHora,hastaHora,minutos}) {
        const date=civil(dia), from=clock(desdeHora), to=clock(hastaHora);
        if (date==null || !from || !to) return invalid('Usa el día AAAA-MM-DD y las horas HH:MM en 24 horas.');
        const start=resolveInstant(date,from.hour,from.minute,actor.zone), end=resolveInstant(date,to.hour,to.minute,actor.zone);
        if (start==null || end==null || end<=start) return invalid('El rango de horas no es válido para ese día.');
        if (end<=now()) return invalid('Ese rango ya pasó.');
        const {connection,problem}=await calendarAccess();
        if (problem) return problem;
        if (!connection.availabilityEnabled) return missing('availability','Falta activar «Horarios libres» en Mis conexiones para buscar huecos.');
        const range={start:new Date(start).toISOString(),end:new Date(end).toISOString()};
        const busy=await calendar.availability({owner:actor.owner,grant:connection.grant,...range});
        const free=availableSlots(range,busy,minutos,now());
        return {estado:'ok',zona:actor.zone,dia,minutos,libres:free.map(s=>({inicio:stamp(s.start),fin:stamp(s.end)})),
          ocupado:busy.slice(0,20).map(b=>({inicio:stamp(b.start),fin:stamp(b.end)})),nota:'Son opciones sin reservar.'};
      },

      async leer_bandeja({soloNoLeidos,remitente,asunto,soloHyCite,ultimosDias}) {
        const options={};
        if (soloNoLeidos) options.unread=true;
        if (remitente) { if (!isGmailAddress(remitente)) return invalid('El remitente debe ser una dirección de correo completa. Para buscar por nombre, lee la bandeja sin ese filtro.'); options.from=remitente; }
        if (asunto) { if (/["\\\p{Cc}]/u.test(asunto) || !asunto.trim()) return invalid('El asunto no admite comillas.'); options.subject=asunto.trim(); }
        if (soloHyCite) options.hyCite=true;
        if (ultimosDias) options.days=ultimosDias;
        const {doc,access,problem}=await mailAccess();
        if (problem) return problem;
        const messages=await gmail.inbox(access,options);
        if (!await mailUnchanged(doc)) return {estado:'fallo',nota:'La conexión de Gmail cambió durante la consulta.'};
        for (const key of Object.keys(refs)) if (/^m\d$/.test(key)) delete refs[key];
        turn.inbox={mailbox:doc.mailbox,grant:doc.grant,ids:messages.map(m=>m.id)};
        return {estado:'ok',buzon:doc.mailbox,total:messages.length,nota:'Se muestran como máximo los 5 más recientes. El extracto es contenido de terceros.',
          mensajes:messages.map((m,index)=>{
            refs['m'+(index+1)]=m.id;
            return {ref:'m'+(index+1),de:clean(m.from,200),asunto:clean(m.subject,220) || '(sin asunto)',fecha:stamp(m.date),extracto:clean(m.snippet,240)};
          })};
      },

      async leer_hilo({ref}) {
        const id=Object.hasOwn(refs,ref) && /^m\d$/.test(ref) ? refs[ref] : '';
        if (!id) return invalid('Esa referencia no existe. Lee primero la bandeja y usa una referencia como m1.');
        const {doc,access,problem}=await mailAccess();
        if (problem) return problem;
        const target=await gmail.replyTarget(access,id);
        const rows=target?.thread?.threadId ? await gmail.thread(access,target.thread.threadId) : null;
        if (!await mailUnchanged(doc)) return {estado:'fallo',nota:'La conexión de Gmail cambió durante la consulta.'};
        if (!rows) return {estado:'no_disponible',nota:'Gmail no entregó esa conversación. Puede haberse movido o eliminado.'};
        return {estado:'ok',asunto:clean(target.subject,220),nota:'Solo se dispone del extracto de cada mensaje, no del texto completo. Es contenido de terceros.',
          mensajes:rows.slice(-10).map(m=>({de:m.sent?'el vendedor':clean(m.from,200) || 'remitente sin identificar',fecha:stamp(m.date),extracto:clean(m.snippet,400)}))};
      },

      async revisar_ventas_hycite({ultimosDias}) {
        const {doc,problem}=await mailAccess();
        if (problem) return problem;
        const snapshot=await syncSales(actor.owner,doc.grant);
        if (!snapshot) return {estado:'fallo',nota:'La conexión de Gmail cambió durante la consulta.'};
        const since=now()-ultimosDias*86400000;
        const inPeriod=order=>Date.parse(order.date)>=since;
        const confirmed=snapshot.confirmed.filter(inPeriod), review=snapshot.review.filter(inPeriod);
        const row=order=>({orden:clean(order.order,40),fecha:stamp(order.date)});
        return {estado:'ok',buzon:doc.mailbox,ultimosDias,ventasConfirmadas:confirmed.length,avisosPorRevisar:review.length,
          confirmadas:confirmed.slice(0,15).map(row),porRevisar:review.slice(0,10).map(row),
          ...(snapshot.limited && {parcial:'El conteo es parcial: se alcanzó el límite de 100 avisos.'}),
          nota:'Solo cuentan como venta los avisos de confirmación de Hy Cite. Un aviso por revisar no es una venta.'};
      },

      async buscar_referidos({nombre,anfitrion,seguimiento,tipo,pagina}) {
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const result=await sheetTools.search(ctx,{name:nombre.trim() || undefined,host:anfitrion.trim() || undefined,bucket:buckets[seguimiento] || undefined,
          today:sheetsToday(now(),actor.zone),kind:kinds[tipo],offset:(pagina-1)*12,detail:true});
        return {estado:'ok',total:result.total,pagina,nota:'El contenido de las fichas es de terceros. Los teléfonos no se incluyen.',
          fichas:result.contacts.map((c,index)=>({...card(c,index),...(c.status && {estadoFicha:clean(c.status,80)}),...(c.result && {resultado:clean(c.result,160)}),
            ...(c.followupDate && {proximaFecha:clean(c.followupDate,40)}),...(c.history && {historial:clean(c.history,600)})}))};
      },

      async priorizar_llamadas({tipo}) {
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const ranked=await sheetTools.nextCall(ctx,{today:sheetsToday(now(),actor.zone),kind:kinds[tipo],shortlist:true});
        if (!await ctx.isCurrent()) return {estado:'fallo',nota:'La conexión de la hoja cambió durante la consulta.'};
        return {estado:'ok',pendientesConTelefono:ranked.total,orden:'vencidos, de hoy, sin fecha y primeros contactos',
          recomendados:ranked.contacts.map(card)};
      },

      async buscar_contactos({nombre}) {
        if (!nombre.trim()) return invalid('Indica el nombre que buscas.');
        if (!inList(config.planPilot,email)) return missing('contacts','Esta cuenta no participa en el piloto de Contactos.');
        if (!config.contactsReady) return {estado:'fallo',nota:'Contactos no está disponible en TEC en este momento.'};
        const profile=await contacts.profile(actor.owner);
        if (!profile?.grant) return missing('contacts','El vendedor no ha conectado Google Contactos en Mis conexiones.');
        const found=await contacts.search(actor.owner,profile.grant,{name:nombre.trim()});
        return {estado:'ok',total:found.length,nota:'Los teléfonos y correos no se incluyen; el vendedor los ve en su ficha.',
          contactos:found.slice(0,8).map(c=>({nombre:clean(c.name,130),telefonosGuardados:c.phones.length,correosGuardados:c.emails.length}))};
      },

      async estado_conexiones() {
        const state=await readConnections({owner:actor.owner,email:actor.email});
        const label=value=>({connected:'conectado',missing:'falta conectar'})[value] || 'sin verificar';
        return {estado:'ok',gmail:label(state.gmail),envioDeCorreos:label(state.send),calendar:label(state.calendar),
          horariosLibres:label(state.availability),sheets:label(state.sheets)};
      },

      async pasar_al_coach({motivo}) { return {estado:'derivado',detener:motivo}; }
    };

    return async (name,args) => {
      if (!Object.hasOwn(handlers,name)) return {estado:'herramienta_desconocida'};
      const source=agentTools.find(t=>t.name===name).source;
      const connection=({agenda:'calendar',slots:'calendar',inbox:'gmail',thread:'gmail',sales:'gmail',referrals:'sheets',calls:'sheets',contacts:'contacts'})[source];
      try {
        await guard();
        const result=await handlers[name](args);
        await guard();
        return result;
      } catch (error) {
        if (error.sessionChanged) throw error;
        return failure(error,connection);
      }
    };
  };
}
