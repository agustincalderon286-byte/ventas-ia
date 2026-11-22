// Catálogo de herramientas del asistente del vendedor. Cada una usa los servicios
// ya existentes con la cuenta del vendedor y devuelve datos, nunca texto para mostrar.
// Ninguna escribe. Las de lectura consultan; las «preparar_…» dejan una propuesta en el mismo
// estado que usa el plan de conversación, y solo el botón del vendedor la confirma allí.
// Los identificadores de Google no salen de aquí: el modelo recibe
// referencias cortas («m1», «f3a9c1») válidas solo para este vendedor y esta conversación.
// El contenido de correos, hojas y contactos es de terceros: se entrega como datos.
// Teléfonos, direcciones y correos de fichas y contactos no se entregan al modelo: recibe un
// marcador como [[tel:ab12cd]] y el servidor pone el dato real en la respuesta que ve el vendedor.
import {createHash} from 'node:crypto';
import {resolveInstant} from './coach-followup-input.js';
import {availableSlots} from './coach-calendar-slots.js';
import {sheetsToday} from './coach-sheets-followup-input.js';
import {openToken} from './coach-hycite-gmail.js';
import {isGmailAddress} from './coach-mail-address.js';
import {initialPlan} from './coach-conversation-plan.js';

const day = {type:'string',maxLength:10,description:'Fecha AAAA-MM-DD en la zona del vendedor.'};
const fichaRef = {type:'string',maxLength:8,description:'Referencia «ref» de la ficha, tal como llegó en una búsqueda de la hoja.'};
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
  tool('buscar_referidos','referrals','Busca fichas en la hoja personal del vendedor (referidos del 4 en 14 y boletas de rifa) por nombre, anfitrión, palabras de sus notas o estado del seguimiento. Tolera nombres incompletos o con una letra distinta. Devuelve estado, resultado, próxima fecha, el final del historial y marcadores del teléfono, la dirección y el correo. 12 por página.',
    {nombre:{type:'string',maxLength:80,description:'Palabras del nombre, o vacío.'},anfitrion:{type:'string',maxLength:80,description:'Palabras del nombre del anfitrión, o vacío.'},
      notas:{type:'string',maxLength:80,description:'Palabras que deben aparecer en el historial, el resultado o el estado, por ejemplo «olla grande». Vacío si no aplica.'},
      seguimiento:{type:'string',enum:['','hoy','vencidos','sin_fecha','futuros']},tipo:{type:'string',enum:['','referidos','boletas']},
      pagina:{type:'integer',minimum:1,maximum:50}}),
  tool('priorizar_llamadas','calls','Ordena hasta cinco personas de la hoja a quienes conviene llamar ahora, con el motivo y el marcador de su teléfono: vencidos, de hoy, sin fecha y primeros contactos.',
    {tipo:{type:'string',enum:['','referidos','boletas']}}),
  tool('leads_por_zona','referrals','Fichas de la hoja por cercanía aproximada. Con «zonas» vacío devuelve las ciudades y códigos postales que aparecen en la hoja y cuántas fichas tiene cada uno. Con zonas, devuelve las fichas de esas ciudades o códigos postales en el orden dado (de la más cercana a la más lejana) y, dentro de cada zona, por urgencia del seguimiento.',
    {zonas:{type:'array',maxItems:12,items:{type:'string',maxLength:40},description:'Ciudades o códigos postales escritos como aparecen en la hoja, del más cercano al más lejano. Vacío para ver el catálogo.'},
      tipo:{type:'string',enum:['','referidos','boletas']},maximo:{type:'integer',minimum:1,maximum:15}}),
  tool('buscar_contactos','contacts','Busca una persona en los Contactos de Google del vendedor. Devuelve su nombre y los marcadores de sus teléfonos y correos.',
    {nombre:{type:'string',maxLength:80}}),
  tool('estado_conexiones','connections','Indica qué cuentas del vendedor están conectadas al Coach y cuáles faltan.'),
  tool('preparar_nota','note','Prepara, sin guardar, una nota en el historial de una ficha de la hoja y, si aplica, su próxima fecha de seguimiento. Sirve para registrar el resultado de una llamada o una visita. El vendedor la guarda con un botón.',
    {ref:fichaRef,nota:{type:'string',maxLength:600,description:'Lo que pasó, con las palabras del vendedor. Vacío si solo cambia la fecha.'},
      proximoSeguimiento:{type:'string',maxLength:10,description:'Fecha AAAA-MM-DD del próximo seguimiento, o vacío.'}}),
  tool('preparar_cita','appointment','Prepara, sin reservar, una cita, demostración o llamada en el calendario «Seguimientos TEC» del vendedor. Comprueba que el horario esté libre. El vendedor la confirma con un botón.',
    {ref:{type:'string',maxLength:8,description:'Referencia de la ficha si la persona está en la hoja, o vacío.'},
      nombre:{type:'string',maxLength:80,description:'Nombre de la persona. Con «ref» se usa el de la ficha.'},dia:day,
      hora:{type:'string',maxLength:5,description:'HH:MM en 24 horas.'},minutos:{type:'integer',minimum:15,maximum:240},
      tipo:{type:'string',enum:['cita','demo','llamada','seguimiento']}}),
  tool('preparar_alta','add','Prepara, sin guardar, el alta de una persona nueva en la hoja: un referido del 4 en 14 con su anfitrión o una boleta de rifa. Una persona por propuesta. El vendedor la confirma con un botón.',
    {nombre:{type:'string',maxLength:80},telefono:{type:'string',maxLength:30,description:'Teléfono tal como lo dijo el vendedor.'},
      tipo:{type:'string',enum:['referido','boleta']},anfitrion:{type:'string',maxLength:80,description:'Nombre del anfitrión para un referido. Vacío para una boleta.'}}),
  tool('preparar_mensaje','sms','Deja listo un mensaje de texto para una persona de la hoja. El vendedor verá el texto y un botón que abre la aplicación Mensajes de su teléfono con el número y el texto puestos; él lo envía. Máximo tres por respuesta.',
    {ref:fichaRef,texto:{type:'string',maxLength:480,description:'Mensaje completo en nombre del vendedor, corto y natural, sin marcadores.'}}),
  tool('pasar_al_coach',null,'Entrega el mensaje al flujo existente del Coach cuando el vendedor pide consejo o entrenamiento de ventas, un correo, un WhatsApp, un cambio en sus Contactos de Google, varias altas a la vez o la captura de una hoja del 4 en 14, un trayecto entre visitas o la postventa enlazada a su hoja.',
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
const markerNote = 'Los valores entre dobles corchetes, como [[tel:ab12cd]], son marcadores de datos privados. Cópialos exactamente en tu respuesta: el vendedor verá el dato real en su lugar.';
export const markerPattern = /\[{0,2}\b(tel|dir|correo):([a-f0-9]{6})\b\]{0,2}/g;

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

const pendingNote = 'Quedó como propuesta: no se ha guardado, reservado ni enviado nada. El vendedor verá la vista previa con su botón de confirmar; no repitas la vista previa ni digas que ya quedó hecho.';

// channels: el mismo preparador de citas que usa el plan de conversación. Los correos siguen en el flujo existente:
// su redactor recibe la dirección del destinatario, y el asistente no entrega direcciones al modelo.
export function createAgentTools({config,calendar,store,gmail,syncSales,sheets,sheetTools,contacts,readConnections,channels}) {
  const inList = (list,email) => Boolean(list?.some(item=>item==='*' || item===email));

  // actor: {owner,session,email,name,zone,current,now}. refs conserva las referencias entre turnos.
  // turn recoge lo leído en este mensaje que el chat necesita para sus botones.
  return function bind(actor, refs={}, turn={}) {
    const email=String(actor.email || '').toLowerCase();
    const now=()=>typeof actor.now==='function' ? actor.now() : Date.now();
    const stamp=value=>localStamp(value,actor.zone);
    // Guarda el dato real para este turno y devuelve el marcador que verá el modelo.
    const marker=(kind,id,value)=>{
      const token=kind+':'+createHash('sha256').update(kind+'\n'+id+'\n'+value).digest('hex').slice(0,6);
      (turn.secrets ||= {})[token]=value;
      return '[['+token+']]';
    };
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
    // La referencia de una ficha es estable entre búsquedas y turnos: identifica a la persona, no su posición en una lista.
    const refOf=c=>{
      const ref='f'+createHash('sha256').update(String(c.identityKey)).digest('hex').slice(0,5);
      delete refs[ref];
      refs[ref]=c.identityKey;
      return ref;
    };
    const knownFicha=ref=>/^f[a-f0-9]{5}$/.test(ref) && Object.hasOwn(refs,ref) ? refs[ref] : '';
    const noFicha=()=>invalid('Esa referencia no existe. Busca primero a la persona en la hoja y usa la «ref» de su ficha.');
    const card=(c)=>({ref:refOf(c),nombre:clean(c.fullName,130),anfitrion:clean(c.host,130),tipo:c.kind==='tickets'?'boleta':'referido',
      hoja:clean(c.sheetTitle,80),fila:c.row,
      ...(c.bucket && {seguimiento:{estado:({today:'hoy',overdue:'vencido',undated:'sin fecha',future:'futuro',new:'primer contacto'})[c.bucket] || c.bucket,
        fecha:c.date || '',motivo:clean(c.reason)}})});

    // Ficha completa para el modelo: datos de gestión en claro y datos de contacto como marcadores.
    const ficha=(c,historyLength)=>({...card(c),...(c.status && {estadoFicha:clean(c.status,80)}),...(c.result && {resultado:clean(c.result,160)}),
      ...(c.followupDate && {proximaFecha:clean(c.followupDate,40)}),...(c.history && {historial:clean(c.history.slice(-historyLength),historyLength)}),
      telefono:c.noCalls ? 'no llamar: la ficha pide no ser contactada' : c.rawPhone ? marker('tel',c.identityKey,clean(c.rawPhone,40)) : 'sin teléfono en la hoja',
      direccion:c.address ? marker('dir',c.identityKey,clean(c.address,240)) : 'sin dirección en la hoja',
      ...(c.rawEmail && {correo:marker('correo',c.identityKey,clean(c.rawEmail,254))})});

    // Propuesta de este turno. `turn.plan.draft` tiene la forma del plan de conversación; el asistente
    // del vendedor la guarda al terminar y muestra los botones. Una propuesta trata de una sola persona
    // de la hoja, y un alta va sola.
    const plan=()=>turn.plan ||= {draft:initialPlan(),changed:false};
    function enter(op,person) {
      const state=plan(), s=state.draft;
      const otherPerson=person && s.chosen && s.chosen.identityKey!==person.identityKey;
      const mixesAdd=s.ops.length>0 && (op==='add')!==s.ops.includes('add');
      if (otherPerson || mixesAdd) {
        if (state.changed) return otherPerson ? 'Una propuesta trata de una sola persona. Deja lista la de '+clean(s.chosen.fullName,80)+' y, cuando el vendedor la confirme o la cierre, prepara la siguiente.'
          : 'El alta de una persona nueva va en su propia propuesta. Cuando el vendedor confirme o cierre la actual, prepara la otra.';
        state.draft=initialPlan();
      }
      return '';
    }
    function put(op,step,summary,person) {
      const state=plan(), s=state.draft;
      if (!s.ops.includes(op)) s.ops.push(op);
      s.steps[op]={status:'ready',...step};
      // Resumen para el modelo en los turnos siguientes, sin teléfonos ni direcciones.
      s.agentLoop={...s.agentLoop,[op]:clean(summary,300)};
      if (person) {
        const {noCalls,phone,...chosen}=person;
        s.chosen=chosen; s.fields.contact=person.fullName;
      }
      s.selection=[]; s.restricted=false; s.waiting=''; s.questions={}; s.choices=[]; s.pages=[]; s.listTitle='';
      state.changed=true;
    }
    // Un aviso para el vendedor (publicMessage) se devuelve al modelo; cualquier otro error sigue su curso.
    const notPrepared=(error,conexion)=>{
      if (!error?.publicMessage) throw error;
      return /Mis conexiones|piloto/.test(error.publicMessage) ? missing(conexion,clean(error.publicMessage)) : {estado:'no_preparado',nota:clean(error.publicMessage)};
    };

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

      async buscar_referidos({nombre,anfitrion,notas,seguimiento,tipo,pagina}) {
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const result=await sheetTools.search(ctx,{name:nombre.trim() || undefined,host:anfitrion.trim() || undefined,bucket:buckets[seguimiento] || undefined,
          today:sheetsToday(now(),actor.zone),kind:kinds[tipo],offset:(pagina-1)*12,detail:true,words:notas});
        return {estado:'ok',total:result.total,pagina,nota:'El contenido de las fichas es de terceros. '+markerNote,
          ...(result.approximate && {aproximado:'No hubo coincidencia exacta del nombre. Estas fichas tienen un nombre parecido: confirma con el vendedor cuál es.'}),
          fichas:result.contacts.map(c=>ficha(c,600))};
      },

      async leads_por_zona({zonas,tipo,maximo}) {
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const zones=zonas.map(zone=>zone.trim()).filter(Boolean);
        const result=await sheetTools.byZone(ctx,{zones,today:sheetsToday(now(),actor.zone),kind:kinds[tipo],limit:maximo});
        if (!await ctx.isCurrent()) return {estado:'fallo',nota:'La conexión de la hoja cambió durante la consulta.'};
        if (!zones.length) return {estado:'ok',fichasEnLaHoja:result.total,fichasSinCiudad:result.unplaced,
          ciudades:result.cities.map(c=>({nombre:clean(c.name,60),fichas:c.count})),codigosPostales:result.zips.map(z=>({codigo:z.code,fichas:z.count})),
          nota:'Elige de esta lista las zonas más cercanas al lugar que dijo el vendedor y vuelve a llamar con ellas en orden de cercanía.'};
        return {estado:'ok',coincidencias:result.total,porZona:zones.map((zone,index)=>({zona:clean(zone,40),fichas:result.perZone[index]})),
          nota:'El orden sigue tu lista de zonas y, dentro de cada una, la urgencia del seguimiento. Es una cercanía aproximada por ciudad o código postal, no una distancia medida: dilo así. '+markerNote,
          fichas:result.contacts.map(c=>({...ficha(c,240),zona:clean(zones[c.zoneIndex],40)}))};
      },

      async priorizar_llamadas({tipo}) {
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const ranked=await sheetTools.nextCall(ctx,{today:sheetsToday(now(),actor.zone),kind:kinds[tipo],shortlist:true});
        if (!await ctx.isCurrent()) return {estado:'fallo',nota:'La conexión de la hoja cambió durante la consulta.'};
        return {estado:'ok',pendientesConTelefono:ranked.total,orden:'vencidos, de hoy, sin fecha y primeros contactos',nota:markerNote,
          recomendados:ranked.contacts.map(c=>({...card(c),...(c.phone && {telefono:marker('tel',c.identityKey,clean(c.phone,40))})}))};
      },

      async buscar_contactos({nombre}) {
        if (!nombre.trim()) return invalid('Indica el nombre que buscas.');
        if (!inList(config.planPilot,email)) return missing('contacts','Esta cuenta no participa en el piloto de Contactos.');
        if (!config.contactsReady) return {estado:'fallo',nota:'Contactos no está disponible en TEC en este momento.'};
        const profile=await contacts.profile(actor.owner);
        if (!profile?.grant) return missing('contacts','El vendedor no ha conectado Google Contactos en Mis conexiones.');
        const found=await contacts.search(actor.owner,profile.grant,{name:nombre.trim()});
        return {estado:'ok',total:found.length,nota:markerNote,
          contactos:found.slice(0,8).map(c=>({nombre:clean(c.name,130),
            telefonos:c.phones.slice(0,4).map(value=>marker('tel',c.resourceName,clean(value,40))),
            correos:c.emails.slice(0,4).map(value=>marker('correo',c.resourceName,clean(value,254)))}))};
      },

      async estado_conexiones() {
        const state=await readConnections({owner:actor.owner,email:actor.email});
        const label=value=>({connected:'conectado',missing:'falta conectar'})[value] || 'sin verificar';
        return {estado:'ok',gmail:label(state.gmail),envioDeCorreos:label(state.send),calendar:label(state.calendar),
          horariosLibres:label(state.availability),sheets:label(state.sheets)};
      },

      async preparar_nota({ref,nota,proximoSeguimiento}) {
        const id=knownFicha(ref);
        if (!id) return noFicha();
        if (proximoSeguimiento && civil(proximoSeguimiento)==null) return invalid('Usa la fecha AAAA-MM-DD para el próximo seguimiento.');
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const person=await sheetTools.card(ctx,id);
        const blocked=enter('note',person);
        if (blocked) return invalid(blocked);
        const made=await sheetTools.prepareNote(ctx,{key:person.key,note:nota,followupDate:proximoSeguimiento,today:sheetsToday(now(),actor.zone)});
        if (!await ctx.isCurrent()) return {estado:'fallo',nota:'La conexión de la hoja cambió durante la preparación.'};
        put('note',{preview:made.preview,proposal:made.proposal},'Nota para '+person.fullName+': «'+nota.trim()+'»'+(proximoSeguimiento ? ' · próximo seguimiento '+proximoSeguimiento : ''),person);
        const s=plan().draft;
        s.sheetGrant=ctx.grant; s.fields.note=nota.trim(); s.fields.noteSchedule=proximoSeguimiento;
        return {estado:'preparado',persona:clean(person.fullName,130),hoja:clean(person.sheetTitle,80),fila:person.row,nota:pendingNote};
      },

      async preparar_cita({ref,nombre,dia,hora,minutos,tipo}) {
        if (civil(dia)==null || !clock(hora)) return invalid('Usa el día AAAA-MM-DD y la hora HH:MM en 24 horas.');
        let person=null, grant='';
        if (ref) {
          const id=knownFicha(ref);
          if (!id) return noFicha();
          const {ctx,problem}=await sheetAccess();
          if (problem) return problem;
          person=await sheetTools.card(ctx,id);
          grant=ctx.grant;
        }
        const name=clean(person?.fullName || nombre,80);
        if (!name || !/^[\p{L}\p{N} .’'-]+$/u.test(name)) return invalid('Indica el nombre de la persona solo con letras y números.');
        const blocked=enter('appointment',person);
        if (blocked) return invalid(blocked);
        // El mismo lector de fechas del plan de conversación: una fecha y una hora sin ambigüedad.
        const schedule=dia+' a las '+hora+({cita:' cita',demo:' demo',llamada:' llamada',seguimiento:''})[tipo]+' durante '+minutos+' minutos';
        let made;
        try { made=await channels.appointment(actor,{},{name,schedule}); }
        catch (error) { return notPrepared(error,'calendar'); }
        if (!made.proposal) return {estado:'no_preparado',motivo:clean(made.question),nota:'No se preparó la cita. Si el horario está ocupado, busca horarios libres y propón otro.'};
        put('appointment',{preview:made.preview,proposal:made.proposal,whenText:made.whenText},tipo+' con '+name+' el '+dia+' a las '+hora+', '+minutos+' min',person);
        const s=plan().draft;
        if (person) s.sheetGrant=grant;
        s.calendarState=made.state || {}; s.fields.schedule=schedule;
        return {estado:'preparado',persona:name,cuando:clean(made.whenText,120),nota:pendingNote};
      },

      async preparar_alta({nombre,telefono,tipo,anfitrion}) {
        const kind=tipo==='boleta' ? 'tickets' : 'referrals';
        const fullName=nombre.trim(), host=anfitrion.trim(), phone=telefono.trim();
        if (!fullName || !phone) return invalid('Para el alta hacen falta el nombre y el teléfono.');
        if (kind==='referrals' && !host) return invalid('Para un referido indica quién es su anfitrión. Pregúntaselo al vendedor.');
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const blocked=enter('add',null);
        if (blocked) return invalid(blocked);
        const made=await sheetTools.prepareAdd(ctx,{kind,fullName,phone,hostName:kind==='referrals' ? host : undefined});
        if (!await ctx.isCurrent()) return {estado:'fallo',nota:'La conexión de la hoja cambió durante la preparación.'};
        put('add',{preview:made.preview,proposal:made.proposal},'Alta de '+fullName+(kind==='referrals' ? ', referido de '+host : ', boleta de rifa'));
        const s=plan().draft;
        delete s.chosen;
        s.sheetGrant=ctx.grant; Object.assign(s.fields,{contact:fullName,phone,leadKind:kind,...(kind==='referrals' && {host})});
        return {estado:'preparado',persona:clean(fullName,130),...(/Posible duplicado/.test(made.preview) && {aviso:'La hoja ya tiene un nombre o teléfono parecido. Díselo al vendedor para que lo revise antes de confirmar.'}),nota:pendingNote};
      },

      async preparar_mensaje({ref,texto}) {
        const id=knownFicha(ref);
        if (!id) return noFicha();
        const text=clean(texto,480);
        if (!text) return invalid('Escribe el mensaje completo.');
        if ((turn.messages || []).length>=3) return invalid('Ya hay tres mensajes en esta respuesta. Prepara los demás en el siguiente turno.');
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const person=await sheetTools.card(ctx,id);
        if (person.noCalls) return {estado:'no_permitido',nota:'La ficha pide no ser contactada. No se preparó el mensaje.'};
        if (!person.phone) return {estado:'no_disponible',nota:'La ficha no tiene un teléfono completo para enviarle un mensaje.'};
        (turn.messages ||= []).push({to:person.phone,name:clean(person.fullName,80),text});
        return {estado:'preparado',persona:clean(person.fullName,130),
          nota:'El vendedor verá el texto y un botón que abre Mensajes en su teléfono con el número y el texto puestos. No se ha enviado nada: lo envía él. No repitas el texto del mensaje en tu respuesta.'};
      },

      async pasar_al_coach({motivo}) { return {estado:'derivado',detener:motivo}; }
    };

    return async (name,args) => {
      if (!Object.hasOwn(handlers,name)) return {estado:'herramienta_desconocida'};
      const source=agentTools.find(t=>t.name===name).source;
      const connection=({agenda:'calendar',slots:'calendar',appointment:'calendar',inbox:'gmail',thread:'gmail',sales:'gmail',
        referrals:'sheets',calls:'sheets',note:'sheets',add:'sheets',sms:'sheets',contacts:'contacts'})[source];
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
