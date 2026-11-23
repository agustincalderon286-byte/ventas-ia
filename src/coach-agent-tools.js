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
  tool('leads_por_distancia','distance','Fichas de la hoja ordenadas por minutos reales de manejo, con tráfico, desde donde está el vendedor (Google Maps). Mide hasta ocho fichas con calle y número dentro de las zonas indicadas. Antes consulta el catálogo con leads_por_zona y zonas vacías. Si responde «no_disponible», usa leads_por_zona y di que la cercanía es aproximada.',
    {origen:{type:'string',maxLength:120,description:'Lugar de salida como lo dijo el vendedor, con ciudad y estado: «Blue Island, IL» o «127th St y Western Ave, Blue Island, IL».'},
      zonas:{type:'array',maxItems:12,items:{type:'string',maxLength:40},description:'Ciudades o códigos postales del catálogo de la hoja, del más cercano al más lejano.'},
      tipo:{type:'string',enum:['','referidos','boletas']},maximo:{type:'integer',minimum:1,maximum:8}}),
  tool('buscar_contactos','contacts','Busca una persona en los Contactos de Google del vendedor. Devuelve su nombre y los marcadores de sus teléfonos y correos.',
    {nombre:{type:'string',maxLength:80}}),
  tool('estado_conexiones','connections','Indica qué cuentas del vendedor están conectadas al Coach y cuáles faltan.'),
  tool('preparar_nota','note','Prepara, sin guardar, una nota en el historial de una ficha de la hoja y, si aplica, su próxima fecha de seguimiento. Sirve para registrar el resultado de una llamada o una visita, o lo que un correo dice sobre esa persona. El vendedor la guarda con un botón.',
    {ref:fichaRef,nota:{type:'string',maxLength:600,description:'Lo que pasó, con las palabras del vendedor, o el resumen de lo que dice un correo que acabas de leer, indicando que viene de un correo y su fecha. Sin direcciones de correo. Vacío si solo cambia la fecha.'},
      proximoSeguimiento:{type:'string',maxLength:10,description:'Fecha AAAA-MM-DD del próximo seguimiento, o vacío.'}}),
  tool('preparar_cita','appointment','Prepara, sin reservar, una cita, demostración o llamada en el calendario «Seguimientos TEC» del vendedor. Comprueba que el horario esté libre. El vendedor la confirma con un botón.',
    {ref:{type:'string',maxLength:8,description:'Referencia de la ficha si la persona está en la hoja, o vacío.'},
      nombre:{type:'string',maxLength:80,description:'Nombre de la persona. Con «ref» se usa el de la ficha.'},dia:day,
      hora:{type:'string',maxLength:5,description:'HH:MM en 24 horas.'},minutos:{type:'integer',minimum:15,maximum:240},
      tipo:{type:'string',enum:['cita','demo','llamada','seguimiento']}}),
  tool('preparar_cambio_cita','move','Prepara, sin cambiar nada, mover a otro día u hora una cita que el Coach creó en «Seguimientos TEC». Comprueba que el horario nuevo esté libre. El vendedor lo confirma con un botón. Una cita por propuesta.',
    {nombre:{type:'string',maxLength:80,description:'Palabras del nombre de la persona o del título de la cita. Vacío si se identifica solo por día y hora.'},
      diaActual:{type:'string',maxLength:10,description:'Día AAAA-MM-DD en que está hoy la cita, o vacío si el nombre basta.'},
      horaActual:{type:'string',maxLength:5,description:'HH:MM en 24 horas de la cita actual, o vacío.'},dia:day,
      hora:{type:'string',maxLength:5,description:'Hora nueva, HH:MM en 24 horas.'},
      minutos:{type:'integer',minimum:0,maximum:240,description:'Duración nueva, o 0 para conservar la que tiene.'}}),
  tool('preparar_borrado_cita','cancel','Prepara, sin borrar nada, la eliminación de una cita que el Coach creó en «Seguimientos TEC». El vendedor la confirma con un botón. Una cita por propuesta.',
    {nombre:{type:'string',maxLength:80,description:'Palabras del nombre de la persona o del título de la cita. Vacío si se identifica solo por día y hora.'},
      diaActual:{type:'string',maxLength:10,description:'Día AAAA-MM-DD de la cita, o vacío si el nombre basta.'},
      horaActual:{type:'string',maxLength:5,description:'HH:MM en 24 horas de la cita, o vacío.'}}),
  tool('leer_agenda_hoja','tecagenda','Lee la pestaña Agenda de la Hoja TEC del vendedor, donde el telemarketer anota las citas que agenda: fecha, hora, cliente, origen, notas del telemarketer y resultado. Indica si cada cita ya está en su calendario «Seguimientos TEC». Las fechas son AAAA-MM-DD.',
    {desde:day,hasta:day}),
  tool('preparar_resultado_cita','result','Prepara, sin guardar, el resultado de una cita de la Agenda de la Hoja TEC: resultado, monto de la venta, referidos obtenidos y una nota del vendedor. El vendedor lo guarda con un botón.',
    {ref:{type:'string',maxLength:8,description:'Referencia «ref» de la cita, tal como llegó de leer_agenda_hoja.'},
      resultado:{type:'string',enum:['Venta','Demo sin venta','No estaba','Canceló','Reagendar','Pendiente']},
      venta:{type:'number',minimum:0,maximum:100000,description:'Monto vendido en dólares, o 0 si no hubo venta.'},
      referidos:{type:'integer',minimum:-1,maximum:200,description:'Referidos que sacó en la visita, o -1 si no lo dijo.'},
      nota:{type:'string',maxLength:600,description:'Nota corta del vendedor sobre la visita, o vacío.'}}),
  tool('leer_resumen','summary','Lee el Resumen de la Hoja TEC: leads nuevos, citas, demos, ventas, porcentaje de cierre, total vendido, referidos por demo, de esta semana, este mes y el mes pasado, y la tabla de cada telemarketer de este mes. Úsala para «¿cómo voy?» o «¿cómo va mi telemarketer?».'),
  tool('leer_mi_dia','myday','Lee «Mi día» del vendedor: su meta de leads de la semana (la estándar de TEC es 100, entre leads y referencias, y la personal si la puso), lo que lleva hoy y esta semana (leads, llamadas, demos y ventas), sus citas de hoy, sus exhibidores próximos y qué le conviene hacer ahora. Úsala para «¿qué hago ahora?», «¿cómo voy con mi meta?» o después de que cuente el resultado de una demo.'),
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
export const markerPattern = /\[{0,2}\b(tel|dir|correo|min):([a-f0-9]{6})\b\]{0,2}/g;

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
// routes: minutos de manejo de Google Maps, dentro del cupo gratuito que administra coach-route-allowance.js.
export function createAgentTools({config,calendar,store,gmail,syncSales,sheets,sheetTools,contacts,readConnections,channels,routes,myDay=null}) {
  const inList = (list,email) => Boolean(list?.some(item=>item==='*' || item===email));

  // actor: {owner,session,email,name,zone,current,now}. refs conserva las referencias entre turnos.
  // turn recoge lo leído en este mensaje que el chat necesita para sus botones.
  return function bind(actor, refs={}, turn={}) {
    const email=String(actor.email || '').toLowerCase();
    const now=()=>typeof actor.now==='function' ? actor.now() : Date.now();
    const stamp=value=>localStamp(value,actor.zone);
    // Guarda el dato real para este turno y devuelve el marcador que verá el modelo.
    // `person` es el nombre de la ficha o del contacto: el chat lo usa para el botón «Llamar a …».
    const marker=(kind,id,value,person='')=>{
      const token=kind+':'+createHash('sha256').update(kind+'\n'+id+'\n'+value).digest('hex').slice(0,6);
      (turn.secrets ||= {})[token]=value;
      if (person) (turn.labels ||= {})[token]=clean(person,80);
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
      telefono:c.noCalls ? 'no llamar: la ficha pide no ser contactada' : c.rawPhone ? marker('tel',c.identityKey,clean(c.rawPhone,40),c.fullName) : 'sin teléfono en la hoja',
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

    // La cita que el vendedor quiere mover o borrar, entre las que el Coach creó. Debe quedar una sola.
    const tecMissing={estado:'no_disponible',nota:'La hoja conectada no es la Hoja TEC, así que no tiene Agenda ni Resumen. El vendedor puede crearla en Mis conexiones con «Crear mi Hoja TEC». Explícaselo así.'};
    const agendaRef=r=>{const ref='c'+createHash('sha256').update('agenda\n'+r.row+'\n'+r.client+'\n'+r.date).digest('hex').slice(0,5);delete refs[ref];refs[ref]='agenda:'+r.row+':'+r.key;return ref;};

    async function ownAppointment(op,nombre,diaActual,horaActual) {
      if ((diaActual && civil(diaActual)==null) || (horaActual && !clock(horaActual))) return {problem:invalid('Usa el día AAAA-MM-DD y la hora HH:MM de la cita actual, o déjalos vacíos.')};
      if (!nombre.trim() && !diaActual) return {problem:invalid('Indica el nombre de la persona o el día de la cita.')};
      const state=plan();
      if (state.changed && state.draft.steps[op]) return {problem:invalid('Una cita por propuesta. Deja lista esta y, cuando el vendedor la confirme o la cierre, prepara la siguiente.')};
      const blocked=enter(op,null);
      if (blocked) return {problem:invalid(blocked)};
      let found;
      try { found=await channels.findAppointments(actor,{name:nombre,day:diaActual,time:horaActual}); }
      catch (error) { return {problem:notPrepared(error,'calendar')}; }
      const citas=found.matches.slice(0,8).map(row=>({titulo:clean(row.summary,130),dia:row.local.day,hora:row.local.time}));
      if (!citas.length) return {problem:{estado:'sin_coincidencia',nota:'No hay una cita así entre las que el Coach creó en «Seguimientos TEC». El Coach solo puede mover o borrar las citas que él mismo creó: las del calendario principal y las que se hicieron a mano en Google Calendar las cambia el vendedor en su calendario. Explícaselo así, sin decir que falta una conexión.'}};
      if (found.matches.length>1) return {problem:{estado:'varias_citas',citas,nota:'Pregunta al vendedor cuál de estas es y repite con el «diaActual» y la «horaActual» de esa cita.'}};
      return {found,target:found.matches[0]};
    }

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

      async leads_por_distancia({origen,zonas,tipo,maximo}) {
        const origin=clean(origen,120), zones=zonas.map(zone=>zone.trim()).filter(Boolean);
        if (!origin || !zones.length) return invalid('Indica el lugar de salida y las zonas candidatas del catálogo de la hoja.');
        const fallback='Usa leads_por_zona y di que la cercanía es aproximada por ciudad.';
        if (!routes?.enabled) return {estado:'no_disponible',nota:'La medición con Google Maps no está activada este mes. '+fallback};
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const result=await sheetTools.byZone(ctx,{zones,today:sheetsToday(now(),actor.zone),kind:kinds[tipo],limit:8});
        if (!await ctx.isCurrent()) return {estado:'fallo',nota:'La conexión de la hoja cambió durante la consulta.'};
        // Solo se mide una dirección con calle y número: una ciudad sola daría una precisión que no existe.
        const located=result.contacts.filter(c=>/\d/.test(c.address.split(',')[0]));
        const measured=await Promise.all(located.map(async c=>{
          try { return {c,minutes:await routes.minutes(origin,c.address,now())}; }
          catch (error) { return {c,error}; }
        }));
        const timed=measured.filter(item=>item.minutes!=null).sort((a,b)=>a.minutes-b.minutes || a.c.row-b.c.row);
        if (!timed.length) {
          if (measured.some(item=>item.error?.freeAllowance)) return {estado:'no_disponible',nota:'Se agotó el cupo gratuito de Google Maps de este mes. '+fallback};
          return {estado:'no_disponible',nota:(located.length ? 'Google Maps no reconoció el lugar de salida o las direcciones de esas fichas. Pide al vendedor una dirección o un cruce de calles con ciudad y estado, o ' : 'Las fichas de esas zonas no tienen calle y número. ')+fallback};
        }
        return {estado:'ok',origen:origin,fichasEnEsasZonas:result.total,medidas:timed.length,sinMedir:result.contacts.length-timed.length,
          nota:'Están ordenadas de menos a más minutos de manejo con el tráfico de ahora, según Google Maps. El tiempo llega como marcador en «manejo»: cópialo. Solo se midieron las fichas con calle y número; si quedaron fichas sin medir o fuera de las ocho revisadas, dilo. '+markerNote,
          fichas:timed.slice(0,maximo).map(({c,minutes})=>({...ficha(c,240),manejo:marker('min',c.identityKey+'\n'+origin,minutes+' min en auto')}))};
      },

      async priorizar_llamadas({tipo}) {
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const ranked=await sheetTools.nextCall(ctx,{today:sheetsToday(now(),actor.zone),kind:kinds[tipo],shortlist:true});
        if (!await ctx.isCurrent()) return {estado:'fallo',nota:'La conexión de la hoja cambió durante la consulta.'};
        return {estado:'ok',pendientesConTelefono:ranked.total,orden:'vencidos, de hoy, sin fecha y primeros contactos',nota:markerNote,
          recomendados:ranked.contacts.map(c=>({...card(c),...(c.phone && {telefono:marker('tel',c.identityKey,clean(c.phone,40),c.fullName)})}))};
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
            telefonos:c.phones.slice(0,4).map(value=>marker('tel',c.resourceName,clean(value,40),c.name)),
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

      async preparar_cambio_cita({nombre,diaActual,horaActual,dia,hora,minutos}) {
        if (civil(dia)==null || !clock(hora)) return invalid('Usa el día nuevo AAAA-MM-DD y la hora nueva HH:MM en 24 horas.');
        const {found,target,problem}=await ownAppointment('move',nombre,diaActual,horaActual);
        if (problem) return problem;
        let made;
        try { made=await channels.moveAppointment(actor,found,target,dia+' a las '+hora+(minutos ? ' durante '+minutos+' minutos' : '')); }
        catch (error) { return notPrepared(error,'calendar'); }
        if (!made.proposal) return {estado:'no_preparado',motivo:clean(made.question),nota:'No se preparó el cambio. Si el horario está ocupado, busca horarios libres y propón otro.'};
        put('move',{preview:made.preview,proposal:made.proposal,whenText:made.whenText},'Mover «'+target.summary+'» del '+target.local.day+' '+target.local.time+' al '+dia+' '+hora);
        return {estado:'preparado',cita:clean(target.summary,130),antes:target.local.day+' '+target.local.time,cuando:clean(made.whenText,120),nota:pendingNote};
      },

      async preparar_borrado_cita({nombre,diaActual,horaActual}) {
        const {found,target,problem}=await ownAppointment('cancel',nombre,diaActual,horaActual);
        if (problem) return problem;
        let made;
        try { made=await channels.cancelAppointment(actor,found,target); }
        catch (error) { return notPrepared(error,'calendar'); }
        put('cancel',{preview:made.preview,proposal:made.proposal},'Borrar «'+target.summary+'» del '+target.local.day+' '+target.local.time);
        return {estado:'preparado',cita:clean(target.summary,130),cuando:target.local.day+' '+target.local.time,nota:pendingNote};
      },

      async leer_agenda_hoja({desde,hasta}) {
        if (civil(desde)==null || civil(hasta)==null || civil(hasta)<civil(desde)) return invalid('Usa fechas AAAA-MM-DD, con «hasta» igual o posterior a «desde».');
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const data=await sheetTools.agenda(ctx,{from:desde,to:hasta});
        if (!await ctx.isCurrent()) return {estado:'fallo',nota:'La conexión de la hoja cambió durante la consulta.'};
        if (!data) return tecMissing;
        const today=sheetsToday(now(),actor.zone);
        const citas=[];
        for (const r of data.rows.slice(0,25)) {
          let enCalendario='sin revisar';
          if (channels?.findAppointments && r.date && r.date>=today) {
            try { enCalendario=(await channels.findAppointments(actor,{name:r.client,day:r.date})).matches.length ? 'sí' : 'no'; } catch { enCalendario='sin revisar'; }
          }
          citas.push({ref:agendaRef(r),cliente:clean(r.client,130),dia:r.date || 'sin fecha',hora:r.time || 'sin hora',telemarketer:clean(r.telemarketer,80),
            origen:clean(r.origin,40),notasTelemarketer:clean(r.notes,300),resultado:clean(r.result,40) || 'sin resultado',
            ...(r.sale!=='' && {venta:r.sale}),...(r.referrals!=='' && {referidos:r.referrals}),enCalendario,
            telefono:r.phone ? marker('tel','agenda:'+r.row,clean(r.phone,40),r.client) : 'sin teléfono',
            direccion:r.address ? marker('dir','agenda:'+r.row,clean(r.address,240)) : 'sin dirección'});
        }
        return {estado:'ok',zona:actor.zone,total:data.rows.length,...(data.rows.length>25 && {omitidas:data.rows.length-25}),
          nota:'Una cita «nueva» es la que no tiene resultado y todavía no está en el calendario. Para pasarla al calendario usa preparar_cita con el nombre del cliente, su día y su hora (60 minutos, tipo demo). Para anotar cómo le fue usa preparar_resultado_cita con su ref. '+markerNote,citas};
      },

      async preparar_resultado_cita({ref,resultado,venta,referidos,nota}) {
        const saved=/^c[a-f0-9]{5}$/.test(ref) && Object.hasOwn(refs,ref) ? /^agenda:(\d+):(.+)$/.exec(refs[ref]) : null;
        if (!saved) return invalid('Esa referencia no existe. Lee primero la agenda con leer_agenda_hoja y usa la «ref» de la cita.');
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const blocked=enter('result',null);
        if (blocked) return invalid(blocked);
        if (plan().changed && plan().draft.steps.result) return invalid('Un resultado por propuesta. Cuando el vendedor lo confirme, prepara el siguiente.');
        let made;
        try { made=await sheetTools.prepareResult(ctx,{row:Number(saved[1]),key:saved[2],result:resultado,sale:resultado==='Venta' && venta>0 ? venta : null,
          referrals:referidos>=0 ? referidos : null,note:clean(nota,600)}); }
        catch (error) { if (error?.status===409) return {estado:'no_preparado',nota:'La cita cambió en la hoja. Vuelve a leer la agenda.'}; throw error; }
        if (!await ctx.isCurrent()) return {estado:'fallo',nota:'La conexión de la hoja cambió durante la preparación.'};
        put('result',{preview:made.preview,proposal:made.proposal},'Resultado de '+made.proposal.client+': '+resultado);
        plan().draft.sheetGrant=ctx.grant;
        return {estado:'preparado',cliente:clean(made.proposal.client,130),nota:pendingNote};
      },

      async leer_resumen() {
        const {ctx,problem}=await sheetAccess();
        if (problem) return problem;
        const data=await sheetTools.summary(ctx);
        if (!await ctx.isCurrent()) return {estado:'fallo',nota:'La conexión de la hoja cambió durante la consulta.'};
        if (!data) return tecMissing;
        return {estado:'ok',periodos:data.periods,desde:data.range.desde,hasta:data.range.hasta,numeros:data.metrics,telemarketersEsteMes:data.telemarketers,
          nota:'Son los números que calcula la hoja con lo que anotaron el vendedor y los telemarketers. Contesta con los más útiles y compáralos con el mes pasado; no inventes otros.'};
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

      async leer_mi_dia() {
        if (!myDay) return {estado:'no_disponible',nota:'Mi día no está disponible en TEC en este momento.'};
        const s=await myDay.state({owner:actor.owner,email,name:actor.name,zone:actor.zone,session:actor.session});
        const meta=s.goal.personal || s.goal.standard;
        return {estado:'ok',hoy:s.day,hora:s.time,
          meta:{estandarSemanal:s.goal.standard,personalSemanal:s.goal.personal || 'no puso una',leadsEstaSemana:s.goal.weekLeads,faltan:s.goal.remaining,
            metaDeHoy:s.goal.dailyTarget,diasDeTrabajoQueQuedan:s.goal.workDaysLeft,metaQueSeMide:meta},
          numerosDeHoy:{leads:s.today.leads,llamadas:s.today.calls,demos:s.today.demos,ventas:s.today.sales,montoVendido:s.today.amount},
          numerosDeLaSemana:{leads:s.week.leads,llamadas:s.week.calls,demos:s.week.demos,ventas:s.week.sales,montoVendido:s.week.amount},
          citasDeHoy:[...s.citas.events.map(e=>({hora:e.time,titulo:e.title})),...s.citas.agenda.map(a=>({hora:a.time,cliente:a.client,origen:a.origin,deLaAgenda:true}))],
          exhibidoresProximos:s.exhibitors.slice(0,5).map(e=>({dia:e.day,desde:e.start,hasta:e.end,lugar:e.place})),
          ...(s.askExhibitor && {sinExhibidorEstaSemana:'No tiene exhibidor registrado esta semana: pregúntale por qué y sugiérele agregarlo en Mi día.'}),
          recomendacion:s.recommendation.reason,
          nota:'Mi día está en la pestaña «Mi actividad» de la app: ahí elige atender citas, llamar (bloques de 5), prospectar o terminar. Termina tu respuesta preguntando si va a atender otra cita, llamar, prospectar o si ya terminó por hoy.'};
      },

      async pasar_al_coach({motivo}) { return {estado:'derivado',detener:motivo}; }
    };

    return async (name,args) => {
      if (!Object.hasOwn(handlers,name)) return {estado:'herramienta_desconocida'};
      const source=agentTools.find(t=>t.name===name).source;
      const connection=({agenda:'calendar',slots:'calendar',appointment:'calendar',move:'calendar',cancel:'calendar',inbox:'gmail',thread:'gmail',sales:'gmail',
        referrals:'sheets',distance:'sheets',calls:'sheets',note:'sheets',add:'sheets',sms:'sheets',tecagenda:'sheets',result:'sheets',summary:'sheets',contacts:'contacts'})[source];
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
