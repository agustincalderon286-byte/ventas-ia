import {createHash} from 'node:crypto';
import {openToken,sealToken} from './coach-hycite-gmail.js';
import {readFollowup,resolveInstant,validTimeZone} from './coach-followup-input.js';
import {saleScope} from './coach-sheets-agent.js';
import {fold,otherRequest,routeAnswer,routeDetails,routeIntent,usablePlace} from './coach-route-input.js';
import {createTrafficRoutes,estimateItinerary,routeMapLink} from './coach-route-google.js';

const minute=60000, dayLength=86400000;
const connections={type:'connections',label:'Mis conexiones'};
const reply=(respuesta,actions=[],pending='')=>({mode:'coach',coachMailAgent:true,coachRouteAgent:true,respuesta,coachHelpActions:actions,coachRoutePending:pending});
const direct=auth=>Boolean(auth?.user?._id && auth.session?.tokenHash &&
  (auth.session.accessMode==='direct' || (!auth.session.accessMode && auth.user.accountType!=='seat')));
const allowed=(list,email)=>list?.some(item=>item==='*' || item===email);
const clockAt=(time,zone)=>new Intl.DateTimeFormat('es-US',{timeZone:zone,hour:'numeric',minute:'2-digit'}).format(time);
const dayAt=(time,zone)=>new Intl.DateTimeFormat('es-US',{timeZone:zone,weekday:'long',month:'short',day:'numeric'}).format(time);
function atClock(clock, ms, zone) {
  if (!clock) return true;
  const parts = new Intl.DateTimeFormat('en-GB',{timeZone:zone,hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(ms);
  const part=key=>Number(parts.find(p=>p.type===key)?.value);
  return part('minute')===clock.minute && (clock.ambiguous ? part('hour')%12===clock.ambiguous%12 : part('hour')===clock.hour);
}
function manualTravelTimes(text,count) {
  const normalized=fold(text).replace(/^(?:maps|google maps|el mapa) (?:dice|marca|muestra)\s*/,'').replace(/\b(?:el primero|el segundo|minutos?|mins?|y|primero|segundo|son|despues|luego|de ida|de ahi)\b/g,' ').trim();
  if (!/^[\d\s,;.+/-]+$/.test(normalized)) return null;
  const values=normalized.match(/\d+/g)?.map(Number);
  return values?.length===count && values.every(n=>n>0 && n<=1440) ? values : null;
}

export function createCoachRouteAgent({config,calendar,switched,sheets,sheetsGoogle,readSession,now=Date.now,routes=createTrafficRoutes(config,{now})}) {
  return async ({req,auth,sessionId,text})=>{
    const b=req.body||{}, previous=b.coachRoutePending, active=previous && b.coachQuestionFocus==='route';
    const requested=routeIntent(text);
    if (!requested && (!active || otherRequest(text) || /^(?:¿\s*)?(?:qui[eé]n|por qu[eé]|a qui[eé]n|qu[eé] (?:hace|es|puedo)|c[oó]mo (?:funciona|puedo))\b/iu.test(text.trim()))) return null;
    if (!direct(auth) || !sessionId || !allowed(config.planPilot,String(auth.user.email || '').toLowerCase()) ||
      b.accountId!==String(auth.user._id) || switched(req)) return reply('Abre tu propia cuenta del piloto para revisar visitas y trayectos.');
    const owner=auth.user._id, zone=validTimeZone(b.coachTimeZone), timestamp=now();
    if (!zone) return reply('Abre el Coach con una zona horaria válida para comparar tu agenda y el trayecto.');
    const current=async()=>{
      const actor=readSession ? await readSession(req) : auth;
      return direct(actor) && !switched(req) && String(actor.user._id)===String(owner) && actor.session.tokenHash===auth.session.tokenHash;
    };
    try {
      const conn=await calendar.getConnection(owner);
      if (!conn?.grant || !conn.availabilityEnabled) return reply('Conecta tu Google Calendar y activa horarios libres en Mis conexiones para que pueda revisar el calendario principal y Seguimientos TEC.',[connections]);
      const binding='route:'+createHash('sha256').update(JSON.stringify([String(owner),auth.session.tokenHash,sessionId,conn.grant,conn.calendarId,zone])).digest('hex');
      let state={};
      if (active) {
        try {
          if (typeof previous!=='string' || previous.length>10000) throw Error();
          const data=JSON.parse(openToken(previous,config.key,binding));
          if (data.until<=timestamp) throw Error();
          state=data.state;
        } catch {return reply('Esa consulta de ruta venció o la conexión cambió. Pídeme el trayecto de nuevo.');}
        if (/^(?:cancela(?:r)?|descarta(?:r)?|olvidalo|ya no|no gracias)[.!]?$/.test(fold(text))) return reply('Cerré la consulta de ruta.');
      }
      const ask=(field,msg,actions=[])=>reply(msg,actions,sealToken(JSON.stringify({state:{...state,waiting:field},until:timestamp+20*minute}),config.key,binding));
      const ctx={timeZone:zone,now:timestamp};
      if (active && !requested && !routeAnswer(text,state.waiting,ctx)) return null;
      routeDetails(text,state,ctx);
      const today=readFollowup('hoy',{kind:'availability'},ctx).state.day;
      state.day ??= today;
      const midnight=resolveInstant(state.day,0,0,zone), nextDay=resolveInstant(state.day+dayLength,0,0,zone);
      if (!midnight || !nextDay || nextDay<=timestamp) return ask('day','¿Qué día futuro o de hoy quieres revisar?');
      const calendarParams={owner,grant:conn.grant};
      const events=await calendar.routeAppointments({...calendarParams,start:new Date(midnight).toISOString(),end:new Date(state.next ? resolveInstant(state.day+7*dayLength,0,0,zone) : nextDay).toISOString()});
      const upcoming=events.filter(e=>Date.parse(e.start)>timestamp && atClock(state.demoClock,Date.parse(e.start),zone));
      const demos=upcoming.filter(e=>/\b(?:demo(?:stracion)?|cita|visita)\b/.test(fold(e.title)));
      const choices=state.manualDemo ? [] : (demos.length?demos:upcoming);
      let selected=state.eventId && choices.find(e=>e.id===state.eventId);
      if (state.eventId && !selected) {
        delete state.eventId;delete state.choices;
        return ask('day','La cita cambió en Google Calendar. ¿Para qué día quieres volver a revisar la ruta?');
      }
      if (!selected && state.waiting==='choose') {
        const ordinal=fold(text).match(/^(?:la |opcion )?([1-5]|primera|segunda|tercera|cuarta|quinta)(?:[.!])?$/)?.[1];
        const index=Number(ordinal) || ['','primera','segunda','tercera','cuarta','quinta'].indexOf(ordinal);
        if (index>0) selected=choices.find(e=>e.id===state.choices?.[index-1]);
      }
      if (state.waiting==='choose' && /^(?:otra|ninguna)(?: cita| demo)?[.!]?$/i.test(text.trim())) state.manualDemo=true;
      if (!state.manualDemo && !selected && demos.length && (choices.length===1 || state.next)) selected=choices[0];
      if (!state.manualDemo && !selected && choices.length) {
        state.choices=choices.slice(0,5).map(e=>e.id);
        return ask('choose','¿Para cuál cita de Seguimientos TEC calculo el trayecto? Puedes decir el número o precisar la hora.\n'+
          choices.slice(0,5).map((e,i)=>(i+1)+'. '+e.title+' · '+dayAt(Date.parse(e.start),zone)+' '+clockAt(Date.parse(e.start),zone)).join('\n')+'\nSi es otra demo, escribe «otra».');
      }
      if (selected) {
        state.eventId=selected.id;
        state.day=readFollowup('hoy',{kind:'availability'},{timeZone:zone,now:Date.parse(selected.start)}).state.day;
        delete state.next;
      }
      if (!selected && (!state.demoClock || state.demoClock.ambiguous)) {
        const hint=state.demoClock?.ambiguous?'¿La demo es por la mañana o por la tarde? Dime la hora, por ejemplo «5 pm».':'¿Qué día y a qué hora es la demo?';
        return ask('demoTime',hint+' En el calendario principal solo puedo ver los horarios ocupados.');
      }
      const deadline=selected ? Date.parse(selected.start) : resolveInstant(state.day,state.demoClock.hour,state.demoClock.minute,zone);
      if (!deadline || deadline<=timestamp) return ask('demoTime','Dime un día y una hora futura sin ambigüedad para la demo.');
      const target=state.destination || (usablePlace(selected?.location) && selected.location);
      if (!target) return ask('destination','¿Cuál es la dirección de la demo de las '+clockAt(deadline,zone)+'? Incluye calle, número y ciudad, o el nombre del lugar y la ciudad.');
      if (!state.origin) return ask('origin','¿Desde qué dirección vas a salir en auto? Incluye calle, número y ciudad.');
      if (state.visit && state.stopName && sheets && sheetsGoogle && (state.sheetStop || !state.stop && !state.lookedUp) && allowed(config.sheetsPilot,String(auth.user.email).toLowerCase())) {
        state.lookedUp=true;
        try {
          const priorGrant=state.sheetGrant, fromSheet=state.sheetStop;
          if (fromSheet) {delete state.stop;delete state.sheetStop;}
          const {token,grant,...workbook}=await sheets.access(owner);
          if (fromSheet && priorGrant!==grant) return reply('La conexión de la hoja cambió. Vuelve a pedir el trayecto.');
          const ok=async()=>await current() && await sheets.current(owner,grant);
          if (await ok()) {
            const data=await sheetsGoogle.readFollowups(token,workbook,{forRoute:true});
            const words=fold(state.stopName).split(/\s+/);
            const hits=data.rows.filter(row=>saleScope(row,auth.user.name) && words.every(w=>fold(row.fullName).split(/[^\p{L}\p{N}]+/u).includes(w)));
            if (hits.length===1 && hits[0].address && await ok()) {
              const place=[hits[0].address,hits[0].city].filter(Boolean).join(', ').trim();
              if (usablePlace(place)) {state.stop=place;state.sheetGrant=grant;state.sheetStop=true;}
            } else if (hits.length>1) {
              return ask('stop','Hay varias personas que coinciden con '+state.stopName+'. ¿En qué dirección es tu visita?');
            }
          }
        } catch { /* Ask for the address if the optional Sheets connection is unavailable. */ }
      }
      if (state.visit && !state.stop) return ask('stop','¿En qué dirección visitarías '+(state.stopName?'a '+state.stopName:'al otro cliente')+'? Incluye la ciudad.');
      if (state.visit && !state.visitMinutes) return ask('duration','¿Cuántos minutos planeas quedarte con ese cliente antes de seguir a la demo?');
      if (state.startClock?.ambiguous) return ask('departure','¿A qué hora sales, por la mañana o por la tarde? Por ejemplo «3 pm».');
      const fixed=Boolean(state.startClock);
      const earliest=fixed ? resolveInstant(state.day,state.startClock.hour,state.startClock.minute,zone) : Math.max(resolveInstant(state.day,0,0,zone),Math.ceil((timestamp+1000)/minute)*minute);
      if (!earliest || earliest<timestamp || earliest>=deadline) return ask('departure','¿Desde qué hora futura y anterior a la demo puedes salir?');
      if (!await current() || (state.sheetStop && !await sheets.current(owner,state.sheetGrant))) return reply('La cuenta o la hoja de la visita cambió. Vuelve a pedir el trayecto.');
      const map=routeMapLink(state.origin,target,state.visit && state.stop);
      const blocks=await calendar.availability({...calendarParams,start:new Date(earliest).toISOString(),end:new Date(deadline).toISOString()});
      const last=blocks.reduce((ms,block)=>Math.max(ms,Date.parse(block.end)),earliest);
      if (last>=deadline || (fixed && last>earliest)) {
        return reply('Hay otro compromiso en tu calendario principal o en Seguimientos TEC entre la salida y la demo. Con esa agenda no puedo asegurar que llegues. Revisa el horario de salida o la cita.',[map]);
      }
      if (state.waiting==='manual') {
        const manual=manualTravelTimes(text,state.visit?2:1);
        if (manual) state.manual=manual;
      }
      const askTimes=message=>ask('manual',message+' Abre el trayecto en Maps y dime '+(state.visit?'los dos tiempos de manejo en minutos, por ejemplo «12 y 18».':'el tiempo de manejo en minutos, por ejemplo «25 minutos».') ,[map]);
      if (!routes.enabled && !state.manual) return askTimes('El cálculo automático de tráfico todavía no está activo con un cupo gratuito verificado.');
      let result;
      try {
        const minutes=state.manual ? async(_from,_to,_at,i)=>state.manual[i] : (...args)=>routes.minutes(...args);
        result=await estimateItinerary({start:last,deadline,origin:state.origin,stop:state.visit && state.stop,destination:target,
          visitMinutes:state.visit ? state.visitMinutes : 0,fixed,minutes});
      } catch(error) {
        if (error.partialAddress) return askTimes('Google encontró solo parte de una dirección. Verifica los destinos en el mapa.');
        return askTimes(error.freeAllowance?'El cupo gratuito para tráfico se agotó o no se pudo verificar.':'Google Maps no pudo confirmar el tráfico para ese trayecto.');
      }
      if (!await current() || (state.sheetStop && !await sheets.current(owner,state.sheetGrant))) return reply('La cuenta o la hoja de la visita cambió. Pídeme una nueva consulta.');
      const fmt=time=>clockAt(time,zone);
      const fits=result.arrival<=deadline;
      const outcome=state.visit ? (fits?'Sí cabe la visita.':'Con esos tiempos, la visita no cabe en el intervalo libre anterior a la demo.') : fits?'Puedes llegar a tiempo.':'La salida queda demasiado tarde para la demo.';
      const info='\n'+dayAt(deadline,zone)+' ('+zone+') · '+(selected?selected.title:'Demo que indicaste')+' a las '+fmt(deadline)+' en '+target+'.';
      const route='\nSalida de '+state.origin+' a las '+fmt(result.departure)+'. '+(state.visit?
        'Manejas '+result.first+' min hasta '+state.stop+(state.sheetStop?' (dirección de tu hoja)':'')+', haces la visita de '+state.visitMinutes+' min y manejas '+result.second+' min hasta la demo.':
        'Trayecto de '+result.first+' min en auto.')+' Llegada estimada a las '+fmt(result.arrival)+'.';
      return reply(outcome+info+route+'\nRevisé la disponibilidad del calendario principal y de Seguimientos TEC. '+
        (state.manual?'Usé los minutos que me indicaste.':'Tiempos estimados con tráfico de Google Maps.'),[map]);
    } catch(error) {
      return reply(error?.reauthorize?'Google pide volver a autorizar Calendar en Mis conexiones.':'No pude verificar las citas y la disponibilidad con Calendar. Vuelve a intentar la consulta.',[connections]);
    }
  };
}
