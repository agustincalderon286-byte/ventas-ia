import {createHash} from 'node:crypto';
import {interpretCoachPlan,interpretPilotPlan} from './coach-plan-language.js';
import {describeAgentConnections,offlineRead,sellerAgentModel} from './coach-agent-pilot.js';
import {excludedReads} from './coach-plan-responses.js';
import {validTimeZone,readFollowup} from './coach-followup-input.js';
import {salePeriod} from './coach-postsale.js';
import {sheetsToday} from './coach-sheets-followup-input.js';

const labels={note:'Guardar nota',add:'Agregar prospecto',appointment:'Confirmar cita',email:'Enviar correo'};
const titles={note:'Nota en Sheets',add:'Alta en Sheets',appointment:'Cita personal',email:'Correo'};
const direct=auth=>!!(auth?.user && auth.session?.tokenHash) && (auth.session.accessMode==='direct' || (!auth.session.accessMode && auth.user.accountType!=='seat'));
const hash=value=>createHash('sha256').update(value).digest('hex');
const reply=(respuesta,revision='',actions=[])=>({mode:'coach',coachMailAgent:true,coachPlanAgent:true,coachPlanPending:revision,coachHelpActions:actions,respuesta});
const initial=()=>({ops:[],fields:{},steps:{},choices:[],waiting:'',questions:{},calendarState:{},options:[],selection:[]});
const stale='Esta propuesta cambió o caducó. Revisa la versión actual antes de confirmar.';
const clean=v=>String(v||'').replace(/[\p{Cc}\p{Cf}]+/gu,' ').trim().slice(0,130);
const userError=e=>e.publicMessage || ([400,403,409,422].includes(e.status)?e.message:'No pude completar esta operación. Vuelve a revisarla.');
const writable=Object.keys(labels);
const canExecute=(s,k)=>['ready','retry'].includes(s.steps[k]?.status) && (k!=='email' || s.sendEmail);
const command=/^TECPLAN ([a-f0-9]{24}) (apply|slot|contact|close|page) ([a-z0-9,]+)$/;

function view(s,revision,prefix='') {
  const lines=prefix?[prefix]:[], buttons=[];
  const button=(op,arg,label)=>buttons.push({type:'planAction',revision,op,arg,label});
  if (s.listTitle) lines.push(s.listTitle);
  if (s.choices.length) {
    for (const [i,c] of s.choices.entries()) {
      lines.push((i+1)+'. '+clean(c.fullName)+(c.host?' · referido por '+clean(c.host):'')+
        ' ('+clean(c.sheetTitle)+', fila '+c.row+').'+(c.reason?' '+c.reason:''));
      button('contact',String(i+1),'Elegir '+(i+1)+' · '+clean(c.fullName));
    }
  }
  for (const page of s.pages || []) button('page',page.arg,page.label);
  if (s.chosen) {
    lines.push('Prospecto de la hoja: '+clean(s.chosen.fullName)+' ('+clean(s.chosen.sheetTitle)+', fila '+s.chosen.row+').');
    if (s.chosen.reason) lines.push(s.chosen.reason);
    if (s.ops.includes('call') || s.ops.includes('postSale')) {
      if (s.chosen.phone) {
        lines.push('Teléfono de la ficha: '+s.chosen.phone+'.');
        buttons.push({type:'planCall',revision,phone:s.chosen.phone,label:'Llamar a '+clean(s.chosen.fullName)});
      }
    }
  }
  for (const k of s.ops) {
    const step=s.steps[k];
    if (step) lines.push(titles[k]+': '+(step.status==='saved'?'✓ '+step.message:step.preview+(step.message?'\n'+step.message:'')));
  }
  lines.push(...Object.values(s.questions).filter(Boolean));
  for (const [i,label] of (s.slotLabels || []).entries()) button('slot',String(i+1),label);
  const selected=s.selection.filter(k=>canExecute(s,k));
  if (selected.length>1) button('apply',selected.join(','),'Confirmar únicamente: '+selected.map(k=>titles[k]).join(' + '));
  for (const k of writable.filter(k=>canExecute(s,k) && (!s.restricted || selected.includes(k)))) button('apply',k,labels[k]);
  if (!s.running) button('close','0','Cerrar propuesta');
  return reply(lines.filter(Boolean).join('\n\n') || '¿Qué te gustaría preparar?',revision,buttons);
}

function merge(s,intent) {
  const newOps=intent.operations || [];
  for (const op of newOps) if (!s.ops.includes(op)) s.ops.push(op);
  for (const field of ['contact','host','bucket','phone','leadKind','to','mailInstruction','note','noteSchedule','schedule']) {
    if (intent[field]) s.fields[field]=intent[field];
  }
  if (intent.schedule) {s.fields.slots=intent.slots;s.options=[];s.slotLabels=[];delete s.slot;}
  if (newOps.includes('email') || intent.mailInstruction || intent.sendEmail) s.sendEmail=!!intent.sendEmail;
  s.selection=[];s.restricted=false;
}
function invalidate(s,k) {
  if (s.steps[k]?.status==='saved' || s.steps[k]?.status==='retry') return;
  delete s.steps[k];
}
function pick(s,c) {
  s.chosen=c;s.fields.contact=c.fullName;s.choices=[];s.pages=[];s.listTitle='';s.waiting='';s.questions={};
}

export function createConversationPlan({config,plans,sheets,sheetTools,channels,readSession,switched,readSales,interpret=interpretCoachPlan,
  pilotInterpret=interpretPilotPlan,readAgenda,readMail,readConnections,now=Date.now}) {
  return async args=>{
    const {req,auth,sessionId,text}=args, body=req.body||{};
    const cmd=command.exec(text), client=body.coachPlanPending;
    if (body.coachPlanVersion!==1) return null;
    const email=String(auth?.user?.email || '').toLowerCase();
    const inPilot=list=>list?.some(x=>x==='*' || x===email);
    const model=readConnections && sellerAgentModel(email,config.agentModel);
    const allowed=(model || inPilot(config.sheetsPilot)) && inPilot(config.planPilot ?? config.sheetsPilot);
    if (!allowed || !direct(auth) || !sessionId || switched(req) || body.accountId!==String(auth.user._id)) return cmd||client?reply('Abre tu sesión personal del piloto para continuar.'):null;
    if (/^(?:confirmar|descartar|cancelar|elegir horario|registrar horarios|enviar whatsapp)\b.*\b[a-f0-9]{12,32}[.!]?$/i.test(text)) return null;
    const owner=auth.user._id, session=hash(auth.session.tokenHash+':'+sessionId), scope=hash(JSON.stringify([String(owner),session]));
    const current=async()=>{const a=await readSession(req);return direct(a) && !switched(req) && String(a.user._id)===String(owner) && a.session.tokenHash===auth.session.tokenHash;};
    const zone=validTimeZone(body.coachTimeZone);
    const actor={owner,session,current,zone,email:auth.user.email};
    let ctxCache;
    const ctx=async()=>{
      if (!ctxCache) {
        if (!inPilot(config.sheetsPilot)) throw Object.assign(new Error('Tu cuenta aún no participa en el piloto de Google Sheets.'),{status:403});
        const {token,grant,...workbook}=await sheets.access(owner);
        ctxCache={owner,session,grant,workbook,access:token,sellerName:auth.user.name,isCurrent:async()=>await current() && await sheets.current(owner,grant)};
      }
      return ctxCache;
    };
    let record,s;
    const save=async(prefix='')=>{
      if (!await current()) return reply('Tu sesión cambió. Abre de nuevo tu Coach.');
      const revision=await plans.save(scope,record?.revision,s);
      return revision?view(s,revision,prefix):reply(stale);
    };
    try {
      record=await plans.load(scope);
      const recovery=cmd?.[2]==='apply' && record?.data?.running?.from===cmd[1] && record.data.running.arg===cmd[3];
      if ((client || cmd) && (!record?.data || (!recovery && (client!==record.revision || cmd && cmd[1]!==record.revision)))) return reply(stale);
      if (!client && !cmd && record?.data?.running) return reply('Todavía se está confirmando una propuesta de esta conversación.');
      s=(client || cmd ? record?.data : null) || initial();
      const verifyBindings=async()=>{
        if (s.sheetGrant && (await ctx()).grant!==s.sheetGrant) throw Object.assign(new Error('Cambiaste la conexión de Sheets. Pídeme una consulta nueva.'),{status:409});
        if (s.salesGrant) await readSales(actor,{knownGrant:s.salesGrant,verifyOnly:true});
      };
      if (cmd || !model) await verifyBindings();
      if (s.running && (!recovery || now()-s.running.at < 90000)) return reply('Hay una confirmación en curso. Espera su resultado antes de modificar la propuesta.',client);
      if (cmd?.[2]==='apply') {
        const keys=cmd[3].split(',');
        if (!recovery && (new Set(keys).size!==keys.length || keys.some(k=>!canExecute(s,k)) ||
            s.restricted && keys.some(k=>!s.selection.includes(k)) || keys.length>1 && keys.join(',')!==s.selection.join(','))) return reply(stale,client);
        if (!await current()) return reply('Abre de nuevo tu sesión del Coach.');
        if (s.chosen?.order && !recovery) {
          const refreshed=await readSales(actor,{knownGrant:s.salesGrant});
          const checked=await sheetTools.postSale(await ctx(),refreshed,{...s.salesRange,zone,today:sheetsToday(now(),zone)});
          if (!checked.linked.some(c=>c.order===s.chosen.order && c.identityKey===s.chosen.identityKey)) {
            return reply('La confirmación o el historial de postventa cambió. Revisa de nuevo antes de guardar.',client);
          }
        }
        s.running={from:cmd[1],arg:cmd[3],at:now()};
        let version=await plans.save(scope,record.revision,s);
        if (!version) return reply(stale);
        for (const k of keys) {
          const step=s.steps[k];
          if (step.status==='saved') continue;
          try {
            if (!await current()) throw Error();
            if (s.chosen?.identityKey && ['appointment','email'].includes(k)) await sheetTools.revalidate(await ctx(),s.chosen.identityKey);
            const actions={note:async()=>sheetTools.confirmNote(await ctx(),step.proposal),add:async()=>sheetTools.confirmAdd(await ctx(),step.proposal),
              appointment:()=>channels.confirmAppointment(actor,step.proposal),email:()=>channels.confirmEmail(actor,step.proposal)};
            const result=await actions[k]();
            step.status=result.status==='saved'?'saved':result.status==='retry'?'retry':'blocked';step.message=result.message;
          } catch(e) {step.status=[400,403,409,422].includes(e.status)||e.publicMessage?'blocked':'retry';step.message=userError(e);}
          version=await plans.save(scope,version,s);
          if (!version) return reply('La confirmación cambió. Comprueba el resultado antes de repetirla.');
          if (step.status!=='saved') break;
        }
        s.running=null;s.selection=s.selection.filter(k=>s.steps[k]?.status!=='saved');
        record={revision:version};return save('Resultado de tu confirmación');
      }
      if (cmd?.[2]==='close') {
        s=initial();const revision=await plans.save(scope,record.revision,s);
        return reply(revision?'Cerré la propuesta.':'La propuesta ya cambió.');
      }
      let intent,slotChanged=false;
      let requestedPage=0, deferredOrdinal=0;
      if (cmd?.[2]==='page') {
        const page=/^(today|overdue|undated|match|sales)([1-9]\d*)$/.exec(cmd[3]);
        if (!page || !s.pages?.some(p=>p.arg===cmd[3])) return reply(stale,client);
        requestedPage=Number(page[2]);
        intent={kind:'revise',operations:[],bucket:['match','sales'].includes(page[1])?'':page[1]};
      } else if (cmd) intent={kind:'revise',operations:[],ordinal:Number(cmd[3])};
      else {
        const context={operations:s.ops,hasSelection:!!s.chosen,hasReview:Object.values(s.steps).some(x=>x.status==='ready'),waiting:s.waiting};
        if (model) {
          context.connections=await readConnections(actor);
          context.lastRead=s.lastRead;
          if (!await current()) return reply('Tu sesión cambió. Abre de nuevo tu Coach.');
          intent=await pilotInterpret(text,{context,model});
          if (['clarify','unavailable','none','unsupported'].includes(intent.kind) && excludedReads(text).size)
            return reply(intent.question || '¿Qué quieres revisar con las demás conexiones?',client);
          if (['unsupported','none','unavailable','clarify'].includes(intent.kind)) {
            const read=offlineRead(text,s.lastRead);
            if (read && (!s.waiting || !client || s.lastRead==='agenda')) intent={kind:read};
          }
          if (intent.kind==='referralCalls') {
            if (!zone) return reply('Necesito tu zona horaria para revisar los seguimientos de hoy.',client);
            const context=await ctx();
            const ranked=await sheetTools.nextCall(context,{today:sheetsToday(now(),zone),kind:'referrals',shortlist:true});
            if (!await context.isCurrent()) return reply('Tu conexión cambió durante la consulta. Vuelve a abrir tu Coach.',client);
            if (!ranked.total) return reply('Revisé tu 4 en 14. No encontré referidos pendientes con teléfono disponible para llamar hoy.',client);
            const title='De tu 4 en 14, te recomiendo llamar en este orden:';
            const choices=ranked.contacts.map(c=>({...c,reason:({overdue:'Seguimiento vencido el '+c.date,today:'Seguimiento para hoy',undated:'Pendiente sin fecha',new:'Primer contacto'})[c.bucket]+'. '+clean(c.reason)}));
            if (s.ops.length && client) return reply([title,...choices.map((c,i)=>(i+1)+'. '+clean(c.fullName)+' · '+c.reason+' (fila '+c.row+').')].join('\n'),client);
            s=initial();s.ops=['call'];s.sheetGrant=context.grant;s.fields.leadKind='referrals';
            s.listTitle=title;s.choices=choices;s.waiting='choice';
            return save();
          }
          if (['agenda','inbox','sales'].includes(intent.kind)) {
            if (!await current()) return reply('Tu sesión cambió. Abre de nuevo tu Coach.');
            if (intent.kind!=='agenda') {
              const result=await readMail(args,intent.kind);
              return await current()?result:reply('La sesión cambió durante la consulta.');
            }
            const result=await readAgenda(actor,text);
            if (!await current()) return reply('La sesión cambió durante la consulta.');
            // A separate day lookup must leave any previous confirmation buttons usable.
            if (s.ops.length) return reply(result,client);
            s.lastRead='agenda';
            const revision=await plans.save(scope,record?.revision,s);
            return reply(result,revision || '');
          }
          if (intent.kind==='capabilities') return reply(describeAgentConnections(context.connections),client,[{type:'connections',label:'Mis conexiones'}]);
          if (intent.kind==='unsupported') {
            if (args.legacyFocus && !client) return null;
            return reply('No encontré una herramienta para completar ese pedido tal como lo planteaste. Estas son las conexiones de tu cuenta:\n'+describeAgentConnections(context.connections),client);
          }
          if (intent.kind==='unavailable' && (client || /\b(?:hoja|referidos|pendientes|boletas|calendario|agenda|gmail|correos?|ventas)\b/i.test(text)) && !args.legacyFocus) {
            return reply('No pude interpretar la petición en este momento. Vuelve a intentarlo.'+(client?' Tu propuesta sigue disponible.':''),client);
          }
          if (['plan','revise','select'].includes(intent.kind)) await verifyBindings();
        } else intent=await interpret(text,{context});
        if (args.legacyFocus && !client && !['plan','capabilities','unsupported'].includes(intent.kind)) return null;
        if (intent.kind==='none' || intent.kind==='unavailable') return client && intent.kind==='unavailable'?reply('No pude interpretar ese cambio. Tu propuesta sigue disponible.',client):null;
        if (intent.kind==='capabilities') return reply('Con tus conexiones del piloto puedo buscar un referido por nombre o anfitrión, priorizar llamadas, revisar tu postventa de Hy Cite, comprobar trayectos entre visitas, buscar o preparar cambios en tus Contactos de Google, preparar un alta o una nota, consultar horarios, agendar una cita personal y redactar un correo. Puedes combinarlo y elegir qué guardar. También puedes corregirme antes de confirmar.',client);
        if (intent.kind==='unsupported' || intent.kind==='clarify') return reply(intent.question || 'Esa petición incluye una operación que aún no tengo. Puedo preparar una nota, un alta, una cita personal o un correo individual.',client);
      }
      if (!zone) return reply('Necesito la zona horaria de tu dispositivo para preparar el seguimiento.',client);
      if (intent.kind==='select') {
        s.sendEmail=intent.select.includes('email') && intent.sendEmail;
        s.selection=intent.select.filter(k=>canExecute(s,k));s.restricted=true;
        return save(s.selection.length?'Revisa los cambios elegidos y confirma con el botón.':'La selección no contiene un cambio listo para confirmar.');
      }
      const freshQueue=intent.operations?.includes('call');
      const freshSales=intent.operations?.includes('postSale');
      if ((freshQueue || freshSales) && Object.values(s.steps).some(step=>['ready','retry'].includes(step.status))) {
        return reply('Tienes cambios por revisar. Confírmalos o cierra esta propuesta antes de pasar al siguiente contacto.',client);
      }
      const callSeen=client ? s.callSeen || [] : [];
      // A new objective starts fresh. A reference to the last displayed list keeps its candidates.
      if (freshQueue || freshSales || intent.kind==='plan' && (intent.bucket || !intent.ordinal && !(client && s.chosen && !intent.contact && !intent.host && !intent.operations.includes('list')))) s=initial();
      if (intent.contact || intent.host) delete s.fields.bucket;
      if (intent.bucket && !intent.contact && !intent.host) {delete s.fields.host;delete s.fields.contact;}
      const oldChoices=s.choices;
      if ((intent.contact || intent.host || intent.ordinal && s.waiting!=='slot') && s.chosen &&
          (intent.contact!==s.chosen.fullName || intent.host)) {
        delete s.fields.to;delete s.fields.mailInstruction;delete s.fields.note;delete s.fields.noteSchedule;s.sendEmail=false;
        if (Object.values(s.steps).some(step=>['saved','retry'].includes(step.status))) return reply('Esta propuesta ya tiene resultados. Cierra la propuesta antes de cambiar de prospecto.',client);
      }
      if (intent.ordinal) {
        if (s.options.length && (cmd?.[2]==='slot' || !cmd && s.waiting==='slot')) {
          const slot=s.options[intent.ordinal-1]; if (!slot) return reply('Elige uno de los horarios mostrados.',client);
          s.slot=slot;s.options=[];s.slotLabels=[];slotChanged=true;
        } else {
          const candidate=oldChoices[intent.ordinal-1];
          if (candidate) pick(s,candidate);
          else if (!cmd && intent.kind==='plan' && intent.operations.includes('list')) deferredOrdinal=intent.ordinal;
          else return reply('Elige un prospecto de la lista que acabo de mostrarte.',client);
        }
      } else if (intent.contact || intent.host || intent.bucket) {
        const candidate=intent.contact && !intent.host && oldChoices.filter(c=>c.fullName.toLocaleLowerCase('es')===intent.contact.toLocaleLowerCase('es'));
        if (candidate?.length===1) pick(s,candidate[0]);
        else {delete s.chosen;s.choices=[];s.pages=[];s.listTitle='';}
      }
      if (s.chosen && (s.ops.includes('call') || s.ops.includes('postSale')) && intent.operations?.includes('appointment') &&
          /\b(?:llam|marc)/i.test(intent.note+' '+text)) s.calendarState.action='llamar';
      const oldFields={...s.fields};
      const priorDraft=intent.kind==='revise' && !intent.contact && !intent.host && !intent.to && !deferredOrdinal
        && (!intent.ordinal || slotChanged) ? s.steps.email?.baseDraft : null;
      // Capture the previous date even if a contact question interrupted the first request.
      if (intent.schedule && oldFields.schedule && s.calendarState.day==null) {
        s.calendarState=readFollowup(oldFields.schedule,{...s.calendarState,kind:oldFields.slots?'availability':'create'},{timeZone:zone,now:now()}).state;
      }
      merge(s,intent);
      if (intent.ordinal || intent.contact || intent.host) for (const k of writable) invalidate(s,k);
      if (intent.note || intent.noteSchedule) invalidate(s,'note');
      if (intent.schedule || slotChanged) invalidate(s,'appointment');
      if (intent.mailInstruction || intent.to || intent.schedule || slotChanged) invalidate(s,'email');
      if (intent.phone || intent.leadKind) invalidate(s,'add');
      s.questions={};s.waiting='';
      const f=s.fields,today=sheetsToday(now(),zone), needsLookup=s.ops.includes('list') || s.ops.includes('note') || !!f.host && !s.ops.includes('add');
      if (freshQueue && !freshSales) {
        const context=await ctx();s.sheetGrant=context.grant;
        const next=await sheetTools.nextCall(context,{today,seen:callSeen,name:f.contact,host:f.host,bucket:f.bucket,kind:f.leadKind});
        if (!next.contact) return reply(next.total?'Ya recorriste los contactos con teléfono pendientes en esta consulta.':'No encontré una ficha pendiente con un teléfono completo en las dos pestañas para esa búsqueda.');
        pick(s,next.contact);s.callSeen=[...callSeen,next.contact.identityKey];
        const label={overdue:'Venció el '+s.chosen.date,today:'Toca hoy',undated:'Pendiente sin fecha'};
        s.listTitle=label[s.chosen.bucket]+'.';
      }
      if (s.ops.includes('postSale') && !s.chosen) {
        if (!readSales) return reply('La consulta de postventa de Hy Cite no está disponible en este momento.');
        const context=await ctx();s.sheetGrant=context.grant;
        const snapshot=await readSales(actor,{knownGrant:s.salesGrant});
        s.salesGrant=snapshot.grant;
        s.salesRange ||= salePeriod(intent.salesPeriod,today);
        const review=await sheetTools.postSale(context,snapshot,{...s.salesRange,zone,today});
        const page=requestedPage || 1,limit=10,offset=(page-1)*limit;
        const matches=review.pending.filter(c=>(!f.contact || c.fullName.toLocaleLowerCase('es').includes(f.contact.toLocaleLowerCase('es'))) &&
          (!f.host || c.host?.toLocaleLowerCase('es').includes(f.host.toLocaleLowerCase('es'))));
        s.choices=matches.slice(offset,offset+limit);
        s.pages=[];
        if (page>1) s.pages.push({arg:'sales'+(page-1),label:'Página anterior'});
        if (matches.length>offset+limit) s.pages.push({arg:'sales'+(page+1),label:'Más ventas pendientes'});
        const lines=['Postventa del '+s.salesRange.from+' al '+s.salesRange.to+'. '+review.count+' ventas confirmadas. '+matches.length+' pendientes con ficha para esta búsqueda.',
          review.completed+' con oferta de demo de uso registrada. '+review.excluded+' excluidas por indicaciones de contacto.'];
        if (review.unmatched.length) lines.push('Sin vínculo comprobable con tu hoja: '+review.unmatched.join(', ')+'. Puedes anotar «Pedido: NÚMERO» en la ficha que hayas verificado.');
        if (review.ambiguous.length) lines.push('Datos contradictorios o varias fichas: '+review.ambiguous.join(', ')+'. Revisa estos pedidos personalmente.');
        if (review.limited) lines.push('La búsqueda de Gmail alcanzó su límite de avisos. La revisión es parcial.');
        if (matches.length>limit) lines.push('Página '+page+' de '+Math.ceil(matches.length/limit)+'.');
        s.listTitle=lines.join(' ');
        if (matches.length===1 && (f.contact || f.host || s.ops.some(op=>writable.includes(op)))) {
          const summary=s.listTitle;pick(s,matches[0]);s.listTitle=summary;
        } else {
          s.waiting=s.choices.length?'choice':'';
          if (s.choices.length) s.questions.contact='Elige una ficha para llamar o preparar su seguimiento.';
          return save();
        }
      }
      if ((s.ops.includes('call') || s.ops.includes('postSale')) && s.chosen && !s.ops.some(op=>writable.includes(op))) {
        s.questions.result='Después de la llamada dime cómo te fue y cuándo quieres volver a marcar.';
        s.waiting='callResult';return save();
      }
      if (needsLookup && !s.chosen) {
        if (!f.contact && !f.host && !f.bucket && s.ops.includes('note')) { s.questions.contact='¿En la ficha de quién guardamos la nota?';s.waiting='contact';return save(); }
        const independentSlots=async()=>{
          if (!model || !f.slots || !f.schedule || !s.ops.includes('appointment')) return;
          try {
            const found=await channels.appointment(actor,{}, {schedule:f.schedule,slots:true});
            s.questions.availability=found.labels?.length ? 'Horarios disponibles ('+actor.zone+'): '+found.labels.join(' o ')+'.' : found.question;
          } catch(error) {s.questions.availability=userError(error);}
        };
        let context,result;
        const group=f.bucket || (!f.contact && !f.host?'today':'');
        const lookup=(bucket,offset=0)=>sheetTools.search(context,{name:f.contact,host:f.host,bucket,today,offset,kind:f.leadKind});
        const page=requestedPage || 1, limit=10, offset=(page-1)*limit;
        try {
          context=await ctx();s.sheetGrant=context.grant;
          result=await lookup(group,offset);
        } catch(error) {
          if (!model || !f.slots || !s.ops.includes('appointment')) throw error;
          s.questions.contact='Google Sheets: '+userError(error);
          await independentSlots();
          return save();
        }
        s.choices=result.contacts.slice(0,limit);
        s.pages=[];
        if (deferredOrdinal && s.choices[deferredOrdinal-1]) pick(s,s.choices[deferredOrdinal-1]);
        else if (!deferredOrdinal && result.total===1 && (f.contact || f.host || s.ops.length>1)) pick(s,s.choices[0]);
        else {
          const names={today:'Para hoy',overdue:'Vencidos',undated:'Pendientes sin fecha',match:'Coincidencias de tu hoja'};
          const key=group || 'match';
          s.listTitle=names[key]+' ('+result.total+').';
          if (result.total>limit) s.listTitle+=' Página '+page+' de '+Math.ceil(result.total/limit)+'.';
          if (page>1) s.pages.push({arg:key+(page-1),label:'Página anterior'});
          if (offset+limit<result.total) s.pages.push({arg:key+(page+1),label:'Ver más · '+names[key]});
          if (group==='today' && !f.contact && !f.host && page===1) {
            const [late,undated]=await Promise.all([lookup('overdue'),lookup('undated')]);
            s.listTitle+=' Vencidos ('+late.total+'). Pendientes sin fecha ('+undated.total+').';
            for (const [k,total] of [['overdue',late.total],['undated',undated.total]]) {
              if (total) s.pages.push({arg:k+'1',label:'Ver '+names[k].toLowerCase()+' ('+total+')'});
            }
          }
          s.waiting='choice';
          s.questions.contact=deferredOrdinal?'Elige un prospecto de los que aparecen en esta página.':result.total?'¿Con quién seguimos?':'Puedes elegir otro grupo o darme un nombre o anfitrión.';
          if (!requestedPage) await independentSlots();
          return save();
        }
      }
      if (s.ops.includes('add') && s.ops.includes('note')) {s.questions.note='¿Quieres dar de alta al prospecto o anotar una gestión en una ficha existente? Pídeme una de estas acciones primero.';return save();}
      const dateState=readFollowup(f.noteSchedule || '',{},{timeZone:zone,now:now()}).state;
      // Calendar supplies the checked time to a related draft, independent of operation order in the request.
      const relatedMail=s.ops.includes('appointment') && (priorDraft && s.mailUsesTime || /\b(cita|demo|demostraci[oó]n|horarios?|llamada|hora|reuni[oó]n|visita)\b/i.test(f.mailInstruction || '') ||
        model && f.slots && (!f.mailInstruction || /^(?:(?:y|el|un|mi|para|de|en|lo|le|al|la|listo|un[ao]?|d[eé]ja\w*|prep[aá]r\w*|red[aá]ct\w*|correo|borrador|mensaje|seguimiento)\s*)+[.!]?$/i.test(f.mailInstruction)));
      s.mailUsesTime=!!relatedMail;
      for (const k of ['note','add','appointment','email'].filter(op=>s.ops.includes(op))) {
        if (s.steps[k]) continue;
        try {
          let result;
          if (s.chosen?.identityKey && ['appointment','email'].includes(k)) await sheetTools.revalidate(await ctx(),s.chosen.identityKey);
          if (k==='appointment') {
            result=await channels.appointment(actor,s.calendarState,{name:s.chosen?.fullName || f.contact,
              schedule:intent.schedule || (s.slot || oldFields.schedule===f.schedule && s.calendarState.day!=null ? '' : f.schedule) || '',
              slots:f.slots,slot:s.slot});
            s.calendarState=result.state || s.calendarState;
            if (result.options) {s.options=result.options;s.slotLabels=result.labels;}
          } else if (k==='email') {
            const options=model && relatedMail && !s.steps.appointment?.whenText ? s.slotLabels : null;
            if (relatedMail && !s.steps.appointment?.whenText && !options?.length) {
              s.questions.email='Cuando elijas el horario, lo incluiré en el borrador.';continue;
            }
            result=await channels.email(actor,{to:f.to || s.chosen?.email,instruction:f.mailInstruction,
              priorDraft,editInstruction:priorDraft?intent.mailInstruction:'',
              optionLabels:options,whenText:relatedMail?s.steps.appointment?.whenText:''});
          }
          else if (k==='note') {
            if (f.noteSchedule && dateState.day==null) result={question:'¿Para qué fecha queda el seguimiento en la hoja?'};
            else if (!f.note && dateState.day==null) result={question:'¿Qué quieres anotar o para qué día queda el próximo seguimiento?'};
            else result=await sheetTools.prepareNote(await ctx(),{key:s.chosen.key,note:f.note||'',today,
              followupDate:dateState.day!=null?new Date(dateState.day).toISOString().slice(0,10):''});
          } else {
            if (!f.contact || !f.phone || !f.leadKind || f.leadKind==='referrals' && !f.host)
              result={question:'Para el alta dime el nombre, teléfono y si es rifa o referido. Si es referido, dime también el anfitrión.'};
            else {const context=await ctx();s.sheetGrant=context.grant;result=await sheetTools.prepareAdd(context,{kind:f.leadKind,fullName:f.contact,phone:f.phone,hostName:f.host});}
          }
          if (result.proposal) s.steps[k]={status:'ready',preview:result.preview,proposal:result.proposal,...(result.whenText && {whenText:result.whenText}),...(result.baseDraft && {baseDraft:result.baseDraft})};
          else if (result.question) {s.questions[k]=result.question;s.waiting ||= k==='appointment'?(s.options.length?'slot':'schedule'):k==='email'?(f.to || s.chosen?.email?'purpose':'to'):k==='add'?'phone':'note';}
        } catch(e){s.questions[k]=userError(e);s.waiting ||= k==='email'?'to':k==='appointment'?'schedule':k==='note'?'note':'contact';}
      }
      return save();
    } catch(e) {return reply(userError(e),client);}
  };
}
