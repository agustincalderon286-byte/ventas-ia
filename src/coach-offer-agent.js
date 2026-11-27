import {createHash} from 'node:crypto';
import {openToken, sealToken} from './coach-hycite-gmail.js';
import {readFollowup} from './coach-followup-input.js';
import {readSlotHours, slotBounds, slotChoice} from './coach-calendar-slots.js';
import {bookingRequest, fold, offerBody, offerChoice, offerRequest, offerSlots, replyExcerpt, slotLabel} from './coach-offer-text.js';
import {offerDigest} from './coach-mail-offers.js';

// Gmail -> two options -> seller confirms the reply -> prospect chooses -> seller confirms the booking.
// The prospect's email is only read to compare it with the two options; it never instructs the Coach.

const connections = {type:'connections', label:'Mis conexiones'};
const lower = value => String(value || '').trim().toLowerCase();
// Only the keys included here change in the client; Calendar and WhatsApp context stay untouched.
const answer = (respuesta, actions = [], pending) => ({
  mode:'coach', coachMailAgent:true, coachOfferAgent:true, respuesta, coachHelpActions:actions,
  ...(pending !== undefined && {coachMailPending:pending})
});
const source = (mailbox, id, label) => ({type:'mailSource', mailbox, id, label});
const slotButtons = (offer, skip = -1) => offer.labels.map((label, i) => ({type:'calendarSlot', choice:i + 1, searchId:offer.id, label:'Elegir ' + label}))
  .filter(action => action.choice - 1 !== skip);
const format = (date, zone) => new Intl.DateTimeFormat('es-US', {timeZone:zone, weekday:'long', year:'numeric', month:'long', day:'numeric',
  hour:'numeric', minute:'2-digit', hour12:true}).format(new Date(date)) + ' (' + zone + ')';
const calendarLink = raw => {
  try {
    const url = new URL(raw);
    if (url.protocol === 'https:' && !url.username && !url.password && !url.port && ['www.google.com','calendar.google.com'].includes(url.host) && url.pathname.startsWith('/calendar/')) return url.href;
  } catch {}
  return '';
};
const sameAccount = 'Para ofrecer horarios por correo, Gmail y Google Calendar deben estar conectados con la misma cuenta de Google que usas en el Coach. Revisa Mis conexiones.';
const slotCommand = /^Elegir horario ([12]) ([a-f0-9]{32})$/i;
const codeCommand = /^(confirmar|descartar) cita\s+([a-f\d]{12})[.!]?$/;

// Checks shared by the mail agent before and after Gmail sends a reply that carries an offer.
export function createOfferHooks({config, calendar, gmail, offers, now = Date.now}) {
  return {
    async beforeSend({owner, email, mailbox, token, message}) {
      const offer = message.offer;
      const cal = await calendar.getConnection(owner);
      if (!cal?.grant || offerDigest(config.key, cal.grant) !== offer.calendarGrant) return 'La conexión de Google Calendar cambió después de preparar la oferta. Pídeme una oferta nueva.';
      if (lower(cal.mailbox) !== lower(email) || lower(mailbox) !== lower(email)) return sameAccount;
      if (offer.slots.some(slot => Date.parse(slot.start) <= now())) return 'Uno de los horarios ofrecidos ya pasó. Pídeme una oferta nueva.';
      const busy = await calendar.availability({owner,grant:cal.grant,start:offer.slots[0].start,end:offer.slots[1].end});
      if (offer.slots.some(slot => busy.some(block => Date.parse(block.start)<Date.parse(slot.end) && Date.parse(block.end)>Date.parse(slot.start)))) {
        return 'Uno de los horarios se ocupó desde que preparé el correo. Pídeme dos opciones nuevas.';
      }
      const thread = await gmail.thread(token, message.thread.threadId);
      const latest = thread?.at(-1);
      if (!latest || latest.id !== offer.sourceMessageId || latest.from !== offer.prospect) return 'El hilo cambió desde que preparé la respuesta. Vuelve a revisar tus correos antes de ofrecer horarios.';
      return '';
    },
    async afterSend({owner, gmailGrant, message, receipt}) {
      const cal = await calendar.getConnection(owner);
      if (!cal?.grant) return null;
      return offers.record({owner, gmailGrant, calendarGrant:cal.grant, calendarId:cal.calendarId}, message.offer,
        {threadId:message.thread.threadId, sentMessageId:receipt.id});
    }
  };
}

export function createCoachOfferAgent({config, store, gmail, drafts, calendar, offers, findLeads, syncLead, switched, now = Date.now}) {
  const loadMail = id => store.findById(id, 'mailbox encryptedToken grant sendEnabled').lean();
  const access = async doc => (await gmail.refresh(openToken(doc.encryptedToken, config.key, doc._id))).access_token;

  return async ({req, auth, sessionId, text}) => {
    const raw = String(text || '').trim();
    const slot = slotCommand.exec(raw);
    const code = codeCommand.exec(fold(raw));
    const pending = req.body?.coachMailPending;
    const commandForOffer = slot ? offers.isId(slot[2].toLowerCase()) : code ? offers.isCode(code[2].toUpperCase()) : false;
    const offerIntent = !slot && !code ? offerRequest(raw) : null;
    const bookingIntent = !slot && !code && !offerIntent ? bookingRequest(raw) : null;
    const pendingOnly = !commandForOffer && !offerIntent && !bookingIntent;
    if (pendingOnly && (typeof pending !== 'string' || !pending || slot || code)) return null;
    // Requests that may belong to another agent stay silent when this one cannot serve them.
    const quiet = pendingOnly || commandForOffer || bookingIntent?.bare;
    const refuse = message => quiet ? null : answer(message);

    if (req.body?.responseMode === 'audio' && req.body?.coachMailVersion !== 1) return refuse('Actualiza la página del Coach para usar Gmail y Calendar desde el chat de voz.');
    const actor = auth?.user;
    if (!actor?._id || !auth.session?.tokenHash || req.body?.accountId !== String(actor._id) || switched(req)) return refuse('Abre el chat desde tu propia cuenta de Coach para usar Gmail y Calendar.');
    const email = lower(actor.email);
    const calendarPilots = config.calendarPilot ?? config.pilot;
    if (![config.pilot,calendarPilots].every(list => list.includes('*') || list.includes(email))) return refuse('Tu cuenta aún no participa en el piloto de Gmail y Calendar.');
    try {
      if (!config.ready || !(config.calendarReady ?? config.ready)) return refuse('Gmail o Calendar están temporalmente fuera de servicio.');
      const owner = actor._id;
      const doc = await loadMail(owner);
      if (!doc?.mailbox || !doc.grant) return refuse('Conecta tu propia cuenta de Gmail en Mis conexiones para ofrecer horarios.');
      const session = createHash('sha256').update(auth.session.tokenHash + ':' + sessionId).digest('hex');
      const aad = 'draft-question:' + owner + ':' + session + ':' + doc.grant;
      const seal = (context, minutes = 30) => sealToken(JSON.stringify({...context, until:now() + minutes * 60000}), config.key, aad);
      let context = null;
      if (typeof pending === 'string' && pending && pending.length <= 40000) {
        try {
          const saved = JSON.parse(openToken(pending, config.key, aad));
          if (saved.until > now()) context = saved;
        } catch { /* The selection belongs to another seller, session or Gmail consent. */ }
      }
      let chooseFromPending = -1;
      if (pendingOnly) {
        chooseFromPending = context?.kind === 'offerChoose' ? slotChoice(raw) : -1;
        // Only an answer that looks like a day, hours, duration or zone continues the pending offer.
        const continuesOffer = context?.kind === 'offerAsk' && raw.length <= 200 &&
          /\b(?:hoy|manana|pasado|lunes|martes|miercoles|jueves|viernes|sabado|domingo|proximo|tarde|noche|mediodia|minutos?|horas?|utc|\d{1,4})\b|[a-z]+\/[a-z_]+/.test(fold(raw)) &&
          !/\b(?:correos?|gmail|whatsapp|borrador|ventas)\b/.test(fold(raw));
        if (chooseFromPending < 0 && !continuesOffer) return null;
      }
      const cal = await calendar.getConnection(owner);
      if (!cal?.grant || !cal.calendarId) return refuse('Conecta Google Calendar en Mis conexiones para ofrecer horarios de tu propia cuenta.');
      if (lower(doc.mailbox) !== email || lower(cal.mailbox) !== email) return refuse(sameAccount);
      if (!cal.availabilityEnabled) return quiet ? null : answer('Para ofrecer horarios libres, activa el permiso de disponibilidad de Google Calendar en Mis conexiones.', [connections]);
      const b = {owner:String(owner), gmailGrant:doc.grant, calendarGrant:cal.grant, calendarId:cal.calendarId};
      const mailbox = lower(doc.mailbox);

      // ---------- Step 1: prepare a reply in the thread with two free options ----------
      async function prepareOffer(messageId, state, hoursText) {
        let priorSlots;
        if (offerIntent && /\b(?:es[oa]s|est[oa]s|(?:los|las) (?:dos|2)) (?:dos |2 )?(?:horarios|opciones|espacios)\b/.test(fold(hoursText)) &&
            !/\b(?:manana|lunes|martes|miercoles|jueves|viernes|sabado|domingo|tarde|noche|minutos|horas)\b|\d{1,2}:\d{2}/.test(fold(hoursText))) {
          try {
            const encoded = req.body?.coachCalendarPending;
            if (typeof encoded === 'string' && encoded.length <= 8000) {
              const previous=JSON.parse(openToken(encoded,config.key,'calendar-question:'+owner+':'+session+':'+cal.grant));
              if (previous.until > now() && previous.state?.waiting === 'slot' && previous.state.slots?.length === 2) {
                const {day,timeZone,slotHours,durationMinutes,slots}=previous.state;
                state={day,timeZone,slotHours,durationMinutes};
                priorSlots=slots;
              }
            }
          } catch { /* Continue only with a list bound to these grants and this chat. */ }
          if (!priorSlots) return answer('Vuelve a buscar los dos horarios libres y luego dime «Responde al primero con esos dos horarios».',[]);
        }
        const ask = (question, field) => answer(question, [], seal({kind:'offerAsk', messageId, state:{...state, waiting:field}}, 15));
        const parsed = readFollowup(hoursText, {kind:'availability', ...state}, {timeZone:req.body?.coachTimeZone, now:now()});
        if (parsed.question) return ask(parsed.question, 'day');
        const next = {day:parsed.state.day ?? null, timeZone:parsed.state.timeZone || '', durationMinutes:parsed.state.durationMinutes ?? state.durationMinutes};
        const hours = readSlotHours(hoursText, state.slotHours);
        if (hours.question) return ask(hours.question, 'hours');
        next.slotHours = hours.hours;
        if (next.day == null) {
          return ask('¿Para qué día ofrezco los dos horarios? Puedes decir «mañana por la tarde», «el jueves de 14:00 a 18:00» o una fecha como 2027-01-15.', 'day');
        }
        const range = slotBounds(next);
        if (range.question) return ask(range.question, 'day');
        const duration = next.durationMinutes ?? 45;
        if (!Number.isInteger(duration) || duration < 15 || duration > 240) return ask('Indica la duración de la demo, entre 15 y 240 minutos.', 'duration');
        if (Date.parse(range.end) <= now()) return ask('Ese día ya pasó. ¿Qué otro día ofrezco?', 'day');
        const token = await access(doc);
        const target = await gmail.replyTarget(token, messageId);
        if (!target) return answer('No pude verificar un solo remitente y el hilo de ese correo. Ábrelo en Gmail para responder.', [source(doc.mailbox, messageId, 'Abrir correo')]);
        const prospect = lower(target.to);
        const thread = await gmail.thread(token, target.thread.threadId);
        const latest = thread?.at(-1);
        if (!latest || latest.id !== messageId) return answer('Ese hilo tiene un mensaje más reciente. Vuelve a pedirme «Revisa mis correos» y elige el último mensaje de la clienta.', [], '');
        if (latest.sent || latest.from !== prospect || (latest.replyTo && latest.replyTo !== latest.from) || prospect === mailbox) {
          return answer('No puedo ofrecer horarios en ese hilo: el último mensaje no es de un solo remitente que coincida con la dirección de respuesta. Ábrelo en Gmail para revisarlo.', [source(doc.mailbox, messageId, 'Abrir correo')], '');
        }
        // The prospect is linked by the sender's address only. Ambiguity is resolved by the seller, never guessed.
        const leads = findLeads ? await findLeads({owner, email:prospect}) : [];
        let lead = leads.length === 1 ? leads[0] : null;
        if (leads.length > 1) {
          lead = state.waiting === 'lead' ? leads.find(l => String(l.id) === String(req.body?.activeCrmRecordId || '')) || null : null;
          if (!lead) return answer('Hay varias fichas de TEC con el correo de ese remitente: ' + leads.map(l => l.name).join(', ') +
            '. Abre la ficha correcta en TEC y vuelve a pedirme la oferta.', [], seal({kind:'offerAsk', messageId, state:{...next,waiting:'lead'}}, 15));
        }
        const busy = await calendar.availability({owner, grant:cal.grant, start:range.start, end:range.end});
        if (priorSlots?.some(slot => Date.parse(slot.start)<=now() ||
            busy.some(block => Date.parse(block.start)<Date.parse(slot.end) && Date.parse(block.end)>Date.parse(slot.start)))) {
          return answer('Uno de los dos horarios que viste ya se ocupó o pasó. Busca dos horarios libres de nuevo antes de preparar el correo.',[]);
        }
        const slots = priorSlots || offerSlots(range, busy, duration, now());
        if (slots.length < 2) return answer('Revisé tu calendario principal y «Seguimientos TEC» y no encontré dos horarios libres de ' + duration +
          ' minutos ese día. ¿Qué otro día u horario ofrezco?', [], seal({kind:'offerAsk', messageId, state:{timeZone:next.timeZone, durationMinutes:duration, waiting:'day'}}, 15));
        const labels = slots.map(s => slotLabel(s.start, next.timeZone));
        const offer = {id:offers.newId(), slots, labels, timeZone:next.timeZone, sourceMessageId:messageId, prospect,
          calendarGrant:offerDigest(config.key, cal.grant), leadId:lead ? String(lead.id) : '', leadName:lead?.name || '',
          expectedStart:lead?.nextActionAt ?? null, durationMinutes:duration};
        const body = offerBody({name:lead?.name, labels, timeZone:next.timeZone});
        const current = await loadMail(owner);
        if (current?.grant !== doc.grant) return answer('La conexión de Gmail cambió. Vuelve a consultar tu bandeja.', [], '');
        const draft = await drafts.prepare({owner, session, grant:doc.grant, ...target, body, offer});
        return preview(draft, lead);
      }
      const preview = (draft, lead) => {
        const actions = [];
        if (!doc.sendEnabled) actions.push(connections);
        actions.push({type:'mailConfirm', id:draft.id, label:'Confirmar envío'}, {type:'mailCancel', id:draft.id, label:'Cancelar borrador'});
        const crm = lead ? '\nFicha de TEC: ' + lead.name + '. Al agendar actualizaré su próxima acción.'
          : '\nNo encontré una ficha de TEC con el correo de ese remitente. Si eligen horario, la demo quedará solo en tu calendario.';
        return answer('Respuesta en el mismo hilo desde ' + doc.mailbox + '\nPara: ' + draft.to + '\nAsunto: ' + (draft.subject || '(sin asunto)') + '\n\n' + draft.body +
          '\n\nRevisé tu calendario principal y «Seguimientos TEC». No reservo nada hasta que la clienta elija y tú confirmes la cita.' + crm +
          '\nPuedes corregir el saludo o la despedida; los dos horarios se mantienen.\n' +
          (doc.sendEnabled ? '' : 'Para enviar, autoriza el envío en Mis conexiones y vuelve a este chat.\n') +
          'Confirma con el botón o escribe «Confirmar envío ' + draft.id + '». El borrador dura 30 minutos.', actions, seal({kind:'preview', id:draft.id, sellerText:false}));
      };

      if (offerIntent || (pendingOnly && context?.kind === 'offerAsk')) {
        let messageId = context?.kind === 'offerAsk' ? context.messageId : context?.kind === 'replyBody' ? context.id : '';
        let state = context?.kind === 'offerAsk' ? context.state || {} : {};
        if (offerIntent) {
          let ids = context?.kind === 'inbox' ? context.ids : [];
          if (offerIntent.index && (ids.length || !messageId)) {
            if (!ids.length && offerIntent.latest) ids = (await gmail.inbox(await access(doc))).map(m => m.id);
            messageId = ids[offerIntent.index - 1] || '';
            state = {};
            if (!messageId) return answer(ids.length ? 'Ese número no está en la lista. Indica uno de los correos mostrados.' : 'Primero pídeme «Revisa mis correos» y luego dime a cuál responder, por ejemplo «Ofrécele dos horarios al primero para mañana por la tarde».', [], ids.length ? pending : undefined);
          } else if (!messageId) {
            if (ids.length !== 1) return answer(ids.length ? '¿A qué correo de la lista respondo? Por ejemplo, «Ofrécele dos horarios al segundo para el jueves por la tarde».'
              : 'Primero pídeme «Revisa mis correos» y luego dime a cuál responder, por ejemplo «Ofrécele dos horarios al primero para mañana por la tarde».', [], ids.length ? pending : undefined);
            messageId = ids[0];
          }
        }
        return await prepareOffer(messageId, state, raw);
      }

      function inboundAfterOffer(thread,offer) {
        const at=thread?.findIndex(m => m.id===offer.sentMessageId && m.sent);
        if (at==null || at<0) return null;
        const later=thread.slice(at+1);
        if (later.some(m => m.sent || m.from!==offer.prospect || m.replyTo===null || (m.replyTo && m.replyTo!==offer.prospect))) return null;
        return later;
      }
      // Read the prospect's current reply before showing the booking preview.
      async function review(offer, sellerChoice, expectedReply) {
        if (offer.state === 'booked') return receipt({...offer.result, already:true}, offer);
        if (offer.state === 'booking') return answer('Esa demo se está agendando en este momento. Espera unos segundos y vuelve a revisarla.', []);
        if (offer.state !== 'offered' || !offer.slots) return answer('Esa oferta ya no está disponible. Puedes pedirme una nueva.', []);
        const token = await access(doc);
        const thread = await gmail.thread(token, offer.threadId);
        if (!thread) return answer('Ya no encuentro ese hilo en tu Gmail. No agendé nada.', []);
        const later=inboundAfterOffer(thread,offer);
        if (!later) return answer('El hilo cambió: hay otro mensaje enviado, otro remitente o ya no aparece la oferta original. Revisa el correo antes de agendar.',[]);
        const last = later.at(-1);
        if (!last) return answer('Todavía no veo respuesta de ' + (offer.leadName || 'la clienta') + ' en el hilo. Te aviso cuando me lo vuelvas a pedir.', []);
        if (last.sent || last.from === mailbox) return answer('Tu último mensaje del hilo es posterior a la respuesta de la clienta. Revisa el hilo en Gmail antes de agendar.', [source(doc.mailbox, last.id, 'Abrir correo')]);
        if (expectedReply && last.id !== expectedReply) return answer('Llegó un mensaje nuevo al hilo. Vuelve a pedirme «Revisa la respuesta de la oferta» para leerlo antes de agendar.', [], '');
        const excerpt = replyExcerpt(last.snippet);
        const options = offer.labels.map((label, i) => (i + 1) + '. ' + label).join('\n');
        const interpretation=offerChoice(last.snippet,offer.slots,offer.timeZone);
        if (interpretation.reason==='negative') return answer('Respuesta en el hilo: «'+excerpt+'». Pide a tu cliente que confirme una de las dos opciones antes de agendar.',[source(doc.mailbox,last.id,'Abrir respuesta')],'');
        if (sellerChoice>=0 && interpretation.index!=null && sellerChoice!==interpretation.index) return answer('El cliente eligió otro horario en su respuesta. Pídele que confirme el cambio en el hilo antes de agendar.',[source(doc.mailbox,last.id,'Abrir respuesta')],'');
        const index = sellerChoice >= 0 ? sellerChoice : interpretation.index;
        if (index == null || !offer.slots[index]) {
          return answer('Respuesta en el hilo: «' + excerpt + '»\nNo puedo saber con certeza qué opción eligió. ¿Cuál agendo?\n' + options +
            '\nElige con el botón o escribe «la primera» o «la segunda». No agendo nada sin tu confirmación.',
            [...slotButtons(offer), source(doc.mailbox, last.id, 'Abrir respuesta')], seal({kind:'offerChoose', offerId:offer.id, replyId:last.id}, 15));
        }
        const chosen = offer.slots[index];
        if (Date.parse(chosen.start) <= now()) return answer('Ese horario ya pasó. Pídeme una oferta nueva para esa clienta.', [], '');
        const choice = await offers.choose(b, offer.id, {index, replyId:last.id});
        if (!choice) return answer('La oferta cambió mientras la revisaba. Vuelve a pedirme «Revisa la respuesta de la oferta».', [], '');
        const name = offer.leadName || offer.prospect;
        let message = 'Respuesta en el hilo: «' + excerpt + '»\n' + (sellerChoice >= 0 ? 'Elegiste la opción ' : 'Entiendo que eligió la opción ') + (index + 1) + ': ' + offer.labels[index] + '.' +
          '\n\nAgendar «Demo · ' + name + '» el ' + format(chosen.start, offer.timeZone) + '.\nCalendario: Seguimientos TEC · ' + cal.mailbox +
          '\nDuración: ' + offer.durationMinutes + ' min · Aviso 15 min antes.';
        message += offer.leadId ? '\nFicha de TEC: ' + name + '. Actualizaré su próxima acción al confirmar.' : '\nSin ficha de TEC con ese correo: la cita quedará solo en tu calendario.';
        message += '\nVolveré a comprobar tu calendario principal y «Seguimientos TEC» al confirmar. Es una cita personal, sin invitados.' +
          '\nConfirma con el botón o escribe «Confirmar cita ' + choice.code + '».';
        return answer(message, [{type:'calendarConfirm', id:choice.code, label:'Confirmar cita'}, {type:'calendarCancel', id:choice.code, label:'Descartar cita'},
          ...(interpretation.index==null ? slotButtons(offer,index) : [])], seal({kind:'offerChoose',offerId:offer.id,replyId:last.id},15));
      }
      function receipt(result, offer) {
        const code = offer?.choice?.code;
        const retry = code ? [{type:'calendarConfirm', id:code, label:'Confirmar cita'}] : [];
        switch (result?.status) {
          case 'saved': {
            let message = (result.already ? 'Esa demo ya estaba agendada; no creé otra cita. ' : '') +
              'Google Calendar confirmó «' + result.summary + '» para el ' + format(result.start, result.timeZone) + '.';
            if (result.crm === 'updated') message += ' También actualicé la próxima acción del prospecto en TEC.';
            if (result.crm === 'conflict') message += ' La ficha de TEC cambió después de la oferta; conservé la fecha que tiene ahora.';
            if (result.crm === 'expired') message += ' La confirmación tardó demasiado para cambiar la ficha de TEC con seguridad; actualízala a mano si hace falta.';
            message += ' Referencia de la cita: ' + result.reference + '. Si quieres, responde en el hilo para confirmarle el horario a la clienta.';
            const link = calendarLink(result.htmlLink);
            return answer(message, link ? [{type:'calendarEvent', label:'Abrir en Google Calendar', url:link}] : [], '');
          }
          case 'slotBusy': return answer('Ese horario ya no está libre en tu calendario. Pídele al cliente que confirme la otra opción en el hilo o prepara una oferta nueva.',[], '');
          case 'past': return answer('Ese horario ya pasó. No creé la cita. Pídeme una oferta nueva.', [], '');
          case 'calendarBusy': return answer('Tu calendario está confirmando otra cita en este momento. No escribí nada en Google. Vuelve a confirmar en unos segundos.', retry);
          case 'working': return answer('Esta demo se está agendando desde otra pestaña o intento. Comprueba con el mismo botón en un momento; no se duplicará.', retry);
          case 'retry': return answer('Todavía no tengo confirmación de Google Calendar. Reintenta con el mismo botón; no se creará otra cita.', retry);
          case 'crmPending': return answer('La demo ya está en Google Calendar. Vuelve a confirmar para terminar de actualizar la ficha de TEC.', retry);
          case 'reconnect': return answer('Google solicita renovar la conexión de Calendar. Abre Mis conexiones y vuelve a revisar la oferta.', [connections]);
          case 'changed': return answer('Google no aceptó la cita. Revisa tu calendario antes de volver a intentarlo.', [connections]);
          default: return answer('Esa elección caducó o ya no está disponible. Pídeme «Revisa la respuesta de la oferta».', [], '');
        }
      }

      if (slot) {
        const offer = await offers.get(b, slot[2].toLowerCase());
        if (!offer) return req.body?.coachCalendarPending ? null : answer('Esa oferta o lista de horarios ya no está disponible. Pídeme «Revisa la respuesta de la oferta» o una búsqueda nueva.', []);
        if (context?.kind!=='offerChoose' || context.offerId!==offer.id) return answer('Esa selección cambió. Pídeme «Revisa la respuesta de la oferta» para ver las opciones vigentes.',[]);
        return await review(offer, Number(slot[1]) - 1,context.replyId);
      }
      if (pendingOnly && chooseFromPending >= 0) {
        const offer = await offers.get(b, context.offerId);
        if (!offer) return answer('Esa oferta ya no está disponible. Puedes pedirme una nueva.', [], '');
        return await review(offer, chooseFromPending, context.replyId);
      }
      if (code) {
        const offer = await offers.byCode(b, code[2].toUpperCase());
        if (!offer) return null;
        if (code[1] === 'descartar') {
          return answer(await offers.discard(b, offer.choice.code) === 'discarded' ? 'Descarté esa elección. La oferta sigue vigente; puedes volver a revisarla cuando quieras.'
            : 'Esa elección ya cambió o se está agendando.', []);
        }
        if (offer.state === 'offered') {
          // Second confirmation: the reply the seller reviewed must still be the latest message of the thread.
          const thread = await gmail.thread(await access(doc), offer.threadId);
          const last = thread?.at(-1);
          if (!inboundAfterOffer(thread,offer)?.length || !last || last.id !== offer.choice.replyId) {
            return answer('El hilo cambió desde que revisé la respuesta. No agendé nada. Pídeme «Revisa la respuesta de la oferta» para leerlo de nuevo.', [], '');
          }
        }
        if (offer.state === 'booked') return receipt({...offer.result, already:true}, offer);
        return receipt(await offers.book(b,offer.choice.code,{
          saveEvent:async event => {
            if (offer.state==='booking' && !event.reconcileOnly) {
              const later=inboundAfterOffer(await gmail.thread(await access(doc),offer.threadId),offer);
              if (later?.at(-1)?.id !== offer.choice.replyId) {
                const existing=await calendar.saveEvent({...event,reconcileOnly:true});
                if (existing) return existing;
                throw Object.assign(new Error('Cambió la respuesta del hilo.'),{status:409});
              }
            }
            return calendar.saveEvent(event);
          },syncLead
        }),offer);
      }

      // ---------- Booking request in natural language ----------
      const list = await offers.active(b,{limit:bookingIntent.ref?2:50,ref:bookingIntent.ref});
      const named = bookingIntent.email ? list.filter(o => o.prospect === bookingIntent.email) : bookingIntent.name ? list.filter(o => fold(o.leadName || o.result?.summary || '').includes(fold(bookingIntent.name))) : list;
      const open = named.filter(o => o.state === 'offered' || o.state === 'booking');
      if (!open.length) {
        if (bookingIntent.bare && !named.length) return null;
        const booked = named.find(o => o.state === 'booked');
        if (booked?.result) return receipt({...booked.result, already:true}, booked);
        return answer('No tengo ofertas de horarios vigentes enviadas desde el Coach' + (bookingIntent.name ? ' para ese nombre' : '') +
          '. Puedes pedirme «Revisa mis correos» y luego «Ofrécele dos horarios al primero para mañana».', []);
      }
      if (open.length > 1) {
        return answer('Tienes varias ofertas vigentes:\n' + open.slice(0,6).map((o, i) => (i + 1) + '. ' + (o.leadName || o.prospect) + ' · Ref. ' + o.ref).join('\n') +
          '\nElige por referencia, por ejemplo «Revisa la oferta ' + open[0].ref + '».', []);
      }
      return await review(open[0], bookingIntent.index ? bookingIntent.index - 1 : -1);
    } catch (error) {
      if (quiet) return null;
      if (error?.reauthorize) return answer('Google solicita que vuelvas a autorizar tu cuenta. Abre Mis conexiones para revisarla.', [connections]);
      return answer('No pude completar la oferta de horarios. No envié correos ni agendé citas. Revisa Mis conexiones y vuelve a intentarlo.', [connections]);
    }
  };
}
