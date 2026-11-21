import { createHash } from 'node:crypto';
import { classifyMailIntent, resolveMailIntent, composeMailDraft, rewriteSellerBody, mailReadOptions, mailReadClarification } from './coach-mail-intent.js';
import { openToken, sealToken } from './coach-hycite-gmail.js';

import { classifySalesMailReport, draftSalesReport } from './coach-mail-sales-report.js';
import { correction, isDraftCorrection, replyText } from './coach-mail-followup.js';
import { mailPeriod } from './coach-mail-period.js';

const connections = { type: 'connections', label: 'Mis conexiones' };
const answer = (respuesta, actions = [], pending = '') => ({
  mode: 'coach', coachMailAgent: true, respuesta, coachHelpActions: actions, coachMailPending: pending
});
const stamp = date => new Intl.DateTimeFormat('es-US', { dateStyle: 'medium', timeZone: 'America/Chicago' }).format(new Date(date));
const source = (doc, id, label) => ({ type: 'mailSource', mailbox: doc.mailbox, id, label });
const statuses = {
  sent: 'Gmail confirmó el envío. Puedes comprobarlo en Enviados.',
  cancelled: 'Borrador cancelado.',
  sending: 'El envío está en curso. Espera un momento antes de consultar de nuevo.',
  unknown: 'Aún no tengo confirmación de Gmail. Revisa Enviados antes de preparar otra copia.',
  unavailable: 'Ese borrador caducó o pertenece a otra conversación. Puedes pedirme uno nuevo.',
  failed: 'El correo no salió. Revisa el permiso de envío en Mis conexiones y vuelve a preparar el borrador.'
};

// offerHooks (optional): {beforeSend, afterSend} from coach-offer-agent.js for replies that carry two time options.
export function createCoachMailAgent({ config, store, gmail, drafts, syncSales, switched, compose = composeMailDraft, rewrite = rewriteSellerBody, offerHooks = null }) {
  const load = id => store.findById(id, 'mailbox encryptedToken grant sendEnabled').lean();
  const access = async doc => (await gmail.refresh(openToken(doc.encryptedToken, config.key, doc._id))).access_token;
  return async ({ req, auth, sessionId, text }) => {
    const pending = req.body?.coachMailPending;
    let intent;
    if (classifySalesMailReport(text)) intent = { kind: 'report' };
    else intent = pending ? classifyMailIntent(text) : await resolveMailIntent(text);
    if (!intent && !pending) return null;
    if (req.body?.responseMode === 'audio' && req.body?.coachMailVersion !== 1) {
      return answer('Actualiza la página del Coach para consultar Gmail desde el chat de voz.');
    }
    const actor = auth?.user;
    if (!actor?._id || !auth.session?.tokenHash || req.body?.accountId !== String(actor._id) || switched(req)) {
      return answer('Abre el chat desde tu propia cuenta de Coach para usar Gmail.');
    }
    if (!config.pilot.includes('*') && !config.pilot.includes(String(actor.email || '').toLowerCase())) {
      return answer('Tu cuenta aún no participa en el piloto de Gmail.');
    }
    if (intent?.kind === 'help') return answer('Puedo revisar tus correos y ventas de Hy Cite, preparar un mensaje o responder en su hilo. Después de «Revisa mis correos», prueba «Responde al segundo». Puedes corregir el borrador antes de confirmar el envío.', [connections]);
    try {
      if (!config.ready) return answer('La conexión de Gmail está temporalmente fuera de servicio.');
      const doc = await load(actor._id);
      if (!doc?.mailbox || !doc.grant) return answer('Conecta tu propia cuenta de Gmail para usarla desde el chat.', [connections]);
      const session = createHash('sha256').update(auth.session.tokenHash + ':' + sessionId).digest('hex');
      const binding = { owner: actor._id, session, grant: doc.grant };
      const aad = 'draft-question:' + actor._id + ':' + session + ':' + doc.grant;
      const contextToken = (context, minutes = 30) => sealToken(JSON.stringify({ ...context, until:Date.now() + minutes * 60000 }), config.key, aad);
      let context;
      if (typeof pending === 'string' && pending.length <= 40000) {
        try {
          const saved = JSON.parse(openToken(pending, config.key, aad));
          if (saved.until > Date.now()) context = saved;
        } catch { /* Only this seller, login, conversation and grant may use the selection. */ }
      }
      const preview = (draft, sellerText) => {
        const actions = [];
        if (!doc.sendEnabled) actions.push(connections);
        actions.push({ type:'mailConfirm', id:draft.id, label:'Confirmar envío' }, { type:'mailCancel', id:draft.id, label:'Cancelar borrador' });
        const header = draft.thread ? 'Respuesta en el mismo hilo desde ' : 'Borrador desde ';
        const guide = draft.offer ? 'Puedes corregir el saludo o la despedida; los dos horarios se mantienen.'
          : 'Puedes pedirme una corrección, por ejemplo «cambia jueves por viernes».';
        const permission = doc.sendEnabled ? '' : 'Para enviar, abre Mis conexiones y autoriza el envío en Google. Después, vuelve a este chat.\n';
        return answer(header + doc.mailbox + '\nPara: ' + draft.to + '\nAsunto: ' + (draft.subject || '(sin asunto)') + '\n\n' + draft.body + '\n\n' + guide + '\n' + permission + 'Confirma con el botón o escribe «Confirmar envío ' + draft.id + '». El borrador dura 30 minutos.', actions, contextToken({kind:'preview',id:draft.id,sellerText:sellerText === true}));
      };
      const cancelFollowup = /^(?:cancela(?:r)?|ol[vb][íi]dalo|ya no|no gracias|cancelar borrador)[.!]?$/i.test(text.trim());
      if (context?.kind === 'preview' && !['confirm','cancel'].includes(intent?.kind) && isDraftCorrection(text)) {
        const ref = { ...binding, id:context.id };
        const old = await drafts.view(ref);
        if (old.status !== 'pending') return answer(statuses[old.status] || statuses.unavailable);
        let changes = correction(old,text);
        if (changes.freeform && context.sellerText === true) {
          try { changes = await rewrite(old.body,text); }
          catch { /* Keep the precise text replacement option when the model is unavailable. */ }
        }
        if (changes.question) return answer(changes.question, [], pending);
        const updated = await drafts.revise({ ...ref, changes });
        if (updated.status !== 'pending') return answer(statuses[updated.status] || statuses.unavailable);
        if (updated.offerEdit) return answer('Esa corrección cambiaría los dos horarios ofrecidos o añadiría otra fecha. Puedes cambiar el saludo o la despedida; para otros horarios, pídeme una oferta nueva. El borrador anterior sigue igual.', [], pending);
        return preview(updated,context.sellerText);
      }
      if (context?.kind === 'preview' && !intent && cancelFollowup) intent = {kind:'cancel', id:context.id};
      if (!intent && (context?.kind === 'preview' || context?.kind === 'inbox')) {
        // A passive list or preview must not suppress normal language routing for the next request.
        intent = await resolveMailIntent(text);
        if (intent?.kind === 'help') return answer(context.kind === 'preview'
          ? 'Para enviar esta versión, pulsa Confirmar envío en el borrador o escribe «Confirmar envío ' + context.id + '».'
          : 'Puedes elegir un correo de la lista para responder, por ejemplo «Responde al segundo».', [], pending);
      }
      if (context?.kind === 'replyBody' && !intent) {
        if (cancelFollowup) return answer('Dejé de preparar la respuesta.');
        intent = {kind:'reply', instruction:text, messageId:context.id};
      }
      if (intent?.kind === 'reply') {
        if (intent.unsupported) return answer('Elige un solo mensaje de la lista, por ejemplo «Responde al segundo».', [], context?.kind === 'inbox' ? pending : '');
        let ids = context?.kind === 'inbox' ? context.ids : [];
        if (!ids.length && intent.latest && !pending) ids = (await gmail.inbox(await access(doc))).map(m => m.id);
        const id = intent.messageId || ids[intent.index - 1];
        if (!id) return answer(ids.length ? 'Ese número no está en la lista. Indica el número de uno de los correos mostrados.' : 'Primero pídeme «Revisa mis correos» para elegir el mensaje al que quieres responder.', [], ids.length ? pending : '');
        const body = replyText(intent.instruction || '');
        if (!body) return answer('¿Qué quieres decirle en tu respuesta? Puedes escribir el texto exacto después de «Mensaje:».', [], contextToken({kind:'replyBody',id},15));
        if (body.length > 12000) return answer('La respuesta puede tener hasta 12 000 caracteres.', [], contextToken({kind:'replyBody',id},15));
        const target = await gmail.replyTarget(await access(doc),id);
        if (!target) return answer('No pude verificar un solo destinatario y el hilo de ese mensaje. Ábrelo en Gmail para responder.', [source(doc,id,'Abrir correo')]);
        if ((await load(actor._id))?.grant !== doc.grant) return answer('La conexión cambió. Vuelve a consultar tu bandeja.');
        return preview(await drafts.prepare({ ...binding, ...target, body }),true);
      }
      let previousText = '';
      if ((!intent || intent.kind === 'draft') && ['report','draft'].includes(context?.kind)) {
        if (cancelFollowup) return answer('Dejé de preparar ese correo.');
        previousText = context.text;
        intent ||= {kind:context.kind};
      }
      if (!intent) {
        if (!context && pending) return answer('Ya caducó la pregunta del borrador. Vuelve a decirme qué correo quieres preparar.');
        return null;
      }
      if (intent.kind === 'draft' || intent.kind === 'report') {
        let message;
        if (intent.kind === 'report') {
          // The figures in an outbound report come from the seller's authenticated
          // mailbox. Keep the original request separate from the follow-up address.
          const request = previousText || text;
          const clarify = mailReadClarification(request, 'report');
          if (clarify) return answer(clarify);
          const snapshot = await syncSales(actor._id, doc.grant);
          if (!snapshot) return answer('La conexión cambió. Vuelve a pedirme el resumen.');
          message = draftSalesReport(previousText ? text.trim() : request, {
            snapshot, mailbox: doc.mailbox, days: mailReadOptions(request).days, period: mailPeriod(request)
          });
          if (message.question) message.pendingText = request;
        } else {
          message = await compose(text, { previousText });
        }
        if (message.question) {
          const continuation = message.pendingText ? contextToken({text:message.pendingText, kind:intent.kind},15) : '';
          return answer(message.question, [], continuation);
        }
        return preview(await drafts.prepare({ ...binding, ...message }),intent.kind === 'draft');
      }
      if (intent.kind === 'confirm' || intent.kind === 'cancel') {
        if (intent.kind === 'confirm' && !doc.sendEnabled) return answer('Para enviar este borrador, autoriza el envío en Mis conexiones y vuelve a confirmarlo aquí.', [connections]);
        const ref = { ...binding, id: intent.id };
        let offerProblem = '', deliveredOffer;
        const result = intent.kind === 'cancel' ? await drafts.cancel(ref) : await drafts.confirm({
          ...ref, deliver: async message => {
            let token;
            try {
              const current = await load(actor._id);
              if (!current?.sendEnabled || current.grant !== doc.grant) throw new Error();
              token = await access(current);
            } catch { throw Object.assign(new Error('La conexión cambió.'), { beforeSend: true }); }
            if (message.offer) {
              // Re-read the thread and both Google connections right before the reply leaves.
              try {
                offerProblem = offerHooks ? await offerHooks.beforeSend({ owner:actor._id, email:actor.email, mailbox:doc.mailbox, token, message })
                  : 'Las ofertas de horarios no están disponibles en este momento.';
              } catch { offerProblem = 'No pude volver a comprobar el hilo y tu calendario. No envié el correo; pídeme la oferta de nuevo en un momento.'; }
              if (offerProblem) throw Object.assign(new Error(offerProblem), { beforeSend: true });
            }
            const { offer, ...outgoing } = message;
            const receipt = await gmail.send(token, { ...outgoing, from: doc.mailbox });
            if (offer && receipt?.id) {
              deliveredOffer = {offer,thread:{threadId:message.thread.threadId}};
            }
            return receipt;
          }
        });
        if (offerProblem) return answer(offerProblem, [connections]);
        const links = result.status === 'sent' && result.messageId ? [source(doc, result.messageId, 'Ver en Gmail')] : [];
        deliveredOffer ||= result.offerDelivery;
        if (intent.kind==='confirm' && result.status === 'sent' && deliveredOffer) {
          const offerSaved=await offerHooks.afterSend({owner:actor._id,gmailGrant:doc.grant,message:deliveredOffer,receipt:{id:result.messageId}}).catch(()=>null);
          return answer(offerSaved?.ref
            ? 'Gmail confirmó el envío con los dos horarios. Registré la oferta (Ref. ' + offerSaved.ref + '); no reservé nada todavía. Cuando la clienta conteste en el hilo, pídeme «Revisa la respuesta de la oferta».'
            : 'Gmail confirmó el envío. Pulsa «Registrar horarios enviados» para terminar de guardar la oferta en TEC.', offerSaved?.ref ? links :
              [...links,{type:'mailRetryReceipt',id:intent.id,label:'Registrar horarios enviados'}]);
        }
        return answer(statuses[result.status] || statuses.unavailable, links);
      }
      const clarify = mailReadClarification(text, intent.kind);
      if (clarify) return answer(clarify);
      const options = mailReadOptions(text);
      const period = mailPeriod(text);
      if (intent.kind === 'inbox') {
        const messages = await gmail.inbox(await access(doc), period ? { ...options, since: period.since, before: period.before } : options);
        if ((await load(actor._id))?.grant !== doc.grant) return answer('La conexión cambió. Vuelve a pedir la consulta.');
        const filters = [options.unread && 'sin leer', options.from && 'de ' + options.from, period?.label, options.days && 'de los últimos ' + options.days + ' días'].filter(Boolean).join(' ');
        const where = 'en la bandeja de entrada de ' + doc.mailbox + (filters ? ' (' + filters + ')' : '');
        if (!messages.length) return answer('No encontré mensajes ' + where + '.');
        const lines = messages.map((m,i) => `${i+1}. ${m.subject || '(Sin asunto)'} — ${m.from || 'Sin remitente'} · ${stamp(m.date)}${m.snippet ? '\n' + m.snippet : ''}`);
        return answer('Estos son los mensajes más recientes ' + where + ':\n\n' + lines.join('\n\n') + '\n\nPara contestar en el mismo hilo, escribe «Responde al primero» o indica el número del correo.', messages.map((m,i) => source(doc, m.id, 'Abrir correo ' + (i+1))), contextToken({kind:'inbox',ids:messages.map(m => m.id)}));
      }
      if (intent.kind === 'sales') {
        if (options.unread || options.from) return answer('Las ventas se calculan con los avisos de Hy Cite de tu cuenta. Para filtrar por remitente o por correos sin leer, pídeme revisar la bandeja de entrada.');
        if (options.days > 90) return answer('La consulta de ventas cubre los últimos 90 días. Puedes pedirme ese período o uno más corto.');
        const snapshot = await syncSales(actor._id, doc.grant);
        if (!snapshot) return answer('La conexión cambió. Vuelve a pedirme la consulta.');
        const days = options.days || 90;
        const since = period ? period.since * 1000 : Date.now() - days * 86400000;
        const before = period ? period.before * 1000 : Infinity;
        const inPeriod = o => Date.parse(o.date) >= since && Date.parse(o.date) < before;
        const confirmed = snapshot.confirmed.filter(inPeriod);
        const review = snapshot.review.filter(inPeriod);
        const top = confirmed.slice(0,5);
        const rows = top.map(o => 'Orden ' + o.order + ' · ' + stamp(o.date));
        const windowLabel = period ? period.label : days === 1 ? 'las últimas 24 horas' : 'los últimos ' + days + ' días';
        let summary = `Revisé los avisos de Hy Cite de ${doc.mailbox} (${windowLabel}). Encontré ${confirmed.length} ${confirmed.length === 1 ? 'venta confirmada' : 'ventas confirmadas'} y ${review.length} ${review.length === 1 ? 'aviso' : 'avisos'} para revisar.`;
        if (snapshot.limited) summary += ' El conteo es parcial porque se alcanzó el límite de 100 avisos.';
        if (rows.length) summary += '\n\n' + rows.join('\n');
        return answer(summary, [...top.map(o => source(doc,o.id,'Ver orden ' + o.order)), connections]);
      }
      return answer('Puedo revisar Gmail o preparar un borrador. ¿Qué quieres hacer?');
    } catch (error) {
      if (error.reauthorize) return answer('Google solicita que vuelvas a autorizar tu cuenta. Abre Mis conexiones para revisarla.', [connections]);
      return answer('No pude completar la acción con Gmail. Puedes volver a consultar la bandeja o revisar la conexión.', [connections]);
    }
  };
}
