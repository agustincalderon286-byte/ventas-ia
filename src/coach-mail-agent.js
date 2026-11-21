import { createHash } from 'node:crypto';
import { classifyMailIntent, resolveMailIntent, composeMailDraft, mailReadOptions, mailReadClarification } from './coach-mail-intent.js';
import { openToken, sealToken } from './coach-hycite-gmail.js';

import { classifySalesMailReport, draftSalesReport } from './coach-mail-sales-report.js';
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

export function createCoachMailAgent({ config, store, gmail, drafts, syncSales, switched, compose = composeMailDraft }) {
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
    if (intent?.kind === 'help') return answer('Puedo consultar tu bandeja de entrada, revisar las ventas de Hy Cite o preparar un correo. Prueba «Revisa mis correos» o «Prepara un correo para…». Para enviar, te mostraré el texto y te pediré confirmación.', [connections]);
    try {
      if (!config.ready) return answer('La conexión de Gmail está temporalmente fuera de servicio.');
      const doc = await load(actor._id);
      if (!doc?.mailbox || !doc.grant) return answer('Conecta tu propia cuenta de Gmail para usarla desde el chat.', [connections]);
      const session = createHash('sha256').update(auth.session.tokenHash + ':' + sessionId).digest('hex');
      const binding = { owner: actor._id, session, grant: doc.grant };
      const aad = 'draft-question:' + actor._id + ':' + session + ':' + doc.grant;
      let previousText = '';
      let continuationKind = 'draft';
      if (pending && (!intent || intent.kind === 'draft')) {
        try {
          if (typeof pending !== 'string' || pending.length > 40000) throw new Error();
          const decoded = JSON.parse(openToken(pending, config.key, aad));
          if (decoded.until > Date.now()) {
            previousText = decoded.text;
            continuationKind = decoded.kind === 'report' ? 'report' : 'draft';
          }
        } catch { /* A pending question belongs to one connected seller and chat. */ }
        if (!intent && !previousText) return answer('Ya caducó la pregunta del borrador. Vuelve a decirme qué correo quieres preparar.');
        if (!intent && /^(?:cancela(?:r)?|ol[vb][íi]dalo|ya no|no gracias|cancelar borrador)[.!]?$/i.test(text.trim())) {
          return answer('Dejé de preparar ese correo.');
        }
        intent ||= { kind: continuationKind };
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
          const continuation = message.pendingText ? sealToken(JSON.stringify({ text: message.pendingText, kind: intent.kind, until: Date.now() + 15 * 60000 }), config.key, aad) : '';
          return answer(message.question, [], continuation);
        }
        const draft = await drafts.prepare({ ...binding, ...message });
        const actions = [];
        if (!doc.sendEnabled) actions.push(connections);
        actions.push({ type: 'mailConfirm', id: draft.id, label: 'Confirmar envío' }, { type: 'mailCancel', id: draft.id, label: 'Cancelar borrador' });
        return answer(`Borrador desde ${doc.mailbox}\nPara: ${draft.to}\nAsunto: ${draft.subject}\n\n${draft.body}\n\n${doc.sendEnabled ? '' : 'Para enviar, abre Mis conexiones y autoriza el envío en Google. Después, vuelve a este chat.\n'}Confirma con el botón o escribe «Confirmar envío ${draft.id}». El borrador dura 30 minutos.`, actions);
      }
      if (intent.kind === 'confirm' || intent.kind === 'cancel') {
        if (intent.kind === 'confirm' && !doc.sendEnabled) return answer('Para enviar este borrador, autoriza el envío en Mis conexiones y vuelve a confirmarlo aquí.', [connections]);
        const ref = { ...binding, id: intent.id };
        const result = intent.kind === 'cancel' ? await drafts.cancel(ref) : await drafts.confirm({
          ...ref, deliver: async message => {
            let token;
            try {
              const current = await load(actor._id);
              if (!current?.sendEnabled || current.grant !== doc.grant) throw new Error();
              token = await access(current);
            } catch { throw Object.assign(new Error('La conexión cambió.'), { beforeSend: true }); }
            return gmail.send(token, { ...message, from: doc.mailbox });
          }
        });
        const links = result.status === 'sent' && result.messageId ? [source(doc, result.messageId, 'Ver en Gmail')] : [];
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
        return answer('Estos son los mensajes más recientes ' + where + ':\n\n' + lines.join('\n\n'), messages.map((m,i) => source(doc, m.id, 'Abrir correo ' + (i+1))));
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
