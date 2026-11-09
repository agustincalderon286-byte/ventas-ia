import { createHash, randomUUID } from 'node:crypto';

import { createTecMailer, TEC_CONTACT_EMAIL } from './tec-mail.js';

export const SUPPORT_EMAIL = TEC_CONTACT_EMAIL;
export const SUPPORT_TOPICS = Object.freeze({
  acceso: 'Acceso a mi cuenta', membresia: 'Membresía o cobros',
  audio: 'Audio o lectura', tecnico: 'Problema técnico', otro: 'Otra consulta'
});

// The recipient and sender are server-controlled. User text is sent as plain text only.
export function createSupportMailer({ apiKey, from, fetchImpl = fetch, sendMail } = {}) {
  const deliver = sendMail || createTecMailer({ resendApiKey: apiKey, resendFrom: from, fetchImpl });
  if (!deliver) return null;
  return async ({ name, email, topic, message, service, reference }) => deliver({
    to: [SUPPORT_EMAIL], replyTo: email,
    idempotencyKey: `tec-support-${reference}`,
    subject: `[Soporte TEC] ${SUPPORT_TOPICS[topic]} · ${reference.slice(0, 8)}`,
    text: `Nueva consulta de soporte de ${service === 'coach' ? 'TEC Coach' : 'Audiolibros TEC'}\nReferencia: ${reference}\nNombre: ${name || 'No indicado'}\nCorreo para responder: ${email}\nTema: ${SUPPORT_TOPICS[topic]}\n\n${message}`
  });
}

export function registerAudiobookSupport(app, { publicOrigin, sendSupport, now = Date.now }) {
  const origin = new URL(publicOrigin).origin;
  const buckets = new Map();
  const windowMs = 30 * 60 * 1000;
  const limited = (key, maximum) => {
    const time = now();
    for (const [id, item] of buckets) if (item.until <= time) buckets.delete(id);
    const hash = createHash('sha256').update(key).digest('hex');
    let item = buckets.get(hash);
    if (!item) {
      if (buckets.size >= 5000) return true;
      item = { count: 0, until: time + windowMs }; buckets.set(hash, item);
    }
    return ++item.count > maximum;
  };
  app.post('/api/audiolibros/contacto', async (req, res) => {
    res.set({ 'Cache-Control': 'private, no-store', 'Vary': 'Origin' });
    const fail = (status, error) => res.status(status).json({ error });
    if (req.get('Sec-Fetch-Site') === 'cross-site' || (req.get('Origin') && req.get('Origin') !== origin)) {
      return fail(403, 'Abre el formulario de soporte en TEC para enviar tu consulta.');
    }
    if (limited(`ip:${req.ip}`, 8)) {
      res.set('Retry-After', '1800'); return fail(429, 'Espera 30 minutos antes de enviar otra consulta o escríbenos por correo.');
    }
    if (!req.is('application/json')) return fail(415, 'Envía tu consulta desde el formulario de TEC.');
    const body = req.body;
    if (!body || typeof body !== 'object' || Array.isArray(body)) return fail(400, 'Revisa los campos del formulario.');
    if (typeof body.website === 'string' && body.website) return res.json({ ok: true });
    const { email, topic, message, name = '', service = 'audiolibros' } = body;
    if (typeof email !== 'string' || email.length > 254 || !/^[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Z0-9](?:[A-Z0-9.-]*[A-Z0-9])?\.[A-Z]{2,}$/i.test(email) ||
        typeof name !== 'string' || name.length > 100 || /[\x00-\x1f\x7f]/.test(name) ||
        typeof topic !== 'string' || !Object.hasOwn(SUPPORT_TOPICS, topic) ||
        typeof message !== 'string' || message.trim().length < 10 || message.length > 4000 || /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(message) ||
        !['coach', 'audiolibros'].includes(service)) return fail(400, 'Revisa tu correo y escribe un mensaje de entre 10 y 4,000 caracteres.');
    if (limited(`email:${email.toLowerCase()}`, 3)) {
      res.set('Retry-After', '1800'); return fail(429, 'Ya recibimos varias consultas de este correo. Espera 30 minutos o escríbenos directamente.');
    }
    if (!sendSupport) return fail(503, 'El formulario no está disponible. Escríbenos a ' + SUPPORT_EMAIL + '.');
    const reference = randomUUID();
    try {
      await sendSupport({ name: name.trim(), email, topic, message: message.trim(), service, reference });
      return res.json({ ok: true, reference: reference.slice(0, 8) });
    } catch {
      // Never log the message, email address, credentials or provider response.
      console.warn('TEC support: email delivery failed');
      return fail(503, 'No pudimos enviar tu consulta. Tu texto sigue en el formulario; reintenta o escríbenos a ' + SUPPORT_EMAIL + '.');
    }
  });
}
