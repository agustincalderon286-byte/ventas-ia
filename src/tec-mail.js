export const TEC_CONTACT_EMAIL = 'agustin@totalelitecoaching.com';
export const TEC_SENDER_NAME = 'Audiolibros TEC';

// Credentials stay on the server. A mailbox is selected by its exact verified
// address, never by a recipient or resource ID supplied by a website visitor.
export function createTecMailer({
  hostingerToken = '', resendApiKey = '', resendFrom = '', fetchImpl = fetch
} = {}) {
  const fromAddress = typeof resendFrom === 'string' && !/[\r\n]/.test(resendFrom)
    ? (resendFrom.match(/<([^<>]+)>\s*$/)?.[1] || resendFrom.trim()) : '';
  const companySender = /^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@(?:[a-z0-9-]+\.)*totalelitecoaching\.com$/i.test(fromAddress);
  // Never send account or support messages from a personal fallback mailbox.
  if (!hostingerToken && (!resendApiKey || !companySender)) return null;
  let mailboxPromise;
  async function mailbox() {
    if (!mailboxPromise) {
      mailboxPromise = (async () => {
        const response = await fetchImpl('https://api.mail.hostinger.com/api/v1/me', {
          headers: { Authorization: `Bearer ${hostingerToken}` },
          signal: AbortSignal.timeout(10_000)
        });
        if (!response.ok) throw new Error('tec_mail_configuration_failed');
        const payload = await response.json();
        const entry = payload?.data?.mailboxes?.find(item =>
          item.address?.toLowerCase() === TEC_CONTACT_EMAIL);
        if (!entry?.resourceId) throw new Error('tec_mail_configuration_failed');
        return entry.resourceId;
      })().catch(error => { mailboxPromise = undefined; throw error; });
    }
    return mailboxPromise;
  }
  return async ({ to, subject, text, html, replyTo = TEC_CONTACT_EMAIL, idempotencyKey }) => {
    if (!Array.isArray(to) || !to.length || to.some(address =>
      typeof address !== 'string' || /[\r\n]/.test(address)) ||
      typeof subject !== 'string' || /[\r\n]/.test(subject) ||
      typeof replyTo !== 'string' || /[\r\n]/.test(replyTo)) {
      throw new Error('tec_mail_invalid_message');
    }
    let response;
    if (hostingerToken) {
      const resourceId = await mailbox();
      // The current Hostinger API has no Reply-To field. Support messages
      // therefore include the visitor's validated reply address in their body.
      const replyHint = replyTo !== TEC_CONTACT_EMAIL
        ? `\n\nPara responder a esta consulta: mailto:${replyTo}\n` : '';
      response = await fetchImpl(
        `https://api.mail.hostinger.com/api/v1/mailboxes/${encodeURIComponent(resourceId)}/send`, {
          method: 'POST', signal: AbortSignal.timeout(10_000),
          headers: { Authorization: `Bearer ${hostingerToken}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            to, subject, displayName: TEC_SENDER_NAME,
            ...(text !== undefined ? { text: text + replyHint } : {}),
            ...(html !== undefined ? { html } : {})
          })
        });
    } else {
      response = await fetchImpl('https://api.resend.com/emails', {
        method: 'POST', signal: AbortSignal.timeout(10_000),
        headers: {
          Authorization: `Bearer ${resendApiKey}`, 'Content-Type': 'application/json',
          ...(idempotencyKey ? { 'Idempotency-Key': idempotencyKey } : {})
        },
        body: JSON.stringify({
          from: `${TEC_SENDER_NAME} <${fromAddress}>`, to, subject, reply_to: replyTo,
          ...(text !== undefined ? { text } : {}), ...(html !== undefined ? { html } : {})
        })
      });
    }
    // Do not retry uncertain sends or silently switch providers after a failure:
    // either could send a second copy of a sign-in link or support message.
    if (!response.ok) throw new Error('tec_mail_delivery_failed');
    return { accepted: true, provider: hostingerToken ? 'hostinger' : 'resend' };
  };
}

export function createAudioAccountMailer(sendMail) {
  if (!sendMail) return null;
  return async ({ email, purpose, url }) => {
    const messages = {
      checkout: {
        subject: 'Confirma tu correo para continuar tu membresía de Audiolibros TEC',
        text: `Ya habías iniciado una suscripción con este correo. Confírmalo para continuar al pago seguro:\n\n${url}\n\nEl enlace es de un solo uso y caduca en 30 minutos. No activa una cuenta ni completa un cobro. Después de pagar podrás elegir usuario y contraseña.\n\nAyuda: ${TEC_CONTACT_EMAIL}`
      },
      activation: {
        subject: 'Confirma tu correo y crea tu cuenta de Audiolibros TEC',
        text: `Tu pago de la membresía de Audiolibros TEC está confirmado.\n\nAbre este enlace para confirmar tu correo y elegir tu usuario y contraseña:\n\n${url}\n\nEl enlace es de un solo uso y caduca en 24 horas. Si caducó, usa Recuperar acceso en la biblioteca.\n\nAyuda: ${TEC_CONTACT_EMAIL}`
      },
      reset: {
        subject: 'Restablece tu contraseña de Audiolibros TEC',
        text: `Recibimos una solicitud para restablecer tu contraseña.\n\nElige una nueva contraseña aquí:\n\n${url}\n\nEl enlace es de un solo uso y caduca en 30 minutos. Si no lo solicitaste, ignora este correo; tu contraseña no cambia hasta que completes el formulario.\n\nAyuda: ${TEC_CONTACT_EMAIL}`
      },
      password_changed: {
        subject: 'Se cambió tu contraseña de Audiolibros TEC',
        text: `La contraseña de tu cuenta de Audiolibros TEC fue restablecida. Por seguridad cerramos las sesiones anteriores.\n\nIngresa con tu usuario o correo y tu nueva contraseña en https://totalelitecoaching.com/audiolibros/\n\nSi no reconoces este cambio, contacta a ${TEC_CONTACT_EMAIL}.`
      }
    };
    if (!Object.hasOwn(messages, purpose)) throw new Error('tec_mail_invalid_purpose');
    return sendMail({ to: [email], ...messages[purpose] });
  };
}
