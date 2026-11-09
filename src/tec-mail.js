export const TEC_CONTACT_EMAIL = 'agustin@totalelitecoaching.com';
export const TEC_SENDER_NAME = 'Agustin Calderon | Audiolibros TEC';

// Credentials stay on the server. A mailbox is selected by its exact verified
// address, never by a recipient or resource ID supplied by a website visitor.
export function createTecMailer({
  hostingerToken = '', resendApiKey = '', resendFrom = '', fetchImpl = fetch
} = {}) {
  if (!hostingerToken && (!resendApiKey || !resendFrom)) return null;
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
          from: resendFrom, to, subject, reply_to: replyTo,
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
