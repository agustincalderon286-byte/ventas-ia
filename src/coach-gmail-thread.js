import { isGmailAddress } from './coach-mail-address.js';

const messageId = /^<[!-~]{1,248}>$/;
const safeId = value => typeof value === 'string' && messageId.test(value) && /^[^<>\s@]+@[^<>\s@]+$/.test(value.slice(1, -1));
const cleanLine = value => typeof value === 'string' && !/[\p{Cc}\u2028\u2029\u202a-\u202e\u2066-\u2069]/u.test(value);
const unfold = value => String(value ?? '').replace(/\r?\n[ \t]+/g, ' ').replace(/\t/g, ' ').trim();
const gmailId = value => typeof value === 'string' && /^[a-z0-9_-]{1,128}$/i.test(value);

function mailbox(value) {
  if (!cleanLine(value)) return '';
  if (isGmailAddress(value)) return value;
  // A quoted display name may contain a comma; the address list must have one mailbox.
  const match = /^(?:"(?:[^"\\]|\\.)*"|[^,"<>;:]*)\s*<([^<>]+)>$/.exec(value);
  return match && isGmailAddress(match[1]) ? match[1] : '';
}

export function validatedThread(thread) {
  if (!thread || !gmailId(thread.threadId) || !safeId(thread.parent) ||
      !Array.isArray(thread.references) || !thread.references.length || thread.references.length > 20 ||
      thread.references.at(-1) !== thread.parent || !thread.references.every(safeId)) {
    throw new Error('No pude identificar el hilo original.');
  }
  return { threadId:thread.threadId, parent:thread.parent, references:[...thread.references] };
}

export function replyEnvelope(message) {
  const headers = message?.payload?.headers || [];
  function get(name) {
    const found = headers.filter(h => String(h.name).toLowerCase() === name);
    if (found.length > 1) throw new Error('Encabezado ambiguo.');
    return found.length ? unfold(found[0].value) : undefined;
  }
  const parent = get('message-id');
  const replyTo = get('reply-to');
  const to = mailbox(replyTo ?? get('from') ?? '');
  const subject = get('subject') || '';
  if (!to || !cleanLine(subject) || subject.length > 900) throw new Error('No pude verificar el destinatario o el asunto.');
  // Keep the stable parent and the tail of long reference chains. Fold MIME one ID per line.
  const previous = get('references') || get('in-reply-to') || '';
  const ids = previous.split(/\s+/).filter(safeId).filter(id => id !== parent);
  const thread = validatedThread({ threadId: message.threadId, parent, references:[...new Set(ids)].slice(-19).concat(parent) });
  return { to, subject, thread };
}

export function threadSubjectHeader(subject) {
  // Gmail can return an already MIME-encoded header. Preserve it verbatim instead of encoding it a second time.
  return /^[\x20-\x7E]+$/.test(subject) && /=\?[^?\s]+\?[bq]\?/i.test(subject);
}
