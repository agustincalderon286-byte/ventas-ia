import { isDraftCorrection } from './coach-mail-followup.js';
import {offerRequest, bookingRequest} from './coach-offer-text.js';
import { classifyMailIntent } from './coach-mail-intent.js';
import { calendarIntent } from './coach-followup-input.js';
import { whatsAppIntent, foldWa } from './coach-wa-chat-input.js';

// The Coach client keeps one opaque pending token per channel and sends all of
// them. Agents run WhatsApp → Calendar → Gmail, so a draft correction such as
// «cambia jueves por viernes» could be captured by an older Calendar question or
// by the other channel's draft. The client also sends `coachDraftFocus`: the
// channel whose draft preview the seller saw last. For a plain correction this
// helper keeps only that channel's token; explicit requests for another channel
// stay untouched so they remain operable.
//
export const coachPendingFields = Object.freeze({
  mail: 'coachMailPending', calendar: 'coachCalendarPending', whatsapp: 'coachWhatsAppPending'
});
const draftChannels = ['mail', 'whatsapp'];
const mailWords = /\b(?:correos?|gmail|e-?mail)\b/;
const whatsAppWords = /\b(?:whats(?:\s*app)?|wats\s*ap+p?|guatsap|wasap|wsap)\b/;

function explicitOtherChannel(input, focus) {
  if (calendarIntent(input)) return true;
  const folded = foldWa(input);
  if (focus !== 'whatsapp' && (whatsAppIntent(input) || whatsAppWords.test(folded))) return true;
  if (focus !== 'mail' && mailWords.test(folded)) return true;
  return false;
}

export function scopeCoachPending(body, text) {
  if (!body || typeof body !== 'object') return body;
  const { coachDraftFocus, coachQuestionFocus, ...rest } = body;
  const input = String(text || '').trim();
  if (!input || input.length > 12000 || offerRequest(input) || bookingRequest(input)) return rest;
  let focus;
  if (isDraftCorrection(input) && draftChannels.includes(coachDraftFocus) && rest[coachPendingFields[coachDraftFocus]]) {
    focus = coachDraftFocus;
  } else if (Object.hasOwn(coachPendingFields, coachQuestionFocus) &&
      !calendarIntent(input) && !classifyMailIntent(input) && !whatsAppIntent(input) &&
      !/^Elegir horario [12] [a-f0-9]{32}$/i.test(input)) {
    focus = coachQuestionFocus;
  }
  if (!focus || !rest[coachPendingFields[focus]] || explicitOtherChannel(input, focus)) return rest;
  for (const [channel, name] of Object.entries(coachPendingFields)) {
    if (channel !== focus) delete rest[name];
  }
  return rest;
}
