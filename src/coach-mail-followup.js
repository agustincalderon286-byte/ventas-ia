import { isGmailAddress } from './coach-mail-address.js';

const fold = value => value.normalize('NFD').replace(/\p{M}/gu, '').toLowerCase();
const speaker = /^(?:(?:oye|coach(?: tec)?|por favor)[, ]+)*(?:(?:puedes|podr[ií]as)\s+)?/i;
const replyCommand = /^(?:resp[oó]nd(?:e(?:le)?|er(?:le)?)|cont[eé]st(?:a(?:le)?|ar(?:le)?))\b/i;
const ordinal = /^(?:(?:al?|el|la|a ese|ese|este)\s+)*(?:(?:correo|e-?mail|mensaje|hilo)\s*)?(?:(?:n[uú]mero|n[º°])\s*)?(primer(?:[oa])?|segund[oa]|tercer[oa]?|cuart[oa]|quint[oa]|m[aá]s reciente|[0-9]+)\b(?:\s+(?:correo|mensaje|hilo))?/i;
const indices = {primer:1,primero:1,primera:1,segundo:2,segunda:2,tercer:3,tercero:3,tercera:3,cuarto:4,cuarta:4,quinto:5,quinta:5,'mas reciente':1};
const editCommand = /^(?:mejor\b|corr[ií]ge(?:lo|la)?\b|corregir\b|modifica(?:lo|la)?\b|edita(?:lo|la)?\b|c[aá]mbia(?:lo|la)?\b|sustituye\b|reemplaza\b|agrega(?:le)?\b|a[nñ]ade(?:le)?\b|quita(?:le)?\b|pon(?:le)?\b|(?:hazlo|hazla|escr[ií]belo|red[aá]ctalo|reform[uú]lalo|ac[oó]rtalo|res[uú]melo)\b|m[aá]s (?:amable|formal|breve|corto|claro)\b|asunto\s*:|para\s*:|(?:mensaje|texto|cuerpo)\s*:)/i;
const copyPrefix = /^(?:(?:y\s+)?(?:d[ií]le(?: que)?|dici[eé]ndole(?: que)?|cont[eé]stale que|resp[oó]ndele que|que diga|con (?:el |este )?(?:mensaje|texto)|(?:mensaje|texto|cuerpo)\s*:|que)\s*[:,-]?\s*)/i;

export function replyRequest(text) {
  const value = text.trim().replace(/^[¿¡]/, '').replace(speaker, '');
  const command = replyCommand.exec(value);
  if (!command) return null;
  const rest = value.slice(command[0].length).trim();
  const match = ordinal.exec(rest);
  if (!match && !/^(?:(?:(?:al?|el|ese|este|este último|el último)\s+)*(?:correo|hilo|mensaje)\b|a todos\b)/i.test(rest)) return null;
  if (!match) return { kind:'reply', index:null, unsupported:true };
  const word = fold(match[1]);
  const tail = rest.slice(match[0].length).replace(/^[\s,.:;?!-]+/, '').trim();
  if (/^(?:y (?:al?|el)|o (?:al?|el)|a todos\b)/i.test(tail)) return {kind:'reply',index:null,unsupported:true};
  return { kind:'reply', index:indices[word] || Number(word), latest:word === 'mas reciente', instruction:tail };
}

export function replyText(instruction) {
  const literal = /^(?:mensaje|texto|cuerpo)\s*:\s*([\s\S]*)$/i.exec(instruction.trim());
  if (literal) return literal[1].trim();
  const body = instruction.trim().replace(copyPrefix, '').trim();
  if (!body) return '';
  return body.charAt(0).toLocaleUpperCase('es') + body.slice(1);
}

export function isDraftCorrection(text) {
  return editCommand.test(text.trim().replace(speaker, ''));
}

const weekdays = /\b(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)\b/gi;
const cleanQuote = value => value.trim().replace(/^[«“"]([\s\S]*)[»”"]$/, '$1');
const guide = 'Puedes decir «cambia jueves por viernes», «agrega al final: Gracias» o escribir el texto completo después de «Mensaje:».';
const capital = word => word.charAt(0).toLocaleUpperCase('es') + word.slice(1);

export function correction(draft, input) {
  const request = input.trim().replace(speaker, '');
  const value = request.replace(/^(?:corr[ií]ge|edita|modifica)(?: el borrador|lo|la)?\s*[:,]\s*/i, '');
  const body = /^(?:(?:cambia|pon|corrige|reemplaza)\s+(?:el\s+)?)?(?:mensaje|texto|cuerpo)(?:\s+del\s+(?:mensaje|borrador|correo))?\s*(?::|por\s+|a\s+)\s*([\s\S]+)$/i.exec(value);
  if (body) return {body:cleanQuote(body[1])};
  const subject = /^(?:(?:cambia|pon|corrige|reemplaza)\s+(?:el\s+)?)?asunto\s*(?::|por\s+|a\s+)\s*([^\n]+)$/i.exec(value);
  const to = /^(?:para\s*:|(?:cambia|corrige) (?:el )?destinatario (?:a|por))\s*(\S+)\s*$/i.exec(value);
  if (subject || to) {
    if (draft.thread) return {question:'En esta respuesta se conservan el asunto y el destinatario del hilo. Para cambiarlos, pídeme un correo nuevo.'};
    if (subject) return {subject:cleanQuote(subject[1])};
    return isGmailAddress(to[1]) ? {to:to[1]} : {question:'Indica una sola dirección de correo válida.'};
  }
  const addition = /^(?:agrega|a[nñ]ade)(?:le)?(?: al final)?\s*[:,]?\s+([\s\S]+)$/i.exec(value);
  if (addition) return {body:draft.body + '\n' + cleanQuote(addition[1])};
  const substitute = /^(?:cambia|sustituye|reemplaza)\s+(.+?)\s+por\s+([\s\S]+)$/i.exec(value);
  const remove = /^quita(?:le)?\s+(.+)$/i.exec(value);
  if (substitute || remove) {
    const needle = cleanQuote((substitute || remove)[1]), normalized = draft.body.toLowerCase();
    const at = normalized.indexOf(needle.toLowerCase());
    if (!needle || at < 0 || normalized.indexOf(needle.toLowerCase(), at + needle.length) !== -1) return {question:'Indica una frase que aparezca una sola vez en el borrador, o escribe «Mensaje:» con el texto final.'};
    const replacement = substitute ? cleanQuote(substitute[2]) : '';
    const changed = (draft.body.slice(0,at) + replacement + draft.body.slice(at+needle.length)).trim();
    return changed ? {body:changed} : {question:'¿Qué texto quieres conservar en el mensaje?'};
  }
  const day = /^mejor\s+(?:el\s+)?(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo)[.!]?$/i.exec(value);
  if (day) {
    const matches = draft.body.match(weekdays) || [];
    if (matches.length === 1) return {body:draft.body.replace(weekdays, old => old[0] === old[0].toUpperCase() ? capital(day[1].toLowerCase()) : day[1].toLowerCase())};
    return {question:'¿Qué frase del borrador quieres cambiar por el nuevo día? ' + guide};
  }
  return {question:guide, freeform:true};
}
