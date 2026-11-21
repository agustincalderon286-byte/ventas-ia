import { rewriteSellerBody } from './coach-mail-intent.js';

export const foldWa = value => String(value || '').toLowerCase().normalize('NFD').replace(/\p{M}/gu,'').trim();
const channel = /\b(?:whats(?:\s*app)?|wats\s*ap+p?|guatsap|wasap|wsap)\b/i;
const composition = /\b(?:prepar\w*|mand\w*|escrib\w*|envia\w*|redact\w*|respond\w*|contesta\w*|hazme|quiero|crea\w*|mensaje|seguimiento)\b/;

export function whatsAppIntent(text) {
  const t=foldWa(text);
  // «Enviar WhatsApp Business» es el único comando que puede enviar; es distinto del
  // comando personal, que solo devuelve el enlace para abrir la app del vendedor.
  const command=/^(confirmar borrador whatsapp|descartar whatsapp|enviar whatsapp business)\s+([a-f\d]{12})[.!]?$/.exec(t);
  if (command) return {kind:{'descartar whatsapp':'cancel','enviar whatsapp business':'send'}[command[1]] || 'confirm',id:command[2].toUpperCase()};
  if (!channel.test(t)) {
    if (/^(?:prepara(?:me)?|redacta(?:me)?|escribe(?:me)?) (?:un |el )?mensaje de seguimiento\b/.test(t) && !/\b(?:correo|email|sms|telegram)\b/.test(t)) return {kind:'compose'};
    return null;
  }
  if (/^(?:(?:por favor|coach)[, ]+)*(?:agend|reagend|program|mueve|revisa mis correos|responde al (?:primer|segund)|(?:prepara|redacta|envia) (?:un |el )?(?:e-?mail|correo))/.test(t)) return null;
  if (/^(?:confirmar|descartar) (?:borrador )?whatsapp\b/.test(t) || /\b(?:como puedo|como hago|ejemplo de|no (?:envies|mandes|quiero enviar)|no lo envies|podrias explicarme|se puede|conect\w*)\b/.test(t)) return {kind:'help'};
  if (!composition.test(t)) return {kind:'help'};
  return {kind:'compose'};
}

const international=/\+[\d(][\d ()\u00a0.-]*\d/g;
export function waNumber(value) {
  const text=String(value||'').trim();
  if (!/^\+(?:[\d ().\u00a0-]+)$/.test(text)) return '';
  const phone='+'+text.replace(/\D/g,'');
  return /^\+[1-9]\d{6,14}$/.test(phone)?phone:'';
}
function validName(value) {
  const name=value?.trim().replace(/[,.!;?\s]+$/u,'') || '';
  if (!name || name.length>100 || !/^[\p{L}'’ -]+$/u.test(name) || /\b(?:y|o|equipo|todos|todas|clientes|prospectos|preguntarle|decirle|seguimiento|manana|jueves)\b/.test(foldWa(name))) return '';
  return name;
}
export function waRequestParts(value) {
  const text=value.trim();
  const marker=/(?:\b(?:mensaje|texto|cuerpo)(?: exacto)?\s*:\s*|\b(?:d[ií]le que|que diga|dici[eé]ndole que)\s+|\b(?:para (?:preguntarle|decirle|invitarle)|preg[uú]ntale|inv[ií]tale|sobre)\b\s*|:\s*)/i.exec(text);
  const prefix=(marker?text.slice(0,marker.index):text).replace(/\b(?:por|en|desde) (?:mi |el )?(?:whats\s*app|wasap)(?: (?:personal|business))?/gi,'').trim();
  const suffix=marker?text.slice(marker.index+marker[0].length).trim():'';
  const purpose=marker&&/preguntarle|preg[uú]ntale|invitarle|inv[ií]tale|sobre|para decirle/i.test(marker[0])?marker[0]+suffix:'';
  const numbers=[...new Set((prefix.match(international)||[]).map(waNumber).filter(Boolean))];
  const namePart=/(?:^|\s)(?:para|al?|con)\s+(?:(?:mi |el |la |al )?(?:cliente|prospecto|contacto)\s+)?(.+)$/i.exec(prefix);
  const option=namePart?.[1]?.replace(/\s+(?:un|el) (?:mensaje|whats(?:app)?|wasap)\b.*$/i,'').replace(/\s+(?:para |de )?(?:dar(?:le)? )?seguimiento\b.*$/i,'');
  const ambiguousNumbers=(prefix.match(/\+/g)||[]).length>numbers.length || numbers.length>1;
  const ambiguousNames=/\b(?:y|o)\b/i.test(option||'') || /\b(?:todos|todas|clientes|prospectos)\b/i.test(prefix);
  return {to:numbers.length===1?numbers[0]:'',name:validName(option),body:purpose?'':suffix,purpose,
    ambiguous:ambiguousNumbers || ambiguousNames,mentionsPhone:/[\d+]/.test(prefix)};
}
export function recipientAnswer(text) {
  const parts=waRequestParts(text);
  if (!parts.to && !parts.ambiguous && !parts.name) parts.name=validName(text);
  return parts;
}

export async function composeWaBody({body,purpose}) {
  if (body?.trim()) return {body:body.trim()};
  const initial='Hola, te escribo para dar seguimiento. ¿Qué día y hora te conviene que hablemos?';
  if (!purpose) return {body:initial};
  try { return await rewriteSellerBody(initial,'Redacta un WhatsApp breve de seguimiento siguiendo esta instrucción del vendedor: '+purpose); }
  catch { return {question:'¿Qué texto quieres usar? Escríbelo después de «Mensaje:».'}; }
}
