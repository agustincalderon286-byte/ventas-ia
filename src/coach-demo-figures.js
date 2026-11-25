import { containsDemoAmount, demoAmountsIn } from './coach-demo-close-facts.js';

// The Mac worker re-reads every sentence that mentions a price, phone, date or time with a second
// local model and measures audible sound that Whisper left untranscribed next to it. A figure is
// saved automatically only when both readings agree and nothing was lost beside it.
export const FIGURE_GAP_SECONDS = 0.45;
const fold = text => String(text || '').normalize('NFD').replace(/\p{M}/gu, '').replace(/\s+/g, ' ').trim().toLowerCase();
const words = text => fold(text).match(/[\p{L}\d]+/gu) || [];
const digitWords = { cero:'0', uno:'1', una:'1', dos:'2', tres:'3', cuatro:'4', cinco:'5', seis:'6', siete:'7', ocho:'8', nueve:'9' };
const digitsOf = text => fold(text).replace(/[\p{L}]+/gu, word => digitWords[word] ?? ' ').replace(/\D/g, '');
const weekdays = ['lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo', 'manana', 'pasado manana', 'hoy'];
// Whole amounts in a fragment, with digits or spoken ("setecientos noventa y nueve" is 799).
export function amountsIn(text) {
  return [...new Set(demoAmountsIn(String(text || '')).filter(Number.isFinite).map(String))];
}

function longestRun(a, b) {
  let best = 0;
  for (let i = 0; i < a.length; i++) for (let j = 0; j < b.length; j++) {
    let k = 0;
    while (a[i + k] && a[i + k] === b[j + k]) k++;
    if (k > best) best = k;
  }
  return best;
}

const present = {
  amount: (text, value) => containsDemoAmount(text, value),
  phone: (text, value) => digitsOf(text).includes(value),
  time: (text, value) => /^\d+$/.test(value) ? containsDemoAmount(text, value) : fold(text).includes(value)
};

// The sentence(s) of the worker that the quoted evidence came from.
function sentencesFor(chunk, quote) {
  const quoted = words(quote);
  return (chunk.voice || []).map((sentence, position) => ({ sentence, position }))
    .filter(({ sentence }) => {
      const heard = words(sentence.text);
      const run = longestRun(quoted, heard);
      return run >= Math.min(3, heard.length, quoted.length);
    });
}

// Locate a quote in the recording: chunk number plus seconds, for the seller's receipt.
export function demoQuoteMinute(chunks, segment, quote) {
  const chunk = chunks.find(item => Number(item.index) + 1 === segment);
  if (!chunk) return null;
  const offset = chunks.filter(item => item.index < chunk.index).reduce((sum, item) => sum + Number(item.durationSeconds || 0), 0);
  const [first] = sentencesFor(chunk, quote);
  return Math.floor((offset + Number(first?.sentence.start || 0)) / 60);
}

/**
 * Decide whether the figures inside a quoted fact can be saved without asking the seller.
 * Returns { state: 'ok' | 'doubt' | 'unchecked', reason?, heard? }.
 * 'unchecked' means the recording came from a worker without second readings (older format).
 */
export function checkDemoFigures(chunks, { segment, quote, kind, values }) {
  const chunk = chunks.find(item => Number(item.index) + 1 === segment);
  const wanted = (values || []).map(value => kind === 'time' ? fold(value) : String(value)).filter(Boolean);
  if (!chunk || !wanted.length) return { state: 'ok' };
  if (!Array.isArray(chunk.checks) || !Array.isArray(chunk.voice)) return { state: 'unchecked' };
  const sentences = sentencesFor(chunk, quote).filter(({ sentence }) => wanted.some(value => present[kind](sentence.text, value)));
  if (!sentences.length) return { state: 'doubt', reason: 'La cifra no aparece en la lectura con tiempos.' };
  const heard = new Set();
  for (const { sentence, position } of sentences) {
    const check = chunk.checks.find(item => item?.segment === position);
    if (!check || typeof check.second !== 'string' || !check.second.trim()) return { state: 'doubt', reason: 'Falta la segunda lectura de esta cifra.' };
    if (Number(check.gapBefore) >= FIGURE_GAP_SECONDS || Number(check.gapAfter) >= FIGURE_GAP_SECONDS) {
      return { state: 'doubt', reason: 'Se oyó voz sin transcribir junto a la cifra; pudo quedar incompleta.', heard: amountsIn(check.second) };
    }
    const before = fold(sentence.text);
    if (wanted.some(value => kind !== 'phone' && /(?:\.\.\.|…)\s*$/.test(before.slice(0, Math.max(0, before.search(/\d/)))))) {
      return { state: 'doubt', reason: 'La cifra viene justo después de una pausa cortada.', heard: amountsIn(check.second) };
    }
    for (const value of wanted) {
      if (present[kind](sentence.text, value) && !present[kind](check.second, value)) {
        amountsIn(check.second).forEach(item => heard.add(item));
        return { state: 'doubt', reason: 'La segunda lectura oyó otra cifra.', heard: [...heard] };
      }
    }
  }
  return { state: 'ok' };
}

// Guard for the reviewable close: an amount stays out of the draft when its readings disagree.
export function demoFigureGuard(chunks) {
  return (field, value, segment, quote) =>
    checkDemoFigures(chunks, { segment, quote, kind: 'amount', values: [String(Number(value))] }).state !== 'doubt';
}

// Values worth checking inside a fact: every amount in a price note, the hour and day of a
// follow-up, or the phone of a referral.
export function figuresOf(kind, text) {
  if (kind === 'phone') return [digitsOf(text)].filter(value => value.length >= 7);
  if (kind === 'time') {
    const folded = fold(text);
    return [...amountsIn(folded), ...weekdays.filter(day => new RegExp('\\b' + day + '\\b').test(folded))];
  }
  return amountsIn(text);
}
