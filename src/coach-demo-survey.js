import { containsDemoAmount } from './coach-demo-close-facts.js';

const fold = value => String(value || '').normalize('NFD').replace(/\p{M}/gu,'').replace(/\s+/g,' ').trim().toLowerCase();
const yesNo = ['Si', 'No'];
const water = ['Llave', 'Botella', 'Filtro'];
const fields = {
  secondName: { cue:/\b(?:espos[oa]|pareja|segund[oa])\b/, max:120 },
  workingStatus: { cue:/\btrabaj\w*\b/, options:['Solo esposo','Solo esposa','Los dos'], aliases:[/\bsolo (?:mi )?esposo\b/,/\bsolo (?:mi )?esposa\b/,/\b(?:los dos|ambos)\b/] },
  heardRoyal: { cue:/\b(?:royal prestige|escuch\w*|opini\w*)\b/, max:260 },
  familyPriority: { cue:/\b(?:prioridad|importante|prioriz\w*)\b/, options:['Tiempo','Dinero','Salud'] },
  qualityReason: { cue:/\bcalidad\b/, options:['Es una inversion duradera','Ahorra dinero','Es una decision inteligente'], aliases:[/\b(?:inversion duradera|dura mucho)\b/,/\b(?:ahorra dinero|ahorr[oa]r)\b/,/\bdecision inteligente\b/] },
  productLikingScore: { cue:/\b(?:gust\w*|calific\w*|puntua\w*)\b/, bounds:[1,10] },
  cooksForCount: { cue:/\b(?:cocin\w*|comensales)\b/, bounds:[1,50] },
  foodSpendWeekly: { cue:/(?=.*\b(?:comida|alimentos|mandado|supermercado)\b).*\b(?:seman\w*|a la semana)\b/, max:60 },
  mealPrepTime: { cue:/\b(?:cocin\w*|prepar\w*|comida)\b/, max:80 },
  cookingMaterials: { cue:/\b(?:cocin\w*|sarten\w*|ollas?|utensilios?|bateria)\b/, many:true, options:['Aluminio','Teflon','Hierro','Vidrio','Acero inoxidable'] },
  familyConditions: { cue:/\b(?:familia|casa|tenemos|tiene|tengo|padec\w*)\b/, many:true, options:['Diabetes','Colesterol','Presion alta','Problemas cardiacos','Anemia'],
    aliases:[/\bdiabetes\b/,/\bcolesterol\b/,/\b(?:presion alta|hipertension)\b/,/\b(?:problemas cardiacos|problemas del corazon)\b/,/\banemia\b/] },
  lowFatHealthy: { cue:/\b(?:grasa|grasas|aceite)\b/, options:yesNo, statement:/\b(?:bajo en grasa|sin (?:grasa|aceite)).{0,28}\b(no )?es (?:saludable|sano)\b/ },
  lowFatHealthyReason: { cue:/\b(?:grasa|grasas|aceite)\b/, max:220, parent:'lowFatHealthy' },
  cookwareAffects: { cue:/(?=.*\b(?:ollas?|sarten\w*|utensilios?)\b).*\b(?:afect\w*|salud|influ\w*)\b/, options:yesNo },
  cookwareAffectsReason: { cue:/\b(?:ollas?|sarten\w*|utensilios?)\b/, max:220, parent:'cookwareAffects' },
  qualityInterest: { cue:/\bcalidad\b/, options:yesNo },
  qualityInterestReason: { cue:/\bcalidad\b/, max:220, parent:'qualityInterest' },
  drinkingWaterType: { cue:/\b(?:tom\w*|beb\w*)\b/, options:water, aliases:[/\b(?:llave|grifo)\b/,/\b(?:botell\w*|garrafon)\b/,/\b(?:filtro|filtrada|purificador)\b/] },
  cookingWaterType: { cue:/\bcocin\w*\b/, options:water, aliases:[/\b(?:llave|grifo)\b/,/\b(?:botell\w*|garrafon)\b/,/\b(?:filtro|filtrada|purificador)\b/] },
  tapWaterConcern: { cue:/(?=.*\bagua\b).*\b(?:contamina\w*|preocup\w*)\b/, options:yesNo, statement:/\b(no )?(?:me|nos) preocupa\w*\b/ },
  waterSpendWeekly: { cue:/(?=.*\bagua\b).*\b(?:seman\w*|a la semana)\b/, max:60 },
  likesNaturalJuices: { cue:/\bjugos?\b/, options:yesNo, statement:/\b(no )?(?:me|nos) (?:gustan|encantan) los jugos\b/ },
  juiceFrequency: { cue:/\bjugos?\b/, options:['Semanal','Quincenal','Mensual'], aliases:[/\b(?:semanal|cada semana|a la semana)\b/,/\b(?:quincenal|cada quincena)\b/,/\b(?:mensual|cada mes|al mes)\b/] },
  creditProblems: { cue:/(?=.*\bcredito\b).*\b(?:problemas?|dificultades?)\b/, options:yesNo },
  creditImproveInterest: { cue:/(?=.*\bcredito\b).*\bmejor\w*\b/, options:yesNo },
  familyHealthInvestment: { cue:/(?=.*\bsalud\b).*\binvert\w*\b/, options:yesNo },
  weeklyBudget: { cue:/\b(?:seman\w*|a la semana|por semana)\b/, options:['$15','$30','$45','$60'] },
  monthlyBudget: { cue:/\b(?:mensual\w*|al mes|por mes)\b/, options:['$60','$120','$180','$240'] }
};

export const autoSurveyFields = Object.keys(fields);
export const autoSurveyInstructions = 'En survey devuelve únicamente las respuestas explícitas del cliente a campos de la encuesta. En quote incluye la pregunta y la respuesta cuando un sí, no, cifra o categoría por sí sola no revela el significado. Usa los valores de la encuesta cuando corresponda y frases literales para texto libre. Omite sugerencias, preguntas sin respuesta y datos contradictorios. Trabaja con estos campos: ' + Object.entries(fields)
  .map(([name, spec]) => spec.options ? name + ' (' + spec.options.join(', ') + ')' : name).join('; ') +
  '. Cocina para cierto número de personas (cooksForCount), puntuación de gusto de 1 a 10 (productLikingScore), gasto real por semana (foodSpendWeekly y waterSpendWeekly), tiempo que tarda en cocinar (mealPrepTime) y presupuesto aceptado (weeklyBudget y monthlyBudget) son campos distintos. No deduzcas diagnósticos ni deudas. Usa secondName solo para la segunda persona de la encuesta.';

const privateNumbers = /@|https?:|\b(?:contrasena|seguro social|numero de cuenta|tarjeta)\b|(?:\d[ .()-]*){9,}/i;
const negated = /\b(?:no|nunca|jamas|tampoco|nadie|ningun[oa]?)\b/;
const yesNoReply = /^(?:si|no)\s*(?:\b|[,.!?])/;
const alternative = /\b(?:o (?:\d+|uno|una|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|quince|veint\w*|treinta|cuarenta|cincuenta)|entre \d+ (?:y|a) \d+)\b/;
const questionTail = quote => quote.slice(quote.lastIndexOf('?') + 1).trim().replace(/^[-–— ]+/, '');

export function verifiedAutoSurvey(candidates, getEvidence) {
  const found = {}, disputed = new Set();
  for (const candidate of Array.isArray(candidates) ? candidates.slice(0, 55) : []) {
    const field = candidate?.field;
    if (!Object.hasOwn(fields, field) || !Array.isArray(candidate.values)) continue;
    const spec = fields[field], quote = getEvidence(candidate);
    if (!quote || !spec.cue.test(quote) || privateNumbers.test(quote)) continue;
    const tail = questionTail(quote);
    if (!tail || alternative.test(tail)) continue;
    const prompt = quote.includes('?') ? quote.slice(0,quote.lastIndexOf('?')) : '';
    const accepted = /^(?:si|correcto|asi es)[.!]?$/.test(tail) && !negated.test(prompt) && !alternative.test(prompt);
    const observation = accepted && prompt ? prompt : tail;
    const chosen = [];
    for (const value of candidate.values.slice(0, spec.many ? spec.options.length : 1)) {
      if (typeof value !== 'string') continue;
      const text = value.trim();
      if (!text || /[\p{Cc}<>]/u.test(text) || privateNumbers.test(fold(text))) continue;
      if (spec.bounds) {
        const number = Number(text);
        const numericAnswer = field === 'productLikingScore' ? observation.replace(/\b(?:del? )?(?:1|uno) (?:al?|hasta) (?:10|diez)\b/g,'') : observation;
        if (/^\d{1,2}$/.test(text) && Number.isInteger(number) && number >= spec.bounds[0] && number <= spec.bounds[1] &&
            (field !== 'productLikingScore' || /\b(?:al|del|sobre|de)\s+(?:10|diez)\b|\b(?:10|diez)\s*(?:de calificacion|puntos)\b/.test(quote)) && containsDemoAmount(numericAnswer, number)) chosen.push(number);
      } else if (spec.options) {
        const index = spec.options.findIndex(option => fold(option) === fold(text));
        if (index < 0) continue;
        const option = spec.options[index];
        const statement = spec.statement?.exec(tail);
        const polarity = yesNoReply.test(tail) ? tail.match(/^(si|no)\b/)?.[1] : statement ? (statement[1] ? 'no' : 'si') : '';
        const matches = spec.options === yesNo ? polarity === fold(option) :
          option.startsWith('$') ? !negated.test(observation) && containsDemoAmount(observation, option.slice(1)) :
          !negated.test(observation) && (spec.aliases?.[index]?.test(observation) || observation.includes(fold(option)));
        if (matches) chosen.push(option);
      } else if (text.length <= spec.max && observation.includes(fold(text))) chosen.push(text);
    }
    if (!chosen.length) continue;
    const verified = spec.many ? [...new Set(chosen)].sort() : chosen[0];
    if (found[field] !== undefined && JSON.stringify(found[field]) !== JSON.stringify(verified)) disputed.add(field);
    found[field] = verified;
  }
  for (const field of disputed) delete found[field];
  for (const field of Object.keys(found)) if (fields[field].parent && !found[fields[field].parent]) delete found[field];
  return found;
}

export function mergeAutoSurvey(survey, answers) {
  const changes = {};
  const fields = Object.keys(answers).filter(field => Object.hasOwn(survey, field));
  const blank = value => value == null || value === '' || Array.isArray(value) && !value.length;
  for (const [field, value] of Object.entries(answers)) {
    if (blank(survey[field])) changes[field] = value;
  }
  const conflicts = fields.filter(field => !blank(survey[field]) && JSON.stringify(survey[field]) !== JSON.stringify(answers[field])).length;
  return { changes, conflicts };
}

export function createDemoSurveyWriter({ surveys, crm, transaction, summarize, normalizePhone, active }) {
  return async function save(analysis, answers, ownership) {
    if (!Object.keys(answers).length) return null;
    if (!analysis.closeCrmId || !ownership?.ownerUserId || !ownership.generatedByUserId ||
        String(ownership.ownerUserId) !== analysis.autoClose.crmOwner || String(ownership.generatedByUserId) !== String(analysis.ownerUserId)) {
      return { state:'client' };
    }

    return transaction(async session => {
      if (!await active(analysis)) return { state:'stopped' };
      const scope = { _id:analysis.closeCrmId, ownerUserId:ownership.ownerUserId };
      const record = await crm.findOne(scope).session(session).lean();
      const fullName = String(record?.leadName || '').trim().slice(0,120);
      const phone = normalizePhone(record?.phone || '');
      if (!fullName || !/^\d{10}$/.test(phone)) return { state:'client' };

      const surveyScope = { ownerUserId:ownership.ownerUserId };
      const query = record.linkedHealthSurveyId ? { ...surveyScope, _id:record.linkedHealthSurveyId } :
        { ...surveyScope, linkedCrmRecordId:record._id, generatedByUserId:ownership.generatedByUserId };
      const existing = await surveys.findOne(query).sort({ updatedAt:-1 }).session(session).lean();
      if (record.linkedHealthSurveyId && !existing) return { state:'review' };
      if (existing && (fold(existing.fullName) !== fold(fullName) || normalizePhone(existing.phone) !== phone ||
          (existing.linkedCrmRecordId && String(existing.linkedCrmRecordId) !== String(record._id)) ||
          (existing.generatedByUserId && String(existing.generatedByUserId) !== String(ownership.generatedByUserId)))) return { state:'review' };

      const { changes, conflicts } = mergeAutoSurvey(existing || {}, answers);
      const filled = Object.keys(changes).length;
      const now = new Date();
      let id = existing?._id;
      if (existing?.autoDemoVisitIds?.includes(analysis.closeVisitId)) return { state:'saved', id:String(id), filled:0, conflicts:0 };
      if (existing) {
        await surveys.updateOne({ ...surveyScope, _id:id }, {
          ...(filled > 0 && { $set:{ ...changes, summary:summarize({ ...existing, ...changes }), updatedAt:now } }),
          $addToSet:{ autoDemoVisitIds:analysis.closeVisitId }
        }, { session });
      } else {
        const survey = { ...ownership, linkedCrmRecordId:record._id, fullName, phone, ...changes,
          autoDemoVisitIds:[analysis.closeVisitId], createdAt:now, updatedAt:now };
        const [created] = await surveys.create([{ ...survey, summary:summarize(survey) }], { session });
        id = created._id;
      }
      if (!record.linkedHealthSurveyId) {
        // Update the link without changing the CRM status, recent note or sale result.
        await crm.updateOne(scope, { $set:{ linkedHealthSurveyId:id, linkedHealthSurveyName:fullName } }, { session });
      }
      return { state:'saved', id:String(id), filled, conflicts };
    });
  };
}
