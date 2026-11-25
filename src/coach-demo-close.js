const limits = Object.freeze({
  outcome: 12, reason: 180, followUp: 180, cooksForCount: 2, foodSpendWeekly: 60,
  mealPrepTime: 80, drinkingWaterType: 80, cookingWaterType: 80, waterSpendWeekly: 60,
  familyPriority: 24, productInterest: 120, waterReading: 120, calcProduct: 120,
  calcPrice: 15, calcDown: 15, quotedMonthly: 35, saleAmount: 15, sellerNote: 240, productDiscussion: 1800
});
const labels = {
  outcome:'Resultado', reason:'Motivo', followUp:'Seguimiento acordado', cooksForCount:'Personas para quienes cocina',
  foodSpendWeekly:'Gasto semanal en alimentos', mealPrepTime:'Tiempo de preparación', drinkingWaterType:'Agua para tomar',
  cookingWaterType:'Agua para cocinar', waterSpendWeekly:'Gasto semanal en agua', familyPriority:'Prioridad familiar',
  productInterest:'Producto de interés', waterReading:'Lectura de agua en la visita', calcProduct:'Producto cotizado',
  calcPrice:'Precio cotizado', calcDown:'Anticipo cotizado', quotedMonthly:'Mensualidad mencionada',
  saleAmount:'Total de venta indicado', sellerNote:'Nota del vendedor', productDiscussion:'Productos y precios comentados'
};
const outcomeLabels = {venta:'Venta', follow_up:'Seguimiento', no_venta:'No venta'};
const pilotEmails = ['agustincalderon286@gmail.com','calderonrigoberto51@gmail.com'];
const fail = (status,message) => Object.assign(new Error(message),{status});
const objectId = v => typeof v === 'string' && /^[a-f\d]{24}$/i.test(v);
export const validCloseVisit = v => typeof v === 'string' && /^visit-[a-z\d-]{8,90}$/i.test(v);
export function inDemoClosePilot(auth, switched, allowed = pilotEmails) {
  const email = String(auth?.user?.email || '').trim().toLowerCase();
  return !switched && !!auth?.session?.tokenHash && pilotEmails.includes(email) && (allowed.includes(email) || allowed.includes('*')) &&
    (auth.session.accessMode === 'direct' || (!auth.session.accessMode && auth.user.accountType !== 'seat'));
}
const select = 'closeVisitId closeCrmId closeVersion closeConfirmed closeDraft closeAnswered knownOutcome report.closeFacts report.partial report.recordingGaps autoCloseReceipt.appointment.state autoCloseReceipt.survey.state autoCloseReceipt.productSummary autoCloseReceipt.review';
const cleanText = (value, max) => typeof value === 'string' ? value.replace(/[\u0000-\u001f<>]/gu, ' ').trim().slice(0, max) : '';

// Figures the automatic close did not save because the recording left them in doubt. Each one maps to
// the close field the seller's answer fills, so Jorge asks for exactly that value.
function closeDoubts(review) {
  const doubts = [];
  for (const item of Array.isArray(review) ? review : []) {
    if (item?.state !== 'preguntar') continue;
    const text = cleanText(item.text, 120), label = cleanText(item.label, 120);
    const heard = (Array.isArray(item.heard) ? item.heard : []).filter(v => typeof v === 'string' && /^\d{1,9}(?:\.\d{1,2})?$/.test(v)).slice(0, 3);
    let field = '';
    if (item.field === 'price') field = /\b(?:mes|mensual\w*)\b/i.test(text) ? 'quotedMonthly' : /\b(?:anticipo|enganche|entrada|inicial)\b/i.test(text) ? 'calcDown' : 'calcPrice';
    else if (item.field === 'followUp') field = 'followUp';
    else if (item.field === 'survey' && ['cooksForCount', 'foodSpendWeekly', 'waterSpendWeekly'].includes(label)) field = label;
    if (field && !doubts.some(d => d.field === field)) doubts.push({ field, label, text, heard });
  }
  return doubts;
}

function doubtQuestion({ field, label, text, heard }) {
  const other = heard.length && !heard.every(value => text.includes(value)) ? ' (la otra lectura oyó ' + heard.join(' o ') + ')' : '';
  if (field === 'followUp') return 'No me quedó clara la fecha u hora del seguimiento' + (label ? ' («' + label + '»)' : '') + '. ¿Para cuándo quedó?';
  if (field === 'calcPrice' || field === 'calcDown' || field === 'quotedMonthly') {
    const what = { calcPrice: 'el precio', calcDown: 'el anticipo', quotedMonthly: 'la mensualidad' }[field];
    return 'Del ' + (label || 'producto') + ' oí «' + text + '», pero pudo quedar cortado' + other + '. ¿Cuál fue ' + what + '?';
  }
  return 'En ' + (labels[field] || 'la encuesta').toLowerCase() + ' oí «' + text + '», pero no estoy seguro' + other + '. ¿Cuál fue?';
}

function shape(doc) {
  const suggested = Object.fromEntries(Object.entries(doc.report?.closeFacts || {}).filter(([key,val]) => Object.hasOwn(limits,key) && key !== 'sellerNote' && typeof val === 'string' && val));
  if (doc.autoCloseReceipt?.productSummary) suggested.productDiscussion = doc.autoCloseReceipt.productSummary;
  const declaredOutcome = {sale:'venta',follow_up:'follow_up',no_sale:'no_venta'}[doc.knownOutcome];
  return {visitId:doc.closeVisitId,crmId:String(doc.closeCrmId || ''),version:doc.closeVersion || 0,
    fields:{...suggested,...doc.closeDraft}, sellerFields:Object.keys(doc.closeDraft || {}), confirmed:doc.closeConfirmed || {}, declaredOutcome:declaredOutcome || '', answered:!!doc.closeAnswered,
    partial:Boolean(doc.report?.partial || doc.report?.recordingGaps?.length),
    ...(doc.autoCloseReceipt?.appointment?.state === 'saved' && { appointmentSaved:true }),
    ...(doc.autoCloseReceipt?.survey?.state === 'saved' && { surveySaved:true }),
    doubts:closeDoubts(doc.autoCloseReceipt?.review)};
}
function chosenFields(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw) || !Object.keys(raw).length) throw fail(422,'Elige los datos que quieres guardar.');
  const fields = {};
  for (const [key,value] of Object.entries(raw)) {
    if (!Object.hasOwn(limits,key) || typeof value !== 'string' || !value.trim() ||
        value.length > limits[key] || /[\u0000-\u001f]/u.test(key === 'productDiscussion' ? value.replace(/\r?\n/g,'') : value))
      throw fail(422,'Revisa el campo '+(labels[key] || 'seleccionado')+'.');
    const val = value.trim();
    if (key === 'outcome' && !Object.hasOwn(outcomeLabels,val)) throw fail(422,'Elige un resultado válido.');
    if (key === 'cooksForCount' && (!/^\d{1,2}$/.test(val) || +val < 1 || +val > 50)) throw fail(422,'Indica entre 1 y 50 personas.');
    if (['calcPrice','calcDown','saleAmount'].includes(key) && (!/^\d{1,9}(?:\.\d{1,2})?$/.test(val) || (key !== 'calcDown' && +val === 0))) throw fail(422,'Revisa el importe de '+labels[key]+'. Usa números y hasta dos decimales.');
    fields[key] = val;
  }
  return fields;
}
export function createDemoCloseService(analyses) {
  const own = (owner,id) => ({ownerUserId:owner,_id:id,status:'completed',closeVisitId:{$type:'string',$ne:''}});
  async function read(owner,id) {
    if (!objectId(id)) throw fail(404,'Este cierre no está disponible.');
    const doc = await analyses.findOne(own(owner,id)).select(select).lean();
    if (!doc) throw fail(404,'Este cierre no está disponible en tu cuenta.');
    return shape(doc);
  }
  return {
    read,
    async confirm(owner,id,{visitId,version,fields:input}={}) {
      if (!validCloseVisit(visitId) || !Number.isSafeInteger(version) || version < 0) throw fail(400,'Reabre el cierre de la visita.');
      const fields = chosenFields(input);
      const before = await read(owner,id);
      if (before.visitId !== visitId) throw fail(409,'La visita cambió. Revisa el cierre original.');
      const unchanged = close => Object.entries(fields).every(([key,val]) => close.confirmed[key] === val);
      if (unchanged(before)) return before;
      if (before.version === version) {
        const merged = {...before.confirmed,...fields};
        if (fields.outcome && fields.outcome !== 'venta' && !fields.saleAmount) delete merged.saleAmount;
        if (merged.saleAmount && merged.outcome !== 'venta') throw fail(422,'Para registrar un monto de venta, selecciona también Venta. Una cotización va en Precio cotizado.');
        const doc = await analyses.findOneAndUpdate({...own(owner,id),closeVisitId:visitId,closeVersion:version},
          {$set:{closeConfirmed:merged,closeAnswered:true},$inc:{closeVersion:1}},{new:true}).select(select).lean();
        if (doc) return shape(doc);
        const latest = await read(owner,id);
        if (unchanged(latest)) return latest;
      }
      throw fail(409,'El cierre fue actualizado. Revisa la versión guardada antes de confirmar.');
    },
    async inbox(owner) {
      const rows = await analyses.find({ownerUserId:owner,status:'completed',closeVisitId:{$type:'string',$ne:''},closeAnswered:{$ne:true}})
        .sort({startedAt:-1}).limit(3).select(select+' startedAt').lean();
      return rows.map(doc=>({id:String(doc._id),startedAt:doc.startedAt,close:shape(doc)}));
    },
    async clarify(owner,id,{visitId,version,text,conflictField}={},extract) {
      const close = await read(owner,id);
      if (close.visitId!==visitId || version!==close.version) throw fail(409,'El cierre cambió. Vuelve a abrir sus preguntas.');
      if (typeof text!=='string' || !text.trim() || text.length>2000) throw fail(422,'Envía una aclaración breve de esta visita.');
      let questions=demoCloseQuestions(close);
      const priorFields={...(close.declaredOutcome && {outcome:close.declaredOutcome}),...close.fields,...close.confirmed};
      if (typeof conflictField==='string' && Object.hasOwn(labels,conflictField) && Object.hasOwn(close.fields,conflictField) && !close.confirmed[conflictField]) {
        delete priorFields[conflictField]; // Either conflicting value can be the seller’s answer.
        questions=[...questions.filter(q=>q.field!==conflictField).slice(0,1),{field:conflictField,question:'¿Qué valor debe quedar para '+labels[conflictField]+' en esta visita?'}];
      }
      const proposal=await extract(text.trim(),{questions,fields:priorFields});
      const suggested={...proposal?.fields,...(proposal?.note && {sellerNote:proposal.note})};
      if (!Object.keys(suggested).length) return {close,understood:false};
      const fields=chosenFields(suggested);
      // An answered price belongs to the product the recording named, unless the seller already chose one.
      const priced=(close.doubts || []).find(d=>['calcPrice','calcDown','quotedMonthly'].includes(d.field) && fields[d.field] && d.label);
      if (priced && !close.fields.calcProduct && !close.confirmed.calcProduct && !fields.calcProduct) fields.calcProduct=priced.label;
      const set=Object.fromEntries(Object.entries(fields).map(([key,val])=>['closeDraft.'+key,val]));
      const doc=await analyses.findOneAndUpdate({...own(owner,id),closeVisitId:visitId,closeVersion:version},
        {$set:set,$inc:{closeVersion:1}},{new:true}).select(select).lean();
      if (!doc) throw fail(409,'Esta ficha cambió mientras preparabas la respuesta. Revisa el cierre.');
      return {close:shape(doc),understood:true,changed:fields};
    },
    async forChat(owner,visitId,crmId,id) {
      if (!validCloseVisit(visitId) || (crmId && !objectId(crmId)) || (id && !objectId(id))) return null;
      const query = {ownerUserId:owner,closeVisitId:visitId,closeCrmId:crmId || '',status:'completed'};
      if (id) query._id=id;
      const rows = await analyses.find(query).sort({startedAt:-1}).limit(2).select(select).lean();
      return rows.length===1 ? shape(rows[0]) : null;
    }
  };
}
export function demoCloseQuestions(close) {
  const f={...close.fields,...close.confirmed};
  const questions=[];
  // First the figures the recording left in doubt, until the seller answers them.
  for (const doubt of close.doubts || []) {
    if (f[doubt.field] || close.sellerFields.includes(doubt.field) || (doubt.field==='followUp' && close.appointmentSaved)) continue;
    questions.push({field:doubt.field,question:doubtQuestion(doubt)});
  }
  if (close.declaredOutcome && f.outcome && !close.confirmed.outcome && !close.sellerFields.includes('outcome') && f.outcome!==close.declaredOutcome)
    questions.push({field:'outcome',question:'El resultado indicado y lo que recogió el audio difieren. ¿Cómo terminó la visita?'});
  else if (!f.outcome && !close.declaredOutcome)
    questions.push({field:'outcome',question:'¿Cómo terminó la visita: hubo venta, quedó seguimiento o decidieron dejarlo aquí?'});
  const outcome=f.outcome || close.declaredOutcome;
  if (outcome==='venta' && !f.saleAmount && !/monto|importe/i.test(f.sellerNote || '')) questions.push({field:'saleAmount',question:'¿En cuánto quedó la venta, o dejamos el monto pendiente?'});
  const asked=field=>questions.some(q=>q.field===field);
  if (!close.appointmentSaved && outcome==='follow_up' && !f.followUp && !asked('followUp')) questions.push({field:'followUp',question:'¿Qué prometiste hacer y para cuándo quedó el seguimiento?'});
  else if (!close.appointmentSaved && outcome==='follow_up' && f.followUp && !asked('followUp') && !close.confirmed.followUp && !close.sellerFields.includes('followUp') &&
      !/\b(?:\d{1,2}[:.]\d{2}|a las? (?:\d+|una|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce))\b/i.test(f.followUp))
    questions.push({field:'followUp',question:'El seguimiento quedó como «'+f.followUp+'». ¿Acordaron una hora o queda abierta?'});
  return questions.slice(0,2);
}
export function demoCloseText(close) {
  if (!close) return '';
  const describe = (obj) => Object.entries(obj).filter(([key])=>Object.hasOwn(labels,key))
    .map(([key,value])=>labels[key]+': '+(key==='outcome'?outcomeLabels[value]:value)).join('\n');
  const pending = {...close.fields};
  if (!pending.outcome && close.declaredOutcome) pending.outcome=close.declaredOutcome;
  for (const key of Object.keys(close.confirmed)) delete pending[key];
  return [close.partial && 'La grabación fue parcial.', describe(close.confirmed) && 'Confirmado por el vendedor\n'+describe(close.confirmed),
    describe(pending) && 'Propuesta pendiente de confirmar\n'+describe(pending)].filter(Boolean).join('\n\n') || 'La grabación no dejó datos de cierre suficientes.';
}
export function registerDemoClose(app,{active,allowed,service,extract}) {
  const route='/api/coach/demo-analyses/:id/close';
  function handler(action) {
    return async (req,res) => {
      res.set('Cache-Control','private, no-store').vary('Cookie');
      const auth = await active(req,res);
      if (!auth) return;
      if (!allowed(auth,req)) return res.status(403).json({error:'Usa tu propia sesión del piloto.'});
      if (req.method!=='GET' && (req.get('Sec-Fetch-Site') ? req.get('Sec-Fetch-Site')!=='same-origin' : req.get('Origin') !== req.protocol+'://'+req.get('host'))) return res.status(403).json({error:'Confirma desde la página del Coach.'});
      try {
        const owner=auth.user._id;
        if (action==='inbox') {
          const items=await service.inbox(owner);
          return res.json({items:items.map(item=>({...item,questions:demoCloseQuestions(item.close)}))});
        }
        if (action==='clarify') {
          const result=await service.clarify(owner,req.params.id,req.body,extract);
          return res.json({...result,questions:demoCloseQuestions(result.close)});
        }
        const close=action==='confirm' ? await service.confirm(owner,req.params.id,req.body) : await service.read(owner,req.params.id);
        res.json({close,questions:demoCloseQuestions(close)});
      } catch (error) {
        res.status(error.status || 500).json({error:error.status?error.message:'No pude abrir o guardar el cierre. Puedes reintentarlo.'});
      }
    };
  }
  app.get('/api/coach/demo-close/inbox',handler('inbox'));
  app.get(route,handler('read'));
  app.patch(route,handler('confirm'));
  app.post(route+'/clarify',handler('clarify'));
}
