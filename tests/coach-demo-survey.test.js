import test from 'node:test';
import assert from 'node:assert/strict';
import { createDemoSurveyWriter, verifiedAutoSurvey } from '../src/coach-demo-survey.js';

test('la encuesta conserva respuestas respaldadas y evita confundir una pregunta, otra cifra o dos respuestas opuestas', () => {
  const survey = verifiedAutoSurvey([
    { field:'cooksForCount', values:['4'], quote:'¿Para cuántas personas cocina? Para cuatro.' },
    { field:'mealPrepTime', values:['45 minutos'], quote:'Tardo 45 minutos en preparar la comida.' },
    { field:'cookingMaterials', values:['Teflon','Vidrio','Hierro'], quote:'Cocino en ollas de vidrio y sartenes de teflón.' },
    { field:'likesNaturalJuices', values:['Si'], quote:'¿Les gustan los jugos naturales? Sí.' },
    { field:'tapWaterConcern', values:['No'], quote:'¿Le preocupan los contaminantes del agua? No.' },
    { field:'tapWaterConcern', values:['No'], quote:'No nos preocupan los contaminantes del agua.' },
    { field:'cookingWaterType', values:['Llave'], quote:'¿Para cocinar usa agua de la llave? Sí.' },
    { field:'weeklyBudget', values:['$15'], quote:'¿Podría pagar 30 dólares por semana? Quince a la semana sí.' },
    { field:'monthlyBudget', values:['$120'], quote:'¿Te convienen 120 dólares al mes?' },
    { field:'monthlyBudget', values:['$120'], quote:'Podría pagar 120 o 180 al mes.' },
    { field:'drinkingWaterType', values:['Botella'], quote:'Cuando la bebemos solo tomamos agua de botella.' },
    { field:'drinkingWaterType', values:['Filtro'], quote:'Para beber el agua usamos un filtro.' },
    { field:'familyConditions', values:['Diabetes'], quote:'Nadie en mi familia tiene diabetes.' },
    { field:'qualityReason', values:['Ahorra dinero'], quote:'Compro calidad porque ahorra dinero.' },
    { field:'secondName', values:['No corresponde'], quote:'Se habla de la demo.' }
  ], item => item.quote.normalize('NFD').replace(/\p{M}/gu,'').toLowerCase());
  assert.deepEqual(survey, { cooksForCount:4, mealPrepTime:'45 minutos', cookingMaterials:['Teflon','Vidrio'],
    likesNaturalJuices:'Si', tapWaterConcern:'No', cookingWaterType:'Llave', weeklyBudget:'$15', qualityReason:'Ahorra dinero' });
});

function fixture(existing) {
  const record = { _id:'lead', ownerUserId:'owner', leadName:'Ana Prueba', phone:'3125550112', status:'venta', lastNote:'Nota manual',
    ...(existing && { linkedHealthSurveyId:existing._id }) };
  const owner = { ownerUserId:'owner', generatedByUserId:'seller', ownerName:'Propietario' };
  const analysis = { closeCrmId:'lead', closeVisitId:'visit-testing', ownerUserId:'seller', autoClose:{ crmOwner:'owner' } };
  const session = {};
  const query = value => ({ sort() { return this; }, session(arg) { assert.equal(arg,session); return this; }, async lean() { return value; } });
  let survey = existing, updates = 0;
  const run = createDemoSurveyWriter({
    crm:{ findOne(filter) { return query(filter._id === record._id && filter.ownerUserId === record.ownerUserId ? record : null); },
      async updateOne(_filter, update, opts) { assert.equal(opts.session,session); Object.assign(record,update.$set); } },
    surveys:{ findOne() { return query(survey); },
      async create(rows, opts) { assert.equal(opts.session,session); assert.equal(survey,undefined); survey={...rows[0],_id:'survey'}; return [survey]; },
      async updateOne(_filter, update, opts) {
        assert.equal(opts.session,session); Object.assign(survey,update.$set);
        (survey.autoDemoVisitIds ||= []).push(update.$addToSet.autoDemoVisitIds);
        updates++;
      } },
    transaction: task => task(session), summarize: d => String(d.cooksForCount || ''), normalizePhone: value=>String(value).replace(/\D/g,''), active:async()=>true
  });
  return { run, analysis, owner, record, get survey() { return survey; }, get updates() { return updates; } };
}

test('crea una sola encuesta vinculada y un reintento conserva la corrección posterior del vendedor', async () => {
  const f = fixture();
  const result = await f.run(f.analysis,{ cooksForCount:4, topProducts:['Filtro'] },f.owner);
  assert.deepEqual(result,{ state:'saved', id:'survey', filled:2, conflicts:0 });
  assert.equal(f.record.linkedHealthSurveyId,'survey');
  assert.equal(f.record.status,'venta');
  assert.equal(f.record.lastNote,'Nota manual');
  assert.equal(f.survey.familyPriority,undefined);
  f.survey.cooksForCount=5;
  await f.run(f.analysis,{ cooksForCount:4 },f.owner);
  assert.equal(f.survey.cooksForCount,5);
  assert.equal(f.updates,0);
});

test('rellena los huecos de una encuesta manual y conserva los campos ya contestados', async () => {
  const f = fixture({ _id:'manual', fullName:'Ana Prueba', phone:'3125550112', linkedCrmRecordId:'lead', generatedByUserId:'seller',
    cooksForCount:2, topProducts:['Olla'], cookingWaterType:'' });
  const result = await f.run(f.analysis,{ cooksForCount:4, topProducts:['Filtro'], cookingWaterType:'Llave' },f.owner);
  assert.deepEqual(result,{ state:'saved', id:'manual', filled:1, conflicts:2 });
  assert.equal(f.survey.cooksForCount,2);
  assert.deepEqual(f.survey.topProducts,['Olla']);
  assert.equal(f.survey.cookingWaterType,'Llave');
  const invalid = await f.run(f.analysis,{ cookingWaterType:'Botella' },{ ...f.owner, generatedByUserId:'otro' });
  assert.equal(invalid.state,'client');
  assert.equal(f.updates,1);
});
