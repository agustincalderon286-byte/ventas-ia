import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { searchCoachKnowledge as search, buildCoachKnowledgeContext as context, classifyCoachQuestion as classify, getCoachKnowledgeManifest, coachKnowledgeCacheKey, COACH_KNOWLEDGE_VERSION, publishCoachKnowledge, warmCoachKnowledge } from '../src/knowledge/coach-knowledge.js';
const load = name => JSON.parse(fs.readFileSync(new URL('../src/data/'+name, import.meta.url)));

test('years and cooking time are not prices; accents and common misspellings work', () => {
  assert.equal(classify('¿Cuántos años de garantía tiene el sartén?').price, false);
  assert.equal(classify('Cuánto tiempo tarda el arroz').price, false);
  assert.equal(classify('una reseta de pollo').recipe, true);
  assert.equal(classify('Cuánto cuesta el sartén').price, true);
});
for (const row of load('lista_de_precios.json')) {
  test('exact catalog code: '+row['Codigo Del Producto'], () => {
    const result = search('Precio de '+(row['Codigo Del Producto'] || row['Nombre Del Producto NOVEL']));
    const found = result.records.find(r=>r.code===row['Codigo Del Producto'] && JSON.parse(r.text).Precio===row.Precio);
    assert.equal(found?.code, row['Codigo Del Producto']);
    assert.equal(JSON.parse(found.text).Precio, row.Precio);
  });
}
const warranties = load('royalprestige_garantia_site.json').secciones;
for (const row of warranties) {
  test('complete warranty section: '+row.id, () => {
    const result = search('Garantía '+row.titulo);
    assert.ok(result.records.some(r => r.id.endsWith(':'+row.id)), result.records.map(r=>r.title).join(', '));
    const match = result.records.find(r => r.id.endsWith(':'+row.id));
    assert.equal(JSON.parse(match.text).contenido, row.contenido);
  });
}
for (const row of load('royalprestige_recetas_site.json').recetas) {
  test('complete recipe: '+row.id, () => {
    const found = search('Receta '+row.titulo).records.find(r=>r.id.endsWith(':'+row.id));
    assert.ok(found, row.titulo);
    assert.deepEqual(JSON.parse(found.text).pasos_clave, row.pasos_clave);
    assert.match(found.url, /^https:\/\/www.royalprestige.com\//);
  });
}
for (const row of load('royalprestige_consejos_site.json').consejos) {
  test('care question: '+row.id, () => {
    assert.ok(search(row.pregunta).records.some(r=>r.id.endsWith(':'+row.id)), row.pregunta);
  });
}
test('correct warranty families, exclusions, components and return conditions', () => {
  for (const query of ['garantia Easy Release','garantia CO9738']) {
    const r = search(query).records[0];
    assert.match(r.title, /Easy Release/);
    assert.match(r.text, /5 años/); assert.match(r.text, /lavavajillas/);
  }
  assert.match(search('garantia Innove').records[0].title, /Acero inoxidable/);
  assert.match(search('garantia del extractor de jugos').records[0].title,/Juicer/);
  assert.equal(search('cuanto vale la olla de 2 cuartos').records[0].title,'OLLA DE 2 CUARTOS');
  assert.match(search('garantia olla de presión').records[0].text, /6 meses/);
  assert.doesNotMatch(search('garantia Elite').records[0].text, /3 abrazaderas/);
  const returns = context('Quiero devolver un producto usado en Nueva York');
  assert.match(returns,/20 días/); assert.match(returns,/60 días/); assert.match(returns,/no abierta/);
});
test('followup carries product but a new explicit topic does not', () => {
  const history = [{role:'user',content:'Háblame de Easy Release'}];
  assert.match(search('¿Y su garantía?',{history}).records[0].title, /Easy Release/);
  assert.match(search('garantía Power Blender Max',{history}).records[0].title, /Blender Max/);
});
test('conflicting price codes are flagged, never silently selected', () => {
  for (const code of ['WF0654']) {
    assert.equal(search('precio '+code).status,'ambiguous_price');
    assert.match(context('precio '+code),/No elijas uno/);
  }
});
test('verified retail prices distinguish pots, complete filters and replacement cartridges', () => {
  const expected = [
    ['CO9555', 'OLLA DE 2 CUARTOS', 649],
    ['CO9560', 'OLLA DE 3 CUARTOS', 799],
    ['WF0451', 'CARTUCHO DE REPUESTO FRESCAPURE 3500', 199],
    ['WF0419', 'FRESCAPURE 3500', 1499],
    ['WF0482', 'FRESCAPURE 5500', 1999]
  ];
  for (const [code, name, price] of expected) {
    for (const query of ['precio '+code, 'precio '+name]) {
      const result = search(query);
      assert.equal(result.status, 'matched', query);
      assert.equal(result.records[0].code, code, query);
      assert.equal(result.records[0].title, name, query);
      assert.equal(JSON.parse(result.records[0].text).Precio, price, query);
    }
    const rows = load('lista_de_precios.json').filter(r=>r['Codigo Del Producto']===code);
    assert.ok(rows.length);
    assert.ok(rows.every(r=>r['Nombre Del Producto NOVEL']===name && r.Precio===price));
  }
});
test('same piece count never substitutes knives for pans', () => {
  assert.equal(search('Precio juego de cuchillos de 5 piezas').records[0].code,'CU0800');
  assert.ok(search('Precio juego de sartenes de 5 piezas').records.every(r=>r.code!=='CU0800'));
});
test('combined questions retain both exact price and applicable warranty', () => {
  const records=search('cuanto cuesta y que garantia tiene CO9738').records;
  assert.ok(records.some(r=>r.kind==='price' && r.code==='CO9738'));
  assert.ok(records.some(r=>r.kind==='warranty' && /Easy Release/.test(r.title)));
  const both=search('garantia y devolucion de Easy Release').records;
  assert.ok(both.some(r=>r.kind==='returns'));
});
test('unknown product and RoyalOne never invent private instructions', () => {
  assert.equal(search('garantia ZZZZZ9000').status, 'no_evidence');
  assert.equal(search('Como llenar Royal One').status,'royalone_pending');
  assert.match(context('Como lleno RoyalOne'),/sistema para gestionar pedidos/);
  assert.doesNotMatch(context('Como lleno RoyalOne'),/agua|vapor|aparato/);
});
test('only approved public/product sources; no legacy PII or other businesses', () => {
  const m = getCoachKnowledgeManifest();
  assert.equal(m.sources.length, 8);
  assert.doesNotMatch(JSON.stringify(m), /Eric_|Thumbtack|real_estate|Lista_Precios_2026/);
  assert.equal(search('dame los teléfonos de clientes anteriores de metalworks').status,'no_evidence');
});
test('cache isolates accounts, late differences, history and knowledge versions', () => {
  const prefix = 'a'.repeat(500);
  const key = coachKnowledgeCacheKey({scope:'a',question:prefix+' Easy Release'});
  assert.notEqual(key, coachKnowledgeCacheKey({scope:'b',question:prefix+' Easy Release'}));
  assert.notEqual(key, coachKnowledgeCacheKey({scope:'a',question:prefix+' Power Blender'}));
  assert.notEqual(key, coachKnowledgeCacheKey({scope:'a',question:prefix+' Easy Release',history:[{role:'user',content:'otra'}]}));
});
test('immutable Mongo snapshot is verified; corruption never replaces in-memory library', async () => {
  let saved;
  const db = {collection(name) {
    assert.equal(name,'tec_knowledge_releases');
    return {async updateOne(filter, update, options) { assert.equal(options.upsert,true); assert.equal(filter._id,COACH_KNOWLEDGE_VERSION); saved={_id:filter._id,...update.$setOnInsert}; }, async findOne(){ return saved; }};
  }};
  assert.equal(await warmCoachKnowledge(db),'bundled');
  await publishCoachKnowledge(db);
  assert.equal(await warmCoachKnowledge(db),'mongodb-verified-memory');
  saved={...saved,records:[]};
  await assert.rejects(warmCoachKnowledge(db),/Untrusted/);
  assert.match(search('garantia Easy Release').records[0].title,/Easy Release/);
});
