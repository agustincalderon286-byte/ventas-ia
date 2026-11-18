import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { performance } from 'node:perf_hooks';
import { productSearch, retailProducts, retailIndex, internalCodeProducts, findProducts } from '../src/knowledge/product-catalog.js';
import { searchCoachKnowledge, buildCoachKnowledgeContext, isCoachKnowledgeQuestion, getCoachKnowledgeDirectReply } from '../src/knowledge/coach-knowledge.js';

const cases = [
  ['licuadora',['ES0082','ES4200']],
  ['licuadira',['ES0082','ES4200']],
  ['batidora de mano',['ES4200']],
  ['licuadora inalambrica',['ES4200']],
  ['la que sirve para moler cafe',['ES0082']],
  ['para licuar en la olla',['ES4200']],
  ['aparato que corta verduras',['PR2614']],
  ['sacar jugos',['JU0038']],
  ['exprimir naranjas',['PR1459']],
  ['olla express',['CO1253','CO1258']],
  ['sarten que no se pega 10 pulgadas',['CO9738']],
  ['hacer palomitas',['PR0025']],
  ['juego de cuchillos de 5 piezas',['CU0800']],
  ['licuado al vacio',['PE0040']],
];
for (const [q,codes] of cases) test('shared alias/function: '+q,()=>{
  const results = productSearch.search(retailIndex,q);
  assert.deepEqual(results.map(r=>r.code).sort(), codes.slice().sort());
});

test('every retail identity has aliases and is reachable by its exact name/code',()=>{
  for(const row of retailIndex) {
    assert.ok(row.aliases.length>1,row.name);
    for(const query of [row.name,row.code]) assert.ok(productSearch.search(retailIndex,query,{limit:30}).some(r=>r.code===row.code),query);
  }
  assert.equal(productSearch.search(retailIndex,'CO99999').length,0);
  assert.equal(productSearch.search(retailIndex,'sarten antiadherente 99 pulgadas').length,0);
  assert.equal(productSearch.search(retailIndex,'olla express 8 litros').length,0);
  assert.ok(!productSearch.search(retailIndex,'juego de sartenes de 5 piezas').some(r=>r.code==='CU0800'));
  assert.equal(productSearch.search(retailIndex,'WF0654').length,2,'Keep conflicting identities visible');
});

test('browser and Node use identical results without network calls',()=>{
  const sandbox={};
  vm.runInNewContext(fs.readFileSync(new URL('../public/coach/product-search.js',import.meta.url),'utf8'),sandbox);
  const browser=sandbox.TecProductSearch;
  const cached=JSON.parse(JSON.stringify(retailProducts));
  const offline=browser.createIndex(cached);
  for(const [q] of cases) assert.equal(JSON.stringify(browser.search(offline,q).map(r=>r.code)),JSON.stringify(productSearch.search(retailIndex,q).map(r=>r.code)));
  const html=fs.readFileSync(new URL('../private/coach-app.html',import.meta.url),'utf8');
  assert.ok(html.indexOf('/coach/product-search.js')<html.indexOf('/coach/app.js'));
  const sw=fs.readFileSync(new URL('../public/coach/sw.js',import.meta.url),'utf8');
  assert.match(sw,/"\/coach\/product-search\.js\?v=20261025-alias1"/);
});

test('parts match component and dimensions without exposing costs or guessing compatibility',()=>{
  assert.equal(internalCodeProducts.length,465);
  assert.ok(internalCodeProducts.every(r=>Object.keys(r).sort().join(',')==='code,name'));
  for(const [q,code] of [['goma de la olla de presion','RP2287'],['aro de silicona innove 20 cm','SP0430'],['RP9714','RP9714'],['empaque PB MAX','RP3412']]) {
    assert.ok(findProducts(q).some(r=>r.code===code),q);
    assert.ok(searchCoachKnowledge(q).records.some(r=>r.kind==='part' && r.code===code),q);
  }
  const ctx=buildCoachKnowledgeContext('aro de silicona innove 20 cm');
  assert.match(ctx,/SP0430/);
  assert.match(ctx,/NO costos, precios, disponibilidad/);
  assert.doesNotMatch(ctx,/\.json|"Precio"|"cost"/);
  assert.ok(findProducts('goma de la olla de presion').every(r=>!/VALVULA|MANGO/.test(r.name)));
  assert.equal(findProducts('aro innove 99 cm').length,0);
});

test('Coach routes nicknames/functions to verified identities, asks instead of assuming',()=>{
  assert.match(getCoachKnowledgeDirectReply('Quiero la batidora de mano'),/POWER BLENDER GO/);
  assert.doesNotMatch(getCoachKnowledgeDirectReply('Quiero la batidora de mano'),/MAX/);
  assert.match(getCoachKnowledgeDirectReply('goma de la olla de presion'),/modelo y medida/);
  assert.equal(getCoachKnowledgeDirectReply('Cuanto cuesta CO9555'),'');
  assert.equal(getCoachKnowledgeDirectReply('batidora de mano',{history:[{role:'assistant',content:'¿Te refieres a la Power Blender Go?'}]}),'');
  for(const q of ['batidora de mano','aparato que corta verduras','goma de la olla de presion']) {
    assert.ok(isCoachKnowledgeQuestion(q));
    const ctx=buildCoachKnowledgeContext(q);
    assert.match(ctx,/Coincidencias de identidad/);
    assert.match(ctx,/pregunta brevemente/);
  }
  assert.ok(searchCoachKnowledge('batidora de mano').records.some(r=>r.code==='ES4200'));
  assert.ok(searchCoachKnowledge('la que sirve para moler cafe').records.some(r=>r.code==='ES0082'));
  assert.ok(searchCoachKnowledge('aparato que corta verduras').records.filter(r=>['product','price'].includes(r.kind)).every(r=>r.code==='PR2614'));
  assert.match(buildCoachKnowledgeContext('Busco la goma de la olla de presion'),/FALTA IDENTIFICACIÓN DEL EQUIPO/);
  const confirmed=searchCoachKnowledge('si esa',{history:[{role:'user',content:'batidora de mano'},{role:'assistant',content:'¿Te refieres a la Power Blender Go?'}]});
  assert.ok(confirmed.records.some(r=>r.code==='ES4200'));
  assert.doesNotMatch(buildCoachKnowledgeContext('¿Por qué mi olla tiene una capa blanca?'),/Coincidencias de identidad/);
});

test('Coach uses the internal catalog across the photographed parts conversation',()=>{
  const original={role:'user',content:'¿Cuál es el código para los aros de silicón del sartén de cinco piezas, del, ya, del cinco piezas?'};
  const history=[
    original,
    {role:'assistant',content:'¿A qué modelo o medida del sartén de 5 piezas te refieres?'},
    {role:'user',content:'A cualquiera, solamente quiero saber el código.'},
    {role:'assistant',content:'¿Te refieres al sartén de 5 piezas o a otra línea de sartenes?'}
  ];
  const expected=[/RP9713.*Easy Release.*18 cm/,/RP9714.*Easy Release.*20 cm/];
  for(const answer of [original.content,'A cualquiera, solamente quiero saber el código.','¿Qué modelos hay disponibles?','El modelo ER.','A los modelos de tapas de 18 centímetros, 20 centímetros.','Respuestas, códigos.']){
    const reply=getCoachKnowledgeDirectReply(answer,{history});
    expected.forEach(pattern=>assert.match(reply,pattern,answer));
    assert.doesNotMatch(reply,/RP3412|RP2287|PB Max|\?/);
    assert.match(reply,/Lista interna de repuestos y códigos TEC/);
  }
  const withSizes=[...history,
    {role:'user',content:'A las tapas de 18 cm y 20 cm.'},
    {role:'assistant',content:'¿Qué necesitas exactamente, códigos o compatibilidad?'}
  ];
  const matches=searchCoachKnowledge('Códigos.',{history:withSizes});
  assert.deepEqual(matches.records.map(record=>record.code).sort(),['RP9713','RP9714']);
  const single=getCoachKnowledgeDirectReply('A las tapas de 20 cm.',{history});
  assert.match(single,expected[1]);
  assert.doesNotMatch(single,/RP9713/);
  assert.match(getCoachKnowledgeDirectReply('A las tapas de 99 cm.',{history}),/no tiene un código verificado/);
  assert.doesNotMatch(getCoachKnowledgeDirectReply('Necesito una olla de 20 cm.',{history}),/RP9714/);
  assert.doesNotMatch(getCoachKnowledgeDirectReply('A cualquiera.',{
    history:[...history,{role:'user',content:'¿Qué recetas puedo hacer con pollo?'},{role:'assistant',content:'¿Al horno o al sartén?'}]
  }),/RP9714/);
});

test('parts lists stay in the requested component and equipment family',()=>{
  const multi=getCoachKnowledgeDirectReply('Códigos de los aros de silicona para Easy Release de 18 y 20 cm');
  assert.match(multi,/RP9713.*18 cm/);
  assert.match(multi,/RP9714.*20 cm/);
  const blender=getCoachKnowledgeDirectReply('Código del aro de silicona de la tapa Power Blender Max');
  assert.match(blender,/RP3412/);
  assert.doesNotMatch(blender,/RP9714|RP2287/);
  const equipmentParts=getCoachKnowledgeDirectReply('¿Qué repuestos hay para la Power Blender Max?');
  assert.match(equipmentParts,/Power Blender Max/);
  assert.doesNotMatch(equipmentParts,/RP9714|RP2287|folleto/);
  assert.equal(getCoachKnowledgeDirectReply('¿Es compatible el aro de silicona de la tapa de 20 cm con mi sartén?'),'');
});
test('500 local lookups retain low latency',()=>{
  const times=[];
  for(let i=0;i<500;i++) { const start=performance.now();findProducts(cases[i%cases.length][0]);times.push(performance.now()-start); }
  times.sort((a,b)=>a-b);
  assert.ok(times[475]<30,'p95 '+times[475]+'ms');
});
