import test from 'node:test';
import assert from 'node:assert/strict';
import { buildCoachKnowledgeContext as context, getCoachKnowledgeManifest, COACH_KNOWLEDGE_VERSION, applyCoachResponseStyle } from '../src/knowledge/coach-knowledge.js';

test('model context cites human labels without filenames, source IDs or release hashes', () => {
  for (const q of ['precio CO9555','garantía Easy Release','receta arroz con pollo','como limpiar una olla','devolucion en Nueva York','DocuCite','licuadora']) {
    const value=context(q);
    for(const file of getCoachKnowledgeManifest().sources) assert.ok(!value.includes(file),file);
    assert.ok(!value.includes(COACH_KNOWLEDGE_VERSION));
    assert.doesNotMatch(value,/<documento id=|archivo:|Estado: matched/);
    assert.match(value,/Fuente:/);
  }
  assert.match(context('precio CO9555'),/Fuente: Catálogo de precios TEC/);
  assert.match(context('precio CO9555'),/649/);
  assert.match(context('garantía Easy Release'),/5 años/);
});
test('old assistant citations are relabeled before another response without changing user text', () => {
  const messages=[{role:'assistant',content:'Fuente: lista_de_precios.json; royalprestige_garantia_site.json'}, {role:'user',content:'¿Qué es lista_de_precios.json?'}];
  const result=applyCoachResponseStyle({messages});
  assert.match(result.messages[0].content,/Catálogo de precios TEC/);
  assert.doesNotMatch(result.messages[0].content,/\.json/);
  assert.equal(result.messages[1].content,messages[1].content);
  assert.match(messages[0].content,/\.json/);
});
