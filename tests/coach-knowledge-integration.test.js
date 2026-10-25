import test from 'node:test';
import assert from 'node:assert/strict';
process.env.NODE_ENV='test';
const { construirContextoEstaticoCoach, construirAiCacheKey, construirPromptModo } = await import('../server.js');
test('Coach integration uses full policies and no legacy selling scripts', () => {
  const value=construirContextoEstaticoCoach('¿Cuántos años de garantía tiene Easy Release?');
  assert.match(value,/5 años/); assert.match(value,/lavavajillas/);
  assert.doesNotMatch(value,/CALCULOS ESTIMADOS|EXPERIENCIA REAL|GUIA DEL COACH/);
  assert.match(construirPromptModo('coach'),/asistente de consulta/);
});
test('SKU and dimensions do not become dollar amounts; explicit base does', () => {
  assert.doesNotMatch(construirContextoEstaticoCoach('precio CO9738 de 10 pulgadas'),/CALCULOS ESTIMADOS/);
  assert.match(construirContextoEstaticoCoach('1000'),/Base 1000: tax 100, envio 50, total 1150/);
});
test('Coach cache version replaces legacy semantic prefixes and separates scope', () => {
  const q='cuanto cuesta el producto ';
  const a=construirAiCacheKey({mode:'coach',scope:'user-a',question:q+'CO9738'});
  assert.match(a,/^tec:consult:/);
  assert.notEqual(a,construirAiCacheKey({mode:'coach',scope:'user-a',question:q+'CO9739'}));
  assert.notEqual(a,construirAiCacheKey({mode:'coach',scope:'user-b',question:q+'CO9738'}));
});
