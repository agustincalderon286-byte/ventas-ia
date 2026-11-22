import test from 'node:test';
import assert from 'node:assert/strict';
import {markCatalogPronunciations} from '../src/knowledge/catalog-pronunciation.js';
import {expandAddressWords, placeAliases} from '../src/knowledge/place-pronunciation.js';

// Lo que Jorge pronuncia: cada alias en lugar de su texto original.
const spoken=text=>markCatalogPronunciations(text).replace(/<sub alias="([^"]+)">[^<]+<\/sub>/g,'$1');

test('una dirección de Estados Unidos se lee con el rumbo, la calle y el estado completos y sin «th»',()=>{
  assert.equal(expandAddressWords('2345 W 127th St, Blue Island, IL 60406'),'2345 West 127 Street, Blue Island, Illinois 60406');
  assert.equal(expandAddressWords('1500 S Western Ave Chicago Heights IL 60411'),'1500 South Western Avenue Chicago Heights Illinois 60411');
  assert.equal(expandAddressWords('31st St y Pulaski Rd; después King Dr.'),'31 Street y Pulaski Road; después King Drive.');
  assert.equal(expandAddressWords('Cita el 1st de mayo en 22nd Pl'),'Cita el 1 de mayo en 22 Pl');
});

test('las abreviaturas fuera de una dirección no cambian',()=>{
  for (const text of ['Llama al Dr. García de St. Charles.','el Dr Ruiz','Llamar Dr. García mañana','opción N o S del menú','ES PARA MI HERMANA IN SITU','Vive en CA o en NY','pedido 12345 E 7']) {
    assert.equal(expandAddressWords(text),text==='pedido 12345 E 7' ? 'pedido 12345 East 7' : text);
  }
});

test('Jorge pronuncia ciudades y calles en inglés como suenan, sin anidar marcas con el catálogo',()=>{
  assert.equal(spoken('Lidia vive en 2345 W 127th St, Blue Island, IL 60406.'),'Lidia vive en 2345 Uést 127 Estrít, Blú Áiland, Ilinói 60406.');
  assert.equal(spoken('Pedro: 1500 S Western Ave, Chicago Heights, IL'),'Pedro: 1500 Sáut Uéstern Áveniu, Chicago Jáits, Ilinói');
  assert.equal(spoken('Royal Prestige en Cicero Ave, Cicero'),'Róial Prestísh en Sísero Áveniu, Sísero');
  // «Blue Island» gana a «Blue», y cada nombre recibe una sola marca.
  const marked=markCatalogPronunciations('Blue Island y Blue Network en Oak Lawn');
  assert.equal(marked,'<sub alias="Blú Áiland">Blue Island</sub> y <sub alias="Blú Nétuork">Blue Network</sub> en <sub alias="Óuk Lon">Oak Lawn</sub>');
  assert.doesNotMatch(marked,/<sub[^>]*>[^<]*<sub/);
  assert.ok(Object.values(placeAliases).every(alias=>/^[\p{L} ]+$/u.test(alias)));
});
