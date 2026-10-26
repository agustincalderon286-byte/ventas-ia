import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { construirPreciosRelevantesCoach, formatCoachCatalogProduct } from '../server.js';
import { retailProducts, productSearch } from '../src/knowledge/product-catalog.js';

test('online catalog and real offline fallback return identical code, photo and price',async()=>{
  const app=fs.readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');
  const source=app.slice(app.indexOf('let coachOfflineSearchProducts ='),app.indexOf('async function saveTecOfflineRecording'));
  let builds=0;
  const products=retailProducts.map(formatCoachCatalogProduct);
  const sandbox={loadCoachOfflineCatalog:async()=>products,TecProductSearch:{...productSearch,createIndex(rows){builds++;return productSearch.createIndex(rows);}}};
  vm.runInNewContext(source,sandbox);
  for(const q of ['licuadora','licuadira','batidora de mano','sarten que no se pega 10 pulgadas','aparato que corta verduras','CU0800','CO99999']) {
    const online=construirPreciosRelevantesCoach(q).coincidencias;
    const offline=await sandbox.searchCoachOfflineCatalog(q);
    assert.equal(JSON.stringify(offline),JSON.stringify(online),q);
    for(const r of online) {
      assert.ok(Number.isFinite(r.precio_base_catalogo));
      assert.ok(r.imagen_producto);
      assert.ok(!('cost' in r));
    }
  }
  assert.equal(builds,1,'Index reused across keystrokes');
});
