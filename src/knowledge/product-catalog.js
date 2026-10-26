import fs from 'node:fs';
import '../../public/coach/product-search.js';
export const productSearch=globalThis.TecProductSearch;
const read=name=>JSON.parse(fs.readFileSync(new URL('../data/'+name,import.meta.url),'utf8'));
const official=read('royalprestige_productos_site.json').productos;
export const retailProducts=read('lista_de_precios.json').filter(r=>r['Codigo Del Producto']?.trim()).map(r=>{
  const code=r['Codigo Del Producto'].trim();
  const doc=official.find(p=>p.codigo_producto===code);
  return {codigo_producto:code,nombre_producto:r['Nombre Del Producto NOVEL'].trim(),precio_base_catalogo:r.Precio,
    search_description:doc ? [doc.descripcion_corta,doc.descripcion_general].filter(Boolean).join(' ').slice(0,1600) : ''};
});
export const retailIndex=productSearch.createIndex(retailProducts);
const internal=read('codigos_productos_internos_2026.json');
// Explicit field projection: never import internal prices or arbitrary raw attributes.
export const internalCodeProducts=[...internal.productLikeCodes,...internal.partOrInternalCodes].map(r=>({code:String(r.code),name:String(r.description)}));
export const internalCodeIndex=productSearch.createIndex(internalCodeProducts);
export function findProducts(question,{parts=false,limit=5}={}) {
  const q=productSearch.normalize(question);
  const internalIntent=parts || productSearch.isPartQuery(q);
  const retail=productSearch.search(retailIndex,q,{limit});
  const internalMatches=internalIntent || /\b[a-z]{2}\d{3,5}\b/.test(q) ? productSearch.search(internalCodeIndex,q,{limit}) : [];
  return internalIntent ? internalMatches : retail.length ? retail : internalMatches;
}
