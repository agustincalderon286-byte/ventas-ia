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
const replacementCodes = new Set(internal.partOrInternalCodes.map(item => item.code));
export function findProducts(question,{parts=false,limit=5}={}) {
  const q=productSearch.normalize(question);
  const internalIntent=parts || productSearch.isPartQuery(q);
  const retail=productSearch.search(retailIndex,q,{limit});
  const internalMatches=internalIntent || /\b[a-z]{2}\d{3,5}\b/.test(q) ? productSearch.search(internalCodeIndex,q,{limit}) : [];
  return internalIntent ? internalMatches : retail.length ? retail : internalMatches;
}

const partKinds = [
  /\b(?:aro|empaque|junta|goma|sello)\b/,
  /\b(?:mango|agarradera|asa|manija)\b/,
  /\bvalvula\b/,
  /\b(?:perilla|pomo)\b/,
  /\b(?:tapon|tornillo)\b/,
  /\b(?:cartucho|prefiltro|mineralizador)\b/,
  /\b(?:tapa|tapadera)\b/
];
const partFamilies = [
  [/\b(?:deluxe easy release|der)\b/, /\b(?:deluxe easy release|der)\b/],
  [/\b(?:easy release|er)\b/, /\b(?:easy release|er)\b/],
  [/\binnove\b/, /\b(?:innove|innov|innv)\b/],
  [/\bnovel\b/, /\bnovel\b/],
  [/\b(?:power blender|pb) max\b/, /\b(?:power blender|pb) max\b/],
  [/\b(?:power blender|pb) go\b/, /\b(?:power blender|pb) go\b/],
  [/\bpresion\b/, /\bpresion\b/]
];
const partMeasure = /\b(\d{1,3}(?:\.\d+)?(?:\s+(?:(?:y|o)\s+)?\d{1,3}(?:\.\d+)?)*)\s+(cm|pulgada|qt)\b/g;

export function normalizePartQuery(value) {
  return productSearch.normalize(value).replace(/\bsilicon\b/g, 'silicona')
    .replace(/\btapas\b/g, 'tapa').replace(/\bcentimetros?\b/g, 'cm');
}

export function catalogPartKind(query) {
  return partKinds.find(pattern => pattern.test(query));
}

export function isCatalogPartFamilyReply(query) {
  const label = query.replace(/^(?:(?:a|al|el|la|modelo|linea|de|del)\s+)+/, '').replace(/\.+$/, '');
  return partFamilies.some(([pattern]) => label.match(pattern)?.[0] === label);
}

export function hasCatalogPartMeasure(query) {
  return /\b\d{1,3}(?:\.\d+)?\s+(?:cm|pulgada|qt)\b/.test(query);
}

export function findInternalPartOptions(query) {
  const kind = catalogPartKind(query);
  // A follow-up may supply the equipment family after the initial part description.
  const family = partFamilies.map(entry => ({ entry, match: query.match(entry[0]) }))
    .filter(({ match }) => match)
    .sort((a, b) => (b.match.index + b.match[0].length) - (a.match.index + a.match[0].length))[0]?.entry;
  if (!kind && !family) return [];
  const glassLid = kind === partKinds[0] && (/\bvidrio\b/.test(query) ||
    (!family && /\b(?:sarten|tapa)\b/.test(query) && !/\b(?:licuadora|jarra|vaso|olla|blender)\b/.test(query)));
  const silicone = kind === partKinds[0] && /\bsilicona\b/.test(query);
  const verified = internalCodeIndex.filter(item => {
    const title = normalizePartQuery(item.name);
    if (kind && !kind.test(title)) return false;
    if (!kind && (!replacementCodes.has(item.code) ||
      !(partKinds.some(pattern => pattern.test(title)) || /\b(?:base|jarra|aspas|cuchilla|cable|presionador)\b/.test(title)))) return false;
    if (kind === partKinds.at(-1) && !/^tapa\b/.test(title)) return false;
    if (family && !family[1].test(title)) return false;
    if (glassLid && !/\btapa (?:de )?vidrio\b/.test(title)) return false;
    return !silicone || /\bsilicona\b/.test(title);
  });

  // Match each requested measurement independently. Bundle counts are not dimensions.
  const sizes = [...query.matchAll(partMeasure)].flatMap(match =>
    (match[1].match(/\d+(?:\.\d+)?/g) || []).map(amount => amount + ' ' + match[2]));
  const componentQuery = query.replace(partMeasure, ' ').replace(/\b(?:\d+|cinco)\s+pieza\b/g, '').replace(/\b(?:repuestos?|refaccion(?:es)?)\b/g, '');
  const measurements = [...new Set(sizes)];
  const matches = (measurements.length ? measurements : ['']).flatMap(size => {
    const candidates = size ? verified.filter(item => (' ' + normalizePartQuery(item.name) + ' ').includes(' ' + size + ' ')) : verified;
    return productSearch.search(candidates, componentQuery + ' ' + size, {limit:measurements.length > 1 ? 2 : 5});
  });
  return [...new Map(matches.map(item => [item.code, item])).values()];
}
