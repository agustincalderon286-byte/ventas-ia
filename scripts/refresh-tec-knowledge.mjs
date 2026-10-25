// Explicit opt-in refresh. Never run on a customer request or import unrelated libraries.
import fs from 'node:fs/promises';
import { buildProductsDocument, buildWarrantyDocument, buildHelpfulHintsDocument, fetchText, stripHtml, extractSupportHeader } from './fetch-royalprestige-site-knowledge.js';
const returnsUrl = 'https://www.royalprestige.com/apoyo/politca-de-devolucion';
const [products, warranty, care, returnsHtml] = await Promise.all([
  buildProductsDocument(), buildWarrantyDocument(), buildHelpfulHintsDocument(), fetchText(returnsUrl)
]);
const returnsText = stripHtml(extractSupportHeader(returnsHtml, '<h2>Política de Devolución</h2>'));
console.log({ products: products.productos.length, warranty: warranty.secciones.length, care: care.consejos.length, returnsChars: returnsText.length });
if (products.productos.length < 49 || warranty.secciones.length < 39 || care.consejos.length < 15 || !returnsText.includes('60 días') || !returnsText.includes('20 días')) throw new Error('Source validation failed; no files replaced');
const docs = {
  'royalprestige_productos_site.json': products,
  'royalprestige_garantia_site.json': warranty,
  'royalprestige_consejos_site.json': care,
  'tec_returns_us.json': { metadata: { source: returnsUrl, fetched_at: new Date().toISOString(), country: 'US' }, titulo: 'Devolución, reembolso e intercambio en Estados Unidos', contenido: returnsText }
};
for (const [name, doc] of Object.entries(docs)) await fs.writeFile(new URL('../src/data/'+name, import.meta.url), JSON.stringify(doc, null, 2)+'\n');
console.log(JSON.stringify({ products: products.productos.length, warranty: warranty.secciones.length, care: care.consejos.length, returnsChars: returnsText.length }));
