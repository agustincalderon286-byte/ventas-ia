import { readFileSync } from 'node:fs';
import crypto from 'node:crypto';

export const readingCatalog = JSON.parse(readFileSync(new URL('./knowledge/reading-books.json', import.meta.url), 'utf8'));
const texts = new Map();
const escape = s => String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const safeJson = s => JSON.stringify(s).replace(/</g, '\\u003c');
export const readingPath = b => `/audiolibros/leer/${b.id}/`;
export function getReadingText(id) {
  if (!readingCatalog.some(b => b.id === id)) return null;
  if (!texts.has(id)) texts.set(id, JSON.parse(readFileSync(new URL(`./knowledge/reading-texts/${id}.json`, import.meta.url), 'utf8')));
  return texts.get(id);
}
export function readingCards(books = readingCatalog) {
  return books.map(b => `<article class="reading-card" data-reading-card data-search="${escape((b.title+' '+b.author+' '+b.categories.join(' ')).normalize('NFD').replace(/\p{Diacritic}/gu,'').toLowerCase())}"><a class="reading-cover" href="${readingPath(b)}" aria-label="Leer ${escape(b.title)}"><img src="${escape(b.cover)}" alt="Portada de ${escape(b.title)}" loading="lazy" decoding="async" width="600" height="600"></a><div class="reading-card-copy"><p class="reading-kind">LIBRO · ESPAÑOL</p><h3><a href="${readingPath(b)}">${escape(b.title)}</a></h3><p class="reading-author">${escape(b.author)}</p><p class="reading-description">${escape(b.description)}</p><p class="fine">${b.chapterCount} ${b.chapterCount===1?'sección':'secciones'} · Incluido en tu membresía</p><a class="button outline" href="${readingPath(b)}">Leer el libro <span aria-hidden="true">→</span></a></div></article>`).join('');
}
export function readingSection() {
  return `<section class="catalog reading-section" id="libros" role="tabpanel" aria-labelledby="reading-tab"><div class="wrap"><div class="section-head"><div><p class="eyebrow">Una historia, otra forma de disfrutarla</p><h2 id="reading-library-title">Libros para leer</h2></div><p>${readingCatalog.length} títulos en español. Lectura completa incluida en la misma membresía de $20 USD al mes.</p></div><label class="catalog-search"><span aria-hidden="true">⌕</span><input type="search" data-reading-search aria-label="Buscar libros para leer" placeholder="Buscar libro, autor o tema" autocomplete="off"></label><p class="fine" data-reading-count role="status">${readingCatalog.length} libros disponibles</p><div class="reading-grid">${readingCards()}</div><noscript><p>Todos los títulos se muestran aquí. Abre un libro para conocer su edición; la lectura completa requiere ingresar.</p></noscript></div></section>`;
}

export function readingBookHtml(book, publicOrigin = 'https://totalelitecoaching.com') {
  const origin = new URL(publicOrigin).origin, url=origin+readingPath(book);
  const schema={'@context':'https://schema.org','@type':'Book',name:book.title,author:{'@type':'Person',name:book.author},inLanguage:'es',bookFormat:'https://schema.org/EBook',bookEdition:book.edition,description:book.description,image:origin+book.cover,url,isAccessibleForFree:false,requiresSubscription:true,offers:{'@type':'Offer',price:'20.00',priceCurrency:'USD',url:origin+'/audiolibros/#membresia',description:'Membresía mensual de Audiolibros TEC. Incluye libros y audiolibros.'}};
  return `<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>${escape(book.title)} · Leer en español | Audiolibros TEC</title><meta name="description" content="${escape(book.description)}"><link rel="canonical" href="${url}"><meta property="og:title" content="${escape(book.title)} · Libros TEC"><meta property="og:description" content="${escape(book.description)}"><meta property="og:url" content="${url}"><meta property="og:type" content="book"><meta property="og:image" content="${origin+book.cover}"><meta name="twitter:card" content="summary_large_image"><link rel="icon" href="/audiolibros/identidad/favicon-96.png"><link rel="stylesheet" href="/audiolibros/estilos.css?v=libros-20261109"><link rel="stylesheet" href="/audiolibros/libros.css?v=1"><script type="application/ld+json">${safeJson(schema)}</script><script type="module" src="/audiolibros/libros.js?v=1"></script></head><body class="reading-page" data-book-id="${escape(book.id)}"><header class="reading-top"><a class="reading-back" href="/audiolibros/#libros">← Volver a Libros</a><a class="brand" href="/audiolibros/">Audiolibros TEC</a></header><main><section class="reading-intro wrap"><img src="${escape(book.cover)}" alt="Portada de ${escape(book.title)}" width="220" height="220"><div><p class="eyebrow">Tu biblioteca también se lee</p><h1>${escape(book.title)}</h1><p class="reading-author">${escape(book.author)}</p><p>${escape(book.description)}</p><p class="fine">${escape(book.edition)}</p>${book.audioId?`<a class="reading-audio-link" href="/audiolibros/?libro=${escape(book.audioId)}">También disponible como audiolibro →</a>`:''}</div></section><section class="wrap reading-access" data-reading-gate aria-live="polite"><p data-reading-status>Comprobando tu acceso…</p><p>La lectura completa está incluida en la membresía de Audiolibros TEC de $20 USD al mes.</p><div class="actions"><a class="button primary" data-reading-login href="/audiolibros/?lectura=${escape(book.id)}#cuenta">Ingresar para leer</a><a class="button outline" data-reading-subscribe href="/audiolibros/?lectura=${escape(book.id)}#membresia">Conocer la membresía</a></div></section><section class="reading-workspace" data-reading-workspace hidden><div class="reading-toolbar" aria-label="Herramientas de lectura"><label>Capítulo<select data-reading-chapter aria-label="Elegir capítulo"></select></label><div class="reading-size"><button type="button" data-font-small aria-label="Reducir tamaño de letra">A−</button><button type="button" data-font-large aria-label="Aumentar tamaño de letra">A+</button></div><label>Fondo<select data-reading-theme aria-label="Color de fondo de lectura"><option value="paper">Claro</option><option value="sepia">Sepia</option><option value="night">Oscuro</option></select></label><button type="button" class="reading-resume" data-reading-resume hidden>Retomar lectura</button></div><p class="reading-progress" data-reading-progress role="status"></p><article class="reading-paper" id="lectura" aria-labelledby="reading-chapter-title" tabindex="-1"><p class="reading-position" data-reading-position></p><h2 id="reading-chapter-title"></h2><div data-reading-body></div></article><nav class="reading-pager" aria-label="Cambiar capítulo"><button type="button" data-chapter-prev>← Anterior</button><span data-chapter-position></span><button type="button" data-chapter-next>Siguiente →</button></nav></section><aside class="wrap reading-edition"><details><summary>Acerca de esta edición</summary><p>${escape(book.credits)}</p>${book.sources.length?`<ul>${book.sources.map(s=>`<li><a href="${escape(s.url)}" rel="noopener noreferrer" target="_blank">${escape(s.label)}</a></li>`).join('')}</ul>`:''}<p>Se conserva la edición indicada. Algunas obras utilizan español y ortografía históricos.</p></details></aside><noscript><p class="wrap">Activa JavaScript para ingresar y abrir el lector por capítulos.</p></noscript></main><footer class="wrap"><a href="/audiolibros/#libros">Todos los libros</a><a href="/audiolibros/#catalogo">Audiolibros</a><a href="/audiolibros/contacto/">Contacto y soporte</a></footer></body></html>`;
}
export function registerReadingPages(app,{publicOrigin='https://totalelitecoaching.com'}={}) {
  app.get('/audiolibros/leer/:id', (req,res)=>{
    const b=readingCatalog.find(b=>b.id===req.params.id);
    if(!b)return res.status(404).set('X-Robots-Tag','noindex').send('Libro no encontrado.');
    if(!req.path.endsWith('/'))return res.redirect(301,readingPath(b));
    res.type('html').set('Cache-Control','public, max-age=300').send(readingBookHtml(b,publicOrigin));
  });
}
export function registerReadingApi({route,authorize,progressModel,catalog=readingCatalog,getText=getReadingText}) {
  const byId=new Map(catalog.map(b=>[b.id,b]));
  const key=(b,u)=>`${b.id}:${crypto.createHash('sha256').update(String(u._id)).digest('hex')}`;
  const find=(req,res)=>{const b=byId.get(req.params.id);if(!b)res.status(404).json({error:'No encontré ese libro.'});return b;};
  route('get','/lecturas/:id',async(req,res)=>{
    const u=await authorize(req,res);if(!u)return;const b=find(req,res);if(!b)return;
    const t=getText(b.id);const saved=progressModel?await progressModel.findById(key(b,u)).lean():null;
    res.json({book:{...b,chapters:t.chapters.map(c=>({title:c.title,paragraphCount:c.paragraphs.length}))},progress:saved?{chapter:saved.chapter,paragraph:saved.paragraph,updatedAt:saved.updatedAt?new Date(saved.updatedAt).getTime():0}:null,storageKey:key(b,u)});
  });
  route('get','/lecturas/:id/capitulos/:chapter',async(req,res)=>{
    if(!await authorize(req,res))return;const b=find(req,res);if(!b)return;
    const n=/^[1-9]\d{0,2}$/.test(req.params.chapter)?Number(req.params.chapter):0;
    const c=n&&getText(b.id).chapters[n-1];if(!c)return res.status(404).json({error:'No encontré ese capítulo.'});
    res.json({bookId:b.id,chapter:n,...c});
  });
  route('put','/lecturas/:id/progreso',async(req,res)=>{
    const u=await authorize(req,res);if(!u)return;const b=find(req,res);if(!b)return;
    const {chapter,paragraph}=req.body||{},t=getText(b.id);
    if(!Number.isInteger(chapter)||chapter<1||chapter>t.chapters.length||!Number.isInteger(paragraph)||paragraph<0||paragraph>=t.chapters[chapter-1].paragraphs.length)return res.status(400).json({error:'Posición de lectura inválida.'});
    if(!progressModel)return res.status(503).json({error:'No se pudo guardar el avance en la cuenta.'});
    try{await progressModel.updateOne({_id:key(b,u)},{$set:{bookId:b.id,chapter,paragraph,updatedAt:new Date()}},{upsert:true});}
    catch(e){if(e.code!==11000)throw e;await progressModel.updateOne({_id:key(b,u)},{$set:{chapter,paragraph,updatedAt:new Date()}});}
    res.json({saved:true});
  });
}
