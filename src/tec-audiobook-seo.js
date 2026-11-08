import { existsSync, readFileSync } from 'node:fs';
import { audiobookCatalog } from './tec-audiobooks.js';
import { bookEditorial, audioCollections } from './knowledge/audiobook-editorial.js';
import { audiobookGuides } from './knowledge/audiobook-guides.js';

const publicRoot = new URL('../public/', import.meta.url);
const escape = value => String(value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const json = value => JSON.stringify(value).replace(/</g, '\\u003c');
export const bookPath = book => `/audiolibros/libros/${book.id}/`;
export const collectionPath = collection => `/audiolibros/colecciones/${collection.id}/`;
export const guidePath = guide => `/audiolibros/guias/${guide.id}/`;
export const sampleDuration = book => Math.floor(book.duration * 100) / 1000;
export const isoDuration = value => `PT${Math.round(value)}S`;
const time = value => {
  const n = Math.floor(value), h = Math.floor(n / 3600), m = Math.floor(n / 60) % 60;
  return `${h ? h + ':' + String(m).padStart(2, '0') : m}:${String(n % 60).padStart(2, '0')}`;
};

export function buildAudiobookSeo({
  catalog = audiobookCatalog, publicOrigin = 'https://totalelitecoaching.com',
  hasSample = book => existsSync(new URL(`audiolibros/muestras/${book.id}.m4a`, publicRoot)),
  googleVerification = '', bingVerification = ''
} = {}) {
  const origin = new URL(publicOrigin).origin;
  const absolute = path => new URL(path, origin).href;
  const library = absolute('/audiolibros/');
  const guideHub = '/audiolibros/guias/';
  const guides = audiobookGuides.map(g => {
    const books = g.bookIds.map(id => catalog.find(b => b.id === id)).filter(Boolean);
    return { ...g, books, cover: books.find(b => b.id === g.coverBookId) || books[0] };
  }).filter(g => g.books.length);
  const collections = audioCollections.map(c => ({ ...c, books: c.bookIds.map(id => catalog.find(b => b.id === id)).filter(Boolean) })).filter(c => c.books.length);
  const guideWords = g => [g.title, g.intro, ...g.sections.flatMap(s => [s.title, ...s.paragraphs, ...(s.bullets || [])])].join(' ').trim().split(/\s+/).length;
  const readingTime = g => Math.max(1, Math.ceil(guideWords(g) / 180));
  const guideCards = (items, headingTag = 'h3') => `<div class="guide-grid">${items.map(g => `<article class="guide-card"><a class="guide-card-cover" href="${guidePath(g)}" aria-label="Leer: ${escape(g.title)}"><img src="${escape(g.cover.cover)}" alt="Portada de ${escape(g.cover.title)}" width="600" height="800" loading="lazy" decoding="async"></a><div><p class="eyebrow">${escape(g.label)}</p><${headingTag}><a href="${guidePath(g)}">${escape(g.title)}</a></${headingTag}><p>${escape(g.description)}</p><span class="fine">${readingTime(g)} min de lectura aprox.</span><a class="guide-read-link" href="${guidePath(g)}">Leer la guía →</a></div></article>`).join('')}</div>`;
  const guideRecommendations = (items, wrapped = true) => items.length ? `<section class="${wrapped ? 'wrap ' : ''}detail-section"><p class="eyebrow">Para seguir explorando</p><h2>Guías para esta lectura</h2>${guideCards(items)}<p><a href="${guideHub}">Ver todas las guías y lecturas →</a></p></section>` : '';
  const guideTeaser = `<section class="wrap audio-guides" id="guias" aria-labelledby="guides-title"><p class="eyebrow">Encuentra tu próxima idea</p><h2 id="guides-title">Guías y lecturas</h2><p class="intro">Recomendaciones para elegir, conocer los libros y aprovechar cada escucha.</p>${guideCards(guides)}<p><a class="button outline" href="${guideHub}">Explorar Guías y lecturas →</a></p></section>`;
  const collectionLinks = items => `<div class="collection-links">${items.map(c => `<a href="${collectionPath(c)}">${escape(c.label)}</a>`).join('')}</div>`;
  const collectionCards = `<section class="wrap audio-collections" id="colecciones" aria-labelledby="collections-title"><p class="eyebrow">Elige tu camino</p><h2 id="collections-title">Colecciones para cada curiosidad</h2><div class="collection-grid">${collections.map(c => `<a class="collection-card" href="${collectionPath(c)}"><span class="eyebrow">${c.books.length} audiolibros</span><h3>${escape(c.label)}</h3><p>${escape(c.description)}</p><span class="collection-cta">Explorar colección →</span></a>`).join('')}</div></section>`;
  const organization = { '@type': 'Organization', '@id': library + '#organization', name: 'Audiolibros TEC', url: library,
    logo: absolute('/audiolibros/identidad/icon-512.png') };
  const website = { '@type': 'WebSite', '@id': library + '#website', name: 'Audiolibros TEC', alternateName: 'Biblioteca de audiolibros de Total Elite Coaching',
    url: library, inLanguage: 'es', publisher: { '@id': organization['@id'] } };
  const offer = { '@type': 'Offer', name: 'Membresía mensual Audiolibros TEC', url: library + '#membresia',
    price: '20.00', priceCurrency: 'USD', description: 'Acceso al catálogo completo por 20 USD al mes, con renovación mensual. La muestra inicial del 10 % es gratuita.',
    priceSpecification: { '@type': 'UnitPriceSpecification', price: '20.00', priceCurrency: 'USD', billingDuration: 'P1M' } };
  const audiobook = book => ({
    '@type': 'Audiobook', '@id': absolute(bookPath(book)) + '#audiobook', url: absolute(bookPath(book)),
    name: book.title, description: book.description, inLanguage: 'es', duration: isoDuration(book.duration),
    image: absolute(book.cover), bookFormat: 'https://schema.org/AudiobookFormat', bookEdition: book.edition,
    ...(book.categories.includes('Biblia') ? {} : { author: { '@type': 'Person', name: book.author } }),
    genre: book.categories, publisher: { '@id': organization['@id'] }, requiresSubscription: true, isAccessibleForFree: false,
    offers: offer,
    ...(hasSample(book) ? { hasPart: { '@type': 'AudioObject', name: `${book.title} — muestra gratuita del 10 %`,
      contentUrl: absolute(`/audiolibros/muestras/${book.id}.m4a`), encodingFormat: 'audio/mp4',
      duration: isoDuration(sampleDuration(book)), inLanguage: 'es', isAccessibleForFree: true, requiresSubscription: false } } : {})
  });
  function head({ title, description, url, image, graph, type = 'website' }) {
    return `<meta name="description" content="${escape(description)}">
      <title>${escape(title)}</title><link rel="canonical" href="${escape(url)}">
      <meta name="robots" content="index, follow, max-image-preview:large">
      ${googleVerification ? `<meta name="google-site-verification" content="${escape(googleVerification)}">` : ''}
      ${bingVerification ? `<meta name="msvalidate.01" content="${escape(bingVerification)}">` : ''}
      <meta property="og:type" content="${type}"><meta property="og:site_name" content="Audiolibros TEC"><meta property="og:locale" content="es_US">
      <meta property="og:title" content="${escape(title)}"><meta property="og:description" content="${escape(description)}">
      <meta property="og:url" content="${escape(url)}"><meta property="og:image" content="${escape(image)}"><meta property="og:image:alt" content="${escape(title)}">
      <meta name="twitter:card" content="summary_large_image"><meta name="twitter:title" content="${escape(title)}">
      <meta name="twitter:description" content="${escape(description)}"><meta name="twitter:image" content="${escape(image)}">
      <script type="application/ld+json">${json({ '@context': 'https://schema.org', '@graph': graph })}</script>`;
  }
  function card(book) {
    const path = bookPath(book);
    return `<article class="book"><a href="${path}" aria-label="${escape(book.title)}: ver audiolibro"><img src="${escape(book.cover)}" alt="Portada de ${escape(book.title)}" loading="lazy" decoding="async" width="600" height="800"></a><div>
      <p class="eyebrow">${escape(book.categories.join(' · '))}</p><h3><a href="${path}">${escape(book.title)}</a></h3><p class="author">${escape(book.author)}</p>
      <p class="book-description">${escape(book.description)}</p><p class="book-duration">Español · ${time(book.duration)}</p>
      <div class="actions"><a class="button outline" href="${path}">Ver libro</a>${hasSample(book) ? `<a class="button primary" data-audio-sample="${book.id}" href="/audiolibros/?libro=${book.id}">▶ Muestra · ${time(sampleDuration(book))}</a>` : ''}</div>
      </div></article>`;
  }
  const list = { '@type': 'ItemList', '@id': library + '#catalogo', name: 'Catálogo de audiolibros en español', numberOfItems: catalog.length,
    itemListElement: catalog.map((book, index) => ({ '@type': 'ListItem', position: index + 1, url: absolute(bookPath(book)), item: audiobook(book) })) };
  const landingHead = head({ title: 'Audiolibros en español: clásicos y Biblia | Audiolibros TEC',
    description: 'Escucha audiolibros en español de desarrollo personal, estrategia, filosofía y la Biblia. Prueba gratis el 10 % de cada libro, con lectura sincronizada. Membresía: $20 USD al mes.',
    url: library, image: absolute('/audiolibros/identidad/portada-social.jpg'),
    graph: [organization, website, { '@type': 'CollectionPage', '@id': library + '#webpage', url: library, name: 'Audiolibros en español · Audiolibros TEC',
      inLanguage: 'es', isPartOf: { '@id': website['@id'] }, mainEntity: { '@id': list['@id'] } }, list] });
  const template = readFileSync(new URL('audiolibros/index.html', publicRoot), 'utf8');
  const landing = template.replace(/<!-- seo:head:start -->[\s\S]*?<!-- seo:head:end -->/, landingHead)
    .replace('<div class="books" id="books"></div>', `<div class="books" id="books">${catalog.map(card).join('')}</div>`)
    .replace('<!-- audio:collections -->', collectionCards)
    .replace('<!-- audio:guides -->', guideTeaser)
    .replace('Cargando audiolibros…', 'Elige un libro para conocerlo y escuchar su muestra.');
  const baseHead = `<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"><meta name="theme-color" content="#102421">
    <link rel="icon" href="/audiolibros/identidad/favicon-96.png"><link rel="apple-touch-icon" href="/audiolibros/identidad/apple-touch-icon.png">
    <link rel="stylesheet" href="/audiolibros/estilos.css?v=6"><script type="module" src="/audiolibros/analytics.js?v=20261107-guides"></script>`;
  const nav = `<header class="top wrap"><a class="brand" href="/audiolibros/"><span><strong>Audiolibros TEC</strong><small>Total Elite Coaching</small></span></a><nav aria-label="Navegación principal"><a href="/audiolibros/#catalogo">Catálogo</a><a href="/audiolibros/#colecciones">Colecciones</a><a href="${guideHub}">Guías</a><a href="/audiolibros/#membresia">Membresía</a></nav></header>`;
  const footer = `<footer class="wrap"><span>Audiolibros TEC · Total Elite Coaching</span><a href="/audiolibros/#colecciones">Colecciones</a><a href="${guideHub}">Guías y lecturas</a><a href="/audiolibros/privacidad/">Privacidad</a><a href="/audiolibros/contacto/">Contacto y soporte</a></footer>`;
  const pages = new Map(catalog.map(book => {
    const path = bookPath(book), url = absolute(path), sample = hasSample(book);
    const description = `${book.title}, de ${book.author}: audiolibro en español. ${book.description}`;
    const breadcrumbs = { '@type': 'BreadcrumbList', itemListElement: [
      { '@type': 'ListItem', position: 1, name: 'Audiolibros TEC', item: library },
      { '@type': 'ListItem', position: 2, name: book.title, item: url }
    ] };
    const schemaBook = audiobook(book);
    const editorial = bookEditorial[book.id];
    const bookCollections = collections.filter(c => c.bookIds.includes(book.id));
    const related = catalog.filter(item => item.id !== book.id && item.categories.some(category => book.categories.includes(category))).slice(0, 3);
    return [book.id, `<!doctype html><html lang="es"><head>${baseHead}${head({ title: `${book.title} en español | Audiolibros TEC`, description, url,
      image: absolute(book.cover), type: 'book', graph: [organization, website, schemaBook, breadcrumbs,
        { '@type': 'WebPage', '@id': url + '#webpage', url, name: `${book.title} en español`, inLanguage: 'es', isPartOf: { '@id': website['@id'] }, mainEntity: { '@id': schemaBook['@id'] } }] })}</head>
      <body>${nav}<main class="wrap book-detail"><nav class="breadcrumbs" aria-label="Ruta de navegación"><a href="/audiolibros/">Audiolibros en español</a><span aria-hidden="true"> / </span><span>${escape(book.title)}</span></nav>
      <article><div class="book-detail-hero"><img class="detail-cover" src="${escape(book.cover)}" alt="Portada de ${escape(book.title)}" width="600" height="800" fetchpriority="high">
      <div><p class="eyebrow">${escape(book.categories.join(' · '))}</p><h1>${escape(book.title)}</h1><p class="intro">${escape(book.author)}</p>
      <p>${escape(book.description)}</p><p class="fine">Audiolibro en español · Duración ${time(book.duration)}<br>${escape(book.edition)}</p>
      <div class="actions"><a class="button primary" data-audio-sample="${book.id}" href="/audiolibros/?libro=${book.id}">${sample ? 'Escuchar y leer la muestra gratis' : 'Abrir en la biblioteca'}</a><a class="button outline" href="/audiolibros/#membresia">Ver membresía</a></div>
      <p class="fine">Escucha gratis el primer 10 %. El libro completo está incluido en la membresía de $20 USD al mes, con renovación mensual.</p></div></div>
      ${sample ? `<section class="detail-section" aria-labelledby="sample-title"><h2 id="sample-title">Escucha una muestra</h2><p>Primer 10 % del libro · ${time(sampleDuration(book))}. También puedes seguir la lectura sincronizada al abrirlo en la biblioteca.</p><audio controls preload="none" data-audio-preview="${book.id}" src="/audiolibros/muestras/${book.id}.m4a" aria-label="Muestra gratuita de ${escape(book.title)}"></audio></section>` : ''}
      ${editorial ? `<section class="detail-section"><p class="eyebrow">Una mirada al libro</p><h2>¿De qué trata ${escape(book.title)}?</h2>${editorial.summary.map(p => `<p>${escape(p)}</p>`).join('')}${book.originalTitle ? `<p class="fine">Título original: ${escape(book.originalTitle)}</p>` : ''}</section>
      <section class="detail-section"><h2>Ideas que encontrarás</h2><ul class="editorial-ideas">${editorial.ideas.map(idea => `<li>${escape(idea)}</li>`).join('')}</ul></section>
      <section class="detail-section editorial-audience"><div><h2>¿Para quién es?</h2><p>${escape(editorial.audience)}</p></div><aside><h3>Una forma de escucharlo</h3><p>${escape(editorial.tip)}</p></aside></section>` : ''}
      <section class="detail-section"><h2>Qué encontrarás en este audiolibro</h2><ol class="detail-chapters">${book.chapters.map(chapter => `<li>${escape(chapter.title)}</li>`).join('')}</ol></section>
      <section class="detail-section"><h2>La edición de TEC</h2><p>${escape(book.credits)}</p>${book.voiceDisclosure ? `<p>${escape(book.voiceDisclosure)}</p>` : ''}<p>Escucha a tu ritmo, ajusta la velocidad, sigue el texto sincronizado y comparte citas o fragmentos de audio de 15, 30 o 45 segundos con un enlace al libro.</p></section>
      ${editorial ? `<section class="detail-section book-faq"><h2>Antes de empezar</h2>${editorial.questions.map(([q, a]) => `<details><summary>${escape(q)}</summary><p>${escape(a)}</p></details>`).join('')}</section>` : ''}</article>
      ${bookCollections.length ? `<section class="detail-section"><h2>Encuéntralo en estas colecciones</h2>${collectionLinks(bookCollections)}</section>` : ''}
      ${related.length ? `<section class="detail-section"><h2>También puedes escuchar</h2><ul class="related-books">${related.map(item => `<li><a href="${bookPath(item)}">${escape(item.title)}</a><span>${escape(item.author)}</span></li>`).join('')}</ul></section>` : ''}
      <p><a href="/audiolibros/#catalogo">← Ver todos los audiolibros en español</a></p>${guideRecommendations(guides.filter(g => g.bookIds.includes(book.id)), false)}</main>${footer}</body></html>`];
  }));
  const collectionPages = new Map(collections.map(c => {
    const url = absolute(collectionPath(c));
    const list = { '@type': 'ItemList', '@id': url + '#libros', name: c.title, numberOfItems: c.books.length,
      itemListElement: c.books.map((b, i) => ({ '@type': 'ListItem', position: i + 1, url: absolute(bookPath(b)), item: audiobook(b) })) };
    return [c.id, `<!doctype html><html lang="es"><head>${baseHead}${head({ title: c.title + ' | Audiolibros TEC', description: c.description, url,
      image: absolute('/audiolibros/identidad/portada-social.jpg'), graph: [organization, website, { '@type': 'CollectionPage', '@id': url + '#webpage', name: c.title, url, inLanguage: 'es', description: c.description, mainEntity: { '@id': list['@id'] }, isPartOf: { '@id': website['@id'] } }, list,
        { '@type': 'BreadcrumbList', itemListElement: [{ '@type': 'ListItem', position: 1, name: 'Audiolibros TEC', item: library }, { '@type': 'ListItem', position: 2, name: c.label, item: url }] }] })}</head><body>${nav}<main>
      <section class="wrap collection-hero"><div><p class="eyebrow">${c.books.length} libros · En español</p><h1>${escape(c.title)}</h1><p class="intro">${escape(c.description)}</p><a class="button primary" href="#libros">Elegir mi próxima escucha ↓</a></div>
      <div class="collection-cover-stack" aria-hidden="true">${c.books.slice(0, 3).map(b => `<img src="${escape(b.cover)}" alt="" width="600" height="800">`).join('')}</div></section>
      <section class="wrap detail-section"><h2>Un recorrido por ${escape(c.label.toLowerCase())}</h2><p>${escape(c.intro)}</p><ul class="editorial-ideas">${c.ideas.map(idea => `<li>${escape(idea)}</li>`).join('')}</ul><aside class="collection-route"><h3>¿Por dónde empezar?</h3><p>${escape(c.route)}</p></aside></section>
      <section class="catalog" id="libros"><div class="wrap"><div class="section-head"><h2>Los libros de esta colección</h2><p>Prueba gratis el primer 10 % de cada título.</p></div><div class="books">${c.books.map(card).join('')}</div></div></section>
      <section class="wrap detail-section"><h2>Más caminos para explorar</h2>${collectionLinks(collections.filter(item => item.id !== c.id))}<p class="fine">La membresía de Audiolibros TEC incluye el catálogo completo por $20 USD al mes, con renovación mensual.</p><p><a href="/audiolibros/#membresia">Conocer la membresía →</a></p></section>${guideRecommendations(guides.filter(g => g.collectionIds.includes(c.id)))}</main>${footer}</body></html>`];
  }));
  const guideList = { '@type': 'ItemList', '@id': absolute(guideHub) + '#articulos', name: 'Guías y lecturas de Audiolibros TEC', numberOfItems: guides.length,
    itemListElement: guides.map((g, i) => ({ '@type': 'ListItem', position: i + 1, url: absolute(guidePath(g)), name: g.title })) };
  const guideIndex = `<!doctype html><html lang="es"><head>${baseHead}${head({ title: 'Guías y lecturas de audiolibros en español | TEC', description: 'Elige tu próximo audiolibro con las guías de TEC: desarrollo personal, Biblia en audio, autores clásicos y lectura sincronizada.', url: absolute(guideHub), image: absolute('/audiolibros/identidad/portada-social.jpg'),
    graph: [organization, website, { '@type': 'CollectionPage', '@id': absolute(guideHub) + '#webpage', name: 'Guías y lecturas', url: absolute(guideHub), inLanguage: 'es', mainEntity: { '@id': guideList['@id'] }, isPartOf: { '@id': website['@id'] } }, guideList] })}</head><body>${nav}<main class="wrap guide-index"><p class="eyebrow">La biblioteca, un poco más cerca</p><h1>Guías y lecturas</h1><p class="intro">Ideas para elegir tu próxima escucha, conocer dos perspectivas y encontrar tu forma de leer con audio.</p>${guideCards(guides, 'h2')}<section class="detail-section"><h2>Del artículo al audiolibro</h2><p>Las guías son de acceso público. Cada una enlaza con los libros y las colecciones de TEC para que puedas conocer su edición y escuchar una muestra gratuita.</p>${collectionLinks(collections)}</section></main>${footer}</body></html>`;
  const guidePages = new Map(guides.map(g => {
    const url = absolute(guidePath(g));
    const articleSchema = { '@type': 'BlogPosting', '@id': url + '#articulo', headline: g.title, description: g.description, url, inLanguage: 'es',
      datePublished: g.date, dateModified: g.date, author: { '@id': organization['@id'] }, publisher: { '@id': organization['@id'] },
      image: g.books.slice(0, 3).map(b => absolute(b.cover)), wordCount: guideWords(g), isAccessibleForFree: true,
      mainEntityOfPage: { '@id': url + '#webpage' }, isPartOf: { '@id': absolute(guideHub) + '#webpage' },
      about: g.books.map(b => ({ '@type': 'Audiobook', name: b.title, url: absolute(bookPath(b)) })) };
    const articleBody = g.sections.map(s => `<section id="${s.id}" class="guide-section"><h2>${escape(s.title)}</h2>${s.paragraphs.map(p => `<p>${escape(p)}</p>`).join('')}${s.bullets ? `<ul>${s.bullets.map(b => `<li>${escape(b)}</li>`).join('')}</ul>` : ''}${s.bookIds ? `<div class="guide-book-links">${s.bookIds.map(id => catalog.find(b => b.id === id)).filter(Boolean).map(b => `<a href="${bookPath(b)}">Conocer ${escape(b.title)} →</a>`).join('')}</div>` : ''}</section>`).join('');
    return [g.id, `<!doctype html><html lang="es"><head>${baseHead}${head({ title: g.title + ' | Audiolibros TEC', description: g.description, url, image: absolute(g.cover.cover), type: 'article', graph: [organization, website, articleSchema,
      { '@type': 'WebPage', '@id': url + '#webpage', name: g.title, url, inLanguage: 'es', mainEntity: { '@id': articleSchema['@id'] } },
      { '@type': 'BreadcrumbList', itemListElement: [{ '@type': 'ListItem', position: 1, name: 'Audiolibros TEC', item: library }, { '@type': 'ListItem', position: 2, name: 'Guías y lecturas', item: absolute(guideHub) }, { '@type': 'ListItem', position: 3, name: g.title, item: url }] }] })}</head>
      <body>${nav}<main><header class="wrap guide-hero"><nav class="breadcrumbs" aria-label="Ruta de navegación"><a href="/audiolibros/">Audiolibros TEC</a> / <a href="${guideHub}">Guías y lecturas</a></nav><p class="eyebrow">${escape(g.label)}</p><h1>${escape(g.title)}</h1><p class="intro">${escape(g.intro)}</p><p class="guide-byline">Por Audiolibros TEC · <time datetime="${g.date}">7 de noviembre de 2026</time> · ${readingTime(g)} min de lectura aprox.</p></header>
      <div class="wrap guide-layout"><aside><details class="guide-toc" open><summary>En esta guía</summary><nav aria-label="Contenido del artículo"><ol>${g.sections.map(s => `<li><a href="#${s.id}">${escape(s.title)}</a></li>`).join('')}</ol></nav></details></aside><article class="guide-copy" aria-label="${escape(g.title)}">${articleBody}<p class="guide-editorial-note">Guía editorial de Audiolibros TEC, basada en las ediciones y funciones disponibles en nuestra biblioteca.</p></article></div>
      <section class="catalog"><div class="wrap"><div class="section-head"><h2>De esta guía a tu próxima escucha</h2><p>Conoce cada libro y prueba gratis el primer 10 %.</p></div><div class="books">${g.books.map(card).join('')}</div></div></section>
      <section class="wrap detail-section"><h2>Colecciones para continuar</h2>${collectionLinks(collections.filter(c => g.collectionIds.includes(c.id)))}<p class="fine">Los libros completos están incluidos en la membresía de Audiolibros TEC por $20 USD al mes, con renovación mensual.</p><a class="button outline" href="/audiolibros/#membresia">Ver membresía</a></section>${guideRecommendations(guides.filter(item => item.id !== g.id).slice(0, 2))}</main>${footer}</body></html>`];
  }));
  const sitemap = `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${[library, ...catalog.map(book => absolute(bookPath(book))), ...collections.map(c => absolute(collectionPath(c))), absolute(guideHub), ...guides.map(g => absolute(guidePath(g)))].map(url => `<url><loc>${escape(url)}</loc></url>`).join('')}</urlset>\n`;
  const robots = `User-agent: *\nAllow: /audiolibros/\nAllow: /coach/assets/audiobooks/\nAllow: /coach/audiobook-reader.js\nDisallow: /api/\nDisallow: /coach/\nDisallow: /control\nDisallow: /admin\nDisallow: /chat\nDisallow: /ar/\n\nSitemap: ${absolute('/sitemap.xml')}\n`;
  const llms = `# Audiolibros TEC\n\n> Biblioteca de audiolibros en español de Total Elite Coaching.\n\n## Acceso\n\nEl primer 10 % de cada título se puede escuchar gratis. La membresía de Audiolibros TEC cuesta 20 USD al mes con renovación mensual y da acceso al catálogo completo. Es una membresía separada de la aplicación Coach. El texto de lectura completa y los audios completos requieren acceso autorizado. Las páginas públicas incluyen descripciones, duración, autor, capítulos y muestras. No se requiere registrarse para conocer los libros ni escuchar las muestras.\n\n## Páginas públicas\n\n- [Biblioteca y membresía](${library}): catálogo, lectura sincronizada, muestras y suscripción.\n${catalog.map(book => `- [${book.title}](${absolute(bookPath(book))}): ${book.author}. ${book.categories.join(', ')}. Audiolibro en español.`).join('\n')}\n\n## Descubrimiento\n\n- [Sitemap](${absolute('/sitemap.xml')})\n\nLos enlaces de fragmentos compartidos llevan al libro y al punto de escucha. El contenido privado de cuentas, pagos, grabaciones de ventas y Coach no forma parte de este catálogo público. Este archivo es una guía complementaria; las fichas públicas son la fuente de información de cada libro.\n`;
  const discovery = llms.replace('## Descubrimiento', `## Colecciones por tema\n\n${collections.map(c => `- [${c.title}](${absolute(collectionPath(c))}): ${c.description}`).join('\n')}\n\n## Descubrimiento`);
  const guideDiscovery = discovery.replace('## Descubrimiento', `## Guías y lecturas\n\n- [Todas las guías](${absolute(guideHub)})\n${guides.map(g => `- [${g.title}](${absolute(guidePath(g))}): ${g.description}`).join('\n')}\n\n## Descubrimiento`);
  return { landing, pages, collectionPages, guideIndex, guidePages, sitemap, robots, llms: guideDiscovery };
}

export function registerAudiobookSeo(app, options = {}) {
  // Public metadata only: generated once, without Stripe, MongoDB, R2 or AI requests.
  const rendered = buildAudiobookSeo(options);
  const send = (type, value) => (_req, res) => res.type(type).set('Cache-Control', 'public, max-age=300').send(value);
  app.get('/robots.txt', send('text/plain', rendered.robots));
  app.get('/sitemap.xml', send('application/xml', rendered.sitemap));
  app.get(['/llms.txt', '/audiolibros/llms.txt'], send('text/plain', rendered.llms));
  app.get([/^\/audiolibros$/, '/audiolibros/index.html'], (req, res) => res.redirect(301, '/audiolibros/' + (req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '')));
  app.get('/audiolibros/', send('html', rendered.landing));
  app.get('/audiolibros/libros/:id', (req, res) => {
    const page = rendered.pages.get(req.params.id);
    if (!page) return res.status(404).type('html').set('X-Robots-Tag', 'noindex').send('<!doctype html><html lang="es"><title>Audiolibro no encontrado · TEC</title><h1>No encontramos este audiolibro</h1><a href="/audiolibros/">Volver al catálogo de Audiolibros TEC</a></html>');
    if (!req.path.endsWith('/')) return res.redirect(301, bookPath({ id: req.params.id }));
    return send('html', page)(req, res);
  });
  app.get('/audiolibros/colecciones/:id', (req, res) => {
    const page = rendered.collectionPages.get(req.params.id);
    if (!page) return res.status(404).type('html').set('X-Robots-Tag', 'noindex').send('<!doctype html><html lang="es"><title>Colección no encontrada · TEC</title><h1>No encontramos esta colección</h1><a href="/audiolibros/#colecciones">Ver las colecciones de Audiolibros TEC</a></html>');
    if (!req.path.endsWith('/')) return res.redirect(301, collectionPath({ id: req.params.id }));
    return send('html', page)(req, res);
  });
  app.get(/^\/audiolibros\/guias$/, (_req, res) => res.redirect(301, '/audiolibros/guias/'));
  app.get('/audiolibros/guias/', send('html', rendered.guideIndex));
  app.get('/audiolibros/guias/:id', (req, res) => {
    const page = rendered.guidePages.get(req.params.id);
    if (!page) return res.status(404).type('html').set('X-Robots-Tag', 'noindex').send('<!doctype html><html lang="es"><title>Guía no encontrada · TEC</title><h1>No encontramos esta guía</h1><a href="/audiolibros/guias/">Volver a Guías y lecturas</a></html>');
    if (!req.path.endsWith('/')) return res.redirect(301, guidePath({ id: req.params.id }));
    return send('html', page)(req, res);
  });
  return rendered;
}
