import { existsSync, readFileSync } from 'node:fs';
import { audiobookCatalog } from './tec-audiobooks.js';

const publicRoot = new URL('../public/', import.meta.url);
const escape = value => String(value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const json = value => JSON.stringify(value).replace(/</g, '\\u003c');
export const bookPath = book => `/audiolibros/libros/${book.id}/`;
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
      <div class="actions"><a class="button outline" href="${path}">Ver libro</a>${hasSample(book) ? `<a class="button primary" href="/audiolibros/?libro=${book.id}">▶ Muestra · ${time(sampleDuration(book))}</a>` : ''}</div>
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
    .replace('Cargando audiolibros…', 'Elige un libro para conocerlo y escuchar su muestra.');
  const baseHead = `<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"><meta name="theme-color" content="#102421">
    <link rel="icon" href="/audiolibros/identidad/favicon-96.png"><link rel="apple-touch-icon" href="/audiolibros/identidad/apple-touch-icon.png">
    <link rel="stylesheet" href="/audiolibros/estilos.css?v=4">`;
  const nav = `<header class="top wrap"><a class="brand" href="/audiolibros/"><span><strong>Audiolibros TEC</strong><small>Total Elite Coaching</small></span></a><nav aria-label="Navegación principal"><a href="/audiolibros/#catalogo">Catálogo</a><a href="/audiolibros/#membresia">Membresía</a></nav></header>`;
  const pages = new Map(catalog.map(book => {
    const path = bookPath(book), url = absolute(path), sample = hasSample(book);
    const description = `${book.title}, de ${book.author}: audiolibro en español. ${book.description}`;
    const breadcrumbs = { '@type': 'BreadcrumbList', itemListElement: [
      { '@type': 'ListItem', position: 1, name: 'Audiolibros TEC', item: library },
      { '@type': 'ListItem', position: 2, name: book.title, item: url }
    ] };
    const schemaBook = audiobook(book);
    const related = catalog.filter(item => item.id !== book.id && item.categories.some(category => book.categories.includes(category))).slice(0, 3);
    return [book.id, `<!doctype html><html lang="es"><head>${baseHead}${head({ title: `${book.title} en español | Audiolibros TEC`, description, url,
      image: absolute(book.cover), type: 'book', graph: [organization, website, schemaBook, breadcrumbs,
        { '@type': 'WebPage', '@id': url + '#webpage', url, name: `${book.title} en español`, inLanguage: 'es', isPartOf: { '@id': website['@id'] }, mainEntity: { '@id': schemaBook['@id'] } }] })}</head>
      <body>${nav}<main class="wrap book-detail"><nav class="breadcrumbs" aria-label="Ruta de navegación"><a href="/audiolibros/">Audiolibros en español</a><span aria-hidden="true"> / </span><span>${escape(book.title)}</span></nav>
      <article><div class="book-detail-hero"><img class="detail-cover" src="${escape(book.cover)}" alt="Portada de ${escape(book.title)}" width="600" height="800" fetchpriority="high">
      <div><p class="eyebrow">${escape(book.categories.join(' · '))}</p><h1>${escape(book.title)}</h1><p class="intro">${escape(book.author)}</p>
      <p>${escape(book.description)}</p><p class="fine">Audiolibro en español · Duración ${time(book.duration)}<br>${escape(book.edition)}</p>
      <div class="actions"><a class="button primary" href="/audiolibros/?libro=${book.id}">${sample ? 'Escuchar y leer la muestra gratis' : 'Abrir en la biblioteca'}</a><a class="button outline" href="/audiolibros/#membresia">Ver membresía</a></div>
      <p class="fine">Escucha gratis el primer 10 %. El libro completo está incluido en la membresía de $20 USD al mes, con renovación mensual.</p></div></div>
      ${sample ? `<section class="detail-section" aria-labelledby="sample-title"><h2 id="sample-title">Escucha una muestra</h2><p>Primer 10 % del libro · ${time(sampleDuration(book))}. También puedes seguir la lectura sincronizada al abrirlo en la biblioteca.</p><audio controls preload="none" src="/audiolibros/muestras/${book.id}.m4a" aria-label="Muestra gratuita de ${escape(book.title)}"></audio></section>` : ''}
      <section class="detail-section"><h2>Qué encontrarás en este audiolibro</h2><ol class="detail-chapters">${book.chapters.map(chapter => `<li>${escape(chapter.title)}</li>`).join('')}</ol></section>
      <section class="detail-section"><h2>La edición de TEC</h2><p>${escape(book.credits)}</p>${book.voiceDisclosure ? `<p>${escape(book.voiceDisclosure)}</p>` : ''}<p>Escucha a tu ritmo, ajusta la velocidad, sigue el texto sincronizado y comparte citas o fragmentos de audio de 15, 30 o 45 segundos con un enlace al libro.</p></section></article>
      ${related.length ? `<section class="detail-section"><h2>También puedes escuchar</h2><ul class="related-books">${related.map(item => `<li><a href="${bookPath(item)}">${escape(item.title)}</a><span>${escape(item.author)}</span></li>`).join('')}</ul></section>` : ''}
      <p><a href="/audiolibros/#catalogo">← Ver todos los audiolibros en español</a></p></main><footer class="wrap"><span>Audiolibros TEC · Total Elite Coaching</span><a href="/audiolibros/#membresia">Membresía</a></footer></body></html>`];
  }));
  const sitemap = `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${[library, ...catalog.map(book => absolute(bookPath(book)))].map(url => `<url><loc>${escape(url)}</loc></url>`).join('')}</urlset>\n`;
  const robots = `User-agent: *\nAllow: /audiolibros/\nAllow: /coach/assets/audiobooks/\nAllow: /coach/audiobook-reader.js\nDisallow: /api/\nDisallow: /coach/\nDisallow: /control\nDisallow: /admin\nDisallow: /chat\nDisallow: /ar/\n\nSitemap: ${absolute('/sitemap.xml')}\n`;
  const llms = `# Audiolibros TEC\n\n> Biblioteca de audiolibros en español de Total Elite Coaching.\n\n## Acceso\n\nEl primer 10 % de cada título se puede escuchar gratis. La membresía de Audiolibros TEC cuesta 20 USD al mes con renovación mensual y da acceso al catálogo completo. Es una membresía separada de la aplicación Coach. El texto de lectura completa y los audios completos requieren acceso autorizado. Las páginas públicas incluyen descripciones, duración, autor, capítulos y muestras. No se requiere registrarse para conocer los libros ni escuchar las muestras.\n\n## Páginas públicas\n\n- [Biblioteca y membresía](${library}): catálogo, lectura sincronizada, muestras y suscripción.\n${catalog.map(book => `- [${book.title}](${absolute(bookPath(book))}): ${book.author}. ${book.categories.join(', ')}. Audiolibro en español.`).join('\n')}\n\n## Descubrimiento\n\n- [Sitemap](${absolute('/sitemap.xml')})\n\nLos enlaces de fragmentos compartidos llevan al libro y al punto de escucha. El contenido privado de cuentas, pagos, grabaciones de ventas y Coach no forma parte de este catálogo público. Este archivo es una guía complementaria; las fichas públicas son la fuente de información de cada libro.\n`;
  return { landing, pages, sitemap, robots, llms };
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
  return rendered;
}
