import { audiobookCatalog } from './tec-audiobooks.js';
import { SOCIAL_NETWORKS, SOCIAL_PIECES, socialCampaign, socialLink, socialPiece } from '../public/audiolibros/social-campaigns.js';

const names = { facebook: 'Facebook', instagram: 'Instagram', youtube: 'YouTube', tiktok: 'TikTok' };
const aliases = { fb: 'facebook', ig: 'instagram', yt: 'youtube', tt: 'tiktok' };
const escape = value => String(value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
export function campaignQuery(campaign) {
  const params = new URLSearchParams();
  for (const [field, key] of [['campaign_source','utm_source'], ['campaign_medium','utm_medium'], ['campaign_name','utm_campaign'], ['campaign_content','utm_content']]) {
    if (campaign[field]) params.set(key, campaign[field]);
  }
  return params.toString();
}
export function socialTarget(source, content) {
  const campaign = socialCampaign(source, content);
  if (!campaign.campaign_name) return null;
  const params = new URLSearchParams(campaignQuery(campaign));
  const piece = socialPiece(content);
  if (piece?.bookId) params.set('libro', piece.bookId);
  const path = content === 'perfil' ? `/audiolibros/redes/${campaign.campaign_source}/` : '/audiolibros/';
  return path + '?' + params;
}
export function registerAudioSocial(app, { catalog = audiobookCatalog, now = () => Date.now() } = {}) {
  const books = new Map(catalog.map(book => [book.id, book]));
  const redirect = (res, target) => res.set({ 'Cache-Control': 'private, no-store', 'X-Robots-Tag': 'noindex, nofollow', 'Referrer-Policy': 'no-referrer' }).redirect(307, target);
  for (const [alias, source] of Object.entries(aliases)) {
    app.get(['/audiolibros/' + alias, '/audiolibros/' + alias + '/'], (_req, res) => redirect(res, socialTarget(source, 'perfil')));
  }
  app.get('/audiolibros/r/:source/:content', (req, res) => {
    // Registry-only redirects never reflect query input or accept an external destination.
    const target = socialTarget(req.params.source, req.params.content);
    if (!target) return res.status(404).set('Cache-Control', 'no-store').end();
    return redirect(res, target);
  });
  app.get('/audiolibros/redes/:source/', (req, res) => {
    const source = req.params.source;
    if (!SOCIAL_NETWORKS.includes(source)) return res.status(404).end();
    const pieces = SOCIAL_PIECES.filter(piece => piece.source === source && piece.format !== 'ad' &&
      (!piece.availableFrom || new Date(piece.availableFrom).getTime() <= now()));
    const ordered = source === 'tiktok' ? pieces : pieces.slice().reverse();
    const cards = ordered.map(piece => {
      const book = books.get(piece.bookId);
      const cover = book?.cover || '/audiolibros/identidad/portada-social.jpg';
      return `<a class="social-piece" href="${escape(socialLink(source, piece.id))}"><img src="${escape(cover)}" alt="" width="180" height="240" loading="lazy"><span><small>${piece.format === 'reel' && book ? 'Fragmento para escuchar' : 'De nuestras publicaciones'}</small><h2>${escape(piece.title)}</h2><p>${escape(book?.title || 'Descubre los clásicos de TEC')}</p><strong>${book ? 'Escuchar gratis' : 'Ver biblioteca'} <span aria-hidden="true">→</span></strong></span></a>`;
    }).join('');
    const all = '/audiolibros/?' + campaignQuery(socialCampaign(source, 'perfil'));
    res.type('html').set({ 'Cache-Control': 'private, no-store', 'X-Robots-Tag': 'noindex, follow' }).send(`<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,follow"><meta name="theme-color" content="#102421"><title>Lo viste en ${names[source]} · Audiolibros TEC</title><meta name="description" content="Encuentra el fragmento que viste y escucha gratis el primer 10 % del audiolibro en español."><link rel="stylesheet" href="/audiolibros/estilos.css?v=20261109-player"><link rel="stylesheet" href="/audiolibros/redes.css?v=20261113-social1"><script type="module" src="/audiolibros/analytics.js?v=20261113-social1"></script></head><body><header class="top wrap"><a class="brand" href="${escape(all)}"><span><strong>Audiolibros TEC</strong><small>Total Elite Coaching</small></span></a><a class="button outline" href="${escape(all)}">Ver biblioteca</a></header><main class="wrap social-main"><p class="eyebrow">Lo viste en ${names[source]}</p><h1>¿Qué historia<br><em>te trajo aquí?</em></h1><p class="intro">Elige el fragmento que viste y sigue escuchando. El primer 10 % de cada libro es gratis, sin registrarte.</p><div class="social-grid">${cards}</div><section class="social-explore"><h2>Tu próxima historia también puede ser otra.</h2><p>Explora los audiolibros en español y encuentra tu próxima escucha.</p><a class="button primary" href="${escape(all)}">Explorar la biblioteca</a><p class="fine">Los libros completos están incluidos en la membresía de US$20 al mes, con renovación mensual.</p></section></main><footer class="wrap"><a href="/audiolibros/privacidad/">Privacidad</a> · <a href="/audiolibros/contacto/">Contacto</a></footer></body></html>`);
  });
}
