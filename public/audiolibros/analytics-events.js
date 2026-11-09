// Public, fixed vocabulary. Never send input values, query strings, quotes or audio.
export const MEASUREMENT_ID = 'G-DZCX3RMST7';
export const BOOK_IDS = Object.freeze(['como-un-hombre-piensa', 'acres-de-diamantes', 'el-arte-de-la-guerra', 'genesis-rv1909', 'exodo-rv1909', 'ayudate', 'la-ciencia-de-hacerse-rico', 'el-kybalion', 'el-hombre-mas-rico-de-babilonia', 'meditaciones-marco-aurelio']);
export const COLLECTION_IDS = Object.freeze(['desarrollo-personal', 'biblia-en-audio', 'clasicos-en-espanol', 'filosofia-y-estrategia']);
export const GUIDE_IDS = Object.freeze(['con-que-audiolibro-de-desarrollo-personal-empezar', 'genesis-o-exodo-por-donde-empezar-biblia-audio', 'james-allen-y-samuel-smiles-desarrollo-personal', 'como-aprovechar-audiolibro-con-lectura-sincronizada']);
export const EVENT_NAMES = Object.freeze(['page_view', 'audio_sample_play', 'audio_membership_click', 'audio_share_open']);
// Campaign labels are public, reviewed constants, never free-form URL input.
// Keep access tokens, click identifiers, search terms and unknown labels out of GA.
const CAMPAIGNS = Object.freeze({
  audiolibros_chicago_es: { sources: ['meta'], media: ['paid_social'], content: ['clasicos_15s'] },
  audiolibros_instagram_chicago_es: { sources: ['instagram'], media: ['paid_social'], content: ['clasicos_15s'] },
  clasicos_es: { sources: ['facebook'], media: ['organic_group'], content: [] },
  verificacion_analytics: { sources: ['tec_test'], media: ['qa'], content: ['conexion_20261108'] }
});
export function safeCampaign(search = '') {
  const params = new URLSearchParams(search);
  const one = key => params.getAll(key).length === 1 ? params.get(key) : '';
  const name = one('utm_campaign');
  if (!Object.hasOwn(CAMPAIGNS, name)) return {};
  const rule = CAMPAIGNS[name], source = one('utm_source'), medium = one('utm_medium');
  if (!rule.sources.includes(source) || !rule.media.includes(medium)) return {};
  const content = one('utm_content');
  return { campaign_source: source, campaign_medium: medium, campaign_name: name,
    ...(rule.content.includes(content) ? { campaign_content: content } : {}) };
}
export function pageContext(pathname) {
  if (['/audiolibros', '/audiolibros/', '/audiolibros/index.html'].includes(pathname)) return { type: 'biblioteca', path: '/audiolibros/' };
  for (const id of BOOK_IDS) if ([`/audiolibros/libros/${id}`, `/audiolibros/libros/${id}/`].includes(pathname)) return { type: 'libro', path: `/audiolibros/libros/${id}/` };
  for (const id of COLLECTION_IDS) if ([`/audiolibros/colecciones/${id}`, `/audiolibros/colecciones/${id}/`].includes(pathname)) return { type: 'coleccion', path: `/audiolibros/colecciones/${id}/` };
  if (['/audiolibros/guias', '/audiolibros/guias/'].includes(pathname)) return { type: 'guias', path: '/audiolibros/guias/' };
  if (['/audiolibros/nuestra-historia', '/audiolibros/nuestra-historia/'].includes(pathname)) return { type: 'historia', path: '/audiolibros/nuestra-historia/' };
  for (const id of GUIDE_IDS) if ([`/audiolibros/guias/${id}`, `/audiolibros/guias/${id}/`].includes(pathname)) return { type: 'guia', path: `/audiolibros/guias/${id}/` };
  return null;
}
export function safeReferrer(value) {
  try {
    const url = new URL(value);
    if (url.protocol !== 'https:') return '';
    if (['www.google.com', 'www.google.com.mx', 'www.bing.com', 'bing.com', 'duckduckgo.com', 'www.facebook.com', 'm.facebook.com', 'l.facebook.com', 'lm.facebook.com', 'www.instagram.com', 'l.instagram.com', 'www.tiktok.com', 'tiktok.com', 't.co'].includes(url.hostname)) return url.origin + '/';
  } catch {}
  return '';
}
export function safeAudioEvent(name, context, bookId) {
  const page = context && pageContext(context.path);
  if (!EVENT_NAMES.includes(name) || !page) return null;
  if (name === 'audio_sample_play' && !BOOK_IDS.includes(bookId)) return null;
  return { page_location: 'https://totalelitecoaching.com' + page.path, page_title: 'Audiolibros TEC · ' + page.type, tec_area: 'audiolibros', ...(name === 'audio_sample_play' ? { tec_audio_book: bookId } : {}) };
}
