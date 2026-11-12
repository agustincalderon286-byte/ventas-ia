import { externalAudioDelivery } from './tec-audio-external.js';
import { parseAudioRange } from './tec-audio-lab.js';

// Callers authorize the member first and verify the manifest-pinned media source.
// This response never opens a media stream or reads audio bytes through Render.
export async function sendDirectAudio(req, res, audio, r2) {
  res.set({ 'Cache-Control': 'private, no-store', Pragma: 'no-cache',
    'X-Robots-Tag': 'noindex, nofollow', 'Referrer-Policy': 'no-referrer' });
  let range;
  try { range = parseAudioRange(req.get('Range'), audio.bytes); }
  catch { return res.status(416).set('Content-Range', `bytes */${audio.bytes}`).end(); }
  if (req.method === 'HEAD') {
    res.set({ 'Content-Type': 'audio/mp4', 'Accept-Ranges': 'bytes',
      'Content-Length': String(range ? range.end - range.start + 1 : audio.bytes) });
    if (range) res.status(206).set('Content-Range', `bytes ${range.start}-${range.end}/${audio.bytes}`);
    return res.end();
  }
  if (audio.provider !== 'internet-archive' && (!audio.r2Key || !r2?.delivery)) return res.status(503).json({ error: 'El audio está temporalmente ocupado. Inténtalo de nuevo.' });
  const delivery = audio.provider === 'internet-archive' ? externalAudioDelivery(audio) : await r2.delivery(audio);
  if (req.query.delivery === '1') return res.json(delivery);
  // Existing bookmarks, native downloads and plain <audio> elements follow this.
  return res.status(307).set('Location', delivery.url).end();
}
