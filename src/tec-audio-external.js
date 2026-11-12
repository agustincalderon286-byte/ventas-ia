// Only curated catalogue media may leave TEC. Never accept a URL from a request.
// No external recordings are currently approved for the TEC catalogue.
// The LibriVox pilot was retired by editorial decision on 2026-11-12.
const sources = new Map();

export function externalAudioReady(book) {
  return book?.audio?.provider === 'internet-archive' &&
    sources.has(book.id) && book.audio.url === sources.get(book.id) &&
    Number.isSafeInteger(book.audio.bytes) && book.audio.bytes > 0;
}

export function externalAudioDelivery(audio, now = Date.now()) {
  if (audio?.provider !== 'internet-archive' || ![...sources.values()].includes(audio.url)) {
    throw new Error('Unapproved external audiobook');
  }
  // This is a playback-session refresh time, not an expiring or signed source URL.
  return { provider: 'internet-archive', url: audio.url, expiresAt: now + 300000 };
}
