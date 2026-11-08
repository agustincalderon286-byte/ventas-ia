// Isolated local preview: only public samples and in-memory shares. No production DB or Stripe.
import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { audiobookCatalog } from '../src/tec-audiobooks.js';
import { registerAudioMembership } from '../src/tec-audio-membership.js';
import { registerAudiobookSeo } from '../src/tec-audiobook-seo.js';
const root = fileURLToPath(new URL('../', import.meta.url));
const port = Number(process.env.TEC_SHARE_PREVIEW_PORT || 4191);
const sources = new Map(audiobookCatalog.map(book => [book.id, path.join(root, 'public/audiolibros/muestras', book.id + '.m4a')]));
const catalog = audiobookCatalog.map(book => ({ ...book, audio: { ...book.audio, r2Key: undefined, bytes: fs.statSync(sources.get(book.id)).size } }));
const rows = new Map();
const lean = value => ({ async lean() { return value; } });
const app = express(); app.use(express.json({ limit: '16kb' }));
registerAudiobookSeo(app, { catalog, publicOrigin: `http://127.0.0.1:${port}` });
registerAudioMembership(app, {
  catalog, publicOrigin: `http://127.0.0.1:${port}`, secure: false,
  accounts: { findById() { return lean(null); } }, sessions: { findOne() { return lean(null); } },
  shares: { async create(row) { rows.set(row._id, row); }, findById(id) { return lean(rows.get(id)); } },
  media: { findOne({ _id }) { return lean({ fileId: _id }); } },
  getBucket: () => ({
    find({ _id }) { return { limit() { return { async toArray() { return [{ length: fs.statSync(sources.get(_id)).size }]; } }; } }; },
    openDownloadStream(id, range = {}) { return fs.createReadStream(sources.get(id), { ...(range.start !== undefined ? { start: range.start } : {}), ...(range.end !== undefined ? { end: range.end - 1 } : {}) }); }
  })
});
app.use(express.static(path.join(root, 'public')));
app.listen(port, '127.0.0.1', () => console.log(`Isolated sample preview: http://127.0.0.1:${port}/audiolibros/`));
