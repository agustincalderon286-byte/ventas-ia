import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";

export const AUDIO_LAB_MAX_BYTES = 16 * 1024 * 1024;
const allowedTypes = new Set(["audio/webm", "audio/mp4", "audio/mpeg", "audio/wav", "audio/x-wav", "audio/ogg", "audio/aac", "audio/x-m4a"]);
export function parseAudioRange(header, length) {
  if (!header) return null;
  const match = /^bytes=(\d*)-(\d*)$/.exec(header);
  if (!match || (!match[1] && !match[2]) || length < 1) throw new Error("range");
  let start = match[1] ? Number(match[1]) : Math.max(0, length - Number(match[2]));
  let end = match[1] && match[2] ? Math.min(length - 1, Number(match[2])) : length - 1;
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start > end || start >= length) throw new Error("range");
  return { start, end };
}
export function canUseAudioLab(user, emails) {
  return emails.includes(String(user?.email || "").trim().toLowerCase());
}
export function registerTecAudioLab(app, { requireAdmin, emails, getBucket, ObjectId, analyses }) {
  const busy = new Set();
  async function authorize(req, res) {
    res.set({ "Cache-Control": "private, no-store", "X-Robots-Tag": "noindex, nofollow", "Vary": "Cookie" });
    const auth = await requireAdmin(req, res);
    if (!auth) return null;
    if (!canUseAudioLab(auth.user, emails)) { res.status(403).json({ error: "Prueba privada todavía no habilitada para esta cuenta." }); return null; }
    return auth;
  }
  const wrap = handler => async (req, res) => {
    try { await handler(req, res); }
    catch (error) {
      console.error("Audio lab:", error.message);
      if (!res.headersSent && !res.destroyed) res.status(503).json({ error: "No pude completar la prueba de audio. Tu archivo original no se modifica." });
      else if (!res.destroyed) res.destroy();
    }
  };
  const fileQuery = (id, user) => ({ _id: new ObjectId(id), "metadata.ownerUserId": String(user._id), "metadata.testOnly": true });
  app.get("/api/control/audio-lab", wrap(async (req, res) => {
    res.set("Cache-Control", "private, no-store");
    const auth = await requireAdmin(req, res);
    if (!auth) return;
    if (!canUseAudioLab(auth.user, emails)) return res.json({ enabled: false });
    const files = await getBucket().find({ "metadata.ownerUserId": String(auth.user._id), "metadata.testOnly": true }).sort({ uploadDate: -1 }).limit(5).toArray();
    res.json({ enabled: true, maxBytes: AUDIO_LAB_MAX_BYTES, maxFiles: 5,
      files: files.map(file => ({ id: String(file._id), size: file.length, createdAt: file.uploadDate,
        label: file.metadata?.label || "Audio de prueba", analysisId: file.metadata?.analysisId || "",
        url: `/api/control/audio-lab/${file._id}/audio` })) });
  }));
  app.post("/api/control/audio-lab", wrap(async (req, res) => {
    const auth = await authorize(req, res);
    if (!auth) return;
    const owner = String(auth.user._id);
    const origin = req.get("Origin");
    if (req.get("Sec-Fetch-Site") === "cross-site" || (origin && new URL(origin).host !== req.get("host"))) return res.status(403).json({ error: "Abre esta prueba desde Torre." });
    if (req.get("X-TEC-Audio-Test") !== "own-test-only") return res.status(400).json({ error: "Confirma que es una grabación tuya de prueba, sin clientes reales." });
    const contentType = String(req.get("Content-Type") || "").split(";")[0].toLowerCase();
    if (!allowedTypes.has(contentType)) return res.status(415).json({ error: "Usa un archivo de audio M4A, MP3, WAV, OGG o WEBM." });
    const claimedSize = Number(req.get("Content-Length") || 0);
    if (claimedSize > AUDIO_LAB_MAX_BYTES) return res.status(413).json({ error: "El máximo de prueba es 16 MB por archivo." });
    if (busy.has(owner)) return res.status(409).json({ error: "Ya hay una carga de prueba en curso." });
    busy.add(owner);
    let upload;
    try {
      const bucket = getBucket();
      const existing = await bucket.find({ "metadata.ownerUserId": owner, "metadata.testOnly": true }).limit(5).toArray();
      if (existing.length >= 5) return res.status(409).json({ error: "Ya hay cinco audios de prueba. Elimina uno antes de subir otro." });
      const analysisId = String(req.query.analysisId || "");
      if (analysisId && (!ObjectId.isValid(analysisId) || !await analyses.findOne({ _id: new ObjectId(analysisId), ownerUserId: auth.user._id }).select("_id").lean())) {
        return res.status(400).json({ error: "Solo puedes vincular tus propios análisis a esta prueba." });
      }
      const label = String(req.query.label || "Audio propio de prueba").slice(0, 100);
      upload = bucket.openUploadStream("tec-private-test-audio", { metadata: { testOnly: true, ownerUserId: owner, label, contentType, analysisId } });
      let size = 0;
      const limiter = new Transform({ transform(chunk, _encoding, done) {
        size += chunk.length;
        if (size > AUDIO_LAB_MAX_BYTES) done(new Error("Audio test exceeds size limit"));
        else done(null, chunk);
      } });
      await pipeline(req, limiter, upload);
      if (!size) { await bucket.delete(upload.id); return res.status(400).json({ error: "El archivo llegó vacío." }); }
      res.status(201).json({ id: String(upload.id), saved: true });
    } catch (error) {
      if (upload) await upload.abort().catch(() => {});
      throw error;
    } finally { busy.delete(owner); }
  }));
  app.get("/api/control/audio-lab/:id/audio", wrap(async (req, res) => {
    const auth = await authorize(req, res);
    if (!auth) return;
    if (!ObjectId.isValid(req.params.id)) return res.status(404).end();
    const bucket = getBucket();
    const [file] = await bucket.find(fileQuery(req.params.id, auth.user)).limit(1).toArray();
    if (!file) return res.status(404).json({ error: "Audio de prueba no encontrado." });
    let range;
    try { range = parseAudioRange(req.get("Range"), file.length); }
    catch { res.set("Content-Range", `bytes */${file.length}`); return res.status(416).end(); }
    res.set({ "Content-Type": file.metadata.contentType, "Accept-Ranges": "bytes",
      "Content-Disposition": "inline; filename=tec-private-test-audio" });
    if (range) {
      res.status(206).set({ "Content-Range": `bytes ${range.start}-${range.end}/${file.length}`, "Content-Length": String(range.end - range.start + 1) });
    } else res.set("Content-Length", String(file.length));
    const stream = bucket.openDownloadStream(file._id, range ? { start: range.start, end: range.end + 1 } : {});
    res.on("close", () => stream.destroy());
    await pipeline(stream, res);
  }));
  app.delete("/api/control/audio-lab/:id", wrap(async (req, res) => {
    const auth = await authorize(req, res);
    if (!auth) return;
    if (req.get("X-TEC-Audio-Test") !== "own-test-only") return res.status(403).end();
    if (!ObjectId.isValid(req.params.id)) return res.status(404).end();
    const bucket = getBucket();
    const [file] = await bucket.find(fileQuery(req.params.id, auth.user)).limit(1).toArray();
    if (!file) return res.status(404).end();
    await bucket.delete(file._id);
    res.json({ deleted: true });
  }));
}
