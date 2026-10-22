import { createHash, randomUUID } from "node:crypto";

const canonical = value => {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
  return value;
};

// Persist a receipt BEFORE executing a write, and its response BEFORE replying.
// Never execute an uncertain write a second time: retain the client's copy for review.
// There is deliberately no TTL: an old offline retry must not create a new sale.
export function createCoachFormWriteGuard({ model, requireAuth, now = Date.now }) {
  return async (req, res, next) => {
    const requestId = String(req.headers["x-tec-request-id"] || "");
    if (!requestId) return next(); // Existing online clients retain their current behavior.
    let auth;
    try { auth = await requireAuth(req, res); } catch {
      return res.status(503).json({ error: "No pude comprobar tu sesión. El envío permanece en tu dispositivo." });
    }
    if (!auth) return;
    const accountId = String(auth.user._id || auth.user.id || "");
    if (String(req.body?.accountId || "") !== accountId) {
      return res.status(403).json({ error: "Este pendiente pertenece a otra cuenta. Entra con la cuenta que lo guardó." });
    }
    if (!/^[a-zA-Z0-9_-]{8,100}$/.test(requestId)) {
      return res.status(400).json({ error: "El identificador del guardado no es válido." });
    }
    const id = createHash("sha256").update(`${accountId}\n${req.path}\n${requestId}`).digest("hex");
    const fingerprint = createHash("sha256").update(JSON.stringify(canonical(req.body || {}))).digest("hex");
    const token = randomUUID();
    let receipt;
    try {
      try {
        await model.create({ _id: id, accountId, fingerprint, token, state: "processing", createdAt: new Date(now()) });
      } catch (error) {
        if (error.code !== 11000) throw error;
        receipt = await model.findById(id).lean();
        if (!receipt) throw new Error("Receipt unavailable");
      }
      if (receipt) {
        if (receipt.fingerprint !== fingerprint) return res.status(409).json({ error: "Este guardado ya se usó con otros datos. Conservamos el pendiente para revisarlo.", code: "form_conflict" });
        if (receipt.state === "complete") return res.status(receipt.statusCode).json(receipt.responseBody);
        if (receipt.state === "processing" && now() - new Date(receipt.createdAt).getTime() < 120000) {
          return res.status(503).json({ error: "Este guardado sigue en proceso. Se comprobará de nuevo sin duplicarlo." });
        }
        return res.status(409).json({ error: "La confirmación de este guardado necesita revisión. Tu copia local sigue protegida; no vuelvas a capturarlo como nuevo.", code: "form_uncertain" });
      }
    } catch {
      return res.status(503).json({ error: "No pude preparar un guardado seguro. Tu copia local sigue pendiente." });
    }
    req.tecFormAuth = auth;
    const send = res.json.bind(res);
    res.json = body => {
      const statusCode = res.statusCode || 200;
      Promise.resolve().then(async () => {
        const result = await model.updateOne({ _id: id, token }, { $set: {
          state: statusCode >= 500 ? "uncertain" : "complete", statusCode,
          ...(statusCode < 500 ? { responseBody: body } : {}), updatedAt: new Date(now())
        } });
        if (result?.matchedCount === 0) throw new Error("Receipt unavailable");
        send(body);
      }).catch(() => {
        res.status(503);
        send({ error: "No pude confirmar la respuesta del guardado. Conservamos tu copia local y no lo duplicaremos.", code: "form_uncertain" });
      });
      return res;
    };
    try { await next(); } catch {
      res.status(500).json({ error: "El guardado quedó pendiente de revisión. Tu copia local se conserva." });
    }
  };
}
