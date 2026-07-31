export function registerMetalworksCrmAccessRoutes(app, dependencies) {
  const {
    normalizeEmail,
    getAllowedEmails,
    getMetalworksPasswordForEmail,
    getCrmLoginThrottle,
    metalworksCrmConfigured,
    respondError,
    compareSecrets,
    recordCrmLoginFailure,
    createSession,
    clearCrmLoginFailures,
    getMetalworksCrmProfile,
    destroySession,
  } = dependencies;

  app.post("/api/metalworks-crm/login", async (req, res) => {
    const email = normalizeEmail(req.body?.email || "");
    const password = String(req.body?.password || "");
    const allowedEmails = getAllowedEmails();
    const expectedPassword = getMetalworksPasswordForEmail(email);
    const throttle = getCrmLoginThrottle(req, email);

    if (throttle.blocked) {
      res.set("Retry-After", String(throttle.retryAfterSeconds));
      return respondError(res, 429, "Demasiados intentos. Intenta de nuevo en unos minutos.");
    }

    if (!metalworksCrmConfigured()) {
      return respondError(
        res,
        503,
        "Primero configura METALWORKS_CRM_PASSWORD o METALWORKS_CRM_USER_PASSWORDS_JSON en el backend.",
      );
    }

    if (!email || !password) {
      return respondError(res, 400, "Correo y password son requeridos.");
    }

    if (!allowedEmails.includes(email) || !expectedPassword || !compareSecrets(password, expectedPassword)) {
      const failedAttempt = recordCrmLoginFailure(req, email);

      if (failedAttempt.blocked) {
        res.set("Retry-After", String(failedAttempt.retryAfterSeconds));
        return respondError(res, 429, "Demasiados intentos. Intenta de nuevo en unos minutos.");
      }

      return respondError(res, 401, "Correo o password incorrectos.");
    }

    try {
      await createSession(req, res, email);
      clearCrmLoginFailures(req, email);
      res.json({ ok: true, email, profile: getMetalworksCrmProfile(email) });
    } catch (error) {
      console.error("Error logging into Metal Works CRM:", error.message);
      respondError(res, 500, "No pude iniciar sesion en el CRM.");
    }
  });

  app.post("/api/metalworks-crm/logout", async (req, res) => {
    try {
      await destroySession(req, res);
      res.json({ ok: true });
    } catch (error) {
      console.error("Error logging out of Metal Works CRM:", error.message);
      respondError(res, 500, "No pude cerrar la sesion.");
    }
  });
}
