import { createSheetsTransfer } from './coach-sheets-transfer.js';

const path = '/api/coach/conexiones/sheets/';

export function registerCoachSheetsRoutes(app, { own, sheets, transfer = createSheetsTransfer() }) {
  const route = handler => own(async (req, res, actor) => {
    const claimed = req.method === 'GET' ? req.query.accountId : req.body?.accountId;
    if (claimed !== String(actor.id) || (req.get('X-TEC-Account-ID') && req.get('X-TEC-Account-ID') !== claimed)) {
      return res.status(403).json({ error: 'Abre Google Sheets desde tu propia cuenta del Coach.' });
    }
    try {
      return res.json(await handler(req, actor));
    } catch (error) {
      const status = [400, 401, 403, 404, 409, 410, 413, 422, 429].includes(error.status) ? error.status : 503;
      return res.status(status).json({
        error: status === 503 ? 'Google Sheets no respondió. Vuelve a revisar este envío.' : error.message,
        ...(error.preview && { preview: error.preview }), ...(error.retryable && { retryable: true })
      });
    }
  });
  app.get(path + 'context', route(async (_req, actor) => ({
    connected: Boolean(await sheets.getConnection(actor.id)), csrf: actor.session
  })));
  async function context(actor) {
    const {token,grant,...workbook} = await sheets.access(actor.id);
    if (!workbook?.spreadsheetId) throw Object.assign(new Error('Elige tu archivo y sus dos pestañas en Mis conexiones.'), { status: 409 });
    return {
      owner: actor.id, session: actor.session, sellerName: actor.sellerName,
      grant, access: token, workbook,
      isCurrent: async () => await actor.isSessionCurrent() && await sheets.current(actor.id, grant)
    };
  }
  app.post(path + 'preview', route(async (req, actor) => transfer.preview(await context(actor), req.body)));
  app.post(path + 'confirm', route(async (req, actor) => transfer.confirm(await context(actor), req.body)));
}
