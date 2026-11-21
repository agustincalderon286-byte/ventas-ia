const actions = '/api/coach/conexiones/gmail/';
const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[char]));
const date = value => new Intl.DateTimeFormat('es-US', { dateStyle: 'medium', timeZone: 'America/Chicago' }).format(new Date(value));
export const notices = {
  connected: 'Gmail conectado. Vuelve al chat de Coach para revisar tu correo o tus ventas.',
  sendEnabled: 'Ya puedes confirmar el envío de tus correos desde el chat de Coach.',
  wrongMailbox: 'Elige en Google la dirección que aparece aquí como conectada para activar el envío.',
  disconnected: 'Se desconectó Gmail y se borraron los resultados guardados en Coach.',
  localOnly: 'Se eliminó la conexión de Coach. Revisa también el permiso de TEC en tu cuenta de Google.',
  updated: 'La consulta de ventas está actualizada.',
  expired: 'La autorización caducó o cambió la cuenta activa. Inicia la conexión desde aquí.',
  denied: 'Google no autorizó la conexión.',
  failed: 'No se pudo completar la consulta. Puedes volver a intentarlo.',
  reconnect: 'Google solicita autorización de nuevo. Desconecta la cuenta y vuelve a conectarla.',
  busy: 'La conexión cambió. Revisa el estado actual.'
};

export function connectionsPage({ doc, pilot, ready, csrf, message }) {
  const form = (action, label, secondary = false) => `<form method="post" action="${actions}${action}"><input type="hidden" name="csrf" value="${esc(csrf)}"><button class="${secondary ? 'secondary' : ''}" type="submit">${label}</button></form>`;
  function rows(title, items, empty) {
    const links = items.map(item => {
      const href = 'https://mail.google.com/mail/?authuser=' + encodeURIComponent(doc.mailbox) + '#all/' + encodeURIComponent(item.id);
      return `<li><div><strong>${item.order ? 'Orden ' + esc(item.order) : 'Aviso sin número de orden'}</strong><small>${esc(item.subject)} · ${date(item.date)}</small></div><a href="${esc(href)}" target="_blank" rel="noopener noreferrer">Ver correo</a></li>`;
    });
    return `<section><h2>${title} <span>${items.length}</span></h2>${items.length ? '<ul>' + links.join('') + '</ul>' : '<p>' + empty + '</p>'}</section>`;
  }
  const connected = Boolean(doc?.mailbox);
  let content;
  if (!pilot) content = '<p>El acceso al piloto se habilita por vendedor.</p>';
  else if (connected) content = `<p>Gmail conectado · <strong>${esc(doc.mailbox)}</strong></p>
      <p>Escribe «Revisa mis correos» en el chat de Coach para ver los cinco mensajes más recientes de tu bandeja de entrada. Pídele «Revisa mis ventas de Hy Cite» para consultar las órdenes confirmadas.</p>
      <p>${doc.sendEnabled ? 'Puedes revisar un borrador y confirmar su envío desde el chat.' : 'Para enviar un borrador desde Coach, autoriza también el envío en Google.'}</p>
      <div class="actions">${ready ? form('sync', 'Consultar ventas') : '<p>TEC debe completar la configuración de Gmail.</p>'}${ready && !doc.sendEnabled ? form('enable-send', 'Autorizar envío en Google') : ''}${form('disconnect', 'Desconectar y borrar datos', true)}</div>`;
  else if (!ready) content = '<p>La conexión estará disponible cuando TEC complete la configuración de Google.</p>';
  else content = `<p>Autoriza la lectura de Gmail para consultar desde tu Coach el remitente, asunto y un extracto de tus correos recientes. Al consultar ventas de Hy Cite, Coach revisa los últimos 90 días y guarda el asunto, la fecha y el número de orden para que puedas comprobarlos.</p>${form('start', 'Conectar mi Gmail')}`;
  const snapshot = doc?.snapshot;
  const results = connected && snapshot ? `<p>Última consulta el ${date(doc.syncedAt)}. ${snapshot.limited ? 'Se alcanzó el límite de 100 avisos. El conteo es parcial.' : 'Período consultado: últimos 90 días.'}</p>
    ${rows('Ventas confirmadas', snapshot.confirmed, 'Sin ventas confirmadas en los avisos consultados.')}
    ${rows('Avisos para revisar', snapshot.review, 'Sin avisos pendientes de revisión.')}` : '';
  return `<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Mis conexiones · Coach TEC</title><link rel="stylesheet" href="/coach/connections.css"></head><body><main>
    <a href="/coach/app/?abrirCoach=1">← Volver al chat del Coach</a><h1>Mis conexiones</h1>
    ${message ? '<p role="status" class="notice">' + esc(message) + '</p>' : ''}
    <section><h2>Gmail y ventas de Hy Cite</h2>${content}</section>${results}</main></body></html>`;
}
