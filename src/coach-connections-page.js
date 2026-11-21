const actions = '/api/coach/conexiones/gmail/';
const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[char]));
const date = value => new Intl.DateTimeFormat('es-US', { dateStyle: 'medium', timeZone: 'America/Chicago' }).format(new Date(value));
export const notices = {
  sheetsDisabled: 'La conexión de Google Sheets está pendiente de configuración.',
  sheetsConnected: 'Google Sheets autorizado. Elige el archivo y sus dos pestañas.',
  sheetsWrongAccount: 'Elige en Google la cuenta con el mismo correo que usas en tu Coach.',
  sheetsMapped: 'Los destinos de rifas y referidos están guardados en tu Coach.',
  sheetsBadTabs: 'Elige una pestaña distinta para cada tipo de lead.',
  sheetsFileUnavailable: 'Comprueba que tienes permiso de edición y que el archivo tiene dos pestañas visibles. Vuelve a elegirlo en Google.',
  sheetsDisconnected: 'El Coach eliminó tu conexión con Google Sheets.',
  sheetsReconnect: 'Vuelve a autorizar Google Sheets en Mis conexiones.',
  sheetsFailed: 'Google Sheets no pudo completar la operación. Revisa tu conexión y vuelve a intentarlo.',
  waProfileSaved: 'Guardaste tu número de referencia para los próximos borradores de WhatsApp.',
  waProfileRemoved: 'Borraste tu número de referencia para los próximos borradores de WhatsApp.',
  waPhoneInvalid: 'Escribe un solo número de WhatsApp con el signo + y el prefijo del país.',
  waProfileFailed: 'No se pudo actualizar tu número de referencia. Vuelve a intentarlo.',
  calendarConnected: 'Google Calendar conectado. Ya puedes agendar seguimientos desde el chat.',
  availabilityConnected: 'Ya puedes pedir dos horarios libres para una demo en el chat del Coach.',
  availabilityMissing: 'Tu calendario de seguimientos está conectado. Para consultar horarios libres, activa el permiso de disponibilidad en Google.',
  calendarDisconnected: 'Se desconectó Google Calendar de Coach. Tus citas siguen en tu calendario.',
  calendarPending: 'Google aún no confirmó la creación del calendario. Comprueba si ya aparece en tu cuenta.',
  calendarWrongAccount: 'Para este piloto, elige la cuenta de Google con el mismo correo que usas en tu Coach.',
  calendarWrongId: 'Ese identificador no corresponde al calendario personal que creó Coach.',
  calendarDisabled: 'La conexión de Google Calendar todavía no está configurada.',
  calendarReconnect: 'Abre Mis conexiones y vuelve a autorizar Google Calendar con la misma cuenta.',
  calendarFailed: 'Google Calendar no pudo completar la operación. Revisa el estado de la conexión abajo.',
  gmailDisconnectedLocal: 'Se eliminó de Coach el acceso a Gmail y sus resultados. Google Calendar conserva su conexión.',
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

const lineStatus = {
  notConfigured: 'TEC todavía no completó la conexión de tu línea con Meta.',
  unreachable: 'No se pudo comprobar la línea con Meta en este momento. Coach no enviará hasta verificarla.',
  token: 'La autorización de Meta para tu línea caducó o fue retirada. TEC debe renovarla.',
  rejected: 'Meta no reconoce la línea configurada para tu cuenta.',
  mismatch: 'Meta indica un número distinto al configurado para tu cuenta. Coach no enviará hasta que TEC lo corrija.'
};
function businessSection(line, coachEmail, template) {
  const status = line.ok
    ? `<p class="notice">Conectada · Meta confirma el número <strong>${esc(line.phone)}</strong>${line.name ? ' (' + esc(line.name) + ')' : ''}.</p>`
    : `<p class="notice">Sin conexión verificada. ${esc(lineStatus[line.reason] || lineStatus.unreachable)}</p>`;
  return `<section><h2>WhatsApp Business</h2>
    <p>Coach de <strong>${esc(coachEmail)}</strong></p>${status}
    <p>Tu número sigue en tu app de WhatsApp Business. Coach envía desde esa misma línea por Meta solo después de que revises el destinatario y el texto y pulses «Enviar por WhatsApp Business».</p>
    <p>«Aceptado por Meta» significa que Meta recibió el mensaje para enviarlo; la entrega y la lectura se comprueban en tu app. Si el resultado queda sin confirmar, Coach no lo reintenta.</p>
    <p>Meta solo admite texto libre si la persona te escribió en las últimas 24 horas; Coach lo comprueba con los mensajes que Meta le notifica y no envía texto libre si no puede confirmarlo. ${template
      ? 'Fuera de ese plazo, Coach solo te ofrecerá la plantilla de texto fijo aprobada por Meta que configuró TEC: verás su texto exacto, no se puede editar y, al enviarla, confirmas que la persona aceptó tus mensajes y que el texto corresponde a lo que te pidió.'
      : 'TEC todavía no configuró una plantilla aprobada para escribir fuera de ese plazo, así que en ese caso Coach no prepara envíos: escríbele desde tu app de WhatsApp Business.'}</p>
    <a href="/coach/app/?abrirCoach=1">Preparar un WhatsApp en el Coach</a></section>`;
}

export function connectionsPage({ doc, cal, pilot, ready, calendarReady, sheets, sheetsReady, sheetsPilot, personalWhatsAppReady, senderPhone, businessLine, businessPending = false, businessTemplate = false, coachEmail, csrf, message }) {
  // Alta de la línea Business incompleta: se mantiene el flujo manual y se avisa sin datos de la configuración.
  const pendingNote = businessPending ? '<p class="notice">TEC todavía no completó la conexión de tu línea de WhatsApp Business con Meta. Mientras tanto, prepara tus borradores aquí y envíalos desde tu app.</p>' : '';
  const form = (action, label, secondary = false, prefix = actions, fields = '') => `<form method="post" action="${prefix}${action}"><input type="hidden" name="csrf" value="${esc(csrf)}">${fields}<button class="${secondary ? 'secondary' : ''}" type="submit">${label}</button></form>`;
  const calForm = (action,label,secondary,fields) => form(action,label,secondary,'/api/coach/conexiones/calendar/',fields);
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
      <p>${doc.sendEnabled ? 'Puedes responder en el mismo hilo, pedir cambios al borrador y confirmar el envío desde el chat.' : 'Para enviar un borrador desde Coach, autoriza también el envío en Google.'}</p>
      <div class="actions">${ready ? form('sync', 'Consultar ventas') : '<p>TEC debe completar la configuración de Gmail.</p>'}${ready && !doc.sendEnabled ? form('enable-send', 'Autorizar envío en Google') : ''}${form('disconnect', 'Desconectar y borrar datos', true)}</div>`;
  else if (!ready) content = '<p>La conexión estará disponible cuando TEC complete la configuración de Google.</p>';
  else content = `<p>Autoriza la lectura de Gmail para consultar desde tu Coach el remitente, asunto y un extracto de tus correos recientes. Al consultar ventas de Hy Cite, Coach revisa los últimos 90 días y guarda el asunto, la fecha y el número de orden para que puedas comprobarlos.</p>${form('start', 'Conectar mi Gmail')}`;
  let calendar = '';
  if (pilot && calendarReady) {
    const availabilityField = '<input type="hidden" name="availability" value="yes">';
    const availabilityForm = calForm('start','Activar horarios libres',false,availabilityField);
    const availabilityInfo = 'Para buscar horarios, el Coach consulta los bloques ocupados de tu calendario principal y de «Seguimientos TEC».';
    const offerReady = cal?.availabilityEnabled && doc?.sendEnabled && doc.mailbox &&
      [cal.mailbox, coachEmail].every(account => typeof account === 'string' && account.toLowerCase() === doc.mailbox.toLowerCase());
    const offerHelp = offerReady ? '<p>Para ofrecer una demo por Gmail, escribe «Revisa mis correos» y luego «Ofrécele dos horarios al primero para mañana por la tarde». Revisa y confirma el correo. Cuando el cliente conteste, pide «Revisa la respuesta de la oferta» para elegir y confirmar la cita.</p>' : '';
    let controls;
    if (cal?.grant && cal.calendarId) {
      controls = `<p>Conectado con <strong>${esc(cal.mailbox)}</strong>. Tus citas se guardan en el calendario «Seguimientos TEC». Pide en el chat «Agenda una llamada con María mañana a las diez de la mañana».</p>
        <p>${cal.availabilityEnabled ? 'Horarios libres activados. Pide «Busca dos horarios libres mañana para una demo de 45 minutos».' : availabilityInfo}</p>${offerHelp}
        <div class="actions">${cal.availabilityEnabled ? '' : availabilityForm}${calForm('start','Volver a autorizar',true,cal.availabilityEnabled ? availabilityField : '')}${calForm('disconnect','Desconectar Calendar',true)}</div>`;
    } else if (cal?.grant && cal.provisionState) {
      controls = `<p>Comprueba en Google Calendar si ya aparece «Seguimientos TEC». Si aparece, abre sus ajustes y copia el ID de «Integrar calendario».</p>
        ${calForm('prepare','Vincular mi calendario',false,'<label>ID de Seguimientos TEC <input name="calendarId" required maxlength="280"></label>')}
        ${Date.now()-new Date(cal.provisionTime).getTime()>=120000 ? calForm('prepare','Crear después de comprobar',true,'<label><input name="noCalendar" type="checkbox" value="yes" required> Ya comprobé en Google que el calendario no se creó.</label>') : ''}
        ${calForm('disconnect','Desconectar Calendar',true)}`;
    } else if (cal?.grant) {
      controls = `<p>Google autorizó tu cuenta ${esc(cal.mailbox)}.</p>${calForm('prepare','Crear mi calendario de seguimientos')}${calForm('start','Volver a autorizar',true)}${calForm('disconnect','Desconectar Calendar',true)}`;
    } else {
      controls = `<p>Conecta la cuenta de Google que usas en el piloto. Coach creará «Seguimientos TEC» para tus citas y recordatorios. ${availabilityInfo}</p><div class="actions">${calForm('start','Conectar mi Google Calendar')}${availabilityForm}</div>`;
    }
    calendar = `<section><h2>Google Calendar</h2>${controls}<p>Al desconectar, Coach elimina su permiso guardado. Tus citas permanecen en Google. Puedes retirar el acceso de todo TEC desde <a href="https://myaccount.google.com/connections" target="_blank" rel="noopener noreferrer">tu cuenta de Google</a>.</p></section>`;
  }
  const snapshot = doc?.snapshot;
  const waApi = '/api/coach/conexiones/whatsapp/';
  const personalWhatsApp = businessLine ? businessSection(businessLine, coachEmail, businessTemplate) : pilot && personalWhatsAppReady ? `<section><h2>WhatsApp</h2>
    <p>Coach de <strong>${esc(coachEmail)}</strong></p>${pendingNote}
    ${senderPhone === null ? '<p>Tu número de referencia no está disponible en este momento.</p>' :
      `<p>Puedes guardar el número desde el que envías tus seguimientos. El Coach lo mostrará como referencia al revisar tus próximos borradores.</p>
      <div class="actions">${form('save','Guardar mi número',false,waApi,
        '<label>Mi número de WhatsApp (con prefijo de país) <input type="tel" inputmode="tel" autocomplete="tel" name="phone" value="'+esc(senderPhone)+'" required maxlength="50"></label> ')}
      ${senderPhone ? form('remove','Borrar mi número',true,waApi) : ''}</div>`}
    <p>Escribe «Prepara un WhatsApp de seguimiento para María» en el chat del Coach. Revisa el número y el texto, o pide una corrección.</p>
    <p>Al confirmar, abre el borrador en WhatsApp Messenger o WhatsApp Business en tu teléfono. Comprueba que la cuenta activa es la tuya y pulsa Enviar en la app.</p>
    <a href="/coach/app/?abrirCoach=1">Preparar un WhatsApp en el Coach</a></section>` : '';
  const results = connected && snapshot ? `<p>Última consulta el ${date(doc.syncedAt)}. ${snapshot.limited ? 'Se alcanzó el límite de 100 avisos. El conteo es parcial.' : 'Período consultado: últimos 90 días.'}</p>
    ${rows('Ventas confirmadas', snapshot.confirmed, 'Sin ventas confirmadas en los avisos consultados.')}
    ${rows('Avisos para revisar', snapshot.review, 'Sin avisos pendientes de revisión.')}` : '';
  return `<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Mis conexiones · Coach TEC</title><link rel="stylesheet" href="/coach/connections.css"></head><body><main>
    <a href="/coach/app/?abrirCoach=1">← Volver al chat del Coach</a><h1>Mis conexiones</h1>
    ${message ? '<p role="status" class="notice">' + esc(message) + '</p>' : ''}
    ${personalWhatsApp}<section><h2>Gmail y ventas de Hy Cite</h2>${content}</section>${calendar}${sheetsPilot ? sheetsSection({form,profile:sheets,ready:sheetsReady,email:coachEmail}) : ''}${results}</main></body></html>`;
}

const sheetsActions='/api/coach/conexiones/sheets/';
const suggestedWorkbook='1CrzmSJLtW32pGC6LsUPRjGOPgbZVgIn8_kFVu2eHx6Y';
const pilotBookFor=email=>['agustincalderon286@gmail.com','calderonrigoberto51@gmail.com'].includes(email);
const sheetField=(name,value)=>`<input type="hidden" name="${name}" value="${esc(value)}">`;
const sheetPage=(title,body,scripts='')=>`<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${esc(title)} · Coach TEC</title><link rel="stylesheet" href="/coach/connections.css"></head><body><main><a href="/mis-conexiones/">← Mis conexiones</a><h1>${esc(title)}</h1>${body}</main>${scripts}</body></html>`;

function sheetsSection({form,profile,ready,email}) {
  if (!ready) return '<section><h2>Google Sheets</h2><p>La conexión estará disponible cuando TEC complete la configuración de Google Sheets.</p></section>';
  const hint=pilotBookFor(email) ? `<p>Para el piloto, elige <a href="https://docs.google.com/spreadsheets/d/${suggestedWorkbook}/edit" target="_blank" rel="noopener noreferrer">Base de datos Eric – Young Dreamers Chicago</a>.</p>`:'';
  const controls=profile?.grant ? `<p>Cuenta conectada · <strong>${esc(profile.mailbox)}</strong></p>
    ${profile.spreadsheetId?`<p><strong>${esc(profile.spreadsheetTitle)}</strong><br>Rifas → ${esc(profile.ticketsTitle)}<br>Referidos con anfitrión → ${esc(profile.referralsTitle)}</p>`:hint}
    <div class="actions">${form('picker',profile.spreadsheetId?'Cambiar hoja o pestañas':'Elegir mi hoja de Google',false,sheetsActions)}${form('disconnect','Desconectar Google Sheets',true,sheetsActions)}</div>` :
    `<p>Elige una hoja de tu cuenta de Google para registrar rifas y referencias. El Coach te mostrará los posibles duplicados antes de que confirmes cada guardado.</p>${hint}${form('start','Conectar mi Google Sheets',false,sheetsActions)}`;
  return `<section id="google-sheets"><h2>Google Sheets</h2>${controls}</section>`;
}

export function pickerPage({csrf,grant,email}) {
  return sheetPage('Elegir mi hoja de Google',`<p>Abre el selector de Google y elige un archivo en el que puedas editar las pestañas de rifas y referidos.</p>
    ${pilotBookFor(email)?'<p>Para el piloto, busca <strong>Young Dreamers Chicago</strong>.</p>':''}
    <button type="button" id="open-picker">Abrir Google Drive</button><p role="status" id="picker-status"></p>
    <form method="post" action="${sheetsActions}choose" id="picker-choice">${sheetField('csrf',csrf)}${sheetField('grant',grant)}${sheetField('spreadsheetId','')}</form>`,
    '<script defer src="https://apis.google.com/js/api.js"></script><script defer src="/coach/sheets-picker.js?v=1"></script>');
}

export function sheetMappingPage({book,csrf,doc}) {
  const fold=text=>text.normalize('NFD').replace(/\p{M}/gu,'').toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();
  const guessed=(title)=>book.tabs.find(tab=>fold(tab.title)===title)?.sheetId;
  const isCurrent=book.id===doc.spreadsheetId;
  const select=(name,label,selected)=>`<p><label>${label}<br><select name="${name}" required><option value="">Elige una pestaña</option>${book.tabs.map(tab=>
    `<option value="${tab.sheetId}"${selected===tab.sheetId?' selected':''}>${esc(tab.title)}</option>`).join('')}</select></label></p>`;
  return sheetPage('Destino de tus leads',`<p>Archivo · <strong>${esc(book.title)}</strong></p><form method="post" action="${sheetsActions}map">
    ${sheetField('csrf',csrf)}${sheetField('grant',doc.grant)}${sheetField('spreadsheetId',book.id)}
    ${select('ticketsSheetId','Pestaña para rifas',isCurrent?doc.ticketsSheetId:guessed('boletas nuevas'))}
    ${select('referralsSheetId','Pestaña para referidos y anfitrión',isCurrent?doc.referralsSheetId:guessed('4 14 agustin y rigoberto'))}
    <p>Elige dos pestañas diferentes con la estructura de telemarketing del piloto. El Coach comprobará las columnas antes de preparar un guardado.</p>
    <button type="submit">Guardar destinos en mi Coach</button></form>`);
}
