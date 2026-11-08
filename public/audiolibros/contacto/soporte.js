// No analytics, local storage, recording hooks or offline queue on this form.
for (const form of document.querySelectorAll('[data-support-form]')) {
  form.addEventListener('submit', async event => {
    event.preventDefault();
    const button = form.querySelector('[type="submit"]');
    if (button.disabled || !form.reportValidity()) return;
    const status = form.querySelector('[role="status"]');
    const say = (text, error = false) => { status.textContent = text; status.dataset.error = String(error); };
    if (!navigator.onLine) { say('Necesitas conexión para enviar la consulta. Tu texto sigue aquí; reintenta cuando tengas internet.', true); return; }
    button.disabled = true; button.textContent = 'Enviando…'; say('Estamos enviando tu consulta.');
    try {
      const fields = Object.fromEntries(new FormData(form));
      const response = await fetch('/api/audiolibros/contacto', {
        method: 'POST', credentials: 'omit', headers: { 'Content-Type': 'application/json' },
        signal: AbortSignal.timeout(15_000), body: JSON.stringify({ ...fields, service: form.dataset.service })
      });
      const data = await response.json();
      if (!response.ok || !data.ok) throw new Error(data.error || 'No pudimos enviar tu consulta. Reintenta o escríbenos por correo.');
      form.reset(); say(`Consulta enviada${data.reference ? ` · Referencia ${data.reference}` : ''}. Te responderemos por correo.`);
    } catch (error) {
      say(error.name === 'TimeoutError' || error.name === 'TypeError' || error instanceof SyntaxError
        ? 'No pudimos confirmar el envío. Tu texto sigue aquí; reintenta o escribe a agustincalderon286@gmail.com.' : error.message, true);
    } finally { button.disabled = false; button.textContent = 'Enviar consulta'; }
  });
}
