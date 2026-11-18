(() => {
  'use strict';
  const path = '/api/coach/closing-supervisor';
  function init({ userId, request }) {
    const section = document.querySelector('[data-closing-supervisor]');
    if (!section || !userId || !request || section.dataset.initialized) return;
    section.dataset.initialized = 'true';

    const form = section.querySelector('form');
    const name = form.elements.namedItem('name');
    const phone = form.elements.namedItem('phone');
    const fields = form.querySelector('fieldset');
    const call = section.querySelector('[data-supervisor-call]');
    const edit = section.querySelector('[data-supervisor-edit]');
    const retry = section.querySelector('[data-supervisor-retry]');
    const cancel = form.querySelector('[data-supervisor-cancel]');
    const message = section.querySelector('[data-supervisor-status]');
    const options = { headers: { 'X-TEC-Account-ID': String(userId) }, timeoutMs: 12000 };
    let contact = null;

    function showContact() {
      call.hidden = !contact;
      if (contact) {
        call.href = 'tel:' + contact.phone;
        call.textContent = 'Llamar a ' + contact.name;
      }
      edit.textContent = contact ? 'Editar contacto' : 'Guardar supervisor';
      edit.hidden = false;
    }

    async function load() {
      retry.hidden = true;
      message.textContent = 'Cargando contacto…';
      try {
        ({ contact } = await request(path, options));
        showContact();
        message.textContent = '';
      } catch (error) {
        message.textContent = navigator.onLine
          ? error.message || 'No se pudo cargar tu contacto.'
          : 'Conéctate a internet para cargar tu contacto.';
        retry.hidden = false;
      }
    }

    function closeEditor() {
      form.hidden = true;
      showContact();
      edit.setAttribute('aria-expanded', 'false');
      edit.focus({ preventScroll: true });
    }

    edit.addEventListener('click', () => {
      name.value = contact?.name || '';
      phone.value = contact?.phone || '';
      message.textContent = '';
      form.hidden = false;
      edit.hidden = true;
      edit.setAttribute('aria-expanded', 'true');
      name.focus();
    });
    retry.addEventListener('click', load);
    cancel.addEventListener('click', () => {
      message.textContent = '';
      closeEditor();
    });
    form.addEventListener('submit', async event => {
      event.preventDefault();
      if (fields.disabled) return;
      const body = { name: name.value, phone: phone.value };
      fields.disabled = true;
      message.textContent = 'Guardando contacto…';
      try {
        const response = await request(path, { ...options, method: 'PUT', body });
        if (!response.contact) throw new Error('No pude confirmar que se guardó. Inténtalo otra vez.');
        contact = response.contact;
        closeEditor();
        message.textContent = 'Contacto guardado.';
      } catch (error) {
        message.textContent = navigator.onLine
          ? error.message || 'No se pudo guardar tu contacto.'
          : 'Conéctate a internet para guardar. Tus datos siguen en el formulario.';
      } finally {
        fields.disabled = false;
      }
    });

    section.hidden = false;
    load();
  }

  window.TecClosingSupervisor = Object.freeze({ init });
})();
