(() => {
  'use strict';
  // «Mi día»: el Coach pregunta qué vas a hacer ahora (citas, llamar, prospectar o terminar) y cada respuesta
  // abre pasos cortos. Los datos del vendedor se pintan con textContent; nada se inserta como HTML.
  const api = '/api/coach/mi-dia';
  const OPTIONS = [['citas', 'Atender citas'], ['llamar', 'Llamar'], ['prospectar', 'Prospectar'], ['terminar', 'Terminé por hoy']];
  const zone = (() => { try { return Intl.DateTimeFormat().resolvedOptions().timeZone || 'America/Chicago'; } catch { return 'America/Chicago'; } })();
  const mapsSearch = query => 'https://www.google.com/maps/search/?api=1&query=' + encodeURIComponent(query);
  const mapsGo = address => 'https://www.google.com/maps/dir/?api=1&destination=' + encodeURIComponent(address);
  const plural = (n, one, many) => n + ' ' + (n === 1 ? one : many);

  function el(tag, props = {}, ...children) {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(props)) {
      if (value == null || value === false) continue;
      if (key === 'text') node.textContent = value;
      else if (key === 'class') node.className = value;
      else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
      else node.setAttribute(key, value === true ? '' : value);
    }
    for (const child of children.flat()) if (child != null && child !== false) node.append(child);
    return node;
  }
  const button = (label, onclick, kind = 'secondary') => el('button', { type: 'button', class: 'tec-my-day__button tec-my-day__button--' + kind, text: label, onclick });
  const link = (label, href, kind = 'secondary') => el('a', { class: 'tec-my-day__button tec-my-day__button--' + kind, href, target: href.startsWith('tel:') ? null : '_blank', rel: 'noopener', text: label });

  function init({ userId, request, openWorkspace, askCoach, remember, openTicketPhoto, openLeadForm, openFourteen }) {
    const root = document.querySelector('[data-my-day-root]');
    if (!root || !userId || typeof request !== 'function') return null;
    const card = root.closest('.tec-my-day-card');
    const id = String(userId);
    let s = null, view = 'ask', block = null, results = {}, busy = false, notice = '', routeStep = 'where', routeResult = null, afterDemo = false;
    let collapsed = sessionStorage.getItem('tec-my-day-collapsed') === '1';

    const call = (path, body) => request(api + path, body ? { method: 'POST', body: { accountId: id, zone, ...body }, timeoutMs: 20000 }
      : { headers: { 'X-TEC-Account-ID': id }, timeoutMs: 20000 });
    const say = text => { notice = text || ''; render(); };

    async function load() {
      try {
        s = await call('?accountId=' + encodeURIComponent(id) + '&zone=' + encodeURIComponent(zone));
        root.hidden = false;
      } catch (error) {
        if (error?.status === 403) { root.hidden = true; card?.classList.add('is-off'); return; }
        notice = error?.message || 'No pude cargar Mi día.';
        root.hidden = !s;
      }
      card?.classList.toggle('is-off', root.hidden);
      render();
    }

    async function choose(mode) {
      afterDemo = false;
      view = mode; notice = '';
      remember?.('Mi día: eligió ' + (OPTIONS.find(o => o[0] === mode)?.[1] || mode).toLowerCase(), '');
      call('/elegir', { mode }).catch(() => null);
      if (mode === 'llamar' && !block) await nextBlock(false);
      if (mode === 'prospectar') routeStep = s?.route ? 'capture' : 'where';
      if (mode === 'terminar' || mode === 'citas') await load(); else render();
    }

    async function nextBlock(more) {
      busy = true; render();
      try { block = await call('/llamadas', { more }); results = {}; notice = block.note || ''; }
      catch (error) { notice = error?.message || 'No pude preparar tus llamadas.'; }
      finally { busy = false; render(); }
    }

    async function setResult(person, result) {
      results[person.ref] = result;
      render();
      try { const data = await call('/llamada', { ref: person.ref, result }); if (s) s.today.calls = data.calls; }
      catch (error) { notice = error?.message || 'No pude anotar la llamada.'; }
      if (result === 'cita') {
        remember?.('Mi día: sacó cita por teléfono', person.name);
        askCoach?.('Saqué cita con ' + person.name + '. Agéndala para el ');
      }
      render();
    }

    function nextChoices(title, exclude, citasLabel = 'Atender citas') {
      return el('div', { class: 'tec-my-day__next' }, el('p', { class: 'tec-my-day__question', text: title }),
        el('div', { class: 'tec-my-day__options' }, OPTIONS.filter(([mode]) => mode !== exclude).map(([mode, label]) =>
          button(mode === 'citas' ? citasLabel : label, () => choose(mode), s?.recommendation?.mode === mode ? 'primary' : 'secondary'))));
    }

    function header() {
      const goal = s.goal.personal || s.goal.standard;
      const pct = Math.min(100, Math.round(100 * s.goal.weekLeads / Math.max(1, goal)));
      return el('div', { class: 'tec-my-day__head' },
        el('div', { class: 'tec-my-day__title' }, el('strong', { text: 'Mi día' }),
          el('span', { text: 'Semana: ' + s.goal.weekLeads + ' de ' + goal + ' leads' + (s.goal.personal ? ' (tu meta) · estándar ' + s.goal.standard : '') })),
        el('div', { class: 'tec-my-day__bar', role: 'progressbar', 'aria-valuemin': '0', 'aria-valuemax': String(goal), 'aria-valuenow': String(s.goal.weekLeads) },
          el('span', { style: 'width:' + pct + '%' })),
        el('div', { class: 'tec-my-day__tools' },
          button(collapsed ? 'Abrir' : 'Minimizar', () => { collapsed = !collapsed; sessionStorage.setItem('tec-my-day-collapsed', collapsed ? '1' : '0'); render(); }, 'link'),
          !collapsed && button('Meta', () => { view = 'meta'; render(); }, 'link'),
          !collapsed && button('Exhibidores', () => { view = 'exhibidores'; render(); }, 'link'),
          !collapsed && pushButton()));
    }

    function askView() {
      const nodes = [];
      if (s.askExhibitor) nodes.push(exhibitorQuestion());
      const today = s.today;
      nodes.push(el('p', { class: 'tec-my-day__stats', text: 'Hoy: ' + plural(today.leads, 'lead', 'leads') + ' · ' + plural(today.calls, 'llamada', 'llamadas') + ' · ' +
        plural(today.demos, 'demo', 'demos') + ' · ' + plural(today.sales, 'venta', 'ventas') + (s.goal.remaining ? ' · Meta de hoy: ' + plural(s.goal.dailyTarget, 'lead', 'leads') : ' · Meta semanal cumplida') }));
      nodes.push(el('p', { class: 'tec-my-day__reason', text: s.recommendation.reason }));
      nodes.push(nextChoices(afterDemo ? '¿Qué sigue? ¿Tienes otra cita, vas a llamar o a prospectar?' : '¿Qué vas a hacer ahora?', '', afterDemo ? 'Otra cita' : 'Atender citas'));
      return nodes;
    }

    function exhibitorQuestion() {
      const reasons = ['No conseguí lugar', 'Esta semana no me toca'];
      return el('div', { class: 'tec-my-day__alert' }, el('p', { text: 'No veo ningún exhibidor esta semana. ¿Por qué no hay uno?' }),
        el('div', { class: 'tec-my-day__options' }, button('Agregar exhibidor', () => { view = 'exhibidores'; render(); }, 'primary'),
          reasons.map(reason => button(reason, async () => {
            try { await call('/exhibidor/sin', { reason }); s.askExhibitor = false; say('Anotado. Busca un lugar para la próxima semana.'); }
            catch (error) { say(error?.message); }
          }))));
    }

    function citasView() {
      const rows = [
        ...s.citas.events.map(e => el('li', {}, el('strong', { text: e.time }), ' ', e.title)),
        ...s.citas.agenda.map(a => el('li', {}, el('strong', { text: a.time || 'Sin hora' }), ' ', a.client + (a.origin ? ' · ' + a.origin : ''), a.address ? ' ' : null,
          a.address ? link('Cómo llegar', mapsGo(a.address), 'link') : null))
      ];
      return [
        el('h3', { text: 'Tus citas de hoy' }),
        rows.length ? el('ul', { class: 'tec-my-day__list' }, rows) : el('p', { text: s.citas.calendarConnected ? 'No veo citas para hoy en tu calendario ni en la Agenda.' : 'Conecta Google Calendar en Mis conexiones para ver tus citas aquí.' }),
        s.citas.upcoming?.length ? el('h3', { text: 'Tus próximas citas' }) : null,
        s.citas.upcoming?.length ? el('ul', { class: 'tec-my-day__list' }, s.citas.upcoming.map(e => el('li', {}, el('strong', { text: e.when }), ' ', e.title))) : null,
        s.citas.busy ? el('p', { class: 'tec-my-day__muted', text: 'Tu calendario principal tiene ' + plural(s.citas.busy, 'bloque ocupado', 'bloques ocupados') + ' hoy.' }) : null,
        ...s.citas.notes.map(n => el('p', { class: 'tec-my-day__muted', text: n })),
        el('div', { class: 'tec-my-day__options' }, button('Abrir Cierre', () => openWorkspace?.('cierre'), 'primary')),
        el('p', { class: 'tec-my-day__muted', text: 'Al terminar la demo, reporta el resultado en Cierre y te pregunto qué sigue.' })
      ];
    }

    function llamarView() {
      if (busy && !block) return [el('p', { text: 'Preparando tus 5 llamadas…' })];
      if (!block) return [button('Darme 5 personas para llamar', () => nextBlock(false), 'primary')];
      if (!block.people.length) return [el('p', { text: 'No hay más personas pendientes de llamar.' }), nextChoices('¿Qué sigue?', 'llamar')];
      const done = block.people.every(p => results[p.ref]);
      return [
        el('h3', { text: 'Llama a estas ' + plural(block.people.length, 'persona', 'personas') }),
        el('ol', { class: 'tec-my-day__calls' }, block.people.map(person => el('li', { class: 'tec-my-day__call' },
          el('div', {}, el('strong', { text: person.name }), el('span', { class: 'tec-my-day__muted', text: ' · ' + person.origin })),
          el('p', { class: 'tec-my-day__why', text: person.reason }),
          el('div', { class: 'tec-my-day__options' }, link('Llamar', 'tel:' + person.phone, 'primary'),
            Object.entries(s.callResults).map(([key, label]) => el('button', { type: 'button', class: 'tec-my-day__chip' + (results[person.ref] === key ? ' is-on' : ''),
              'aria-pressed': results[person.ref] === key ? 'true' : 'false', text: label, onclick: () => setResult(person, key) })))))),
        el('p', { class: 'tec-my-day__muted', text: 'Cuando saques una cita, el Coach se abre para agendarla en tu calendario con su botón. Para anotar algo en la hoja, cuéntaselo al Coach.' }),
        el('div', { class: 'tec-my-day__options' }, block.left ? button('Dame 5 más (' + block.left + ' pendientes)', () => nextBlock(true), done ? 'primary' : 'secondary') : null,
          button('Terminé de llamar', () => { view = 'next-llamar'; render(); }, 'link'))
      ];
    }

    function prospectarView() {
      if (routeStep === 'where') {
        const today = s.exhibitorsToday[0];
        const start = async (place, href) => {
          if (href) window.open(href, '_blank', 'noopener');
          try { await call('/ruta', { action: 'start', place }); } catch {}
          remember?.('Mi día: salió a prospectar', place);
          routeStep = 'capture'; render();
        };
        return [el('h3', { text: '¿Dónde vas a prospectar?' }),
          el('div', { class: 'tec-my-day__stack' },
            today ? button('Mi exhibidor de hoy: ' + today.place + (today.start ? ' · ' + today.start : ''), () => start('exhibidor', mapsGo(today.address)), 'primary') : null,
            s.places.map(place => button(place.label, () => start(place.id, mapsSearch(place.query)))),
            button('Otro lugar', () => start('otro', ''), 'link')),
          el('p', { class: 'tec-my-day__muted', text: 'Google Maps te muestra los lugares cerca de donde estás. Elige tu ruta y vuelve aquí para capturar tus leads.' })];
      }
      if (routeStep === 'capture') {
        return [el('h3', { text: 'Captura tus leads aquí' }),
          el('p', { class: 'tec-my-day__stats', text: 'Hoy llevas ' + plural(s.today.leads, 'lead', 'leads') + (s.goal.remaining ? '. Meta de hoy: ' + s.goal.dailyTarget + '.' : '.') }),
          el('div', { class: 'tec-my-day__stack' }, button('Foto de boletas', () => openTicketPhoto?.(), 'primary'), button('Formulario de lead', () => openLeadForm?.()),
            button('Hoja 4 en 14', () => openFourteen?.())),
          el('p', { class: 'tec-my-day__muted', text: 'Se guardan en TEC y, si tienes tu hoja de Google conectada, puedes pasarlos a tu hoja.' }),
          el('div', { class: 'tec-my-day__options' }, button('Actualizar mis números', load, 'link'), button('Terminé mi ruta', () => { routeStep = 'report'; render(); }))];
      }
      if (routeStep === 'report') {
        const places = el('input', { type: 'number', min: '0', max: '100', inputmode: 'numeric', value: '1', 'aria-label': 'Lugares visitados' });
        const note = el('input', { type: 'text', maxlength: '300', placeholder: 'Qué funcionó o qué no (opcional)', 'aria-label': 'Nota de la ruta' });
        return [el('h3', { text: '¿Cómo te fue en tu ruta?' }),
          el('label', { class: 'tec-my-day__field' }, 'Lugares que visitaste', places), el('label', { class: 'tec-my-day__field' }, 'Nota', note),
          button('Guardar mi ruta', async () => {
            try { routeResult = await call('/ruta', { action: 'finish', places: Number(places.value || 0), note: note.value }); routeStep = 'done'; await load(); }
            catch (error) { say(error?.message); }
          }, 'primary')];
      }
      return [el('p', { class: 'tec-my-day__stats', text: 'Tu ruta: ' + plural(routeResult?.places || 0, 'lugar', 'lugares') + ' y ' + plural(routeResult?.leads || 0, 'lead nuevo', 'leads nuevos') + '.' }),
        nextChoices('¿Qué sigue?', 'prospectar')];
    }

    function terminarView() {
      const goal = s.goal.personal || s.goal.standard;
      return [el('h3', { text: 'Así va tu día' }),
        el('ul', { class: 'tec-my-day__list' },
          el('li', { text: 'Leads: ' + s.today.leads + ' hoy · ' + s.week.leads + ' esta semana' }),
          el('li', { text: 'Llamadas: ' + s.today.calls + ' hoy · ' + s.week.calls + ' esta semana' }),
          el('li', { text: 'Demos: ' + s.today.demos + ' hoy · ' + s.week.demos + ' esta semana' }),
          el('li', { text: 'Ventas: ' + s.today.sales + ' hoy · ' + s.week.sales + ' esta semana' + (s.week.amount ? ' ($' + s.week.amount.toLocaleString('en-US') + ')' : '') })),
        el('p', { class: 'tec-my-day__reason', text: s.goal.remaining ? 'Te faltan ' + s.goal.remaining + ' leads para tu meta de ' + goal + ' esta semana. Con ' + s.goal.dailyTarget + ' por día llegas.' : '¡Llegaste a tu meta de ' + goal + ' leads esta semana!' }),
        el('p', { class: 'tec-my-day__muted', text: 'Descansa. Mañana empiezas fresco.' })];
    }

    function metaView() {
      const input = el('input', { type: 'number', min: '0', max: '1000', inputmode: 'numeric', value: String(s.goal.personal || ''), placeholder: String(s.goal.standard), 'aria-label': 'Mi meta de leads por semana' });
      return [el('h3', { text: 'Tu meta de leads por semana' }),
        el('p', { class: 'tec-my-day__muted', text: 'La meta estándar de TEC es ' + s.goal.standard + ' leads por semana, entre leads y referencias. Si pones la tuya, verás las dos.' }),
        el('label', { class: 'tec-my-day__field' }, 'Mi meta', input),
        el('div', { class: 'tec-my-day__options' }, button('Guardar', async () => {
          try { await call('/meta', { leads: Number(input.value || 0) }); view = 'ask'; await load(); } catch (error) { say(error?.message); }
        }, 'primary'))];
    }

    function exhibidoresView() {
      const f = { day: el('input', { type: 'date', 'aria-label': 'Día', value: s.day }), start: el('input', { type: 'time', 'aria-label': 'Hora de inicio' }),
        end: el('input', { type: 'time', 'aria-label': 'Hora de fin' }), place: el('input', { type: 'text', maxlength: '80', placeholder: 'Lugar, por ejemplo Fresh Market', 'aria-label': 'Lugar' }),
        address: el('input', { type: 'text', maxlength: '200', placeholder: 'Dirección', 'aria-label': 'Dirección' }) };
      return [el('h3', { text: 'Mis exhibidores' }),
        s.exhibitors.length ? el('ul', { class: 'tec-my-day__list' }, s.exhibitors.map(e => el('li', {},
          el('strong', { text: e.day + (e.start ? ' ' + e.start : '') + (e.end ? '–' + e.end : '') }), ' ' + e.place + ' · ', link('Cómo llegar', mapsGo(e.address), 'link'), ' ',
          button('Borrar', async () => { try { await call('/exhibidor/borrar', { id: e.id }); await load(); } catch (error) { say(error?.message); } }, 'link'))))
          : el('p', { text: 'Todavía no tienes exhibidores próximos.' }),
        el('div', { class: 'tec-my-day__form' }, el('label', { class: 'tec-my-day__field' }, 'Día', f.day), el('label', { class: 'tec-my-day__field' }, 'Desde', f.start),
          el('label', { class: 'tec-my-day__field' }, 'Hasta', f.end), el('label', { class: 'tec-my-day__field' }, 'Lugar', f.place), el('label', { class: 'tec-my-day__field' }, 'Dirección', f.address)),
        el('div', { class: 'tec-my-day__options' }, button('Agregar exhibidor', async () => {
          try { await call('/exhibidor', { day: f.day.value, start: f.start.value, end: f.end.value, place: f.place.value, address: f.address.value }); await load(); say('Exhibidor guardado.'); }
          catch (error) { say(error?.message); }
        }, 'primary'))];
    }

    // Avisos al teléfono a las 8 am, 1 pm y 9 pm. Usa la misma suscripción del navegador que el chat del equipo.
    let pushState = 'unknown';
    const pushSupported = () => Boolean(window.Notification && window.PushManager && navigator.serviceWorker && window.isSecureContext);
    const ios = /iPad|iPhone|iPod/.test(navigator.userAgent || '') || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
    const installed = () => window.matchMedia?.('(display-mode: standalone)').matches || navigator.standalone === true;
    const keyBytes = value => Uint8Array.from(atob(value.replace(/-/g, '+').replace(/_/g, '/')), c => c.charCodeAt(0));
    async function pushParts() {
      const [config, registration] = await Promise.all([request(api + '/push/config', { headers: { 'X-TEC-Account-ID': id }, timeoutMs: 10000 }),
        Promise.race([navigator.serviceWorker.ready, new Promise((_, reject) => setTimeout(() => reject(new Error('Vuelve a cargar TEC para preparar los avisos.')), 10000))])]);
      if (config.accountId !== id) throw new Error('La cuenta cambió. Vuelve a entrar en TEC.');
      return { config, registration };
    }
    async function subscribe(config, registration) {
      let subscription = await registration.pushManager.getSubscription();
      const key = keyBytes(config.publicKey), existing = subscription?.options?.applicationServerKey;
      if (subscription && existing && Array.from(new Uint8Array(existing)).join(',') !== Array.from(key).join(',')) { await subscription.unsubscribe(); subscription = null; }
      if (!subscription) subscription = await registration.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: key });
      const result = await request(api + '/push/subscribe', { method: 'POST', body: { accountId: id, zone, subscription: subscription.toJSON() }, timeoutMs: 10000 });
      return result.enabled === true;
    }
    async function preparePush() {
      if (!pushSupported() || (ios && !installed())) { pushState = 'unsupported'; return; }
      try {
        const { config, registration } = await pushParts();
        const subscription = await registration.pushManager.getSubscription();
        let enabled = Boolean(subscription && (await request(api + '/push/status', { method: 'POST', body: { accountId: id, subscription: subscription.toJSON() }, timeoutMs: 10000 })).enabled);
        // Si el vendedor ya los había activado y cambió su sesión, se renuevan sin preguntar.
        if (!enabled && localStorage.getItem('tec-my-day-push') === '1' && Notification.permission === 'granted') enabled = await subscribe(config, registration).catch(() => false);
        pushState = enabled ? 'on' : 'off';
      } catch { pushState = 'off'; }
      render();
    }
    async function togglePush() {
      if (ios && !installed()) { say('En Safari: Compartir → Añadir a pantalla de inicio. Abre TEC desde ese icono y activa los avisos aquí.'); return; }
      if (!pushSupported()) { say('Este navegador no admite avisos. Abre TEC desde su icono instalado.'); return; }
      // El permiso se pide dentro del toque, antes de esperar nada (lo exige el iPhone).
      const permission = pushState === 'on' ? Promise.resolve('granted') : Notification.permission === 'default' ? Notification.requestPermission() : Promise.resolve(Notification.permission);
      try {
        if ((await permission) !== 'granted') { say('Permite las notificaciones de TEC en los ajustes del teléfono.'); return; }
        const { config, registration } = await pushParts();
        if (pushState === 'on') {
          const subscription = await registration.pushManager.getSubscription();
          if (subscription) await request(api + '/push/unsubscribe', { method: 'POST', body: { accountId: id, subscription: subscription.toJSON() }, timeoutMs: 10000 });
          localStorage.removeItem('tec-my-day-push'); pushState = 'off'; say('Avisos de Mi día desactivados.'); return;
        }
        const on = await subscribe(config, registration);
        pushState = on ? 'on' : 'off';
        if (on) localStorage.setItem('tec-my-day-push', '1');
        say(on ? 'Listo: te aviso a las 8 am, 1 pm y 9 pm.' : 'No pude activar los avisos. Vuelve a intentar.');
      } catch (error) { say(error?.message || 'No pude activar los avisos.'); }
    }
    function pushButton() {
      if (pushState === 'unsupported' && !ios) return null;
      return button(pushState === 'on' ? 'Avisos: sí' : 'Activar avisos', togglePush, 'link');
    }

    function render() {
      root.replaceChildren();
      if (!s) { if (notice) root.append(el('p', { class: 'tec-my-day__notice', text: notice })); return; }
      root.append(header());
      if (collapsed) return;
      const body = el('div', { class: 'tec-my-day__body' });
      const views = { ask: askView, citas: citasView, llamar: llamarView, prospectar: prospectarView, terminar: terminarView, meta: metaView, exhibidores: exhibidoresView,
        'next-llamar': () => [el('p', { class: 'tec-my-day__stats', text: 'Hoy llevas ' + plural(s.today.calls, 'llamada', 'llamadas') + '.' }), nextChoices('¿Qué sigue?', 'llamar')] };
      body.append(...[].concat((views[view] || askView)()).filter(Boolean));
      if (view !== 'ask') body.append(button('Ver opciones', () => { view = 'ask'; render(); }, 'link'));
      if (notice) body.append(el('p', { class: 'tec-my-day__notice', role: 'status', text: notice }));
      root.append(body);
    }

    // Mi día vive en la pestaña «Mi actividad»: después de una demo o al tocar un aviso, la app lleva ahí.
    const show = () => { if (root.hidden) return; openWorkspace?.('actividad'); requestAnimationFrame(() => root.scrollIntoView({ behavior: 'smooth', block: 'start' })); };
    window.addEventListener('coach:demo-outcome-saved', async () => {
      afterDemo = true; view = 'ask'; collapsed = false;
      await load();
      show();
    });
    document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'visible' && s && view === 'ask') load(); });
    const fromNotice = new URLSearchParams(location.search).get('miDia') === '1';
    if (fromNotice) collapsed = false;
    load().then(() => { if (fromNotice) show(); if (!root.hidden) preparePush(); });
    return { reload: load };
  }
  window.TecMyDay = { init };
})();
