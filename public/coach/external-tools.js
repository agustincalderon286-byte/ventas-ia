/* Konverta remains a separate service: no TEC contact data or credentials are passed. */
(() => {
  'use strict';
  const URL = 'https://dash.konverta.app/signin';
  let dialog, frame, opener, bodyStyle, scrollY, active = false;
  function close(fromHistory = false) {
    if (!active) return;
    active = false;
    dialog.close(); frame.removeAttribute('src');
    document.body.style.cssText = bodyStyle;
    window.scrollTo(0, scrollY); opener?.focus({preventScroll:true});
    if (!fromHistory && history.state?.tecKonverta) history.replaceState({...history.state,tecKonverta:false},'',location.href);
  }
  async function openKonverta() {
    if (active) return;
    if (!dialog) {
      dialog = document.createElement('dialog'); dialog.className = 'tec-external-tool';
      dialog.setAttribute('aria-label', 'Konverta');
      dialog.innerHTML = `<header><button type="button" data-konverta-close>← Volver a TEC</button><div><strong>Konverta</strong><span>dash.konverta.app</span></div><a href="${URL}" target="_blank" rel="noopener noreferrer">Abrir aparte ↗</a></header><iframe title="Konverta · iniciar sesión" referrerpolicy="no-referrer" sandbox="allow-forms allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox allow-downloads"></iframe><p>Si tu inicio de sesión no funciona en esta vista, usa «Abrir aparte».</p>`;
      document.body.append(dialog); frame = dialog.querySelector('iframe');
      dialog.querySelector('[data-konverta-close]').addEventListener('click',()=>close());
      dialog.addEventListener('cancel',event=>{event.preventDefault();close();});
      window.addEventListener('popstate',()=>{if(active&&!history.state?.tecKonverta)close(true);});
    }
    opener = document.activeElement; bodyStyle = document.body.style.cssText; scrollY = window.scrollY;
    document.body.style.position='fixed';document.body.style.top=`-${scrollY}px`;document.body.style.width='100%';
    active = true; dialog.showModal();
    const state={...history.state,tecKonverta:true,tecKonvertaSlot:true};
    if(history.state?.tecKonvertaSlot)history.replaceState(state,'',location.href);else history.pushState(state,'',location.href);
    frame.src = URL;
  }
  window.TecExternalTools = Object.freeze({openKonverta});
  document.addEventListener('click',event=>{
    const link=event.target.closest('[data-open-konverta]');
    if(!link||event.defaultPrevented||event.metaKey||event.ctrlKey||event.shiftKey||event.altKey)return;
    event.preventDefault(); openKonverta();
  });
})();
