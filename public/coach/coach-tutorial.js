(() => {
  'use strict';
  function init(root=document) {
    const dialog=root.querySelector('[data-coach-tutorial]');
    const openers=Array.from(root.querySelectorAll('[data-open-coach-tutorial]'));
    if(!dialog || !openers.length || dialog.dataset.ready)return;
    dialog.dataset.ready='true';
    const close=dialog.querySelector('[data-close-coach-tutorial]'),video=dialog.querySelector('video'),source=dialog.querySelector('[data-tutorial-video-src]');
    const search=dialog.querySelector('[data-tutorial-search]'),result=dialog.querySelector('[data-tutorial-search-result]'),items=Array.from(dialog.querySelectorAll('[data-tutorial-faq]'));
    let opener=null;
    const pause=()=>video?.pause();
    const restore=()=>{pause();dialog.hidden=true;opener?.focus({preventScroll:true});};
    const fold=text=>String(text).normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
    for(const button of openers)button.addEventListener('click',()=>{
      const menu=button.closest('[data-visit-account-menu]');opener=menu?.querySelector('summary') || button;if(menu)menu.open=false;
      dialog.hidden=false;
      if(source && (!source.getAttribute('src') || video.error)){source.setAttribute('src',source.dataset.tutorialVideoSrc);video.load();}
      if(typeof dialog.showModal==='function')dialog.showModal();else dialog.setAttribute('open','');
      close.focus({preventScroll:true});
    });
    close.addEventListener('click',()=>{if(typeof dialog.close==='function')dialog.close();else{dialog.removeAttribute('open');restore();}});
    dialog.addEventListener('close',restore);
    dialog.addEventListener('cancel',()=>{if(typeof dialog.close!=='function'){dialog.removeAttribute('open');restore();}});
    const fail=()=>{dialog.querySelector('[data-tutorial-video-error]').hidden=false;};
    video.addEventListener('error',fail);source.addEventListener('error',fail);
    video.addEventListener('loadeddata',()=>{dialog.querySelector('[data-tutorial-video-error]').hidden=true;});
    video.addEventListener('play',()=>{if(dialog.hidden || !dialog.open || document.hidden){pause();return;}root.querySelectorAll('audio,video').forEach(media=>{if(media!==video)media.pause();});});
    document.addEventListener('visibilitychange',()=>{if(document.hidden)pause();});
    window.addEventListener('pagehide',pause);
    search.addEventListener('input',()=>{
      const query=fold(search.value.trim());let count=0;
      for(const item of items){const match=!query || fold(item.textContent).includes(query);item.hidden=!match;item.open=Boolean(query)&&match;if(match)count++;}
      result.textContent=query?(count?`${count} ${count===1?'respuesta encontrada':'respuestas encontradas'}.`:'No hay coincidencias. Prueba con audio, tarjetas o mensajes.') : '';
    });
    return {close:()=>close.click()};
  }
  window.TecCoachTutorial=Object.freeze({init});init();
})();
