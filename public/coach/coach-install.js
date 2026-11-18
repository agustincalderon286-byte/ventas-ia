(() => {
  'use strict';
  function init(root=document,env=window){
    const openers=Array.from(root.querySelectorAll('[data-open-tec-install]'));
    if(!openers.length || root.querySelector('[data-tec-install-dialog]'))return;
    const dialog=root.createElement('dialog');
    dialog.className='tec-install';dialog.id='tec-install-dialog';dialog.dataset.tecInstallDialog='';
    dialog.setAttribute('aria-labelledby','tec-install-title');dialog.hidden=true;
    dialog.innerHTML='<header><div><span class="tec-install__eyebrow">TEC a un toque</span><h2 id="tec-install-title">Instala TEC en tu teléfono</h2></div><button type="button" data-install-close aria-label="Cerrar instalación">Cerrar ×</button></header><div class="tec-install__body"><p data-install-description></p><button class="tec-install__primary" type="button" data-install-native hidden>Instalar TEC</button><p class="tec-install__status" data-install-status role="status" aria-live="polite"></p><details data-install-steps><summary>Ver los pasos del navegador</summary><ol data-install-instructions></ol><p class="tec-install__note">Si aparece Crear acceso directo, esa opción añade un icono que abre el navegador. Para una app independiente, elige Instalar cuando esté disponible.</p></details><p class="tec-install__note">La instalación no cambia tu cuenta ni tu membresía. Usa tu acceso de TEC al abrirla.</p></div>';
    root.body.append(dialog);
    const close=dialog.querySelector('[data-install-close]'),install=dialog.querySelector('[data-install-native]'),status=dialog.querySelector('[data-install-status]'),description=dialog.querySelector('[data-install-description]'),instructions=dialog.querySelector('[data-install-instructions]'),steps=dialog.querySelector('[data-install-steps]');
    const mode=env.matchMedia?.('(display-mode: standalone)');
    const ios=/iPad|iPhone|iPod/.test(env.navigator?.userAgent||'') || (env.navigator?.platform==='MacIntel' && env.navigator?.maxTouchPoints>1);
    const android=/Android/.test(env.navigator?.userAgent||'');
    const title=dialog.querySelector('#tec-install-title');if(title)title.textContent=ios||android?'Instala TEC en tu teléfono':'Instala TEC en este dispositivo';
    const standalone=()=>Boolean(mode?.matches || env.navigator?.standalone);
    let deferred=null,pending=false,accepted=false,opener=null;
    const guidance=ios?['Abre totalelitecoaching.com/coach/ en Safari.','Abre Compartir y elige Añadir a pantalla de inicio. Si hace falta, toca Ver más.','Deja activado Abrir como app web si aparece y toca Añadir.','Abre TEC Coach desde el icono de tu pantalla de inicio.']:['Abre totalelitecoaching.com/coach/ en Chrome.','Abre los tres puntos del navegador y elige Instalar o Añadir a pantalla de inicio.','Elige Instalar y confirma cuando el navegador ofrezca esa opción.','Espera a que termine y abre TEC desde el nuevo icono.'];
    if(!ios&&!android)guidance.splice(0,guidance.length,'Abre totalelitecoaching.com/coach/ en Chrome o Edge.','Busca Instalar TEC en la barra de direcciones o en el menú del navegador.','Confirma Instalar cuando el navegador ofrezca esa opción.','Abre TEC desde su icono en las aplicaciones de tu computadora.');
    for(const text of guidance){const li=root.createElement('li');li.textContent=text;instructions.append(li);}
    const render=()=>{
      install.hidden=standalone()||accepted||(!deferred&&!pending);install.disabled=pending;
      install.textContent=pending?'Esperando al navegador…':'Instalar TEC';
      description.textContent=standalone()?'Ya abriste TEC como app independiente.':ios?'Añade TEC desde el menú Compartir de Safari.':deferred?'Tu navegador ofrece instalar TEC como app. Toca el botón y confirma.':'Puedes añadir TEC desde el menú del navegador. Si ofrece Instalar, elígelo para abrirla como app independiente.';
      if(standalone())status.textContent='TEC está abierta en modo app.';
      else if(accepted)status.textContent='Solicitud de instalación aceptada. Espera a que aparezca el nuevo icono y ábrelo.';
    };
    const restore=()=>{dialog.hidden=true;opener?.focus({preventScroll:true});};
    for(const button of openers){button.setAttribute('aria-controls',dialog.id);button.addEventListener('click',()=>{
      opener=button;const menu=button.closest('[data-visit-account-menu]');if(menu)menu.open=false;
      root.querySelectorAll('audio,video').forEach(media=>media.pause());
      render();steps.open=!deferred&&!standalone()&&!accepted;dialog.hidden=false;
      if(typeof dialog.showModal==='function')dialog.showModal();else dialog.setAttribute('open','');
      close.focus({preventScroll:true});
    });}
    close.addEventListener('click',()=>{if(typeof dialog.close==='function')dialog.close();else{dialog.removeAttribute('open');restore();}});
    dialog.addEventListener('close',restore);
    dialog.addEventListener('cancel',()=>{if(typeof dialog.close!=='function'){dialog.removeAttribute('open');restore();}});
    env.addEventListener('beforeinstallprompt',event=>{event.preventDefault();deferred=event;accepted=false;status.textContent='';render();});
    env.addEventListener('appinstalled',()=>{deferred=null;pending=false;accepted=true;render();});
    mode?.addEventListener?.('change',render);
    install.addEventListener('click',async()=>{
      if(!deferred||pending||standalone()||accepted)return;
      const event=deferred;deferred=null;pending=true;status.textContent='Confirma la instalación en la ventana de tu navegador.';render();
      try{
        await event.prompt();const choice=await event.userChoice;
        if(!accepted)status.textContent=choice?.outcome==='accepted'?'Instalación solicitada. Espera a que termine el navegador y abre el icono nuevo.':'No se instaló TEC. Puedes volver a intentarlo desde el menú del navegador.';
      }catch{if(!accepted)status.textContent='No se pudo abrir la instalación. Usa los pasos del menú de tu navegador.';}
      finally{pending=false;render();}
    });
    render();return{dialog};
  }
  window.TecCoachInstall=Object.freeze({init});init();
})();
