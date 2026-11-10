(() => {
  'use strict';
  const catalog=globalThis.TecProductModelCatalog;
  if(!catalog)return;
  let dialog,title,stage,status,retry,note,reset,viewer,opener,activeModel,loadPromise,loadTimer;
  let revision=0,previousOverflow='',active=false;
  const node=(tag,cls,text)=>{const el=document.createElement(tag);if(cls)el.className=cls;if(text)el.textContent=text;return el;};
  const asset=(model,name)=>`/api/coach/product-models/${encodeURIComponent(model.id)}/${name}`;
  function releaseViewer(){
    window.clearTimeout(loadTimer);
    if(viewer){viewer.removeAttribute('src');viewer.remove();viewer=null;}
    stage.replaceChildren();reset.hidden=true;
  }
  function finishClose(){
    if(!active||dialog.open)return;
    active=false;revision++;releaseViewer();activeModel=null;
    document.body.style.overflow=previousOverflow;
    if(opener?.isConnected)opener.focus({preventScroll:true});
    opener=null;
  }
  function close(){if(dialog?.open){dialog.close();finishClose();}}
  function createDialog(){
    dialog=node('dialog','tec-model-dialog');dialog.setAttribute('aria-labelledby','tec-model-title');
    const header=node('header','tec-model-header'),titles=node('div');
    title=node('h2');title.id='tec-model-title';
    titles.append(node('p','tec-model-eyebrow','Royal Prestige · 3D'),title);
    const back=node('button','tec-model-close','✕ Cerrar y volver');back.type='button';back.autofocus=true;back.addEventListener('click',close);
    header.append(titles,back);
    const body=node('div','tec-model-body');
    stage=node('div','tec-model-stage');stage.setAttribute('aria-label','Visor del producto en 3D');
    status=node('p','tec-model-status');status.setAttribute('role','status');
    const tools=node('div','tec-model-tools');
    reset=node('button','secondary-button','Centrar producto');reset.type='button';reset.hidden=true;
    reset.addEventListener('click',()=>{if(!viewer||!activeModel)return;viewer.cameraOrbit=activeModel.orbit;viewer.fieldOfView='auto';viewer.jumpCameraToGoal();});
    retry=node('button','secondary-button','Reintentar modelo');retry.type='button';retry.hidden=true;
    retry.addEventListener('click',()=>load(activeModel));
    tools.append(reset,retry);
    note=node('p','tec-model-note');
    body.append(stage,status,tools,note);dialog.append(header,body);document.body.append(dialog);
    dialog.addEventListener('close',finishClose);
    dialog.addEventListener('cancel',event=>{event.preventDefault();close();});
    window.addEventListener('pagehide',close);
    window.addEventListener('coach:workspace-change',close);
  }
  function ensureViewer(){
    if(!loadPromise)loadPromise=import('/coach/vendor/model-viewer/4.4.0/model-viewer.min.js').then(()=>{ customElements.get('model-viewer').modelCacheSize=1; }).catch(error=>{loadPromise=null;throw error;});
    return loadPromise;
  }
  async function load(model){
    if(!model||!dialog.open)return;
    const turn=++revision;releaseViewer();retry.hidden=true;
    if(navigator.onLine===false){status.textContent='Necesitas conexión para abrir el modelo. Los datos de tu calculadora se conservan.';retry.hidden=false;return;}
    status.textContent='Abriendo el producto en 3D…';
    const timeout=loadTimer=window.setTimeout(()=>{if(turn===revision&&dialog.open){status.textContent='El modelo está tardando en cargar. Puedes reintentar o volver a la calculadora.';retry.hidden=false;}},30000);
    try{
      await ensureViewer();if(turn!==revision||!dialog.open)return;
      const current=node('model-viewer');viewer=current;
      current.setAttribute('alt',`Modelo 3D de ${model.title}`);
      current.setAttribute('camera-controls','');current.setAttribute('touch-action','pan-y');
      current.setAttribute('ar','');
      // Scene Viewer cannot forward the Coach session. Quick Look is generated
      // locally from the loaded GLB; WebXR reuses the authenticated model.
      current.setAttribute('ar-modes','webxr quick-look');
      current.setAttribute('environment-image','neutral');current.setAttribute('shadow-intensity','0.8');
      current.setAttribute('camera-orbit',model.orbit);current.setAttribute('interaction-prompt','none');
      current.setAttribute('poster',asset(model,'poster'));
      const ar=node('button','tec-model-ar','Ver en mi espacio');ar.type='button';ar.slot='ar-button';current.append(ar);
      current.addEventListener('load',()=>{
        if(turn!==revision)return;
        window.clearTimeout(timeout);retry.hidden=true;reset.hidden=false;
        status.textContent=current.canActivateAR?'Gira y acerca el producto. Toca «Ver en mi espacio» para colocarlo con la cámara.':'Gira y acerca el producto con tus dedos o el mouse. La vista con cámara depende del dispositivo y navegador.';
      });
      current.addEventListener('error',()=>{
        if(turn!==revision)return;
        window.clearTimeout(timeout);status.textContent='No se pudo abrir el modelo. Comprueba tu conexión o vuelve a iniciar sesión en TEC.';retry.hidden=false;
      });
      current.addEventListener('ar-status',event=>{
        if(turn===revision&&event.detail.status==='failed')status.textContent='Este navegador no pudo iniciar la cámara en AR. Puedes seguir viendo el producto en 3D.';
      });
      stage.append(current);current.setAttribute('src',asset(model,'model.glb'));
    }catch{
      window.clearTimeout(timeout);if(turn!==revision)return;
      status.textContent='No se pudo cargar el visor. Revisa tu conexión y toca Reintentar modelo.';retry.hidden=false;
    }
  }
  function open(model,button){
    if(!model)return;if(!dialog)createDialog();if(dialog.open)return;
    activeModel=model;opener=button;title.textContent=model.title;
    note.textContent=(model.note?model.note+' ':'')+'Recreación visual. Sus proporciones no son una referencia de medidas.';
    previousOverflow=document.body.style.overflow;dialog.showModal();document.body.style.overflow='hidden';active=true;
    load(model);
  }
  function mount(container,product){
    if(!container)return;
    container.replaceChildren();const model=catalog.forProduct(product);container.hidden=!model;if(!model)return;
    const button=node('button','secondary-button tec-product-model-button','◇ Ver en 3D / AR');button.type='button';
    button.setAttribute('aria-haspopup','dialog');button.setAttribute('aria-label',`Ver ${model.title} en 3D o realidad aumentada`);
    button.addEventListener('click',event=>{event.preventDefault();open(model,button);});container.append(button);
  }
  window.TecProductModels=Object.freeze({mount});
})();
