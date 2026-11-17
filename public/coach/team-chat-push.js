(() => {
  'use strict';
  const path='/api/coach/team-chat/push';
  const bytes=value=>Uint8Array.from(atob(value.replace(/-/g,'+').replace(/_/g,'/')),c=>c.charCodeAt(0));
  function init({root,userId,request}){
    const wrap=document.createElement('div');wrap.className='tec-team-chat__push';
    const button=document.createElement('button');button.type='button';button.textContent='Activar avisos';button.disabled=true;
    const status=document.createElement('small');status.setAttribute('role','status');status.setAttribute('aria-live','polite');
    wrap.append(button,status);root.append(wrap);
    let destroyed=false,busy=false,enabled=false,config=null,registration=null;
    const ios=/iPad|iPhone|iPod/.test(navigator.userAgent||'')||(navigator.platform==='MacIntel'&&navigator.maxTouchPoints>1);
    const installed=()=>window.matchMedia?.('(display-mode: standalone)').matches||navigator.standalone===true;
    const supported=()=>Boolean(window.Notification&&window.PushManager&&navigator.serviceWorker&&window.isSecureContext);
    const tell=text=>{if(!destroyed)status.textContent=text;};
    function render(){if(destroyed)return;button.disabled=busy;button.textContent=busy?'Preparando…':enabled?'Avisos activados · desactivar':'Activar avisos';button.setAttribute('aria-pressed',enabled?'true':'false');button.title='Notificaciones de este grupo en este dispositivo';}
    const fields=subscription=>({accountId:String(userId),roomId:config.roomId,subscription:subscription.toJSON()});
    async function prepare(){
      if(destroyed)return;
      if(ios&&!installed()){render();return;}
      if(!supported()){render();return;}
      busy=true;render();
      try{
        [config,registration]=await Promise.all([request(path+'/config',{headers:{'X-TEC-Account-ID':String(userId)},timeoutMs:10000}),Promise.race([navigator.serviceWorker.ready,new Promise((_,reject)=>setTimeout(()=>reject(new Error('Vuelve a cargar TEC para preparar los avisos.')),10000))])]);
        if(destroyed)return;
        if(config.accountId!==String(userId))throw new Error('La cuenta cambió. Vuelve a entrar en TEC.');
        const subscription=await registration.pushManager.getSubscription();
        enabled=Boolean(subscription&&(await request(path+'/status',{method:'POST',body:fields(subscription),timeoutMs:10000})).enabled);
      }catch(error){tell(error.message||'No pude preparar los avisos. Pulsa para reintentar.');}
      finally{busy=false;render();}
    }
    async function toggle(){
      if(destroyed||busy)return;
      if(ios&&!installed()){tell('En Safari: Compartir → Añadir a pantalla de inicio. Abre TEC desde ese icono y activa los avisos aquí.');return;}
      if(!supported()){tell('Este navegador no admite los avisos de TEC. Usa un navegador compatible o abre TEC desde su icono instalado.');return;}
      if(!config||!registration){await prepare();return;}
      // Request permission directly inside the tap, before any await (required on iPhone).
      const permission=enabled?Promise.resolve('granted'):Notification.permission==='default'?Notification.requestPermission():Promise.resolve(Notification.permission);
      busy=true;render();
      try{
        const granted=await permission;if(destroyed)return;
        if(granted!=='granted'){tell('Permite las notificaciones de TEC en los ajustes del navegador.');return;}
        let subscription=await registration.pushManager.getSubscription();if(destroyed)return;
        if(enabled){
          if(subscription){const result=await request(path+'/unsubscribe',{method:'POST',body:fields(subscription),timeoutMs:10000});if(!destroyed&&!result.anyEnabled)await subscription.unsubscribe();}
          enabled=false;tell('Avisos desactivados para este grupo en este dispositivo.');return;
        }
        const key=bytes(config.publicKey),existingKey=subscription?.options?.applicationServerKey;
        if(subscription&&existingKey&&Array.from(new Uint8Array(existingKey)).join(',')!==Array.from(key).join(',')){await subscription.unsubscribe();subscription=null;}
        if(!subscription)subscription=await registration.pushManager.subscribe({userVisibleOnly:true,applicationServerKey:key});
        const result=await request(path+'/subscribe',{method:'POST',body:fields(subscription),timeoutMs:10000});
        if(destroyed)return;
        enabled=result.enabled===true;tell(enabled?'Recibirás avisos de este grupo aunque TEC esté cerrado.':'No pude activar los avisos. Vuelve a intentar.');
      }catch(error){tell(error.message||'No pude activar los avisos. Vuelve a intentar.');}
      finally{busy=false;render();}
    }
    button.addEventListener('click',toggle);void prepare();
    return {destroy(){destroyed=true;button.removeEventListener('click',toggle);}};
  }
  window.TecTeamChatPush={init};
})();
