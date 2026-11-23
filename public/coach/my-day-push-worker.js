(() => {
  'use strict';
  // Avisos de «Mi día». El proveedor solo entrega una señal genérica; el texto se pide a TEC con la sesión del vendedor.
  async function inbox(){
    const subscription=await self.registration.pushManager.getSubscription();if(!subscription)return null;
    const response=await fetch('/api/coach/mi-dia/push/config',{credentials:'include',cache:'no-store'});if(!response.ok)return null;
    const config=await response.json();
    const result=await fetch('/api/coach/mi-dia/push/inbox',{method:'POST',credentials:'include',cache:'no-store',headers:{'Content-Type':'application/json','X-TEC-Account-ID':config.accountId},body:JSON.stringify({accountId:config.accountId,subscription:subscription.toJSON()})});
    if(!result.ok)return null;
    return result.json();
  }
  const target=()=>new URL('/coach/app/?miDia=1',self.location.origin).href;
  const short=(value,max)=>String(value||'').replace(/\s+/g,' ').trim().slice(0,max);
  self.addEventListener('push',event=>{
    let signal;try{signal=event.data?.json();}catch{return;}
    if(signal?.type!=='tec-day')return;
    event.waitUntil((async()=>{
      const latest=await inbox().catch(()=>null);
      // Sin respuesta de TEC igual se avisa, con un texto general, para no perder el recordatorio.
      const title=latest?.notify?short(latest.title,80):'TEC · Mi día';
      const body=latest?.notify?short(latest.body,240):'Abre TEC para ver tu plan y tu meta de hoy.';
      await self.registration.showNotification(title||'TEC · Mi día',{body,icon:'/coach/icons/icon-192.png?v=20261021',badge:'/coach/icons/icon-192.png?v=20261021',tag:'tec-my-day',renotify:true,data:{type:'tec-day'}});
    })());
  });
  self.addEventListener('notificationclick',event=>{
    if(event.notification?.data?.type!=='tec-day')return;
    event.notification.close();
    event.waitUntil((async()=>{
      const windows=await self.clients.matchAll({type:'window',includeUncontrolled:true});
      const existing=windows.find(client=>{try{const u=new URL(client.url);return u.origin===self.location.origin&&u.pathname.startsWith('/coach/');}catch{return false;}});
      if(existing&&typeof existing.navigate==='function'){await existing.navigate(target());await existing.focus();}
      else await self.clients.openWindow(target());
    })());
  });
})();
