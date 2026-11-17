(() => {
  'use strict';
  async function inbox(){
    const subscription=await self.registration.pushManager.getSubscription();if(!subscription)return null;
    const response=await fetch('/api/coach/team-chat/push/config',{credentials:'include',cache:'no-store'});if(!response.ok)return null;
    const config=await response.json();
    const result=await fetch('/api/coach/team-chat/push/inbox',{method:'POST',credentials:'include',cache:'no-store',headers:{'Content-Type':'application/json','X-TEC-Account-ID':config.accountId},body:JSON.stringify({accountId:config.accountId,roomId:config.roomId,subscription:subscription.toJSON()})});
    if(!result.ok)return null;
    return result.json();
  }
  function safeTarget(value){
    try{const url=new URL(value,self.location.origin);if(url.origin===self.location.origin&&url.pathname==='/coach/app/'&&url.searchParams.get('openTeamChat')==='1'&&/^[a-f0-9]{24}$/i.test(url.searchParams.get('teamChatOwnerId')||''))return url.href;}catch{}
    return new URL('/coach/app/?openTeamChat=1',self.location.origin).href;
  }
  self.addEventListener('push',event=>{
    let signal;try{signal=event.data?.json();}catch{return;}
    if(signal?.type!=='tec-chat')return;
    event.waitUntil((async()=>{
      const latest=await inbox();if(!latest?.notify)return;
      await self.registration.showNotification('TEC · Chat del equipo',{body:'Tienes un mensaje nuevo en tu equipo.',icon:'/coach/icons/icon-192.png?v=20261021',badge:'/coach/icons/icon-192.png?v=20261021',tag:'tec-team-chat',renotify:false,data:{type:'tec-chat'}});
    })().catch(()=>{}));
  });
  self.addEventListener('notificationclick',event=>{
    if(event.notification?.data?.type!=='tec-chat')return;
    event.notification.close();
    event.waitUntil((async()=>{
      let latest=null;try{latest=await inbox();}catch{}
      const target=safeTarget(latest?.notify?latest.url:null);
      const windows=await self.clients.matchAll({type:'window',includeUncontrolled:true});
      const existing=windows.find(client=>{try{const u=new URL(client.url);return u.origin===self.location.origin&&u.pathname.startsWith('/coach/');}catch{return false;}});
      if(existing&&typeof existing.navigate==='function'){await existing.navigate(target);await existing.focus();}
      else await self.clients.openWindow(target);
    })());
  });
})();
