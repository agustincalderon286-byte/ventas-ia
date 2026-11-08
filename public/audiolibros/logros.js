import {BADGES,ListeningTracker,evaluateBadges} from './logros-core.js?v=1';
import {AchievementStore} from './logros-store.js?v=1';

const root=document.querySelector('#logros'),audio=document.querySelector('#audio');
if(root&&audio){
  const grid=root.querySelector('[data-badge-grid]'),status=root.querySelector('[data-badge-status]'),notice=root.querySelector('[data-badge-notice]');
  let catalog=[],context=null,renderTimer,lastPersist=0,lastSync=0,known=new Set(),baseline=true,identity=null;
  const node=(tag,text,cls)=>{const el=document.createElement(tag);if(text!==undefined)el.textContent=text;if(cls)el.className=cls;return el;};
  const request=async(method,body)=>{
    if(!navigator.onLine)throw new Error('offline');
    const json=body?JSON.stringify(body):null;
    const response=await fetch('/api/audiolibros/logros',{method,credentials:'same-origin',cache:'no-store',signal:AbortSignal.timeout(10_000),
      ...(json?{headers:{'Content-Type':'application/json'},body:json,keepalive:json.length<60000}:{})});
    const data=await response.json();if(!response.ok)throw new Error('achievement sync unavailable');return data;
  };
  let storage;try{storage=window.localStorage;}catch{storage={getItem(){return null;},setItem(){throw new Error('storage unavailable');}};}
  const store=new AchievementStore({storage,request,onChange:()=>{if(!renderTimer)renderTimer=setTimeout(()=>{renderTimer=null;render();},1000);},onStatus:text=>{status.textContent=text;}});
  const tracker=new ListeningTracker((a,b,reading)=>{if(context)store.record(context,a,b,reading);});
  function medal(badge){
    const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');svg.setAttribute('viewBox','0 0 100 110');svg.setAttribute('aria-hidden','true');svg.classList.add('badge-medal');
    const shape=document.createElementNS(ns,'path');shape.setAttribute('d','M50 3 88 18v36c0 24-22 42-38 51C34 96 12 78 12 54V18Z');shape.setAttribute('class','badge-shield');
    const circle=document.createElementNS(ns,'circle');circle.setAttribute('cx','50');circle.setAttribute('cy','45');circle.setAttribute('r','26');circle.setAttribute('class','badge-ring');
    const text=document.createElementNS(ns,'text');text.setAttribute('x','50');text.setAttribute('y','52');text.setAttribute('text-anchor','middle');text.setAttribute('class','badge-stamp');text.textContent=badge.stamp;
    const star=document.createElementNS(ns,'path');star.setAttribute('d','m50 77 2.5 5 5.5.8-4 4 .9 5.5-4.9-2.6-4.9 2.6.9-5.5-4-4 5.5-.8Z');star.setAttribute('class','badge-star');svg.append(shape,circle,text,star);return svg;
  }
  function render(){
    if(!catalog.length)return;
    const view=evaluateBadges(store.state,catalog),earned=view.badges.filter(b=>b.earned),newBadges=earned.filter(b=>!known.has(b.id));
    if(!baseline&&newBadges.length)notice.textContent='¡Nuevo logro! '+newBadges.map(b=>b.name).join(' · ');
    baseline=false;known=new Set(earned.map(b=>b.id));
    root.querySelector('[data-badge-count]').textContent=view.earned+' de '+view.total;
    const progress=root.querySelector('progress');progress.value=view.earned;progress.max=view.total;
    root.querySelector('[data-badge-summary]').textContent=Math.floor(view.seconds/60)+' min de contenido · '+view.completed+' '+(view.completed===1?'libro completado':'libros completados');
    const cards=view.badges.map(b=>{
      const card=node('article',undefined,'badge-card '+(b.earned?'is-earned':'is-locked'));card.dataset.group=b.group;card.dataset.badge=b.id;
      card.append(medal(b),node('span',b.earned?'Desbloqueada':'Por descubrir','badge-state'),node('h3',b.name),node('p',b.description));
      const amount=b.group==='tiempo'||b.group==='lectura'?Math.floor(b.current/60)+' / '+b.target/60+' min':Math.floor(b.current)+' / '+b.target;
      card.append(node('span',b.earned?'✓ Logro conseguido':amount,'badge-amount'));return card;
    });grid.replaceChildren(...cards);
  }
  function tick(playingOverride){
    if(!context)return;
    const reading=document.querySelector('#player').open&&document.querySelector('#reader-panel').open&&!document.querySelector('#reader-panel').hidden&&!document.hidden;
    tracker.tick({position:audio.currentTime,now:Date.now(),playing:playingOverride??(!audio.paused&&!audio.ended),rate:audio.playbackRate,reading,seeking:audio.seeking});
  }
  function checkpoint(sync=false){tick();store.persist();lastPersist=Date.now();if(sync){lastSync=Date.now();void store.sync();}}
  window.TecAudioAchievements={contextChanged(data){
    catalog=data.catalog||catalog;
    const nextIdentity=data.account?.authenticated&&/^[a-f0-9]{64}$/.test(data.account.achievementKey||'')?data.account.achievementKey:'';
    if(identity!==nextIdentity){checkpoint(false);context=null;tracker.reset();identity=nextIdentity;baseline=true;known=new Set();void store.setIdentity(identity).then(()=>{if(identity===nextIdentity){baseline=true;render();}});tracker.reset();}
    root.querySelector('.logros-login').hidden=Boolean(identity);
    const book=data.book;
    if(context?.id!==book?.id||context?.full!==book?.full){checkpoint(true);context=book||null;tracker.reset();tick();}
    render();
  }};
  for(const event of ['play','playing','pause','ended','ratechange'])audio.addEventListener(event,()=>{tick();if(event==='pause'||event==='ended')checkpoint(true);});
  audio.addEventListener('waiting',()=>tick(false));
  audio.addEventListener('seeking',()=>tracker.reset());audio.addEventListener('seeked',()=>{tracker.reset();tick();});
  audio.addEventListener('timeupdate',()=>{tick();if(Date.now()-lastPersist>10000){store.persist();lastPersist=Date.now();}if(Date.now()-lastSync>30000){lastSync=Date.now();void store.sync();}});
  document.querySelector('#reader-panel').addEventListener('toggle',()=>tick());
  window.addEventListener('pagehide',()=>checkpoint(true));document.addEventListener('visibilitychange',()=>checkpoint(true));
  window.addEventListener('online',()=>{void store.sync();});window.addEventListener('storage',event=>{if(event.key===store.key)store.absorbStorage();});
  root.querySelector('[data-badge-login]').addEventListener('click',()=>document.querySelector('#account-button').click());
  document.addEventListener('tec-audio-account-context',event=>window.TecAudioAchievements.contextChanged(event.detail));
  document.dispatchEvent(new Event('tec-audio-achievements-ready'));
}
