import {BADGES,ListeningTracker,evaluateBadges} from './logros-core.js?v=1';
import {badgeSvg} from './insignias-art.js?v=2';
import {BadgeSharingController} from './insignias-compartir.js?v=2';
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
  const shareDialog=document.querySelector('#badge-share');
  const sharing=shareDialog?new BadgeSharingController({dialog:shareDialog,eligible:id=>evaluateBadges(store.state,catalog).badges.some(b=>b.id===id&&b.earned)}):null;
  function medal(badge){
    const svg=new DOMParser().parseFromString(badgeSvg(badge.id,badge.earned),'image/svg+xml').documentElement;
    const element=document.importNode(svg,true);element.setAttribute('aria-hidden','true');element.classList.add('badge-medal');return element;
  }
  function render(){
    if(!catalog.length)return;
    const view=evaluateBadges(store.state,catalog),earned=view.badges.filter(b=>b.earned),newBadges=earned.filter(b=>!known.has(b.id));
    if(!baseline&&newBadges.length)notice.textContent='¡Nuevo logro! '+newBadges.map(b=>b.name).join(' · ');
    baseline=false;known=new Set(earned.map(b=>b.id));
    root.querySelector('[data-badge-count]').textContent=view.earned+' de '+view.total;
    const progress=root.querySelector('progress');progress.value=view.earned;progress.max=view.total;
    root.querySelector('[data-badge-summary]').textContent=Math.floor(view.seconds/60)+' min de contenido · '+view.completed+' '+(view.completed===1?'libro completado':'libros completados');
    for(const b of view.badges){
      let card=grid.querySelector('[data-badge="'+b.id+'"]');
      if(!card){
        card=node('article');card.dataset.group=b.group;card.dataset.badge=b.id;
        card.append(medal(b),node('span',undefined,'badge-state'),node('h3',b.name),node('p',b.description),node('span',undefined,'badge-amount'));grid.append(card);
      }
      const state=b.earned?'earned':'locked';
      if(card.dataset.state!==state){card.querySelector('.badge-medal').replaceWith(medal(b));card.dataset.state=state;}
      card.className='badge-card '+(b.earned?'is-earned':'is-locked');
      card.querySelector('.badge-state').textContent=b.earned?'Desbloqueada':'Por descubrir';
      const amount=b.group==='tiempo'||b.group==='lectura'?Math.floor(b.current/60)+' / '+b.target/60+' min':Math.floor(b.current)+' / '+b.target;
      card.querySelector('.badge-amount').textContent=b.earned?'✓ Logro conseguido':amount;
      let share=card.querySelector('.badge-share-button');
      if(b.earned&&sharing&&!share){share=node('button','Compartir insignia','button outline badge-share-button');share.type='button';share.setAttribute('aria-label','Compartir '+b.name);share.addEventListener('click',()=>sharing.open(b.id,share));card.append(share);}
      if(!b.earned&&share)share.remove();
    }
    sharing?.eligibilityChanged();
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
