import { nextRouteStop, routeNavigationUrl } from './property-route-state.js?v=20261028-walk2';
import { propertyVisitKey } from './property-visit-helpers.js';
import { createLabNavigator, labDistance, labTime } from './property-lab-guidance.js?v=20261029-lab1';

const node=(tag,text,cls)=>{const n=document.createElement(tag);if(text!=null)n.textContent=text;if(cls)n.className=cls;return n;};
const button=(text,cls)=>{const n=node('button',text,cls);n.type='button';return n;};
const svgIcon=kind=>{const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 24 24');svg.setAttribute('fill','none');svg.setAttribute('stroke','currentColor');svg.setAttribute('stroke-width','1.8');svg.setAttribute('stroke-linecap','round');svg.setAttribute('stroke-linejoin','round');svg.setAttribute('aria-hidden','true');const paths={walking:'M13 3h.01 M11 7l3 3 3 1 M11 7L8 12l-3 1 M11 7l1 7-4 7 M12 14l5 7',driving:'M4 10l2-6h12l2 6 M3 10h18v8H3z M6 18v3 M18 18v3 M6 14h2 M16 14h2',center:'M12 2v4 M12 18v4 M2 12h4 M18 12h4 M12 7a5 5 0 1 0 0 10 5 5 0 0 0 0-10',voice:'M4 9h4l5-4v14l-5-4H4z M17 8q5 4 0 8 M20 5q7 7 0 14',overview:'M3 5l6-2 6 2 6-2v16l-6 2-6-2-6 2z M9 3v16 M15 5v16'};const path=document.createElementNS('http://www.w3.org/2000/svg','path');path.setAttribute('d',paths[kind]);svg.append(path);return svg;};

const owner=p=>(p.ownerNames?.length?p.ownerNames:[p.ownerName]).filter(Boolean).join(' · ')||'Nombre no disponible';
const vendor='/coach/vendor/maplibre/5.13.0/';
let library;
export function loadLabMapLibre(){
  if(window.maplibregl)return Promise.resolve(window.maplibregl);
  if(!library)library=new Promise((resolve,reject)=>{
    if(!document.querySelector('[data-tec-maplibre-css]')){const css=node('link');css.rel='stylesheet';css.href=vendor+'maplibre-gl.css';css.dataset.tecMaplibreCss='true';document.head.append(css);}
    const script=node('script');script.src=vendor+'maplibre-gl.js';
    const timeout=setTimeout(()=>{script.remove();reject(Error('El mapa tardó demasiado en cargar. Puedes usar la lista o el mapa actual.'));},15000);
    script.onload=()=>{clearTimeout(timeout);window.maplibregl?resolve(window.maplibregl):reject(Error('No pude cargar el mapa de TEC.'));};
    script.onerror=()=>{clearTimeout(timeout);script.remove();reject(Error('No pude cargar el mapa. La ruta sigue guardada.'));};document.head.append(script);
  }).catch(error=>{library=null;throw error;});
  return library;
}

export function createPropertyLab({getRoute,request,complete,followup,onFallback=()=>{},loadMap=loadLabMapLibre,geolocation=navigator.geolocation,demo=false,setMode=async()=>false,autoStart=true}){
  const dialog=node('dialog',null,'tec-map-lab');dialog.setAttribute('aria-label','Recorrido de visitas TEC');
  const header=node('header',null,'tml-header'),brand=button('','tml-back');brand.setAttribute('aria-label','Volver al Coach');
  const logo=node('img');logo.src='/coach/assets/brand/tec-total-elite-coaching-transparent.png';logo.alt='TEC';brand.append(node('span','‹'),logo);
  const title=node('div',null,'tml-title'),headerProgress=node('span');title.append(node('strong','Recorrido'),headerProgress);
  const houses=button('☷ Casas','tml-button');header.append(brand,title,houses);const close=brand;
  const workspace=node('div',null,'tml-workspace'),panel=node('aside',null,'tml-panel');panel.setAttribute('aria-label','Casa y seguimiento');
  const modes=node('div',null,'tml-modes'),modeButtons=[];
  for(const [label,value] of [['A pie','walking'],['En carro','driving']]){const b=button(label,'tml-button');b.prepend(svgIcon(value));b.dataset.travelMode=value;modes.append(b);modeButtons.push(b);b.addEventListener('click',async()=>{
    if(busy||getRoute().travelMode===value)return;busy=true;update();feedback.textContent='Guardando modo de viaje…';
    try{if(await setMode(value)===false)throw Error('No pude guardar el modo de viaje. Reintenta.');feedback.textContent='';}
    catch(e){feedback.textContent=e.message;}finally{busy=false;update();}
  });}
  const mapArea=node('div',null,'tml-map-area'),mapBox=node('div',null,'tml-map');mapBox.setAttribute('aria-label','Mapa del recorrido');mapArea.append(mapBox);
  const turn=node('button',null,'tml-turn');turn.type='button';turn.setAttribute('aria-label','Leer indicación completa');
  const arrow=node('span','↑','tml-arrow'),turnText=node('div'),turnDistance=node('strong','Tu recorrido','tml-turn-distance'),instruction=node('p','Buscando tu ubicación…','tml-instruction'),remaining=node('p','','tml-remaining');
  instruction.setAttribute('role','status');turnText.append(turnDistance,instruction,remaining);turn.append(arrow,turnText);mapArea.append(turn);
  const mapTools=node('div',null,'tml-map-tools'),center=button('⌖','tml-button'),overview=button('▧','tml-button'),voice=button('◖))','tml-button');
  for(const [b,label] of [[center,'Centrar en mi ubicación'],[overview,'Ver recorrido completo'],[voice,'Opciones de voz']]){b.setAttribute('aria-label',label);b.title=label;}for(const [b,k] of [[center,'center'],[voice,'voice'],[overview,'overview']])b.replaceChildren(svgIcon(k));mapTools.append(center,voice,overview);mapArea.append(mapTools);
  const voiceMenu=node('div',null,'tml-voice-menu'),repeat=button('Repetir indicación','tml-button'),mute=button('Silenciar voz','tml-button'),view=button('Vista 3D','tml-button');voiceMenu.hidden=true;voiceMenu.append(repeat,mute,view);mapArea.append(voiceMenu);
  const nextCard=node('section',null,'tml-next'),cardHeading=node('div',null,'tml-card-heading'),progress=node('p',null,'tml-progress'),dataButton=button('Ver datos','tml-text-button'),address=node('h2'),name=node('p',null,'tml-owner');cardHeading.append(progress,dataButton);nextCard.append(cardHeading,address,name);
  const footer=node('footer',null,'tml-footer'),navControls=node('div',null,'tml-nav-controls'),start=button('Iniciar GPS','tml-start'),retry=button('Reintentar ruta','tml-button'),gpsStatus=node('span','GPS en pausa','tml-gps-status');retry.hidden=true;navControls.append(gpsStatus,retry,start);mapArea.append(navControls);
  const actions=node('div',null,'tml-actions'),actionButtons=[];
  for(const [label,value] of [['✕ No atendieron','not_answered'],['✓ Atendieron','answered'],['▤ Notas / cita','notes'],['Siguiente casa →','next']]){
    const b=button(label,'tml-button');b.dataset.labAction=value;actions.append(b);actionButtons.push(b);
    b.addEventListener('click',async()=>{
      const current=nextRouteStop(getRoute());if(!current||busy)return;
      const resume=value==='notes'&&navigatorLab.get().active;if(value==='notes')navigatorLab.stop('Notas abiertas. Tu recorrido se conserva.');
      busy=true;update();feedback.textContent='Guardando en TEC…';let saved=false;
      try{saved=value==='next'?await complete(current):await followup(current.property,value==='notes'?'':value);if(saved===false)feedback.textContent='No pude confirmar el guardado. La casa sigue pendiente; revisa el seguimiento y reintenta.';else feedback.textContent='';}
      catch(error){feedback.textContent=error.message||'No pude guardar el cambio. Reintenta.';}
      finally{busy=false;update();if(resume&&saved!==false&&dialog.open&&!document.hidden){navigatorLab.start();acquireWake();}}
    });
  }
  const feedback=node('p',null,'tml-feedback');feedback.setAttribute('role','status');const navigationMessage=node('p',null,'tml-navigation-message');navigationMessage.setAttribute('role','status');
  const upcoming=node('section',null,'tml-upcoming'),upcomingList=node('div');upcoming.append(node('h3','Próximas casas'),upcomingList);
  const details=node('dialog',null,'tml-house-dialog'),list=node('ol'),listClose=button('Volver al recorrido','tml-button');details.setAttribute('aria-label','Casas del recorrido');details.append(node('h2','Casas del recorrido'),listClose,list);list.setAttribute('aria-label','Casas y propietarios');listClose.onclick=()=>details.close();houses.onclick=()=>details.showModal();
  const info=node('dialog',null,'tml-house-dialog'),infoTitle=node('h2'),infoBody=node('p'),infoClose=button('Volver al recorrido','tml-button');info.append(infoTitle,infoBody,infoClose);infoClose.onclick=()=>info.close();
  dataButton.onclick=()=>{const n=nextRouteStop(getRoute());infoTitle.textContent=n?.property.formattedAddress||n?.property.address||'Recorrido terminado';infoBody.textContent=n?'Propietario registrado: '+owner(n.property):'Tu avance sigue guardado.';info.showModal();};
  turn.onclick=()=>{infoTitle.textContent=turnDistance.textContent;infoBody.textContent=instruction.textContent+' '+remaining.textContent;info.showModal();};
  const help=node('div',null,'tml-help'),note=node('p',demo?'Simulación · 50 casas ficticias':'Mantén TEC visible y la pantalla encendida.'),fallback=button('Vista sencilla','tml-text-button');help.append(note,fallback);
  const links=node('div',null,'tml-links');
  const external=[['Google Maps','google'],['Apple Maps','apple']].map(([label,provider])=>{const link=node('a',label);link.target='_blank';link.rel='noopener noreferrer';link.addEventListener('click',()=>navigatorLab.stop());links.append(link);return{link,provider};});help.append(links);
  footer.append(actions,feedback,navigationMessage,upcoming,help);panel.append(modes,nextCard,footer);workspace.append(mapArea,panel);dialog.append(header,workspace);document.body.append(dialog,details,info);

  let map=null,pins=[],locationPin=null,follow=true,tilted=false,muted=false,busy=false,opening=false,mapReady=false,wake=null,timer=null,ownSpeech=false,previousPlan=null,openedBy=null;
  const captureBusy=()=>!!document.querySelector('[data-demo-recorder][data-capture-busy="true"]');
  const releaseWake=()=>{wake?.release().catch(()=>{});wake=null;};
  async function acquireWake(){if(document.hidden || !navigatorLab.get().active || wake)return;try{const value=await navigator.wakeLock?.request('screen');if(!navigatorLab.get().active || document.hidden)value?.release().catch(()=>{});else {wake=value;value?.addEventListener('release',()=>{if(wake===value)wake=null;});}}catch{}}
  function cancelVoice(){if(ownSpeech)window.speechSynthesis?.cancel();ownSpeech=false;}
  function speak(text){
    if(muted||document.hidden||captureBusy()||!window.speechSynthesis)return;
    const utterance=new SpeechSynthesisUtterance(text);utterance.lang='es-MX';utterance.rate=.95;
    const voices=window.speechSynthesis.getVoices(),spanish=voices.find(v=>/^es-MX/i.test(v.lang))||voices.find(v=>/^es/i.test(v.lang));if(spanish)utterance.voice=spanish;
    cancelVoice();ownSpeech=true;speechSynthesis.speak(utterance);
  }
  function showNavigation(state){
    if(!dialog.open)return;
    arrow.textContent=state.arrow;instruction.textContent=state.instruction;navigationMessage.textContent=state.active&&['guiding','arrived'].includes(state.phase)?'':state.instruction;
    turnDistance.textContent=state.phase==='error'?'No pude calcular la ruta':state.phase==='routing'?'Calculando ruta…':state.phase==='arrived'?'Llegaste a la casa':state.phase==='guiding'?labDistance(state.nextMeters):state.active?'Conectando GPS':'Tu recorrido, más claro';
    remaining.textContent=state.phase==='guiding'||state.phase==='arrived'?`${labTime(state.seconds)} · ${labDistance(state.remaining)} hasta la casa`:state.active?'La ubicación y la ruta requieren conexión':'Mantén la pantalla encendida';
    start.textContent=state.active?'Pausar':'Iniciar GPS';gpsStatus.textContent=state.active?(state.phase==='waiting'?'Buscando GPS…':'GPS activo'):'GPS en pausa';start.classList.toggle('is-active',state.active);retry.hidden=!state.active||!['error','offroute'].includes(state.phase);
    start.disabled=!nextRouteStop(getRoute());repeat.disabled=!state.active;turn.hidden=!state.active;
    if(mapReady){
      if(state.plan!==previousPlan){previousPlan=state.plan;map.getSource('tml-route')?.setData(routeLine(state.plan));}
      if(state.position)paintPosition(state.position,state.active && follow);
    }
  }
  const navigatorLab=createLabNavigator({getRoute,request,geolocation,captureBusy,canSpeak:()=>!muted&&!document.hidden,onSpeak:speak,onStop:()=>{cancelVoice();releaseWake();},onState:showNavigation});
  const empty={type:'FeatureCollection',features:[]};
  function routeLine(plan){return plan?{type:'Feature',properties:{},geometry:{type:'LineString',coordinates:plan.coordinates}}:empty;}
  function paintPosition(point,move){
    if(!mapReady)return;
    if(!locationPin){const element=node('div','▲','tml-position');element.setAttribute('aria-label','Tu ubicación');locationPin=new window.maplibregl.Marker({element,rotationAlignment:'map',pitchAlignment:'map'}).setLngLat([point.lon,point.lat]).addTo(map);}
    locationPin.setLngLat([point.lon,point.lat]).setRotation(point.bearing||0);
    if(move)map.easeTo({center:[point.lon,point.lat],zoom:getRoute().travelMode==='driving'?17:18,pitch:tilted?45:0,bearing:tilted?(point.bearing||map.getBearing()):0,duration:850,padding:mapPadding()});
  }
  function mapPadding(){const w=mapBox.clientWidth||320,h=mapBox.clientHeight||300;return {top:Math.min(140,h*.3),bottom:Math.min(65,h*.18),left:Math.min(30,w*.08),right:Math.min(70,w*.2)};}
  function fitRoute(){
    if(!mapReady)return;const points=getRoute().stops.map(s=>[s.property.lon,s.property.lat]);if(!points.length)return;
    const bounds=new window.maplibregl.LngLatBounds(points[0],points[0]);points.slice(1).forEach(p=>bounds.extend(p));
    map.fitBounds(bounds,{padding:mapPadding(),maxZoom:17,pitch:0,bearing:0,duration:500});
  }
  function drawStops(){
    if(!mapReady)return;pins.forEach(pin=>pin.remove());pins=[];
    const route=getRoute(),next=nextRouteStop(route);
    route.stops.forEach((stop,index)=>{
      const p=stop.property,element=button(String(index+1),`tml-pin${stop.completedAt?' is-complete':stop===next?' is-next':''}`);element.setAttribute('aria-label',`${index+1}. ${p.address}. ${owner(p)}${stop.completedAt?'. Visitada':''}`);
      const content=node('div',null,'tml-popup');content.append(node('strong',p.formattedAddress||p.address),node('p','Propietario registrado: '+owner(p)),node('p',stop.completedAt?'Visitada':'Pendiente'));
      const popup=new window.maplibregl.Popup({offset:18,maxWidth:'280px'}).setDOMContent(content);
      element.style.zIndex=stop===next?'10':stop.completedAt?'0':'1';
      pins.push(new window.maplibregl.Marker({element}).setLngLat([p.lon,p.lat]).setPopup(popup).addTo(map));
    });
  }
  function update(){
    if(!dialog.open)return;const route=getRoute(),next=nextRouteStop(route),done=route.stops.filter(s=>s.completedAt).length;
    headerProgress.textContent=`${done}/${route.stops.length} visitadas`;progress.textContent=next?`SIGUIENTE CASA · ${route.stops.indexOf(next)+1}`:'RECORRIDO TERMINADO';
    modeButtons.forEach(b=>{b.setAttribute('aria-pressed',String(b.dataset.travelMode===route.travelMode));b.disabled=busy;});
    address.textContent=next?`${next.property.formattedAddress||next.property.address}`:route.stops.length?'Recorrido terminado':'Agrega casas en el mapa de TEC';
    name.textContent=next?'Propietario: '+owner(next.property):'Tu ruta y avance se conservan.';
    actionButtons.forEach(b=>b.disabled=!next||busy);start.disabled=!next;dataButton.disabled=!next;
    function row(stop,i){const item=node('li',null,stop.completedAt?'is-complete':''),b=button('', 'tml-house-row');b.append(node('strong',`${i+1}. ${stop.property.address}`),node('span',owner(stop.property)),node('small',stop.completedAt?'Visitada':'Pendiente'));b.onclick=()=>{follow=false;if(mapReady)map.easeTo({center:[stop.property.lon,stop.property.lat],zoom:18,padding:{top:0,bottom:0,left:0,right:0}});if(details.open)details.close();};item.append(b);return item;}
    list.replaceChildren(...route.stops.map(row));const nextIndex=route.stops.indexOf(next);upcomingList.replaceChildren(...route.stops.map((stop,i)=>({stop,i})).filter(x=>x.i>nextIndex&&!x.stop.completedAt).slice(0,3).map(x=>row(x.stop,x.i)));upcoming.hidden=!next;
    external.forEach(({link,provider})=>{const url=routeNavigationUrl(provider,next,route.travelMode);if(url)link.href=url;else link.removeAttribute('href');});
    navigatorLab.update();showNavigation(navigatorLab.get());drawStops();
  }
  function resize(){if(dialog.open)map?.resize();}
  const observer=window.ResizeObserver?new ResizeObserver(resize):null;observer?.observe(mapBox);
  async function open(){
    if(opening||dialog.open)return;opening=true;openedBy=document.activeElement;dialog.showModal();update();
    timer=setInterval(()=>navigatorLab.tick(),5000);
    try{
      const lib=await loadMap();if(!dialog.open)return;
      if(typeof lib.supported==='function' && !lib.supported({failIfMajorPerformanceCaveat:false}))throw Error('Este navegador no puede mostrar el mapa vectorial. Usa la lista o vuelve al mapa actual.');
      if(!map){
        const p=nextRouteStop(getRoute())?.property||getRoute().anchor;
        map=new lib.Map({container:mapBox,style:'https://tiles.openfreemap.org/styles/liberty',center:p?[p.lon,p.lat]:[0,0],zoom:p?15:1,pitch:0,attributionControl:true,canvasContextAttributes:{antialias:false}});
        map.on('dragstart',()=>follow=false);map.on('rotatestart',event=>{if(event.originalEvent)follow=false;});
        map.on('error',()=>{if(dialog.open)feedback.textContent='No pude cargar algunas calles. Tus casas y el avance siguen disponibles.';});
        map.on('style.load',()=>{
          mapReady=true;map.addSource('tml-route',{type:'geojson',data:empty});
          map.addLayer({id:'tml-route-outline',type:'line',source:'tml-route',layout:{'line-cap':'round','line-join':'round'},paint:{'line-color':'#ffffff','line-width':10}});
          map.addLayer({id:'tml-route-blue',type:'line',source:'tml-route',layout:{'line-cap':'round','line-join':'round'},paint:{'line-color':'#1677ed','line-width':6}});
          previousPlan=null;drawStops();map.resize();fitRoute();
          const state=navigatorLab.get();if(state.plan){previousPlan=state.plan;map.getSource('tml-route').setData(routeLine(state.plan));}if(state.position)paintPosition(state.position,state.active&&follow);
        });
      } else {map.resize();fitRoute();}
      if(autoStart&&dialog.open&&nextRouteStop(getRoute())){navigatorLab.start();acquireWake();}
    }catch(error){feedback.textContent=error.message;}
    finally{opening=false;}
  }
  close.addEventListener('click',()=>dialog.close());
  dialog.addEventListener('close',()=>{navigatorLab.stop();if(details.open)details.close();if(info.open)info.close();clearInterval(timer);timer=null;cancelVoice();releaseWake();openedBy?.focus?.({preventScroll:true});});
  start.addEventListener('click',()=>{if(navigatorLab.get().active)navigatorLab.stop();else{follow=true;navigatorLab.start();acquireWake();}});
  voice.addEventListener('click',()=>{voiceMenu.hidden=!voiceMenu.hidden;voice.setAttribute('aria-expanded',String(!voiceMenu.hidden));});
  mute.addEventListener('click',()=>{muted=!muted;mute.textContent=muted?'Activar voz':'Silenciar voz';voice.setAttribute('aria-label',muted?'Opciones de voz, silenciada':'Opciones de voz');if(muted)cancelVoice();});
  repeat.addEventListener('click',()=>navigatorLab.repeat());retry.addEventListener('click',()=>navigatorLab.retry());
  center.addEventListener('click',()=>{follow=true;const p=navigatorLab.get().position;if(p)paintPosition(p,true);else{fitRoute();gpsStatus.textContent='Esperando tu ubicación';}});
  overview.addEventListener('click',()=>{follow=false;fitRoute();});
  view.addEventListener('click',()=>{tilted=!tilted;view.textContent=tilted?'Vista 2D':'Vista 3D';view.setAttribute('aria-pressed',String(tilted));if(map)map.easeTo({pitch:tilted?45:0,bearing:tilted?map.getBearing():0,duration:500});});
  fallback.addEventListener('click',()=>{dialog.close();onFallback();});
  document.addEventListener('visibilitychange',()=>{if(document.hidden){cancelVoice();releaseWake();}else if(dialog.open){resize();navigatorLab.tick();acquireWake();}});
  window.addEventListener('pagehide',()=>navigatorLab.stop());
  return {open,update,close:()=>{navigatorLab.stop();if(dialog.open)dialog.close();},isOpen:()=>dialog.open};
}
