import { nextRouteStop, routeNavigationUrl } from './property-route-state.js?v=20261028-walk2';
import { propertyVisitKey } from './property-visit-helpers.js';
import { createLabNavigator, labDistance, labTime } from './property-lab-guidance.js?v=20261029-lab1';

const node=(tag,text,cls)=>{const n=document.createElement(tag);if(text!=null)n.textContent=text;if(cls)n.className=cls;return n;};
const button=(text,cls)=>{const n=node('button',text,cls);n.type='button';return n;};
const owner=p=>(p.ownerNames?.length?p.ownerNames:[p.ownerName]).filter(Boolean).join(' · ')||'Nombre no disponible';
const vendor='/coach/vendor/maplibre/5.13.0/';
let library;
export function loadLabMapLibre(){
  if(window.maplibregl)return Promise.resolve(window.maplibregl);
  if(!library)library=new Promise((resolve,reject)=>{
    if(!document.querySelector('[data-tec-maplibre-css]')){const css=node('link');css.rel='stylesheet';css.href=vendor+'maplibre-gl.css';css.dataset.tecMaplibreCss='true';document.head.append(css);}
    const script=node('script');script.src=vendor+'maplibre-gl.js';
    const timeout=setTimeout(()=>{script.remove();reject(Error('El mapa tardó demasiado en cargar. Puedes usar la lista o el mapa actual.'));},15000);
    script.onload=()=>{clearTimeout(timeout);window.maplibregl?resolve(window.maplibregl):reject(Error('No pude cargar el mapa del LAB.'));};
    script.onerror=()=>{clearTimeout(timeout);script.remove();reject(Error('No pude cargar el mapa. La ruta sigue guardada.'));};document.head.append(script);
  }).catch(error=>{library=null;throw error;});
  return library;
}

export function createPropertyLab({getRoute,request,complete,followup,onFallback=()=>{},loadMap=loadLabMapLibre,geolocation=navigator.geolocation,demo=false}){
  const dialog=node('dialog',null,'tec-map-lab');dialog.setAttribute('aria-label','Mapa LAB · recorrido de visitas');
  const header=node('header',null,'tml-header'),brand=node('div',null,'tml-brand');brand.append(node('strong','TEC Mapas'),node('span',demo?'LAB · DEMO':'LAB','tml-badge'));
  const close=button('Volver a TEC','tml-button');header.append(brand,close);
  const mapArea=node('div',null,'tml-map-area'),mapBox=node('div',null,'tml-map');mapBox.setAttribute('aria-label','Mapa vectorial del recorrido');mapArea.append(mapBox);
  const turn=node('section',null,'tml-turn'),arrow=node('span','↑','tml-arrow'),turnText=node('div');
  const turnDistance=node('strong','Tu recorrido, más claro','tml-turn-distance'),instruction=node('p','Inicia GPS para recibir indicaciones.','tml-instruction'),remaining=node('p','Mapa experimental · pantalla encendida','tml-remaining');
  instruction.setAttribute('role','status');turnText.append(turnDistance,instruction,remaining);turn.append(arrow,turnText);
  const repeat=button('↻','tml-repeat');repeat.setAttribute('aria-label','Repetir indicación de voz');turn.append(repeat);mapArea.append(turn);
  const mapTools=node('div',null,'tml-map-tools'),center=button('⌖ Centrar','tml-button'),overview=button('Ver ruta','tml-button'),view=button('2D','tml-button');view.setAttribute('aria-pressed','false');mapTools.append(center,overview,view);mapArea.append(mapTools);
  const nextCard=node('section',null,'tml-next'),progress=node('p',null,'tml-progress'),address=node('h2'),name=node('p',null,'tml-owner');nextCard.append(progress,address,name);
  const footer=node('footer',null,'tml-footer'),navControls=node('div',null,'tml-nav-controls'),start=button('Iniciar GPS','tml-start'),voice=button('Voz activada','tml-button'),retry=button('Reintentar ruta','tml-button');retry.hidden=true;voice.setAttribute('aria-pressed','true');navControls.append(start,voice,retry);
  const actions=node('div',null,'tml-actions'),actionButtons=[];
  for(const [label,value] of [['No atendieron','not_answered'],['Atendieron','answered'],['Notas / volver','notes'],['Siguiente','next']]){
    const b=button(label,'tml-button');b.dataset.labAction=value;actions.append(b);actionButtons.push(b);
    b.addEventListener('click',async()=>{
      const current=nextRouteStop(getRoute());if(!current||busy)return;busy=true;update();feedback.textContent='';
      try{const saved=value==='next'?await complete(current):await followup(current.property,value==='notes'?'':value);if(saved===false)feedback.textContent='No pude confirmar el guardado. La casa sigue pendiente; revisa el seguimiento y reintenta.';}
      catch(error){feedback.textContent=error.message||'No pude guardar el cambio. Reintenta.';}
      finally{busy=false;update();}
    });
  }
  const details=node('details',null,'tml-list'),summary=node('summary','Ver las casas y el avance'),list=node('ol');list.setAttribute('aria-label','Casas del recorrido LAB');details.append(summary,list);
  const feedback=node('p',null,'tml-feedback');feedback.setAttribute('role','status');
  const help=node('div',null,'tml-help'),note=node('p',demo?'DEMO: GPS simulado y 50 casas ficticias. No usa tu ubicación ni tu cuenta.':'Web LAB: mantén la pantalla encendida. Usa el mismo recorrido y guardado de TEC.'),fallback=button('Usar mapa actual','tml-text-button');help.append(note,fallback);
  const links=node('div',null,'tml-links');
  const external=[['Google Maps','google'],['Apple Maps','apple']].map(([label,provider])=>{const link=node('a',label);link.target='_blank';link.rel='noopener noreferrer';link.addEventListener('click',()=>navigatorLab.stop());links.append(link);return{link,provider};});help.append(links);
  footer.append(navControls,actions,feedback,details,help);dialog.append(header,mapArea,nextCard,footer);document.body.append(dialog);

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
    arrow.textContent=state.arrow;instruction.textContent=state.instruction;
    turnDistance.textContent=state.phase==='arrived'?'Llegaste a la casa':state.phase==='guiding'?labDistance(state.nextMeters):state.active?'Conectando GPS':'Tu recorrido, más claro';
    remaining.textContent=state.phase==='guiding'||state.phase==='arrived'?`${labTime(state.seconds)} · ${labDistance(state.remaining)} hasta la casa`:state.active?'La ubicación y la ruta requieren conexión':'Mapa experimental · pantalla encendida';
    start.textContent=state.active?'Detener GPS':'Iniciar GPS';start.classList.toggle('is-active',state.active);retry.hidden=!state.active||!['error','offroute'].includes(state.phase);
    start.disabled=!nextRouteStop(getRoute());repeat.disabled=!state.active;
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
    if(move)map.easeTo({center:[point.lon,point.lat],zoom:getRoute().travelMode==='driving'?17:18,pitch:tilted?45:0,bearing:tilted?(point.bearing||map.getBearing()):0,duration:850,padding:{top:110,bottom:15,left:0,right:0}});
  }
  function fitRoute(){
    if(!mapReady)return;const points=getRoute().stops.map(s=>[s.property.lon,s.property.lat]);if(!points.length)return;
    const bounds=new window.maplibregl.LngLatBounds(points[0],points[0]);points.slice(1).forEach(p=>bounds.extend(p));
    map.fitBounds(bounds,{padding:{top:170,bottom:65,left:40,right:40},maxZoom:17,pitch:0,bearing:0,duration:500});
  }
  function drawStops(){
    if(!mapReady)return;pins.forEach(pin=>pin.remove());pins=[];
    const route=getRoute(),next=nextRouteStop(route);
    route.stops.forEach((stop,index)=>{
      const p=stop.property,element=button(String(index+1),`tml-pin${stop.completedAt?' is-complete':stop===next?' is-next':''}`);element.setAttribute('aria-label',`${index+1}. ${p.address}. ${owner(p)}${stop.completedAt?'. Visitada':''}`);
      const content=node('div',null,'tml-popup');content.append(node('strong',p.formattedAddress||p.address),node('p','Propietario registrado: '+owner(p)),node('p',stop.completedAt?'Visitada':'Pendiente'));
      const popup=new window.maplibregl.Popup({offset:18,maxWidth:'280px'}).setDOMContent(content);
      pins.push(new window.maplibregl.Marker({element}).setLngLat([p.lon,p.lat]).setPopup(popup).addTo(map));
    });
  }
  function update(){
    if(!dialog.open)return;const route=getRoute(),next=nextRouteStop(route),done=route.stops.filter(s=>s.completedAt).length;
    progress.textContent=`${done} visitadas · ${route.stops.length-done} pendientes · ${route.stops.length} casas${route.pending?' · Guardado en este dispositivo':''}`;
    address.textContent=next?`${route.stops.indexOf(next)+1}. ${next.property.formattedAddress||next.property.address}`:route.stops.length?'Recorrido terminado':'Agrega casas en el mapa de TEC';
    name.textContent=next?'Propietario registrado: '+owner(next.property):'Tu ruta y avance se conservan.';
    actionButtons.forEach(b=>b.disabled=!next||busy);start.disabled=!next;
    list.replaceChildren(...route.stops.map((stop,i)=>{const item=node('li',null,stop.completedAt?'is-complete':'');item.dataset.propertyKey=propertyVisitKey(stop.property);item.append(node('strong',`${i+1}. ${stop.property.address}`),node('span',owner(stop.property)),node('small',stop.completedAt?'Visitada':'Pendiente'));return item;}));
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
        const p=nextRouteStop(getRoute())?.property||getRoute().anchor||{lon:-87.6298,lat:41.8781};
        map=new lib.Map({container:mapBox,style:'https://tiles.openfreemap.org/styles/liberty',center:[p.lon,p.lat],zoom:15,pitch:0,attributionControl:true,canvasContextAttributes:{antialias:false}});
        map.on('dragstart',()=>follow=false);map.on('rotatestart',event=>{if(event.originalEvent)follow=false;});
        map.on('error',()=>{if(dialog.open)feedback.textContent='No pude cargar algunas calles. Tus casas y el avance siguen disponibles.';});
        map.on('load',()=>{
          mapReady=true;map.addSource('tml-route',{type:'geojson',data:empty});
          map.addLayer({id:'tml-route-outline',type:'line',source:'tml-route',layout:{'line-cap':'round','line-join':'round'},paint:{'line-color':'#ffffff','line-width':10}});
          map.addLayer({id:'tml-route-blue',type:'line',source:'tml-route',layout:{'line-cap':'round','line-join':'round'},paint:{'line-color':'#1677ed','line-width':6}});
          previousPlan=null;drawStops();map.resize();fitRoute();
          const state=navigatorLab.get();if(state.plan){previousPlan=state.plan;map.getSource('tml-route').setData(routeLine(state.plan));}if(state.position)paintPosition(state.position,state.active&&follow);
        });
      } else {map.resize();fitRoute();}
    }catch(error){feedback.textContent=error.message;}
    finally{opening=false;}
  }
  close.addEventListener('click',()=>dialog.close());
  dialog.addEventListener('close',()=>{navigatorLab.stop();clearInterval(timer);timer=null;cancelVoice();releaseWake();openedBy?.focus?.({preventScroll:true});});
  start.addEventListener('click',()=>{if(navigatorLab.get().active)navigatorLab.stop();else{follow=true;navigatorLab.start();acquireWake();}});
  voice.addEventListener('click',()=>{muted=!muted;voice.textContent=muted?'Voz silenciada':'Voz activada';voice.setAttribute('aria-pressed',String(!muted));if(muted)cancelVoice();});
  repeat.addEventListener('click',()=>navigatorLab.repeat());retry.addEventListener('click',()=>navigatorLab.retry());
  center.addEventListener('click',()=>{follow=true;const p=navigatorLab.get().position||nextRouteStop(getRoute())?.property;if(p)paintPosition(p,true);});
  overview.addEventListener('click',()=>{follow=false;fitRoute();});
  view.addEventListener('click',()=>{tilted=!tilted;view.textContent=tilted?'3D':'2D';view.setAttribute('aria-pressed',String(tilted));if(map)map.easeTo({pitch:tilted?45:0,bearing:tilted?map.getBearing():0,duration:500});});
  fallback.addEventListener('click',()=>{dialog.close();onFallback();});
  document.addEventListener('visibilitychange',()=>{if(document.hidden){cancelVoice();releaseWake();}else if(dialog.open){resize();navigatorLab.tick();acquireWake();}});
  window.addEventListener('pagehide',()=>navigatorLab.stop());
  return {open,update,close:()=>{navigatorLab.stop();if(dialog.open)dialog.close();},isOpen:()=>dialog.open};
}
