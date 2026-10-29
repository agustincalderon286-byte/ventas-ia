import {nextRouteStop,routeNavigationUrl} from './property-route-state.js?v=20261028-walk2';
import {propertyVisitKey} from './property-visit-helpers.js';
import {navigationFix} from './route-guidance.js?v=20261028-walk2';
const el=(tag,text,cls)=>{const n=document.createElement(tag);if(text)n.textContent=text;if(cls)n.className=cls;return n;};
const owner=p=>(p.ownerNames?.length?p.ownerNames:[p.ownerName]).filter(Boolean).join(' · ')||'Nombre no disponible';
export function createPropertyWalk({getRoute,request,loadLeaflet,followup,complete}) {
 const dialog=el('dialog',null,'tec-walk');dialog.setAttribute('aria-label','Recorrido de visitas');
 const top=el('header'),title=el('h2','Mi recorrido'),close=el('button','Volver al mapa','nav-button');close.type='button';top.append(title,close);
 const progress=el('p',null,'tec-walk-progress'),address=el('h3'),name=el('p',null,'tec-walk-owner'),instruction=el('p','Inicia la ubicación para seguir el trayecto.','tec-walk-instruction');instruction.setAttribute('role','status');
 const controls=el('div',null,'tec-walk-controls'),start=el('button','Iniciar navegación','primary-button'),center=el('button','Centrar','nav-button'),stop=el('button','Detener GPS','nav-button');
 for(const b of [start,center,stop])b.type='button'; controls.append(start,center,stop);
 const mapBox=el('div',null,'tec-walk-map');mapBox.setAttribute('aria-label','Mapa de las casas del recorrido');
 const capability=el('p',window.TecNativeRoute ? 'App TEC: al iniciar, la ubicación y la voz pueden continuar con la pantalla bloqueada.' : 'Web: mantén TEC visible. La navegación con pantalla bloqueada requiere la app de iPhone actualizada.','tec-walk-note');
 const status=el('p',null,'tec-walk-status');status.setAttribute('role','status');
 const actions=el('div',null,'tec-walk-actions');let actionBusy=false;
 const buttons=[];
 for(const [label,value] of [['No atendieron','not_answered'],['Atendieron','answered'],['Volver después','return_later'],['Notas / cita',''],['Siguiente sin nota','next']]){
  const b=el('button',label,'nav-button');b.type='button';buttons.push(b);actions.append(b);
  b.addEventListener('click',async()=>{const current=nextRouteStop(getRoute());if(!current||actionBusy)return;actionBusy=true;update();try{if(value==='next'){if(!await complete(current))throw Error('No pude guardar el avance. Reintenta.');}else await followup(current.property,value);}catch(e){status.textContent=e.message;}finally{actionBusy=false;update();}});
 }
 const links=el('div',null,'tec-walk-links'),google=el('a','Google Maps','nav-button'),apple=el('a','Apple Maps','nav-button');for(const link of [google,apple]){link.target='_blank';link.rel='noopener noreferrer';}links.append(google,apple);
 const details=el('details'),summary=el('summary','Las casas del recorrido'),list=el('ol');details.append(summary,list);
 const info=el('p','Los números muestran el orden de visita. El GPS no marca visitas automáticamente. El nombre corresponde al propietario registrado, no necesariamente a quien vive allí.','tec-walk-note');
 const credits=el('p',null,'tec-walk-note');
 for(const [label,url] of [['Mapa © OpenStreetMap','https://www.openstreetmap.org/copyright'],['Rutas FOSSGIS / OSRM','https://routing.openstreetmap.de/about.html'],['Corregir mapa','https://www.openstreetmap.org/fixthemap']]){const a=el('a',label);a.href=url;a.target='_blank';a.rel='noopener noreferrer';credits.append(a,document.createTextNode(' · '));}
 dialog.append(top,progress,address,name,controls,instruction,mapBox,status,capability,actions,links,details,info,credits);document.body.append(dialog);
 let map,markers,line,gpsMarker,watch=null,active=false,native=false,currentKey='',plan=null,position=null,fetching=false,generation=0,lastAttempt=0,lastSpoken='',wake=null,followPosition=true;
 const captureBusy=()=>!!document.querySelector('[data-demo-recorder][data-capture-busy="true"]');
 const stopGPS=async()=>{
  active=false;generation++;if(watch!==null)navigator.geolocation.clearWatch(watch);watch=null;
  if(native)await window.TecNativeRoute?.stop().catch(()=>{});native=false;
  window.speechSynthesis?.cancel();wake?.release().catch(()=>{});wake=null;
  instruction.textContent='Navegación detenida. Tu avance sigue guardado.';updateControls();
 };
 function updateControls(){start.disabled=active||!nextRouteStop(getRoute());stop.disabled=!active;start.textContent=active?'Navegación activa':'Iniciar navegación';}
 function speak(text,id){if(native||captureBusy()||id===lastSpoken||!window.speechSynthesis)return;lastSpoken=id;const u=new SpeechSynthesisUtterance(text);u.lang='es-MX';speechSynthesis.cancel();speechSynthesis.speak(u);}
 function draw(){if(!map)return;const L=window.L;markers.clearLayers();const r=getRoute(),next=nextRouteStop(r);
  r.stops.forEach((s,i)=>{const p=s.property,m=L.circleMarker([p.lat,p.lon],{radius:s===next?13:9,color:'#fff',weight:2,fillColor:s.completedAt?'#16704b':s===next?'#d51e36':'#123d69',fillOpacity:1}).addTo(markers);m.bindTooltip(String(i+1),{permanent:true,direction:'center',className:'tec-property-number-label'});const popup=el('div');popup.append(el('strong',p.address),el('p','Propietario registrado: '+owner(p)));m.bindPopup(popup);});
 }
 function paintPlan(value){plan=value;if(map){if(line)map.removeLayer(line);line=window.L.polyline(value.coordinates.map(p=>[p[1],p[0]]),{color:'#1976ca',weight:6,opacity:.85}).addTo(map);} }
 async function calculate(){const next=nextRouteStop(getRoute());if(!active||native||fetching||!position||!next||Date.now()-lastAttempt<10000)return;
  const version=generation;fetching=true;lastAttempt=Date.now();instruction.textContent='Calculando trayecto por calles…';
  try{const value=await request('/api/coach/property-navigation',{method:'POST',body:{from:{lat:position.lat,lon:position.lon},to:{lat:next.property.lat,lon:next.property.lon},mode:getRoute().travelMode},timeoutMs:20000});if(version!==generation||!active)return;paintPlan(value);status.textContent='Trayecto por calles · '+value.source;renderFix();}
  catch(e){if(version===generation){instruction.textContent=e.message;status.textContent='La ruta y las casas siguen guardadas. Puedes reintentar o usar los mapas externos.';}}
  finally{fetching=false;}
 }
 function renderPosition(){if(!map||!position)return;const point=[position.lat,position.lon];if(!gpsMarker)gpsMarker=window.L.circleMarker(point,{radius:8,color:'#fff',weight:3,fillColor:'#1687ed',fillOpacity:1}).addTo(map);else gpsMarker.setLatLng(point);if(followPosition)map.panTo(point,{animate:false});}
 function renderFix(){renderPosition();const next=nextRouteStop(getRoute());if(!plan||!next)return;const fix=navigationFix(position,plan,next.property);instruction.textContent=fix.message;
  if(fix.state==='offroute'){if(Date.now()-lastAttempt>30000)calculate();return;}
  if(fix.state!=='imprecise')speak(fix.message,`${currentKey}:${fix.state}:${fix.instructionId}`);
 }
 async function startGPS(){if(active||!nextRouteStop(getRoute()))return;if(captureBusy()){status.textContent='Termina la grabación antes de iniciar la navegación con voz.';return;}
  active=true;lastSpoken='';lastAttempt=0;updateControls();native=!!window.TecNativeRoute;
  if(native){status.textContent='App TEC: ubicación y voz pueden continuar con la pantalla bloqueada. Detén el GPS al terminar.';try{await sendNativeDestination();}catch(e){await stopGPS();status.textContent=e.message;}}
  else{status.textContent='Web: mantén TEC visible para recibir ubicación e indicaciones. La pantalla bloqueada requiere la actualización de la app de iPhone.';
   if(!navigator.geolocation){await stopGPS();status.textContent='Este navegador no ofrece ubicación.';return;}
   watch=navigator.geolocation.watchPosition(p=>{if(!active)return;position={lat:p.coords.latitude,lon:p.coords.longitude,accuracy:p.coords.accuracy};if(captureBusy()){stopGPS();return;}if(position.accuracy>40){instruction.textContent='Esperando una ubicación más precisa…';return;}renderPosition();if(!plan)calculate();else renderFix();},e=>{instruction.textContent=e.code===1?'Permite la ubicación en los ajustes del navegador para navegar.':'No pude obtener tu ubicación. Busca un lugar con mejor señal GPS.';if(e.code===1)stopGPS().then(()=>{instruction.textContent='Ubicación denegada. Puedes usar la lista o los mapas externos.';});},{enableHighAccuracy:true,maximumAge:5000,timeout:20000});
   try{wake=await navigator.wakeLock?.request('screen');}catch{}
  }
 }
 function sendNativeDestination(){const next=nextRouteStop(getRoute());return window.TecNativeRoute.start({destination:{lat:next.property.lat,lon:next.property.lon,address:next.property.address},mode:getRoute().travelMode});}
 function update(){if(!dialog.open)return;const r=getRoute(),next=nextRouteStop(r),done=r.stops.filter(s=>s.completedAt).length;
  progress.textContent=`${done} ${done===1?'visitada':'visitadas'} · ${r.stops.length-done} pendientes · ${r.stops.length} casas`;
  address.textContent=next?`${r.stops.indexOf(next)+1}. ${next.property.formattedAddress||next.property.address}`:'Recorrido terminado';
  name.textContent=next?'Propietario registrado: '+owner(next.property):'Tu avance quedó guardado.';
  buttons.forEach(b=>b.disabled=!next||actionBusy);list.replaceChildren(...r.stops.map((s,i)=>{const item=el('li');item.append(el('strong',`${i+1}. ${s.property.address}${s.completedAt?' · Visitada':''}`),el('span',owner(s.property)));return item;}));
  for(const [link,provider] of [[google,'google'],[apple,'apple']]){const url=routeNavigationUrl(provider,next,r.travelMode);if(url)link.href=url;else link.removeAttribute('href');}
  const key=next?propertyVisitKey(next.property)+':'+r.travelMode:'';
  if(key!==currentKey){currentKey=key;generation++;plan=null;lastAttempt=0;lastSpoken='';if(line&&map){map.removeLayer(line);line=null;}
   if(active){if(!next)stopGPS();else if(native)sendNativeDestination().catch(e=>{stopGPS();status.textContent=e.message;});else calculate();}
  }draw();updateControls();
 }
 window.addEventListener('tec:native-route',event=>{if(!dialog.open||!native)return;const v=event.detail;if(v.position){position=v.position;renderPosition();}if(v.plan)paintPlan(v.plan);if(v.message)instruction.textContent=v.message;if(v.active===false){active=false;native=false;updateControls();}});
 document.addEventListener('visibilitychange',()=>{if(!document.hidden&&active){if(native)window.TecNativeRoute.status().then(v=>window.dispatchEvent(new CustomEvent('tec:native-route',{detail:v}))).catch(()=>{});else navigator.wakeLock?.request('screen').then(v=>wake=v).catch(()=>{});}});
 window.addEventListener('pagehide',()=>{stopGPS();});
 for(const link of [google,apple])link.addEventListener('click',()=>stopGPS());
 close.addEventListener('click',()=>dialog.close());dialog.addEventListener('close',()=>{stopGPS();});start.addEventListener('click',startGPS);stop.addEventListener('click',stopGPS);
 center.addEventListener('click',()=>{const p=position||nextRouteStop(getRoute())?.property;if(p&&map){followPosition=true;map.setView([p.lat,p.lon],18);}});
 return {update,async open(){dialog.showModal();update();try{const L=await loadLeaflet();if(!dialog.open)return;if(!map){map=L.map(mapBox);map.on('dragstart',()=>followPosition=false);L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'}).addTo(map).on('tileerror',()=>status.textContent='No pude cargar las calles. La lista y el seguimiento siguen disponibles.');markers=L.layerGroup().addTo(map);}map.invalidateSize();draw();const points=getRoute().stops.map(s=>[s.property.lat,s.property.lon]);if(points.length)map.fitBounds(points,{padding:[25,25],maxZoom:18});}catch(e){status.textContent=e.message;}}};
}
