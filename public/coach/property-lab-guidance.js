import { meters, projectRoute } from './route-guidance.js?v=20261028-walk2';
import { nextRouteStop } from './property-route-state.js?v=20261028-walk2';
import { propertyVisitKey } from './property-visit-helpers.js';

export const labDistance = value => !Number.isFinite(value) ? '—' : value >= 1000 ? `${(value / 1000).toFixed(1)} km` : `${Math.max(0, Math.round(value / 5) * 5)} m`;
export const labTime = seconds => !Number.isFinite(seconds) ? '—' : `${Math.max(1, Math.ceil(seconds / 60))} min`;
export function freshLabFix(value, now = Date.now()) {
  return !!value && Number.isFinite(value.lat) && Math.abs(value.lat) <= 85 && Number.isFinite(value.lon) && Math.abs(value.lon) <= 180 && Number.isFinite(value.accuracy) && value.accuracy >= 0 && value.accuracy <= 40 && Number.isFinite(value.timestamp) && now - value.timestamp <= 15000 && now - value.timestamp >= -2000;
}
export function labBearing(a, b) {
  const rad = Math.PI / 180, y = Math.sin((b.lon - a.lon) * rad) * Math.cos(b.lat * rad);
  const x = Math.cos(a.lat * rad) * Math.sin(b.lat * rad) - Math.sin(a.lat * rad) * Math.cos(b.lat * rad) * Math.cos((b.lon - a.lon) * rad);
  return (Math.atan2(y, x) / rad + 360) % 360;
}
function alongPoint(coordinates, along) {
  let distance = 0;
  for (let i = 1; i < coordinates.length; i++) {
    const a = {lon:coordinates[i-1][0],lat:coordinates[i-1][1]}, b = {lon:coordinates[i][0],lat:coordinates[i][1]}, segment = meters(a,b);
    if (distance + segment >= along || i === coordinates.length - 1) {
      const t = segment ? Math.min(1,Math.max(0,(along-distance)/segment)) : 0;
      return {lon:a.lon+(b.lon-a.lon)*t,lat:a.lat+(b.lat-a.lat)*t};
    }
    distance += segment;
  }
  return {lon:coordinates[0][0],lat:coordinates[0][1]};
}
export function labArrow(text = '') {
  if (/vuelta|retorno/i.test(text)) return '↶';
  if (/glorieta/i.test(text)) return '⟳';
  if (/izquierda/i.test(text)) return '↰';
  if (/derecha/i.test(text)) return '↱';
  if (/destino|confirma el número/i.test(text)) return '⚑';
  return '↑';
}
export function labNavigationFix(point, plan, destination, now = Date.now()) {
  if (!freshLabFix(point, now)) return {phase:'waiting',instruction:'Esperando una ubicación GPS reciente y precisa.',arrow:'⌖'};
  const projection = projectRoute(point, plan.coordinates);
  const remaining = Math.max(0,projection.length-projection.along);
  const seconds = plan.distance > 0 ? plan.duration * remaining / plan.distance : null;
  if (projection.distance > 60) return {phase:'offroute',instruction:'Fuera del trayecto. Buscando una ruta nueva…',arrow:'↻',remaining,seconds,position:point};
  if (meters(point,destination) < 25 && point.accuracy <= 25) return {phase:'arrived',instruction:'Confirma el número de la casa y registra la visita.',arrow:'⚑',nextMeters:0,remaining,seconds,position:point};
  let index = plan.steps.findIndex(step => step.along > projection.along + 10);
  if (index < 0) index = plan.steps.length - 1;
  const step = plan.steps[index], nextMeters = step ? Math.max(0, step.along-projection.along) : remaining;
  const snapped = alongPoint(plan.coordinates, projection.along), ahead = alongPoint(plan.coordinates, projection.along+20);
  const bearing = Number.isFinite(point.heading) && point.speed >= 1 ? point.heading : labBearing(snapped,ahead);
  return {phase:'guiding',instruction:step?.text || 'Continúa hacia la casa.',arrow:labArrow(step?.text),nextMeters,remaining,seconds,position:{...snapped,accuracy:point.accuracy,bearing},stepIndex:index};
}
function validPlan(value) {
  return Array.isArray(value?.coordinates) && value.coordinates.length >= 2 && value.coordinates.length <= 30000 && value.coordinates.every(p => Array.isArray(p) && p.length >= 2 && p.slice(0,2).every(Number.isFinite)) && Array.isArray(value.steps) && value.steps.length > 0 && value.steps.every(s => typeof s.text === 'string' && Number.isFinite(s.along)) && Number.isFinite(value.distance) && value.distance >= 0 && Number.isFinite(value.duration) && value.duration >= 0;
}

// The LAB reads the existing route; only the shared visit callbacks may write it.
// Generation checks keep old GPS/HTTP callbacks from restoring a previous house.
export function createLabNavigator({getRoute,request,geolocation,now=Date.now,onState=()=>{},onSpeak=()=>{},onStop=()=>{},captureBusy=()=>false,canSpeak=()=>true}) {
  let state={active:false,phase:'idle',instruction:'Inicia GPS cuando estés listo.',arrow:'↑',plan:null,position:null};
  let watch=null,generation=0,fetching=null,lastAttempt=-Infinity,lastSpoken='',offrouteCount=0,latest=null,currentKey='';
  const emit = value => { state={...state,...value}; onState(state); };
  const key = () => {const r=getRoute(),n=nextRouteStop(r); return n ? `${propertyVisitKey(n.property)}:${n.property.lat}:${n.property.lon}:${r.travelMode}` : '';};
  function stop(message='Navegación detenida. Tu recorrido sigue guardado.') {
    generation++; if(watch!==null) geolocation?.clearWatch(watch); watch=null; fetching=null;
    emit({active:false,phase:'idle',instruction:message,arrow:'↑'}); onStop();
  }
  function render() {
    const next=nextRouteStop(getRoute()); if(!state.active || !state.plan || !next || !latest)return;
    const fix=labNavigationFix(latest,state.plan,next.property,now()); emit(fix);
    if(fix.phase==='offroute') {if(++offrouteCount>=2 && now()-lastAttempt>=15000)calculate();return;}
    offrouteCount=0;
    if(!['guiding','arrived'].includes(fix.phase) || !canSpeak())return;
    const cue=fix.phase==='arrived'?'arrived':`${fix.stepIndex}:${fix.nextMeters<40?'now':fix.nextMeters<150?'near':'far'}`;
    const id=currentKey+':'+cue;
    if(id!==lastSpoken){lastSpoken=id;onSpeak(fix.phase==='arrived'?'Estás cerca de la casa. Confirma el número.':`En ${labDistance(fix.nextMeters).replace(' m',' metros').replace(' km',' kilómetros')}, ${fix.instruction}`);}
  }
  async function calculate() {
    const next=nextRouteStop(getRoute()); if(!state.active || fetching!==null || !next || !freshLabFix(latest,now()) || now()-lastAttempt<10000)return;
    const version=generation;fetching=version;lastAttempt=now();
    emit({phase:'routing',instruction:'Calculando el trayecto por calles…',arrow:'↻'});
    try {
      const value=await request('/api/coach/property-navigation',{method:'POST',body:{from:{lat:latest.lat,lon:latest.lon},to:{lat:next.property.lat,lon:next.property.lon},mode:getRoute().travelMode},timeoutMs:20000});
      if(version!==generation || !state.active)return;
      if(!validPlan(value))throw Error('El servicio devolvió una ruta incompleta.');
      emit({plan:value});offrouteCount=0;render();
    } catch(error) {
      if(version===generation && state.active)emit({phase:'error',instruction:error.message || 'No pude calcular el trayecto. Toca Reintentar ruta.',arrow:'↻'});
    } finally {if(fetching===version)fetching=null;}
  }
  function acceptFix(point,version=generation) {
    if(version!==generation || !state.active)return;
    if(captureBusy()){stop('Navegación pausada mientras grabas la demostración.');return;}
    latest=point;
    if(!freshLabFix(point,now())) {emit({phase:'waiting',instruction:'Esperando una ubicación GPS reciente y precisa.',arrow:'⌖'});return;}
    emit({position:point});
    if(state.plan)render();else calculate();
  }
  function subscribe() {
    const version=generation;
    watch=geolocation.watchPosition(value=>acceptFix({lat:value.coords.latitude,lon:value.coords.longitude,accuracy:value.coords.accuracy,heading:value.coords.heading,speed:value.coords.speed,timestamp:value.timestamp},version),error=>{
      if(version!==generation || !state.active)return;
      if(error.code===1)stop('Permite la ubicación en el navegador para iniciar GPS.');
      else emit({phase:'waiting',instruction:'No llega una señal GPS reciente. Acércate a un lugar con mejor señal.',arrow:'⌖'});
    },{enableHighAccuracy:true,maximumAge:0,timeout:15000});
  }
  function start() {
    if(state.active || !nextRouteStop(getRoute()))return;
    if(captureBusy()){emit({instruction:'Termina la grabación antes de iniciar GPS con voz.'});return;}
    if(!geolocation){emit({instruction:'Este navegador no ofrece ubicación. Puedes usar la lista o los mapas externos.'});return;}
    generation++;currentKey=key();lastAttempt=-Infinity;lastSpoken='';latest=null;
    emit({active:true,phase:'waiting',instruction:'Buscando tu ubicación GPS…',plan:null,arrow:'⌖'});
    try{subscribe();}catch{stop('No pude iniciar GPS. Revisa los permisos del navegador.');}
  }
  function update() {
    const nextKey=key();if(currentKey===nextKey)return;
    currentKey=nextKey;generation++;fetching=null;lastAttempt=-Infinity;lastSpoken='';offrouteCount=0;
    emit({plan:null,phase:state.active?'waiting':'idle',instruction:nextKey?'Inicia GPS cuando estés listo.':'Recorrido terminado. Tu avance sigue guardado.',arrow:nextKey?'↑':'⚑'});
    if(state.active){
      if(!nextKey){stop('Recorrido terminado. Tu avance sigue guardado.');return;}
      if(watch!==null)geolocation.clearWatch(watch);
      try{subscribe();}catch{stop('No pude continuar con GPS. Reintenta.');return;}
      calculate();
    }
  }
  function tick() {
    if(!state.active)return;
    if(captureBusy()){stop('Navegación pausada mientras grabas la demostración.');return;}
    if(!freshLabFix(latest,now()))emit({phase:'waiting',instruction:'Esperando una ubicación GPS reciente y precisa.',arrow:'⌖'});
  }
  return {start,stop,update,tick,retry:calculate,repeat:()=>{if(state.active && canSpeak())onSpeak(state.instruction);},get:()=>state};
}
