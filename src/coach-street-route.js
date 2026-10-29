import { spanishManeuver, projectRoute } from '../public/coach/route-guidance.js';
const fail=(status,message)=>Object.assign(new Error(message),{status});
export function routePoint(value) {
 if(!value || typeof value.lat!=='number' || typeof value.lon!=='number' || !Number.isFinite(value.lat)||!Number.isFinite(value.lon)||value.lat<41||value.lat>43||value.lon<-89||value.lon>-87) throw fail(400,'La navegación está disponible en el área de Cook County.');
 return {lat:value.lat,lon:value.lon};
}
// Public FOSSGIS: one request/second, no bulk prefetch. One active leg only.
// For growth, replace these OSRM endpoints with a dedicated service.
export function createStreetRouter({fetchImpl=fetch,now=Date.now}={}) {
 const cache=new Map(); let nextAllowed=0;
 async function route(input) {
  const from=routePoint(input?.from),to=routePoint(input?.to),mode=input?.mode;
  if(!['walking','driving'].includes(mode)) throw fail(400,'Selecciona a pie o en auto.');
  const key=JSON.stringify({from,to,mode}),old=cache.get(key);
  if(old&&old.expires>now()) return old.value;
  if(now()<nextAllowed) throw fail(429,'El servicio de rutas está ocupado. Reintenta en unos segundos.');
  nextAllowed=now()+1100;
  const base=mode==='walking'?(process.env.TEC_OSRM_FOOT_URL||'https://routing.openstreetmap.de/routed-foot'):(process.env.TEC_OSRM_CAR_URL||'https://routing.openstreetmap.de/routed-car');
  const url=`${base}/route/v1/driving/${from.lon},${from.lat};${to.lon},${to.lat}?overview=full&geometries=geojson&steps=true&alternatives=false`;
  let data;
  try { const response=await fetchImpl(url,{headers:{Accept:'application/json','User-Agent':'TEC-Coaching/1.0 (+https://totalelitecoaching.com)'},signal:AbortSignal.timeout(15000)}); if(!response.ok)throw Error();data=await response.json(); }
  catch {throw fail(503,'No pude calcular el trayecto por calles. Reintenta o abre Apple Maps o Google Maps.');}
  const r=data.routes?.[0],coordinates=r?.geometry?.coordinates;
  if(data.code!=='Ok'||!Array.isArray(coordinates)||coordinates.length<2||coordinates.length>30000||!coordinates.every(p=>Array.isArray(p)&&p.length>=2&&p.slice(0,2).every(Number.isFinite)))throw fail(503,'No hay un trayecto disponible por calles.');
  const steps=(r.legs||[]).flatMap(leg=>leg.steps||[]).map(step=>({text:spanishManeuver(step),along:projectRoute({lon:step.maneuver.location[0],lat:step.maneuver.location[1]},coordinates).along}));
  const value={coordinates,steps,distance:r.distance,duration:r.duration,source:'OpenStreetMap · FOSSGIS / OSRM'};
  if(cache.size>=100)cache.delete(cache.keys().next().value);
  cache.set(key,{value,expires:now()+900000});return value;
 }
 return {route};
}
export function registerStreetRoutes(app,{requireCoachActivo,service=createStreetRouter(),now=Date.now}) {
 const quota=new Map();
 app.post('/api/coach/property-navigation',async(req,res)=>{
  res.set('Cache-Control','private, no-store');
  try {
   const auth=await requireCoachActivo(req,res);if(!auth)return;
   const id=String(auth.user._id),time=now();
   for(const [key,b] of quota)if(b.until<time)quota.delete(key);
   const bucket=quota.get(id)||{count:0,until:time+3600000};
   if(quota.size>2000||++bucket.count>150)throw fail(429,'Alcanzaste el límite temporal de navegación. Usa Apple Maps o Google Maps.');
   quota.set(id,bucket);res.json(await service.route(req.body));
  }catch(error){if(error.status===429)res.set('Retry-After','5');res.status(error.status||503).json({error:error.status?error.message:'No pude calcular el trayecto.'});}
 });
}
