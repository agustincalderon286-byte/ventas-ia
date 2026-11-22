import {createRouteAllowance} from './coach-route-allowance.js';

const minute = 60000;
const fieldMask = 'routes.duration,fallbackInfo,geocodingResults.origin.partialMatch,geocodingResults.destination.partialMatch';

export function routeMapLink(origin, destination, stop) {
  const url = new URL('https://www.google.com/maps/dir/');
  url.search = new URLSearchParams({api:'1',origin,destination,travelmode:'driving',...(stop && {waypoints:stop})});
  return {type:'routeMap',label:'Ver trayecto en Google Maps',url:url.href};
}

export function createTrafficRoutes(config, {fetcher=fetch,now=Date.now,allowance=createRouteAllowance(config,{now})}={}) {
  const key=config.mapsRoutesKey;
  return {
    get enabled() { return Boolean(key) && allowance.configured(); },
    async minutes(origin,destination,departure) {
      if (!key || !await allowance.reserve()) {
        throw Object.assign(Error('Cupo gratuito sin verificar o agotado.'),{freeAllowance:true});
      }
      const res = await fetcher('https://routes.googleapis.com/directions/v2:computeRoutes',{
        method:'POST', redirect:'error', signal:AbortSignal.timeout(10000),
        headers:{'Content-Type':'application/json','X-Goog-Api-Key':key,'X-Goog-FieldMask':fieldMask},
        body:JSON.stringify({origin:{address:origin},destination:{address:destination},travelMode:'DRIVE',
          routingPreference:'TRAFFIC_AWARE',...(departure>now()+5000 && {departureTime:new Date(departure).toISOString()})})
      });
      if (!res.ok) throw Error('Google Maps no pudo verificar el trayecto.');
      const data = await res.json();
      if (data.geocodingResults?.origin?.partialMatch || data.geocodingResults?.destination?.partialMatch) {
        throw Object.assign(Error('Ubicación por verificar.'),{partialAddress:true});
      }
      const seconds = /^(\d+(?:\.\d{1,9})?)s$/.exec(data.routes?.[0]?.duration || '');
      if (!seconds || data.fallbackInfo || !(Number(seconds[1]) >= 0 && Number(seconds[1]) <= 86400)) {
        throw Error('Google Maps no confirmó una estimación con tráfico.');
      }
      return Math.max(1,Math.ceil(Number(seconds[1])/60));
    }
  };
}

// Each leg uses its actual departure, including time spent visiting the prospect.
export async function estimateItinerary({start, deadline, origin, stop, destination, visitMinutes=0, fixed, minutes}) {
  const from = async departure => {
    const first = await minutes(origin,stop || destination,departure,0);
    const secondStart = departure + (first+visitMinutes)*minute;
    const second = stop ? await minutes(stop,destination,secondStart,1) : 0;
    return {departure, first, second, arrival:secondStart+second*minute};
  };
  const early = await from(start);
  if (fixed || early.arrival >= deadline) return early;
  const laterStart = start + Math.floor((deadline-early.arrival)/minute)*minute;
  if (laterStart-start<minute) return early;
  let candidate=laterStart;
  // Traffic at the proposed departure can differ substantially from the first
  // estimate. Check an earlier departure with the observed overrun before
  // falling back to the already verified itinerary.
  for (let attempt=0;attempt<2;attempt++) {
    let later;
    try { later=await from(candidate); } catch { return early; }
    if (later.arrival<=deadline) return later;
    candidate-=Math.ceil((later.arrival-deadline)/minute)*minute + minute;
    if (candidate<=start) break;
  }
  return early;
}
