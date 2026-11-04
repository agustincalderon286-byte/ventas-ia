import dns from 'node:dns';
import net from 'node:net';
import ipaddr from 'ipaddr.js';
import {Agent,fetch as undiciFetch} from 'undici';
const blocked=()=>Object.assign(new Error('El destino debe ser una dirección HTTPS pública autorizada.'),{code:'OUTBOUND_DESTINATION_BLOCKED'});
export function publicAddress(address){try{return ipaddr.process(address).range()==='unicast';}catch{return false;}}
export function validateDestination(value,{hosts=process.env.COACH_OUTBOUND_ALLOWED_HOSTS||''}={}){
  let url;try{url=new URL(value);}catch{throw blocked();}
  const host=url.hostname.replace(/^\[|\]$/g,'').toLowerCase();
  if(url.protocol!=='https:'||url.username||url.password||(url.port&&url.port!=='443')||!host||host==='localhost'||host.endsWith('.localhost')||host.endsWith('.local')||host.endsWith('.internal')||(net.isIP(host)&&!publicAddress(host)))throw blocked();
  const allowed=String(hosts).split(',').map(x=>x.trim().toLowerCase()).filter(Boolean);
  if(allowed.length&&!allowed.some(x=>host===x||(x.startsWith('*.')&&host.endsWith(x.slice(1)))))throw blocked();
  return url;
}
export function publicLookup(lookup=dns.lookup){return(host,options,callback)=>{
  lookup(host,{all:true,verbatim:true},(error,records)=>{
    if(error)return callback(error);
    if(!records?.length||records.some(row=>!publicAddress(row.address)))return callback(blocked());
    if(options?.all)return callback(null,records);
    const row=records.find(row=>!options?.family||row.family===options.family)||records[0];callback(null,row.address,row.family);
  });
};}
export async function secureWebhookFetch(value,options={},deps={}){
  let current=validateDestination(value,deps),method=options.method||'POST',body=options.body;
  const dispatcher=deps.dispatcher||new Agent({connect:{lookup:publicLookup(deps.lookup)}});
  const fetchImpl=deps.fetchImpl||undiciFetch;
  try{
    for(let redirects=0;redirects<=3;redirects++){
      const response=await fetchImpl(current.href,{...options,method,...(body==null?{body:undefined}:{body}),dispatcher,redirect:'manual'});
      if([301,302,303,307,308].includes(response.status)){
        const location=response.headers.get('location');await response.body?.cancel?.();
        if(!location||redirects===3)throw blocked();
        current=validateDestination(new URL(location,current).href,deps);
        if(response.status===303||([301,302].includes(response.status)&&method==='POST')){method='GET';body=undefined;}
        continue;
      }
      // Delivery callers need only status. Do not retain arbitrary response data.
      const result={ok:response.ok,status:response.status};await response.body?.cancel?.();return result;
    }
    throw blocked();
  }finally{if(!deps.dispatcher)await dispatcher.close();}
}
