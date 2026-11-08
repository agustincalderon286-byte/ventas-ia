import {BADGES} from './logros-core.js?v=1';
export const BADGE_CARD_FORMATS=Object.freeze({square:{width:1080,height:1080},story:{width:1080,height:1920},preview:{width:1200,height:630}});
export function badgeShareInfo(id,format='square',origin='https://totalelitecoaching.com'){
  const badge=BADGES.find(b=>b.id===id),size=BADGE_CARD_FORMATS[format];
  if(!badge||!size)throw new TypeError('Unknown badge or card format');
  const base=new URL(origin).origin,path='/audiolibros/logros/'+badge.id+'/';
  if(!/^https?:$/.test(new URL(base).protocol))throw new TypeError('Invalid share origin');
  return {badge,format,...size,path,url:base+path,image:base+'/audiolibros/insignias/v2/'+badge.id+'-'+format+'.png',downloadName:'Audiolibros-TEC-'+badge.id+'-'+format+'.png'};
}
export const badgeShareText=info=>`¡Desbloqueé «${info.badge.name}» en Audiolibros TEC!\nEscucha gratis el primer 10 % de cada libro.\n${info.url}`;
export async function nativeBadgeShare(nav,info,file){
  if(!nav?.share)return 'unsupported';
  try{
    const withImage=Boolean(file&&nav.canShare?.({files:[file]}));
    // Invoked directly by the user's click; the file is fetched before enabling the button.
    await nav.share(withImage?{files:[file],title:info.badge.name,text:badgeShareText(info)}:{title:info.badge.name,text:badgeShareText(info).slice(0,-info.url.length-1),url:info.url});
    return withImage?'shared-file':'shared-link';
  }catch(error){if(error.name==='AbortError')return 'cancelled';return 'failed';}
}
