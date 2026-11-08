import {mkdir,writeFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import sharp from 'sharp';
import QRCode from 'qrcode';
import {BADGES} from '../public/audiolibros/logros-core.js';
import {badgeSvg} from '../public/audiolibros/insignias-art.js';
import {badgeShareInfo,BADGE_CARD_FORMATS} from '../public/audiolibros/insignias-share-core.js';

const escape=value=>String(value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function wrap(text,max){const rows=[];let line='';for(const word of text.split(/\s+/)){if(line&&(line+' '+word).length>max){rows.push(line);line=word;}else line+=(line?' ':'')+word;}if(line)rows.push(line);return rows;}
export async function badgeCardSvg(id,format='square'){
  const info=badgeShareInfo(id,format),{badge,width:w,height:h}=info,preview=format==='preview',story=format==='story';
  const logo=badgeSvg(id,true).replace('<svg ','<svg x="0" y="0" ');
  const medalSize=preview?330:story?570:400,medalX=preview?90:(w-medalSize)/2,medalY=preview?146:story?430:195;
  const titleSize=preview?52:story?72:60,lines=wrap(badge.name,preview?24:25),titleX=preview?480:w/2,titleY=preview?260:story?1120:700;
  const title=lines.map((line,i)=>`<text x="${titleX}" y="${titleY+i*titleSize*1.16}" text-anchor="${preview?'start':'middle'}" fill="#FFF4DA" font-family="Georgia,serif" font-weight="bold" font-size="${titleSize}">${escape(line)}</text>`).join('');
  const subY=titleY+(lines.length-1)*titleSize*1.16+54,qrSize=preview?112:200,qrX=story?80:w-qrSize-(preview?64:80),qrY=h-qrSize-(preview?45:story?300:65);
  const qr=await QRCode.toBuffer(info.url,{width:qrSize,margin:4,errorCorrectionLevel:'M',color:{dark:'#133D31',light:'#FFFDF5'}});
  const brandIcon='<rect width="64" height="64" rx="18" fill="none" stroke="#EAC985" stroke-width="2"/><path d="M13 32h4m6-11v22m9-31v40m9-31v22m6-11h4" stroke="#EAC985" stroke-width="3" stroke-linecap="round"/>';
  const background=`<defs><linearGradient id="bg" x1="0" x2="1" y2="1"><stop stop-color="#315A48"/><stop offset=".5" stop-color="#142F27"/><stop offset="1" stop-color="#0B211D"/></linearGradient></defs><rect width="${w}" height="${h}" fill="url(#bg)"/><circle cx="${preview?240:w/2}" cy="${medalY+medalSize*.5}" r="${medalSize*.65}" fill="none" stroke="#D9C183" opacity=".16"/><circle cx="${preview?240:w/2}" cy="${medalY+medalSize*.5}" r="${medalSize*.78}" fill="none" stroke="#D9C183" opacity=".08"/><rect x="25" y="25" width="${w-50}" height="${h-50}" rx="${preview?22:32}" fill="none" stroke="#BBA26A" stroke-width="2" opacity=".6"/>`;
  const motto=preview?'UN LOGRO, UNA NUEVA HISTORIA':'MI COLECCIÓN DE HISTORIAS SIGUE CRECIENDO';
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">${background}<g transform="translate(${preview?66:80} ${preview?55:story?240:72})">${brandIcon}<text x="85" y="30" fill="#FFF4DA" font-family="DejaVu Sans,sans-serif" font-weight="bold" font-size="${preview?27:31}">Audiolibros TEC</text><text x="85" y="53" fill="#D3DCC9" font-family="DejaVu Sans,sans-serif" font-size="17">Historias que van contigo</text></g><text x="${preview?480:w/2}" y="${preview?195:story?360:172}" text-anchor="${preview?'start':'middle'}" fill="#EAC985" font-family="DejaVu Sans,sans-serif" font-size="${preview?19:24}" letter-spacing="4">INSIGNIA DESBLOQUEADA</text><svg x="${medalX}" y="${medalY}" width="${medalSize}" height="${medalSize*1.1}" viewBox="0 0 200 220">${logo.replace(/^<svg[^>]*>/,'').replace(/<\/svg>$/,'')}</svg>${title}<text x="${titleX}" y="${subY}" text-anchor="${preview?'start':'middle'}" fill="#D6E1D3" font-family="DejaVu Sans,sans-serif" font-size="${preview?23:28}">${preview?'Una historia más para descubrir.':'Cada escucha deja huella.'}</text>${story?`<text x="${w/2}" y="${subY+110}" text-anchor="middle" fill="#EAC985" font-family="DejaVu Sans,sans-serif" font-size="20" letter-spacing="3">${motto}</text>`:''}<line x1="${preview?66:80}" y1="${qrY-30}" x2="${w-(preview?64:80)}" y2="${qrY-30}" stroke="#6E856B"/><text x="${preview?66:story?310:80}" y="${qrY+25}" fill="#EAC985" font-family="DejaVu Sans,sans-serif" font-size="${preview?23:27}" font-weight="bold">Escucha una muestra gratis</text><text x="${preview?66:story?310:80}" y="${qrY+65}" fill="#FFF4DA" font-family="DejaVu Sans,sans-serif" font-size="${preview?21:25}">totalelitecoaching.com/audiolibros/</text><text x="${preview?66:story?310:80}" y="${qrY+96}" fill="#C0D0C1" font-family="DejaVu Sans,sans-serif" font-size="${preview?16:19}">Primer 10 % de cada libro · Escanea el QR</text><image x="${qrX}" y="${qrY}" width="${qrSize}" height="${qrSize}" href="data:image/png;base64,${qr.toString('base64')}"/></svg>`;
}
export async function generateBadgeCards(){
  const output=new URL('../public/audiolibros/insignias/v2/',import.meta.url);await mkdir(output,{recursive:true});let bytes=0;
  for(const badge of BADGES){
    await writeFile(new URL(badge.id+'.svg',output),badgeSvg(badge.id));
    for(const format of Object.keys(BADGE_CARD_FORMATS)){
      const png=await sharp(Buffer.from(await badgeCardSvg(badge.id,format))).png({compressionLevel:9}).toBuffer();bytes+=png.length;
      await writeFile(new URL(badge.id+'-'+format+'.png',output),png);
    }
  }
  console.log(JSON.stringify({badges:BADGES.length,cards:BADGES.length*3,bytes,output:fileURLToPath(output)}));
}
if(process.argv[1]&&fileURLToPath(import.meta.url)===process.argv[1])await generateBadgeCards();
