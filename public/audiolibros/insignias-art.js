// Original TEC vector artwork. Symbols, frames and colors are shared by the UI and export cards.
const icons={
 seed:'<path d="M32 54V30M32 38C12 37 10 23 13 17c14-2 22 8 19 21Zm0-8C32 15 42 9 53 11c1 12-7 22-21 19Z"/><path d="M17 55h30M8 9l4 3M52 3v5M58 33h5"/>',
 coffee:'<path d="M12 26h34v16a13 13 0 0 1-26 0V26Zm34 2h5a8 8 0 0 1 0 16h-5M8 57h45M21 17c-6-6 5-8 0-14M33 17c-6-6 5-8 0-14"/>',
 hourglass:'<path d="M15 6h34M15 58h34M19 7v9c0 8 8 10 13 16-5 6-13 8-13 16v9m26-50v9c0 8-8 10-13 16 5 6 13 8 13 16v9M23 49l9-9 9 9Z"/><path d="M24 16h16"/>',
 moon:'<path d="M44 8A24 24 0 1 0 56 45C28 51 19 23 44 8Z"/><path d="m49 18 2 5 5 2-5 2-2 5-2-5-5-2 5-2ZM19 5v6M16 8h6"/>',
 idea:'<path d="M22 40c-3-8-9-9-9-19a19 19 0 0 1 38 0c0 10-6 11-9 19ZM23 47h18M26 54h12M32 12v7m-7 5 7 7 7-7M32 31v9M3 21h4M57 21h4"/>',
 owl:'<path d="M11 10l14 6h14l14-6-3 24c1 17-8 25-18 25S13 51 14 34Z"/><circle cx="23" cy="29" r="9"/><circle cx="41" cy="29" r="9"/><path d="m28 41 4 5 4-5M22 57l-3 5m23-5 3 5"/><circle cx="23" cy="29" r="2" fill="currentColor"/><circle cx="41" cy="29" r="2" fill="currentColor"/>',
 mountain:'<path d="m3 56 23-43 13 23 8-12 15 32ZM19 26l7 5 6-5M39 36l5 8 7-2M4 61h57"/><circle cx="49" cy="10" r="5"/>',
 comet:'<path d="m3 9 25 13M8 25l13 7M17 3l19 14M51 24l3 9 9 3-8 5-1 10-8-6-9 3 3-10-6-8 10 1ZM6 48l15-7M19 59l12-10"/>',
 compass:'<circle cx="32" cy="32" r="26"/><path d="m43 20-7 17-17 8 8-18ZM32 5V1m0 62v-4M1 32h4m54 0h4"/><path d="m27 27 9 10"/>',
 telescope:'<path d="m11 29 30-17 9 16-30 17ZM41 12l8-4 9 16-8 4M13 33l-7 4 5 9 7-4M31 40v19M20 59l11-19 13 19"/><path d="m9 9 2 5 5 2-5 2-2 5-2-5-5-2 5-2Z"/>',
 map:'<path d="m4 13 18-7 20 7 18-7v45l-18 7-20-7-18 7Zm18-7v45m20-38v45"/><path d="m12 37 6-8 11 8 8-11 13 5M48 16l9 9m0-9-9 9"/>',
 book:'<path d="M32 14c-10-8-20-8-28-4v42c11-3 21-1 28 6 7-7 17-9 28-6V10c-8-4-18-4-28 4Zm0 0v44M11 20c5-1 9 0 14 3m-14 8c5-1 9 0 14 3m14-11c5-3 9-4 14-3m-14 14c5-3 9-4 14-3"/>',
 stack:'<path d="m8 14 35-8 15 9-35 9ZM8 14v9l15 10 35-9v-9M7 33l16 10 35-10M7 33v9l16 10 35-10M7 49l16 10 35-10M23 24v9m0 10v9"/>',
 shelf:'<path d="M3 57h58M8 10h11v41H8Zm13 7h11v34H21Zm18-7 11-3 10 41-11 3ZM11 17h5m8 8h5m14-6 7-2"/>',
 crown:'<path d="m7 14 13 14L32 7l12 21 13-14-6 33H13ZM14 55h36M14 39h37"/><circle cx="6" cy="11" r="3"/><circle cx="32" cy="5" r="3"/><circle cx="58" cy="11" r="3"/><path d="m29 31 3-4 3 4-3 5Z"/>',
 tree:'<path d="M32 58V31m0 10-11-8m11 1 12-10M20 59h25"/><path d="M16 40A12 12 0 0 1 8 20a14 14 0 0 1 19-12 14 14 0 0 1 22 8 13 13 0 0 1 3 24c-8 4-15 2-20-2-6 5-11 6-16 2Z"/>',
 diamond:'<path d="m3 23 13-14h32l13 14-29 35ZM3 23h58M16 9l7 14 9-14 9 14 7-14M23 23l9 35 9-35"/><path d="M5 5v7M1 8h8m49 41v9m-4-4h8"/>',
 rook:'<path d="M16 6h8v9h6V6h8v9h6V6h8v19l-9 7 3 17H18l3-17-9-7V6ZM17 25h30M17 49l-4 9h38l-4-9M26 38h12"/>',
 creation:'<circle cx="32" cy="35" r="22"/><path d="M12 29h12l4-10 7-3 3 10 10 3 2 11-10 4-6 12-7-10-8-5ZM32 2v5M8 9l5 5m38-5-5 5M58 28h5M1 28h5"/>',
 sea:'<path d="M3 20c8-10 17 10 24 0M3 31c8-10 17 10 24 0M3 42c8-10 17 10 24 0M39 20c8-10 15 10 23 0M39 31c8-10 15 10 23 0M39 42c8-10 15 10 23 0M22 61 30 9h6l8 52"/><path d="M32 2v4m-9-3 3 4m15-4-3 4"/>',
 torch:'<path d="M26 37h13l-3 24h-7ZM21 30h23v7H21Z"/><path d="M30 27c-16-3-8-17 1-25-2 10 13 12 11 20-1 6-8 8-12 5Z"/><path d="m12 9 5 5M51 9l-5 5M6 25h8m37 0h8"/>',
 rise:'<path d="M8 56h50M14 49V35h9v14m6 0V25h9v24m6 0V14h9v35M9 27l17-13 11 4L56 4M44 4h12v12"/>',
 harmony:'<circle cx="32" cy="20" r="16"/><circle cx="20" cy="40" r="16"/><circle cx="44" cy="40" r="16"/><path d="m10 60 22-50 22 50ZM32 30v15"/>',
 read:'<path d="M10 27v-5a22 22 0 0 1 44 0v5M7 27h9v15H7Zm41 0h9v15h-9M32 42c-7-5-14-5-19-3v19c6-2 13-1 19 3 6-4 13-5 19-3V39c-5-2-12-2-19 3Zm0 0v19"/>',
 worlds:'<circle cx="32" cy="32" r="22"/><ellipse cx="32" cy="32" rx="10" ry="22"/><path d="M10 32h44M15 18h34M15 46h34"/><path d="M7 10c-11 23 30 54 51 44M51 52l8 2-2 8M8 3v8M4 7h8"/>'
};
const entries=[
 ['tiempo-60','seed','#E8BE69','#28463B','round'],['tiempo-300','coffee','#EA9B70','#58392F','shield'],['tiempo-900','hourglass','#A5B5F2','#343F69','hex'],['tiempo-1800','moon','#CEACEF','#4A3565','round'],
 ['tiempo-3600','idea','#F4CB67','#5C482A','hex'],['tiempo-10800','owl','#B6B1F5','#413E6D','shield'],['tiempo-18000','mountain','#91C7C4','#284D52','hex'],['tiempo-36000','comet','#EA9DAD','#5E3347','round'],
 ['muestras-1','compass','#85CFC5','#25574F','round'],['muestras-3','telescope','#A3C4EA','#304E72','shield'],['muestras-5','map','#C4CC87','#47512C','hex'],
 ['libros-1','book','#F0C582','#5E462B','shield'],['libros-3','stack','#B0D5B1','#315437','hex'],['libros-5','shelf','#9AC5DD','#2A5366','round'],['libros-8','crown','#F5CF7D','#5A462B','shield'],
 ['titulo-como-un-hombre-piensa','tree','#9FC89A','#365B38','round'],['titulo-acres-de-diamantes','diamond','#9BD6D1','#245453','hex'],['titulo-el-arte-de-la-guerra','rook','#D8AD91','#563D34','shield'],
 ['titulo-genesis-rv1909','creation','#E4CD8A','#5B5032','round'],['titulo-exodo-rv1909','sea','#9ACAE4','#29566E','hex'],['titulo-ayudate','torch','#F0AE76','#633D28','shield'],
 ['titulo-la-ciencia-de-hacerse-rico','rise','#BFCC88','#4A542B','hex'],['titulo-el-kybalion','harmony','#C1B0EA','#483769','round'],['lectura-300','read','#E3B5C9','#633C53','shield'],['temas-3','worlds','#9DD0B9','#2C5848','round']
];
export const BADGE_VISUALS=Object.freeze(Object.fromEntries(entries.map(([id,icon,accent,base,shape])=>[id,Object.freeze({icon,accent,base,shape})])));
export function badgeSvg(id,earned=true){
  const art=BADGE_VISUALS[id];if(!art)throw new TypeError('Unknown badge artwork');
  const accent=earned?art.accent:'#A3B4AD',base=earned?art.base:'#263C34',ink=earned?'#FFF3CF':'#BECCC4';
  const shape=art.shape==='round'?'<circle cx="100" cy="98" r="83"/>':art.shape==='hex'?'<path d="M100 9 179 53v90l-79 48-79-48V53Z"/>':'<path d="M100 9 178 36v70c0 43-38 75-78 96-40-21-78-53-78-96V36Z"/>';
  const rim=art.shape==='round'?'<circle cx="100" cy="98" r="74"/>':art.shape==='hex'?'<path d="m100 20 69 39v78l-69 42-69-42V59Z"/>':'<path d="m100 20 67 24v62c0 34-30 62-67 83-37-21-67-49-67-83V44Z"/>';
  return `<svg xmlns="http://www.w3.org/2000/svg" width="200" height="220" viewBox="0 0 200 220"><g fill="${base}" stroke="${accent}" stroke-width="4">${shape}</g><g fill="none" stroke="${accent}" stroke-width="1.3" opacity=".65">${rim}</g><circle cx="100" cy="96" r="57" fill="${base}" stroke="${accent}" stroke-width="1" opacity=".6"/><g transform="translate(52 48) scale(1.5)" color="${ink}" fill="none" stroke="${ink}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round">${icons[art.icon]}</g><path d="m100 168 3 6 7 1-5 5 1 7-6-3-6 3 1-7-5-5 7-1Z" fill="${accent}"/>${earned?`<path d="m151 166 5 5 10-12" fill="none" stroke="${accent}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>`:''}</svg>`;
}
