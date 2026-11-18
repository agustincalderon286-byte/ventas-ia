// Generate the released guide IDs and the current Coach offline shell from its HTML and imports.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {audiobookGuides} from '../src/knowledge/audiobook-guides.js';
const root=fileURLToPath(new URL('../',import.meta.url));
const read=file=>fs.readFileSync(path.join(root,file),'utf8');
const write=(file,value)=>{
  const target=path.join(root,file);
  if(fs.existsSync(target)&&fs.readFileSync(target,'utf8')===value)return;
  if(process.argv.includes('--check'))throw Error('Regenera el archivo '+file);
  fs.writeFileSync(target,value);
};
write('public/audiolibros/guide-ids.js','// Generated from the public guide catalog.\nexport const GUIDE_IDS = Object.freeze('+JSON.stringify(audiobookGuides.map(g=>g.id),null,2)+');\n');
const worker='public/coach/sw.js',source=read(worker);
const pattern=/const TEC_CORE_ASSETS = (\[[\s\S]*?\]);/;
const old=JSON.parse(source.match(pattern)[1]);
const found=new Set(['/coach/vendor/leaflet/leaflet.js','/coach/vendor/leaflet/leaflet.css']);
const entries=['private/coach-app.html',...['index.html','login/index.html','planes/index.html','success/index.html','cancel/index.html','document.html'].map(n=>'public/coach/'+n)];
for(const file of entries){
  for(const match of read(file).matchAll(/(?:src|href)="(\/(?:coach|audiolibros)\/[^"]+?\.(?:js|css)(?:\?[^"]*)?)"/g)){
    if(!match[1].startsWith('/coach/vendor/pdfjs/'))found.add(match[1]);
  }
}
const pushWorker=source.match(/importScripts\("(\/coach\/team-chat-push-worker[^"]+)"\)/);
if(pushWorker)found.add(pushWorker[1]);
for(const url of found){
  const file='public'+url.split('?')[0];
  if(!fs.existsSync(path.join(root,file)))throw Error('Falta '+file);
  if(!file.endsWith('.js')||file.includes('/vendor/'))continue;
  for(const m of read(file).matchAll(/(?:from\s*|import\s*\(\s*|import\s*)["'](\.\.?\/[^"']+\.js(?:\?[^"']*)?)["']/g)){
    const next=new URL(m[1],'https://tec.invalid'+url);found.add(next.pathname+next.search);
  }
}
const rest=old.filter(u=>!/[.](js|css)(?:\?|$)/.test(u));
write(worker,source.replace(pattern,()=> 'const TEC_CORE_ASSETS = '+JSON.stringify([...found].sort().concat(rest),null,2)+';'));
