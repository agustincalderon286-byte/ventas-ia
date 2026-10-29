// Local-only preview with fictitious properties. Never loads TEC credentials or DB.
import http from 'node:http';
import path from 'node:path';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {createStreetRouter} from '../src/coach-street-route.js';
const root=path.resolve(fileURLToPath(new URL('../',import.meta.url))),router=createStreetRouter();
const types={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.png':'image/png','.svg':'image/svg+xml','.json':'application/json'};
http.createServer(async(req,res)=>{
 try{
  const url=new URL(req.url,'http://127.0.0.1');
  if(req.method==='GET'&&url.pathname==='/integration/'){
   const html=await readFile(path.join(root,'private/coach-app.html'),'utf8');
   const article=html.match(/<article[^>]*data-property-workspace[\s\S]*?<\/article>/)?.[0]?.replace('data-property-workspace hidden','data-property-workspace');
   if(!article)throw Error('Missing property workspace');
   res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});
   res.end(`<!doctype html><html lang="es"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>TEC LAB · integración aislada</title><link rel="stylesheet" href="/coach/property-map.css?v=20261028-walk2"><link rel="stylesheet" href="/coach/property-lab.css?v=20261029-lab1"><style>body{font:16px system-ui;color:#102e50;background:#eef3f9}button,a{cursor:pointer}button{min-height:44px}.nav-button,.primary-button{padding:10px;border-radius:10px;background:white;color:#102e50;border:1px solid #bdcddd}</style><h1>Integración aislada · 50 casas ficticias</h1><p>Se usa el módulo real de TEC, con un guardado de prueba independiente.</p>${article}<script type="module" src="/lab/integration.js"></script></html>`);return;
  }
  if(req.method==='POST'&&url.pathname==='/api/coach/property-navigation'){
   let body='';for await(const chunk of req){body+=chunk;if(body.length>2000)throw Error('Request too large');}
   try{const value=await router.route(JSON.parse(body));res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify(value));}catch(error){res.writeHead(error.status||500,{'Content-Type':'application/json'});res.end(JSON.stringify({error:error.message}));}return;
  }
  const relative=url.pathname==='/'?'tests/fixtures/map-lab.html':url.pathname==='/lab/fixture.js'?'tests/fixtures/map-lab.js':url.pathname==='/lab/integration.js'?'tests/fixtures/map-lab-integration.js':url.pathname.startsWith('/coach/')?'public'+url.pathname:null;
  if(!relative||req.method!=='GET'){res.writeHead(404);res.end();return;}
  const file=path.resolve(root,relative);if(!file.startsWith(root+path.sep))throw Error('Invalid path');
  const bytes=await readFile(file);res.writeHead(200,{'Content-Type':types[path.extname(file)]||'application/octet-stream','Cache-Control':'no-store'});res.end(bytes);
 }catch{res.writeHead(404);res.end('Not found');}
}).listen(Number(process.env.MAP_LAB_PORT)||4187,'127.0.0.1',()=>console.log('TEC Mapa LAB: http://127.0.0.1:4187 (50 casas ficticias; sin cuentas ni datos reales)'));
