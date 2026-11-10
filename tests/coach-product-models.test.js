import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync,existsSync} from 'node:fs';
import crypto from 'node:crypto';
import vm from 'node:vm';
import express from 'express';
import {coachProductModels,registerCoachProductModels} from '../src/coach-product-models.js';
import {retailProducts} from '../src/knowledge/product-catalog.js';

const context={};
vm.runInNewContext(readFileSync(new URL('../public/coach/product-models-catalog.js',import.meta.url),'utf8'),context);
const catalog=context.TecProductModelCatalog;
test('18 exact product mappings preserve collections, sizes and unavailable products',()=>{
  assert.equal(catalog.models.length,18);assert.equal(new Set(catalog.models.map(m=>m.id)).size,18);
  assert.deepEqual(Array.from(catalog.models,m=>m.id),coachProductModels.map(m=>m.id));
  for(const model of coachProductModels){
    const client=catalog.byId(model.id);assert.equal(client.title,model.title);
    for(const code of model.codes){
      const rows=retailProducts.filter(p=>p.codigo_producto===code);assert.ok(rows.length,code);
      for(const product of rows)assert.equal(catalog.forProduct(product)?.id,model.id,code);
    }
  }
  for(const product of [null,{}, {nombre_producto:'licuadora'}, {codigo_producto:'ES4200',nombre_producto:'POWER BLENDER GO'},
    {codigo_producto:'CO9269',nombre_producto:'OLLA DE 30 CUARTOS Y PARRILLA INNOVE'},
    {codigo_producto:'CO9315',nombre_producto:'PAELLERA DE 14 PULGADAS INNOVE'},
    {codigo_producto:'CO9560',nombre_producto:'OLLA DE 6 CUARTOS'},
    {codigo_producto:'PR2124',nombre_producto:'JUEGO DE 4 CACEROLAS'}])assert.equal(catalog.forProduct(product),null);
  assert.equal(catalog.forProduct({codigo_producto:'c09560',nombre_producto:'Olla de 3 cuartos'})?.id,'olla-3-cuartos');
});

test('every linked GLB is complete, local and self contained with a matching poster',()=>{
  for(const model of coachProductModels){
    const data=readFileSync(new URL('../'+model.file,import.meta.url));
    assert.equal(data.toString('ascii',0,4),'glTF',model.id);assert.equal(data.readUInt32LE(4),2);assert.equal(data.readUInt32LE(8),data.length);
    assert.equal(crypto.createHash('sha256').update(data).digest('hex'),model.sha256);
    const gltf=JSON.parse(data.subarray(20,20+data.readUInt32LE(12)).toString());
    assert.ok(gltf.scenes?.length&&gltf.meshes?.length,model.id);
    assert.ok(!gltf.cameras?.length,model.id+' contains a studio camera');
    for(const resource of [...gltf.buffers,...(gltf.images||[])])assert.ok(!resource.uri||resource.uri.startsWith('data:'),model.id+' external resource');
    assert.ok(existsSync(new URL('../'+model.poster,import.meta.url)));
    assert.ok(data.length<11000000,model.id+' too heavy for this release');
  }
});

test('models require an active Coach session, stream ranges and reject unlisted files',async t=>{
  const app=express();
  registerCoachProductModels(app,{requireActive:async(req,res)=>{
    if(req.get('X-Test-Session')!=='active'){res.status(req.get('X-Test-Session')==='inactive'?403:401).json({error:'Acceso requerido'});return null;}
    return {user:{_id:'synthetic-test'}};
  }});
  const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});
  t.after(()=>{server.closeAllConnections();server.close();});
  const root='http://127.0.0.1:'+server.address().port+'/api/coach/product-models/';
  const get=(p,session='active',options={})=>fetch(root+p,{...options,headers:{'X-Test-Session':session,...options.headers}});
  assert.equal((await get('olla-3-cuartos/model.glb','')).status,401);
  assert.equal((await get('olla-3-cuartos/model.glb','inactive')).status,403);
  const part=await get('olla-3-cuartos/model.glb','active',{headers:{Range:'bytes=0-19'}});
  assert.equal(part.status,206);assert.equal((await part.arrayBuffer()).byteLength,20);
  assert.match(part.headers.get('content-type'),/model\/gltf-binary/);assert.match(part.headers.get('cache-control'),/no-store/);assert.equal(part.headers.get('vary'),'Cookie');
  for(const model of coachProductModels){
    const head=await get(model.id+'/model.glb','active',{method:'HEAD'});assert.equal(head.status,200);
    assert.equal(Number(head.headers.get('content-length')),readFileSync(new URL('../'+model.file,import.meta.url)).length);
    assert.equal((await get(model.id+'/poster','active',{method:'HEAD'})).status,200);
  }
  for(const path of ['unknown/model.glb','olla-3-cuartos/server.js','olla-3-cuartos/%2e%2e%2fserver.js'])assert.equal((await get(path)).status,404);
});
