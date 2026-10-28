import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=fs.readFileSync(new URL('../server.js',import.meta.url),'utf8');
function fixture(){
 const routes=new Map(),rows=new Map();
 for(const owner of ['alice','bob'])for(const slotType of ['catalogo_privado','lista_precios_privada'])rows.set(owner+slotType,{ownerUserId:owner,slotType,fileData:Buffer.from('%PDF fixture '+owner),fileName:'catalog.pdf',mimeType:'application/pdf',pinHash:`${owner}.${slotType}.1234`});
 const context={app:{post:(path,handler)=>routes.set(path,handler),delete(){}},requireCoachActivo:async(req,res)=>{if(!req.user){res.status(401).json({error:'sign in'});return null;}return {user:{_id:req.user}};},resolverCoachOwnerUserId:user=>user._id,
 normalizarCoachPrivateResourceSlot:slot=>['catalogo_privado','lista_precios_privada'].includes(slot)?slot:'',limpiarPinCoachPrivateResource:pin=>/^\d{4}$/.test(pin)?pin:'',
 extraerPdfBufferCoachPrivateResource:data=>data==='fixture-pdf'?Buffer.from('%PDF fixture'):null,normalizarNombreArchivoPdf:name=>name,
 construirHashPinCoachPrivateResource:(owner,slot,pin)=>`${owner}.${slot}.${pin}`,COACH_PRIVATE_RESOURCE_SLOTS:{catalogo_privado:{defaultFileName:'catalog.pdf'},lista_precios_privada:{defaultFileName:'price.pdf'}},COACH_PRIVATE_RESOURCE_MAX_BYTES:8*1024*1024,
 CoachPrivateResource:{findOne:async query=>rows.get(query.ownerUserId+query.slotType),findOneAndUpdate:(query,change)=>({lean:async()=>{const row={...query,...change.$set};rows.set(query.ownerUserId+query.slotType,row);return row;}})},limpiarCoachPrivateResource:row=>({hasFile:!!row}),responderCoachError:(res,status,error)=>res.status(status).json({error}),Date,console};
 const a=source.indexOf('app.post("/api/coach/private-resources/:slot/upload"'),b=source.indexOf('app.get(["/coach/resources/novel-catalog-digital-3.pdf"',a);
 vm.runInNewContext(source.slice(a,b),context);
 const request=async(suffix,{user='alice',slot='catalogo_privado',body={}}={})=>{const result={status:200,headers:{}};const res={status(code){result.status=code;return this;},json(data){result.data=data;return this;},setHeader(k,v){result.headers[k]=v;},type(){return this;},send(data){result.data=data;return this;}};await routes.get('/api/coach/private-resources/:slot/'+suffix)({user,params:{slot},body},res);return result;};
 return {request,rows};
}
test('distributor edition no longer needs a PIN but still requires a TEC account and its own resource owner',async()=>{
 const {request,rows}=fixture();
 assert.equal((await request('file',{user:''})).status,401);
 const opened=await request('file');assert.equal(opened.status,200);assert.equal(opened.data.toString(),'%PDF fixture alice');assert.equal(opened.headers['Cache-Control'],'private, no-store');
 assert.equal((await request('file',{user:'bob'})).data.toString(),'%PDF fixture bob');
 assert.equal((await request('file',{user:'unknown'})).status,404);
 assert.equal((await request('file',{slot:'lista_precios_privada'})).status,400);
 assert.equal((await request('file',{slot:'lista_precios_privada',body:{pin:'9999'}})).status,403);
 assert.equal((await request('file',{slot:'lista_precios_privada',body:{pin:'1234'}})).status,200);
 assert.equal(rows.size,4);
});
test('uploading the distributor edition omits PIN while unrelated document slots keep their prior policy',async()=>{
 const {request,rows}=fixture();
 const response=await request('upload',{body:{fileData:'fixture-pdf',fileName:'edition.pdf'}});assert.equal(response.status,200);assert.equal(rows.get('alicecatalogo_privado').pinHash,'');
 assert.equal(rows.get('bobcatalogo_privado').pinHash,'bob.catalogo_privado.1234');
 assert.equal((await request('upload',{slot:'lista_precios_privada',body:{fileData:'fixture-pdf'}})).status,400);
 assert.equal((await request('upload',{body:{fileData:'invalid'}})).status,400);
});
