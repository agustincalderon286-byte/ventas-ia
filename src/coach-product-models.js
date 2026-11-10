import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';

export const coachProductModels=JSON.parse(readFileSync(new URL('./data/coach-product-models.json',import.meta.url),'utf8'));
export function registerCoachProductModels(app,{requireActive,models=coachProductModels}={}) {
  const byId=new Map(models.map(model=>[model.id,model]));
  app.get('/api/coach/product-models/:id/:asset',async(req,res)=>{
    res.set({'Cache-Control':'private, no-store','Vary':'Cookie','X-Robots-Tag':'noindex, nofollow'});
    if(!await requireActive(req,res))return;
    const model=byId.get(req.params.id);
    const asset=model&&(req.params.asset==='model.glb'?model.file:req.params.asset==='poster'?model.poster:null);
    if(!asset)return res.status(404).json({error:'Este modelo 3D no está disponible.'});
    if(req.params.asset==='model.glb')res.type('model/gltf-binary');
    res.sendFile(fileURLToPath(new URL('../'+asset,import.meta.url)),{cacheControl:false},error=>{
      if(error&&!res.headersSent)res.status(404).json({error:'No se pudo abrir este modelo. Vuelve a intentarlo.'});
    });
  });
}
