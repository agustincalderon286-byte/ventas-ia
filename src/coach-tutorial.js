import {fileURLToPath} from 'node:url';
export const coachTutorialAssets=Object.freeze(['recorrido-3min.mp4','portada.jpg','inicio.jpg','orden.jpg','encuesta.jpg','calculadora.jpg','voz.jpg','mensajes.jpg','audiolibros.jpg','actividad.jpg','mapa.jpg']);
export function registerCoachTutorial(app,{requireActive,directory=fileURLToPath(new URL('../private/coach-tutorial/',import.meta.url))}={}){
  app.get('/api/coach/tutorial/media/:asset',async(req,res)=>{
    res.set({'Cache-Control':'private, no-store','Vary':'Cookie','X-Robots-Tag':'noindex, nofollow'});
    if(!await requireActive(req,res))return;
    if(!coachTutorialAssets.includes(req.params.asset))return res.status(404).json({error:'Este recurso del tutorial no está disponible.'});
    res.sendFile(req.params.asset,{root:directory,cacheControl:false},error=>{if(error && !res.headersSent)res.status(404).json({error:'No se pudo abrir el recurso. Vuelve a intentarlo.'});});
  });
}
