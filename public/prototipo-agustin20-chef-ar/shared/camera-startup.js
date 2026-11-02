// Permission dialogs can temporarily hide the document on mobile browsers.
export function shouldPauseCamera({hidden,starting,active}) {
 return Boolean(hidden && active && !starting);
}
export async function requestCameraStream(mediaDevices) {
 if(!mediaDevices?.getUserMedia)throw new Error('Abre el enlace con HTTPS en Safari o Chrome para usar la cámara.');
 try{return await mediaDevices.getUserMedia({audio:false,video:{facingMode:{ideal:'environment'},width:{ideal:1280},height:{ideal:720}}});}
 catch(error){
  if(!['OverconstrainedError','AbortError'].includes(error.name)){error.cameraStage='capture';throw error;}
  try{return await mediaDevices.getUserMedia({audio:false,video:{facingMode:{ideal:'environment'}}});}
  catch(retryError){retryError.cameraStage='capture';throw retryError;}
 }
}
export async function playCameraVideo(video,{timeoutMs=12000}={}) {
 let timer,poll,settled=false,check,onError;
 const events=['loadedmetadata','loadeddata','canplay','playing'];
 const cleanup=()=>{clearInterval(poll);for(const event of events)video.removeEventListener(event,check);video.removeEventListener('error',onError);};
 let metadataResolve;
 const metadata=new Promise(resolve=>{metadataResolve=resolve;});
 const frames=new Promise((resolve,reject)=>{
  check=()=>{
   if(video.readyState>=1&&video.videoWidth>0)metadataResolve();
   if(!settled&&video.videoWidth>0&&video.videoHeight>0&&video.readyState>=2){settled=true;cleanup();resolve();}
  };
  onError=()=>{if(!settled){settled=true;cleanup();reject(new Error('La cámara no pudo entregar video. Cierra otras aplicaciones que estén usándola.'));}};
  for(const event of events)video.addEventListener(event,check);video.addEventListener('error',onError);poll=setInterval(check,100);check();
 });
 const deadline=new Promise((resolve,reject)=>{timer=setTimeout(()=>reject(new Error('La cámara no entregó imagen. Vuelve a iniciar la cámara desde Safari.')),timeoutMs);});
 async function startPlayback(){
  // Do not race autoplay with an explicit play call while Safari attaches srcObject.
  if(video.readyState<1||!video.videoWidth)await metadata;
  for(let attempt=0;attempt<2;attempt++){
   try{return await video.play();}
   catch(error){
    if(error.name!=='AbortError')throw error;
    if(video.srcObject===null)throw error;
    if(attempt===0){await new Promise(resolve=>setTimeout(resolve,150));continue;}
    const recoverable=new Error('El permiso ya está concedido. Toca Continuar cámara para iniciar el video.');recoverable.name='CameraPlaybackError';recoverable.cameraStage='video';throw recoverable;
   }
  }
 }
 try{await Promise.race([Promise.all([startPlayback(),frames]),deadline]);}
 finally{settled=true;clearTimeout(timer);cleanup();}
}
