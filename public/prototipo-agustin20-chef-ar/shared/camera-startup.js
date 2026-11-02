// Permission dialogs can temporarily hide the document on mobile browsers.
export function shouldPauseCamera({hidden,starting,active}) {
  return Boolean(hidden && active && !starting);
}

export async function requestCameraStream(mediaDevices) {
  if(!mediaDevices?.getUserMedia)throw new Error('Abre el enlace con HTTPS en Safari o Chrome para usar la cámara.');
  try {
    return await mediaDevices.getUserMedia({audio:false,video:{facingMode:{ideal:'environment'},width:{ideal:1280},height:{ideal:720}}});
  } catch(error) {
    if(error.name!=='OverconstrainedError')throw error;
    return mediaDevices.getUserMedia({audio:false,video:{facingMode:{ideal:'environment'}}});
  }
}

export async function playCameraVideo(video,{timeoutMs=12000}={}) {
  let timer,poll,settled=false;
  const events=['loadedmetadata','loadeddata','canplay','playing'];
  let check,onError;
  const cleanup=()=>{clearInterval(poll);for(const event of events)video.removeEventListener(event,check);video.removeEventListener('error',onError);};
  const frames=new Promise((resolve,reject)=>{
    check=()=>{if(!settled&&video.videoWidth>0&&video.videoHeight>0&&video.readyState>=2){settled=true;cleanup();resolve();}};
    onError=()=>{if(!settled){settled=true;cleanup();reject(new Error('La cámara no pudo reproducir el video. Cierra otras aplicaciones que estén usándola y vuelve a intentar.'));}};
    for(const event of events)video.addEventListener(event,check);
    video.addEventListener('error',onError);
    poll=setInterval(check,100);check();
  });
  const deadline=new Promise((resolve,reject)=>{timer=setTimeout(()=>reject(new Error('El permiso se recibió, pero la cámara no entregó imagen. Abre este enlace directamente en Safari y vuelve a intentarlo.')),timeoutMs);});
  try {await Promise.race([Promise.all([Promise.resolve().then(()=>video.play()),frames]),deadline]);}
  finally {settled=true;clearTimeout(timer);cleanup();}
}
