import {emptyState,mergeStates,recordRange} from './logros-core.js?v=1';

export class AchievementStore {
  constructor({storage,request,onChange=()=>{},onStatus=()=>{}}){Object.assign(this,{storage,request,onChange,onStatus});this.state=emptyState();this.identity=null;this.epoch=0;this.dirty=false;this.needsPull=false;this.version=0;this.pending=null;}
  read(key){try{return mergeStates(JSON.parse(this.storage.getItem(key)||'null'));}catch{return emptyState();}}
  persist(){
    if(!this.key)return;
    this.state=mergeStates(this.read(this.key),this.state);
    try{this.storage.setItem(this.key,JSON.stringify(this.state));this.durable=true;}catch{this.durable=false;this.onStatus('El navegador no permitió guardar en este dispositivo. Mantén abierta la página hasta sincronizar.');}
  }
  async setIdentity(identity){
    if(this.identity===identity)return;
    this.persist();this.identity=identity;this.needsPull=Boolean(identity);const epoch=++this.epoch;
    this.key='tec.audiobooks.logros.v1.'+(identity||'invitado');this.state=this.read(this.key);this.dirty=Object.keys(this.state.books).length>0;this.version++;
    this.onChange(this.state);this.onStatus(identity?'Conectando tus logros…':'Tus logros se guardan en este dispositivo. Ingresa para guardar los de tu cuenta.');
    if(!identity)return;
    try{
      const data=await this.request('GET');if(epoch!==this.epoch)return;
      this.needsPull=false;const cloud=mergeStates(data.state);this.state=mergeStates(cloud,this.state);this.dirty=JSON.stringify(cloud)!==JSON.stringify(this.state);
      this.persist();this.onChange(this.state);
      if(this.dirty)await this.sync();else this.onStatus('Tus logros están guardados en tu cuenta.');
    }catch{if(epoch===this.epoch)this.onStatus(this.durable?'Tus logros quedan en este dispositivo y se sincronizarán al recuperar la conexión y tu sesión.':'No se pudieron cargar los logros de tu cuenta. Mantén la página abierta para reintentar.');}
  }
  record(context,a,b,reading){recordRange(this.state,context,a,b,reading);this.dirty=true;this.version++;this.onChange(this.state);}
  absorbStorage(){if(this.key){this.state=mergeStates(this.state,this.read(this.key));this.dirty=true;this.version++;this.onChange(this.state);}}
  async sync(){
    this.persist();if(!this.identity)return;
    if(this.needsPull&&!this.dirty){
      const epoch=this.epoch;
      try{const data=await this.request('GET');if(epoch!==this.epoch)return;this.state=mergeStates(this.state,data.state);this.needsPull=false;this.persist();this.onChange(this.state);this.onStatus('Tus logros están guardados en tu cuenta.');}
      catch{if(epoch===this.epoch)this.onStatus('No se pudieron cargar los logros de tu cuenta. Reintentaremos al recuperar la conexión.');}
      return;
    }
    if(!this.dirty)return;if(this.pending)return this.pending;
    const epoch=this.epoch,version=this.version,body={state:mergeStates(this.state)};
    const task=Promise.resolve().then(async()=>{
      try{
        if(epoch!==this.epoch)return;
        const result=await this.request('PUT',body);if(epoch!==this.epoch)return;
        this.needsPull=false;this.state=mergeStates(this.state,result.state);this.dirty=this.version!==version;this.persist();this.onChange(this.state);
        this.onStatus(this.durable?'Tus logros están guardados en tu cuenta.':'Tus logros están guardados en tu cuenta; el almacenamiento local no está disponible.');
      }catch{if(epoch===this.epoch)this.onStatus(this.durable?'Guardado en este dispositivo. Se sincronizará al recuperar la conexión y tu sesión.':'No se pudo guardar en el dispositivo ni sincronizar. Mantén abierta la página para reintentar.');}
      finally{if(this.pending===task)this.pending=null;}
    });this.pending=task;return task;
  }
}
