import {badgeShareInfo,badgeShareText,nativeBadgeShare} from './insignias-share-core.js?v=2';

export class BadgeSharingController{
  constructor({dialog,eligible,nav=navigator,fetchImpl=(...args)=>fetch(...args)}){
    Object.assign(this,{dialog,eligible,nav,fetchImpl});this.revision=0;this.file=null;this.objectURL=null;this.info=null;
    const $=s=>dialog.querySelector(s);this.$=$;
    $('[data-badge-share-close]').addEventListener('click',()=>dialog.close());
    $('[data-badge-share-format]').addEventListener('change',()=>this.prepare());
    $('[data-badge-share-native]').addEventListener('click',()=>this.share());
    $('[data-badge-share-copy]').addEventListener('click',()=>this.copy());
    for(const platform of ['instagram','tiktok','mensajes'])$('[data-badge-share-'+platform+']').addEventListener('click',()=>this.platform(platform));
    dialog.addEventListener('close',()=>{this.cleanup();this.returnFocus?.focus({preventScroll:true});});
  }
  status(text){this.$('[data-badge-share-status]').textContent=text;}
  cleanup(){this.revision++;this.pending?.abort();if(this.objectURL)URL.revokeObjectURL(this.objectURL);this.objectURL=null;this.file=null;}
  eligibilityChanged(){if(this.dialog.open&&!this.eligible(this.id))this.dialog.close();}
  open(id,returnFocus){
    if(!this.eligible(id))return;
    this.cleanup();this.id=id;this.returnFocus=returnFocus;this.$('[data-badge-share-format]').value='square';
    if(!this.dialog.open)this.dialog.showModal();return this.prepare();
  }
  async prepare(){
    this.cleanup();const revision=this.revision;this.pending=new AbortController();
    this.info=badgeShareInfo(this.id,this.$('[data-badge-share-format]').value);
    const info=this.info;this.$('[data-badge-share-title]').textContent=info.badge.name;this.$('[data-badge-share-image]').alt='Tarjeta de la insignia '+info.badge.name+' de Audiolibros TEC';
    this.$('[data-badge-share-link]').value=info.url;
    this.$('[data-badge-share-facebook]').href='https://www.facebook.com/sharer/sharer.php?u='+encodeURIComponent(info.url);
    this.$('[data-badge-share-sms]').href='sms:?body='+encodeURIComponent(badgeShareText(info));
    this.$('[data-badge-share-image]').removeAttribute('src');this.$('[data-badge-share-download]').hidden=true;
    for(const control of this.dialog.querySelectorAll('[data-needs-badge-file]'))control.disabled=true;
    this.status('Preparando tu tarjeta…');
    try{
      const path=new URL(info.image).pathname;
      const response=await this.fetchImpl(path,{credentials:'omit',cache:'force-cache',signal:this.pending.signal});
      if(!response.ok)throw new Error('card unavailable');const blob=await response.blob();
      if(revision!==this.revision)return;if(blob.type!=='image/png'||blob.size>2_000_000)throw new Error('invalid card');
      this.file=new File([blob],info.downloadName,{type:'image/png'});this.objectURL=URL.createObjectURL(blob);
      this.$('[data-badge-share-image]').src=this.objectURL;
      const download=this.$('[data-badge-share-download]');download.href=this.objectURL;download.download=info.downloadName;download.hidden=false;
      for(const control of this.dialog.querySelectorAll('[data-needs-badge-file]'))control.disabled=false;
      this.status('Tarjeta lista. Elige dónde compartirla; tú confirmas la publicación en tu aplicación.');
    }catch(error){if(revision===this.revision&&error.name!=='AbortError')this.status('No pudimos cargar la imagen. Puedes compartir o copiar el enlace y volver a abrir esta tarjeta cuando tengas conexión.');}
  }
  async copy(){
    try{await this.nav.clipboard.writeText(this.info.url);this.status('Enlace copiado.');return true;}
    catch{this.$('[data-badge-share-link]').focus();this.$('[data-badge-share-link]').select();this.status('Selecciona y copia el enlace que aparece aquí.');return false;}
  }
  async share(){
    if(!this.eligible(this.id))return this.dialog.close();
    const result=await nativeBadgeShare(this.nav,this.info,this.file);
    if(result==='cancelled')return this.status('Compartir cancelado. Tu tarjeta sigue disponible.');
    if(result==='shared-file')return this.status('Tarjeta enviada al menú de compartir. La publicación se confirma en la aplicación que elijas.');
    if(result==='shared-link')return this.status('Enlace enviado al menú de compartir. Para añadir la imagen, guárdala y adjúntala desde tu aplicación.');
    this.status('Este navegador no permite compartir archivos directamente. Usa Guardar imagen y Copiar enlace.');
  }
  async platform(platform){
    if(!this.file||!this.eligible(this.id))return;
    let canShare=false;try{canShare=Boolean(this.nav.share&&this.nav.canShare?.({files:[this.file]}));}catch{}
    if(canShare)return this.share();
    this.$('[data-badge-share-download]').click();
    const copied=await this.copy();
    const instruction=platform==='instagram'?'Súbela a Instagram. En una historia puedes agregar el enlace con la etiqueta Enlace.':platform==='tiktok'?'En TikTok toca + → Cargar → Fotos y elige la tarjeta. El QR lleva a TEC; el texto de una foto no es un enlace pulsable.':'Adjunta la tarjeta guardada en Mensajes. Puedes abrir también «Texto con enlace».';
    this.status('Descarga preparada'+(copied?' y enlace copiado. ':'. ')+instruction);
  }
}
