import * as THREE from 'three';
import {AR} from '../vendor/js-aruco2/aruco.js';
import {writeMarkerMatrix,videoProjection} from './marker-pose.js';
import {POS} from '../vendor/js-aruco2/posit1.js';

// CPU image detector: does not use TensorFlow, GPU compute shaders or WebXR.
export class SquareTracker {
  constructor({container,onStatus=()=>{},onError=()=>{}}) {
    this.container=container;this.onStatus=onStatus;this.onError=onError;
    this.scene=new THREE.Scene();this.camera=new THREE.PerspectiveCamera(60,1,.01,100);
    this.renderer=new THREE.WebGLRenderer({antialias:true,alpha:true});
    Object.assign(this.renderer.domElement.style,{position:'absolute',inset:'0',zIndex:'1'});container.append(this.renderer.domElement);
    this.canvas=document.createElement('canvas');this.context=this.canvas.getContext('2d',{willReadFrequently:true});
    this.detector=new AR.Detector({dictionaryName:'ARUCO_MIP_36h12',maxHammingDistance:3});this.active=false;this.frames=0;this.lastFrame=-Infinity;this.lastSeen=-Infinity;this.consecutive=0;
    this.position=new THREE.Vector3();this.quaternion=new THREE.Quaternion();this.scale=new THREE.Vector3(1,1,1);this.matrix=new THREE.Matrix4();
    addEventListener('resize',()=>{if(this.active)this.resize();});
  }
  addAnchor() {const group=new THREE.Group();group.matrixAutoUpdate=false;group.visible=false;this.scene.add(group);return this.anchor={group,visible:false,onTargetFound:null,onTargetLost:null,onTargetUpdate:null};}
  async start() {
    this.stop();const generation=this.generation;
    if(!navigator.mediaDevices?.getUserMedia)throw new Error('Esta página necesita HTTPS y acceso a Cámara.');
    const stream=await navigator.mediaDevices.getUserMedia({audio:false,video:{facingMode:{ideal:'environment'},width:{ideal:1280},height:{ideal:720},frameRate:{ideal:30,max:30}}});
    if(generation!==this.generation){stream.getTracks().forEach(t=>t.stop());return;}
    const video=this.video=document.createElement('video');video.muted=true;video.autoplay=true;video.playsInline=true;video.setAttribute('playsinline','');
    Object.assign(video.style,{position:'absolute',inset:'0',width:'100%',height:'100%',objectFit:'cover',zIndex:'0'});this.container.prepend(video);video.srcObject=stream;
    try{await video.play();if(!video.videoWidth)await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(new Error('La cámara no entregó una imagen.')),10000);video.addEventListener('loadedmetadata',()=>{clearTimeout(timer);resolve();},{once:true});});}
    catch(error){this.stop();throw error;}
    if(generation!==this.generation)return;
    this.active=true;this.frames=0;this.lastSeen=-Infinity;this.lastFrame=-Infinity;this.consecutive=0;this.resize();this.onStatus({phase:'search',frames:0});
  }
  resize() {
    const v=this.video;if(!v?.videoWidth)return;
    this.sourceWidth=v.videoWidth;this.sourceHeight=v.videoHeight;
    const factor=Math.min(1,640/Math.max(v.videoWidth,v.videoHeight));this.canvas.width=Math.round(v.videoWidth*factor);this.canvas.height=Math.round(v.videoHeight*factor);
    const w=this.container.clientWidth,h=this.container.clientHeight,projection=videoProjection(this.canvas.width,this.canvas.height,w,h);
    this.focal=projection.focal;this.posit=new POS.Posit(1,this.focal);this.camera.fov=projection.fov;this.camera.aspect=projection.aspect;this.camera.near=.01;this.camera.far=100;this.camera.updateProjectionMatrix();this.renderer.setSize(w,h);
  }
  update(time=performance.now()) {
    if(!this.active||time-this.lastFrame<50||this.video.readyState<2)return;this.lastFrame=time;
    try {
      if(this.sourceWidth!==this.video.videoWidth||this.sourceHeight!==this.video.videoHeight)this.resize();
      const {width:w,height:h}=this.canvas;this.context.drawImage(this.video,0,0,w,h);
      const marker=this.detector.detect(this.context.getImageData(0,0,w,h)).find(m=>m.id===23);
      this.frames++;
      if(marker){
        const points=marker.corners.map(p=>({x:p.x-w/2,y:h/2-p.y})),pose=this.posit.pose(points);
        if(!writeMarkerMatrix(this.matrix,pose))return;
        const p=new THREE.Vector3(),q=new THREE.Quaternion(),s=new THREE.Vector3();this.matrix.decompose(p,q,s);
        if(!this.anchor.visible){this.position.copy(p);this.quaternion.copy(q);}else{this.position.lerp(p,.5);this.quaternion.slerp(q,.5);}
        this.anchor.group.matrix.compose(this.position,this.quaternion,this.scale);this.lastSeen=time;this.consecutive++;
        if(this.consecutive>=2){const found=!this.anchor.visible;this.anchor.visible=this.anchor.group.visible=true;this.anchor.onTargetUpdate?.();if(found)this.anchor.onTargetFound?.();}
      }else {this.consecutive=0;if(time-this.lastSeen>300&&this.anchor.visible){this.anchor.visible=this.anchor.group.visible=false;this.anchor.onTargetLost?.();}}
      if(this.frames%15===0)this.onStatus({phase:this.anchor.visible?'tracking':'search',frames:this.frames,width:w,height:h});
    }catch(error){this.stop();this.onError(error);}
  }
  stop() {
    this.generation=(this.generation||0)+1;this.active=false;
    if(this.video){this.video.srcObject?.getTracks().forEach(t=>t.stop());this.video.srcObject=null;this.video.remove();this.video=null;}
    if(this.anchor){const was=this.anchor.visible;this.anchor.visible=this.anchor.group.visible=false;if(was)this.anchor.onTargetLost?.();}
  }
}
