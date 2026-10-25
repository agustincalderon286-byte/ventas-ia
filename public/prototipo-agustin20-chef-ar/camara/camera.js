import * as THREE from 'three';
import {GLTFLoader} from 'three/addons/loaders/GLTFLoader.js';
import {OrbitControls} from 'three/addons/controls/OrbitControls.js';
import {RoomEnvironment} from 'three/addons/environments/RoomEnvironment.js';
import {SquareTracker} from '../shared/square-tracker.js?v=2';
import {createCaseSystem,PIECE_ORDER} from '../shared/case-system.js';
import {installPiecePointer} from '../shared/touch-pieces.js';
const $=id=>document.getElementById(id),stage=$('stage');
let engine,system,pointer,orbit,world,basis,anchor,tracking=false,preview=false,running=false,starting=false,state=null,initialized=null;
function sync(value=state) {
  if(value)state=value;if(!state)return;
  const interactive=preview||tracking;
  $('piece-picker').disabled=!interactive;$('mode').disabled=!interactive;$('size').disabled=!interactive;$('reset').disabled=!interactive;
  const p=system.pieces.get(state.selected);$('piece-picker').value=state.selected;
  $('take').disabled=!interactive||!p||p.out||state.held>0;$('put').disabled=!interactive||!p||!p.out||state.held>0;
  $('status').textContent=interactive?state.message:'Busca la tarjeta V2 en blanco y negro. Acerca la cámara hasta ver sus cuatro bordes completos.';
  document.body.dataset.out=JSON.stringify(state.pieces.filter(p=>p.out).map(p=>p.id));document.body.dataset.selected=state.selected;
  document.body.dataset.poses=JSON.stringify(system.snapshot());document.body.dataset.tracking=String(tracking);document.body.classList.toggle('searching',!preview&&!tracking);
}
async function initialize() {
  engine=new SquareTracker({container:stage,onStatus:info=>{document.body.dataset.detectorFrames=info.frames;$('mode-badge').textContent=info.phase==='tracking'?'Tarjeta detectada · AR activo':'Cámara activa · busca la tarjeta V2';},onError:error=>{console.error(error);stopVideo();showWelcome('El seguimiento se detuvo: '+error.message+' Recarga y vuelve a activar la cámara.');}});
  const {renderer,scene,camera}=engine;
  renderer.setPixelRatio(Math.min(devicePixelRatio,1.5));renderer.outputColorSpace=THREE.SRGBColorSpace;renderer.toneMapping=THREE.ACESFilmicToneMapping;renderer.toneMappingExposure=1;
  renderer.domElement.style.zIndex='1';
  const pmrem=new THREE.PMREMGenerator(renderer),room=new RoomEnvironment();scene.environment=pmrem.fromScene(room,.04).texture;room.dispose();pmrem.dispose();
  scene.add(new THREE.HemisphereLight(0xeaf5ff,0x2e2c32,.8));const light=new THREE.DirectionalLight(0xffffff,2.3);light.position.set(1,3,2);scene.add(light);
  world=new THREE.Group();world.matrixAutoUpdate=false;world.visible=false;scene.add(world);
  basis=new THREE.Group();world.add(basis);
  anchor=engine.addAnchor(0);
  anchor.onTargetUpdate=()=>{if(!preview&&anchor.visible){world.matrix.copy(anchor.group.matrix);world.updateMatrixWorld(true);}};
  anchor.onTargetFound=()=>{if(preview)return;tracking=true;world.visible=true;sync();};
  anchor.onTargetLost=()=>{if(preview)return;pointer?.cancel();tracking=false;world.visible=false;sync();};
  const gltf=await new GLTFLoader().loadAsync('../assets/maleta-royal-prestige-ar-v3.glb');basis.add(gltf.scene);
  system=createCaseSystem(THREE,gltf.scene,basis,value=>sync(value));
  for(const id of ['case',...PIECE_ORDER]){const option=document.createElement('option');option.value=id;option.textContent=system.item(id).label;$('piece-picker').append(option);}
  orbit=new OrbitControls(camera,renderer.domElement);orbit.enableDamping=true;orbit.minDistance=.4;orbit.maxDistance=6;orbit.enabled=false;
  pointer=installPiecePointer(THREE,{canvas:renderer.domElement,camera,system,world:basis,controls:orbit,canInteract:()=>preview||tracking,mode:()=>$('mode').value});
  $('piece-picker').addEventListener('change',()=>system.select($('piece-picker').value));$('take').addEventListener('click',()=>system.extract(system.selected));$('put').addEventListener('click',()=>system.store(system.selected));
  $('reset').addEventListener('click',()=>{pointer.cancel();system.reset();});$('size').addEventListener('input',()=>{pointer.cancel();basis.scale.setScalar(Number($('size').value));});
  renderer.setAnimationLoop(time=>{engine.update(time);if(preview)orbit.update();renderer.render(scene,camera);});
  system.update();document.body.dataset.loaded='true';document.body.dataset.pieceCount=system.pieces.size;
}
function markerOrientation(){if(!basis||preview)return;const flat=$('target-position').value==='table';basis.rotation.x=flat?Math.PI/2:.35;basis.position.set(0,flat?0:-.12,flat?0:.02);}
$('target-position').addEventListener('change',()=>{pointer?.cancel();markerOrientation();});
function layoutPreview(){if(!engine||!preview)return;const height=Math.max(220,innerHeight-$('controls').offsetHeight-100);engine.camera.aspect=innerWidth/height;engine.camera.updateProjectionMatrix();engine.renderer.setSize(innerWidth,height);engine.renderer.domElement.style.top='84px';}
function ready(){if(!initialized)initialized=initialize().catch(error=>{initialized=null;throw error;});return initialized;}
function stopVideo() {
  pointer?.cancel();running=false;tracking=false;
  engine?.stop();
  if(world)world.visible=false;$('stop').hidden=true;
}
function showWelcome(message='') { $('welcome').hidden=false;$('controls').hidden=true;$('error').textContent=message;$('start').disabled=false;$('preview').disabled=false; }
$('start').addEventListener('click',async()=>{
  if(starting)return;starting=true;$('start').disabled=true;$('preview').disabled=true;$('error').textContent='Preparando el modelo y la cámara…';
  try {
    await ready();stopVideo();preview=false;system.reset();orbit.enabled=false;
    markerOrientation();basis.scale.setScalar(Number($('size').value));world.visible=false;
    engine.renderer.domElement.style.top='0';engine.camera.position.set(0,0,0);engine.camera.quaternion.identity();engine.scene.background=null;
    $('welcome').hidden=true;$('controls').hidden=false;$('status').textContent='Permite la cámara y apunta a la tarjeta…';
    await engine.start();running=true;$('stop').hidden=false;$('stop').textContent='Parar cámara';$('mode-badge').textContent='Cámara activa · busca la tarjeta V2';sync();
    // Wait for detection before enabling manipulation. No untracked video-overlay fallback.
  } catch(error) {console.error(error);stopVideo();showWelcome('No se pudo iniciar la cámara: '+(error?.message||'permiso denegado')+'. Abre en Safari y permite Cámara.');}
  finally{starting=false;}
});
$('preview').addEventListener('click',async()=>{
  $('preview').disabled=true;$('error').textContent='Cargando vista 3D…';
  try {
    await ready();stopVideo();preview=true;world.matrix.identity();world.visible=true;basis.rotation.set(0,0,0);basis.position.set(0,0,0);basis.scale.setScalar(.6);$('size').value='.6';system.reset();
    const {camera,scene,renderer}=engine;scene.background=new THREE.Color(0x172631);camera.position.set(1.3,1.3,1.8);camera.fov=42;camera.near=.01;camera.far=100;camera.aspect=innerWidth/innerHeight;camera.updateProjectionMatrix();renderer.setSize(innerWidth,innerHeight);
    orbit.target.set(0,.3,.16);orbit.enabled=true;orbit.update();$('welcome').hidden=true;$('controls').hidden=false;$('stop').hidden=false;$('stop').textContent='Volver';$('mode-badge').textContent='Vista 3D de prueba · sin cámara';layoutPreview();system.update();
  }catch(error){console.error(error);showWelcome('No se pudo cargar el modelo. Recarga la página o vuelve a entrar con tu PIN.');}
});
$('stop').addEventListener('click',()=>{stopVideo();preview=false;orbit.enabled=false;$('mode-badge').textContent='Cámara + piezas individuales';showWelcome();});
addEventListener('pagehide',stopVideo);
document.addEventListener('visibilitychange',()=>{if(document.hidden&&running){stopVideo();showWelcome('La cámara se pausó al salir de la página. Toca Activar cámara para continuar.');}});
addEventListener('resize',layoutPreview);

$('options').addEventListener('click',()=>{const expanded=$('controls').classList.toggle('expanded');$('options').setAttribute('aria-expanded',String(expanded));layoutPreview();});
