import * as THREE from 'three';
import {canOccupy,STATIONS} from './locomotion.js?v=6';

export function traceFloorArc(source){
  source.updateWorldMatrix(true,false);
  const origin=new THREE.Vector3().setFromMatrixPosition(source.matrixWorld),direction=new THREE.Vector3(0,0,-1).transformDirection(source.matrixWorld);
  if(direction.y>-.18||origin.y<.15||origin.y>2.8)return null;
  const points=[origin.clone()];let previous=origin.clone(),end=null;
  for(let i=1;i<=60;i++){
    const t=i*.025,p=origin.clone().addScaledVector(direction,4.5*t);p.y-=3*t*t;
    if(p.y<=.018){const alpha=(previous.y-.018)/(previous.y-p.y);end=previous.clone().lerp(p,alpha);points.push(end);break;}
    points.push(p);previous=p;
  }
  if(!end)return null;
  const valid=canOccupy(end.x,end.z);
  let station='floor';
  if(valid){let closest=.4;for(const [id,spot] of Object.entries(STATIONS)){const d=Math.hypot(spot.x-end.x,spot.z-end.z);if(d<closest){closest=d;station=id;}}}
  const destination=station==='floor'?end.clone():new THREE.Vector3(STATIONS[station].x,.018,STATIONS[station].z);
  const correction=destination.clone().sub(end);
  points.forEach((p,i)=>p.addScaledVector(correction,(i/(points.length-1))**2));
  return {points,destination,valid,station};
}

export function createFloorTravel({scene,locomotion,enabled=()=>true,onTravel=()=>{},now=()=>performance.now()}){
  const positions=new Float32Array(61*3),geometry=new THREE.BufferGeometry();geometry.setAttribute('position',new THREE.BufferAttribute(positions,3));geometry.setDrawRange(0,0);
  const material=new THREE.LineBasicMaterial({color:0x75efcf,transparent:true,opacity:.95});
  const line=new THREE.Line(geometry,material);line.frustumCulled=false;line.visible=false;scene.add(line);
  const ring=new THREE.Mesh(new THREE.RingGeometry(.14,.18,40),new THREE.MeshBasicMaterial({color:0x75efcf,side:THREE.DoubleSide}));ring.rotation.x=-Math.PI/2;ring.visible=false;scene.add(ring);
  let owner=null,started=0,target=null,lastValidTime=0;
  function cancel(who){if(who&&who!==owner)return;owner=null;target=null;line.visible=false;ring.visible=false;}
  function aim(who,source){
    if(who!==owner)return false;if(!enabled()){cancel();return false;}
    target=traceFloorArc(source);if(!target){line.visible=false;ring.visible=false;return false;}
    lastValidTime=now();target.points.forEach((p,i)=>p.toArray(positions,i*3));geometry.attributes.position.needsUpdate=true;geometry.setDrawRange(0,target.points.length);
    const ready=target.valid&&now()-started>=350,color=ready?0x75efcf:target.valid?0xffce72:0xf47777;
    material.color.setHex(color);ring.material.color.setHex(color);line.visible=true;ring.visible=true;ring.position.copy(target.destination);return true;
  }
  function begin(who,source){if(owner||!enabled()||!traceFloorArc(source))return false;owner=who;started=now();aim(who,source);return true;}
  function finish(who){if(who!==owner)return false;const choice=target,can=enabled()&&choice?.valid&&now()-started>=350&&now()-lastValidTime<180;cancel();if(!can)return false;const moved=locomotion.teleportTo(choice.destination,choice.station);if(moved)onTravel();return moved;}
  return {begin,aim,finish,cancel,isActive:who=>owner===who,get active(){return owner!==null;}};
}
