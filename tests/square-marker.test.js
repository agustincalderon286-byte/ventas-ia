import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import * as THREE from 'three';
import {AR} from '../public/prototipo-agustin20-chef-ar/vendor/js-aruco2/aruco.js';
import {POS} from '../public/prototipo-agustin20-chef-ar/vendor/js-aruco2/posit1.js';
import {writeMarkerMatrix,videoProjection} from '../public/prototipo-agustin20-chef-ar/shared/marker-pose.js';
const svg=await fs.readFile(new URL('../public/prototipo-agustin20-chef-ar/assets/marcador-23.svg',import.meta.url),'utf8');
const rectangles=[...svg.matchAll(/<rect ([^>]+)\/>/g)].map(([,s])=>Object.fromEntries([...s.matchAll(/(\w+)="([^"]+)"/g)].map(([,k,v])=>[k,k==='fill'?v:Number(v)])));
const sample=(u,v)=>{const x=1+u*8,y=1+v*8;let value=255;for(const r of rectangles)if(x>=r.x&&x<r.x+r.width&&y>=r.y&&y<r.y+r.height)value=r.fill==='black'?0:255;return value;};
const detector=new AR.Detector({dictionaryName:'ARUCO_MIP_36h12',maxHammingDistance:3});
for(const [width,height,yaw,roll,distance]of [[640,480,0,0,3],[360,640,.3,.15,3.5],[640,360,-.6,-.2,4],[640,480,.8,1.5,2.8],[480,640,-.4,3.14,4.8]])test(`printed SVG is detected and correctly projected at ${width}x${height}, yaw ${yaw}, roll ${roll}`,()=>{
 const {focal:f}=videoProjection(width,height,width,height),q=new THREE.Quaternion().setFromEuler(new THREE.Euler(.15,yaw,roll)),qi=q.clone().invert(),normal=new THREE.Vector3(0,0,1).applyQuaternion(q),center=new THREE.Vector3(.12,-.08,-distance),data=new Uint8ClampedArray(width*height*4).fill(245);for(let i=3;i<data.length;i+=4)data[i]=255;
 for(let y=0;y<height;y++)for(let x=0;x<width;x++){const ray=new THREE.Vector3((x-width/2)/f,(height/2-y)/f,-1),point=ray.multiplyScalar(normal.dot(center)/normal.dot(ray)).sub(center).applyQuaternion(qi);if(Math.abs(point.x)<.5&&Math.abs(point.y)<.5){const dst=(y*width+x)*4;data[dst]=data[dst+1]=data[dst+2]=sample(point.x+.5,.5-point.y);}}
 const found=detector.detect({width,height,data}).find(m=>m.id===23);assert.ok(found);
 const pose=new POS.Posit(1,f).pose(found.corners.map(p=>({x:p.x-width/2,y:height/2-p.y}))),matrix=new THREE.Matrix4();assert.ok(writeMarkerMatrix(matrix,pose));assert.ok(Math.abs(pose.bestTranslation[2]-distance)<.18);
 for(const [i,[x,y]]of [[-.5,.5],[.5,.5],[.5,-.5],[-.5,-.5]].entries()){const p=new THREE.Vector3(x,y,0).applyMatrix4(matrix),screen={x:width/2+f*p.x/-p.z,y:height/2-f*p.y/-p.z};assert.ok(Math.hypot(screen.x-found.corners[i].x,screen.y-found.corners[i].y)<3);}
});
test('blank camera frames never create an anchor',()=>assert.equal(detector.detect({width:320,height:240,data:new Uint8ClampedArray(320*240*4).fill(255)}).length,0));
test('portrait CSS cover crop and 3D projection agree',()=>{const w=640,h=360,vw=390,vh=844,{focal,fov,aspect}=videoProjection(w,h,vw,vh),camera=new THREE.PerspectiveCamera(fov,aspect,.01,100);const p=new THREE.Vector3(.15,.2,-3),projected=p.clone().project(camera),cover=Math.max(vw/w,vh/h);const imageX=w/2+focal*p.x/-p.z,imageY=h/2-focal*p.y/-p.z;assert.ok(Math.abs((projected.x+1)*vw/2-(imageX-w/2)*cover-vw/2)<1e-8);assert.ok(Math.abs((1-projected.y)*vh/2-(imageY-h/2)*cover-vh/2)<1e-8);});
