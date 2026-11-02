import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {hologramFrame} from '../public/prototipo-agustin20-chef-ar/holograma/hologram-motion.js';

test('hologram rises smoothly, repeats from zero and keeps brightness bounded',()=>{
 const start=hologramFrame(0),half=hologramFrame(.8),done=hologramFrame(2);
 assert.equal(start.reveal,0);assert.ok(half.reveal>0&&half.reveal<1);assert.equal(done.reveal,1);
 assert.ok(done.height>start.height);assert.ok(hologramFrame(4).rotation>done.rotation);
 assert.equal(hologramFrame(0).reveal,0);assert.equal(hologramFrame(2,10).energy,1.4);assert.equal(hologramFrame(2,-1).energy,.35);
 for(let t=0;t<15;t+=.05){const f=hologramFrame(t);assert.ok(f.scan>=0&&f.scan<1);assert.ok(f.height>=.05&&f.height<=.25);}
});

test('demo uses the existing product and a separate reference card',()=>{
 const html=fs.readFileSync(new URL('../public/prototipo-agustin20-chef-ar/holograma/index.html',import.meta.url),'utf8');
 assert.match(html,/VISTA PREVIA · SIN CÁMARA/);assert.match(html,/Activar cámara AR/);assert.match(html,/tarjeta.html/);
 const model=fs.readFileSync(new URL('../public/prototipo-agustin20-chef-ar/assets/ollas-v01/olla-3-cuartos.glb',import.meta.url));assert.equal(model.toString('ascii',0,4),'glTF');
 const qr=fs.readFileSync(new URL('../public/prototipo-agustin20-chef-ar/holograma/qr-holograma.svg',import.meta.url),'utf8');assert.match(qr,/<svg/);
});


test('holographic appearance ends at exactly five seconds and can restart',()=>{
 assert.equal(hologramFrame(0).holographic,true);
 assert.equal(hologramFrame(4.999).holographic,true);
 assert.equal(hologramFrame(5).holographic,false);
 assert.equal(hologramFrame(60).holographic,false);
 assert.equal(hologramFrame(0).holographic,true);
 assert.ok(Math.abs(hologramFrame(5.001).rotation-hologramFrame(4.999).rotation)<.001);
 assert.ok(hologramFrame(8).height>.1,'The regular product keeps floating');
});
