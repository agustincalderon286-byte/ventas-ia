import test from 'node:test';
import assert from 'node:assert/strict';
import {shouldPauseCamera,requestCameraStream,playCameraVideo} from '../public/prototipo-agustin20-chef-ar/shared/camera-startup.js';
class Video extends EventTarget {
 videoWidth=0;videoHeight=0;readyState=0;
 play(){return Promise.resolve();}
 frame(){this.videoWidth=1280;this.videoHeight=720;this.readyState=2;this.dispatchEvent(new Event('loadeddata'));}
}
test('permission dialog does not cancel pending camera startup',()=>{
 assert.equal(shouldPauseCamera({hidden:true,starting:true,active:false}),false);
 assert.equal(shouldPauseCamera({hidden:true,starting:true,active:true}),false);
 assert.equal(shouldPauseCamera({hidden:true,starting:false,active:true}),true);
 assert.equal(shouldPauseCamera({hidden:false,starting:false,active:true}),false);
});
test('startup waits for an actual image even when play resolves first',async()=>{
 const video=new Video();let done=false;const started=playCameraVideo(video,{timeoutMs:200}).then(()=>done=true);
 await Promise.resolve();assert.equal(done,false);video.frame();await started;assert.equal(done,true);
});
test('frames already available before listeners still start successfully',async()=>{
 const video=new Video();video.frame();await playCameraVideo(video,{timeoutMs:100});
});
test('missing video frames time out instead of leaving the startup stuck',async()=>{
 await assert.rejects(playCameraVideo(new Video(),{timeoutMs:20}),/no entregó imagen/);
});
test('unsupported resolution falls back once, but denial does not request again',async()=>{
 let calls=0;const stream={};assert.equal(await requestCameraStream({getUserMedia:async()=>{if(++calls===1)throw Object.assign(new Error(),{name:'OverconstrainedError'});return stream;}}),stream);assert.equal(calls,2);
 calls=0;await assert.rejects(requestCameraStream({getUserMedia:async()=>{calls++;throw Object.assign(new Error(),{name:'NotAllowedError'});}}));assert.equal(calls,1);
});

test('Safari play promise cannot leave startup hanging even after metadata arrives',async()=>{
 const video=new Video();video.play=()=>new Promise(()=>{});video.frame();
 await assert.rejects(playCameraVideo(video,{timeoutMs:20}),/no entregó imagen/);
});
test('Safari metadata is ready before play and one transient AbortError is retried',async()=>{
 const video=new Video();video.srcObject={};let calls=0;video.play=()=>{calls++;if(calls===1)return Promise.reject(Object.assign(new Error('The operation was aborted.'),{name:'AbortError'}));return Promise.resolve();};
 const started=playCameraVideo(video,{timeoutMs:700});await Promise.resolve();assert.equal(calls,0);video.frame();await started;assert.equal(calls,2);
});
test('repeat playback abort preserves a recoverable error for a second user gesture',async()=>{
 const video=new Video();video.srcObject={};video.frame();video.play=()=>Promise.reject(Object.assign(new Error('The operation was aborted.'),{name:'AbortError'}));
 await assert.rejects(playCameraVideo(video,{timeoutMs:700}),error=>error.name==='CameraPlaybackError'&&/Continuar cámara/.test(error.message));
});
test('cancelled stream is not restarted by automatic playback retry',async()=>{
 const video=new Video();video.frame();video.srcObject=null;let calls=0;video.play=()=>{calls++;return Promise.reject(Object.assign(new Error('cancelled'),{name:'AbortError'}));};
 await assert.rejects(playCameraVideo(video,{timeoutMs:200}),error=>error.name==='AbortError');assert.equal(calls,1);
});
