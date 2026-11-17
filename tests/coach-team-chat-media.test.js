import test from 'node:test';
import assert from 'node:assert/strict';
import sharp from 'sharp';
import ffmpeg from 'ffmpeg-static';
import { spawnSync } from 'node:child_process';
import { decodeChatMedia, prepareChatMedia, chatMediaRange, CHAT_MEDIA_MAX_BYTES } from '../src/coach-team-chat-media.js';
import { pcmToWav } from '../src/coach-voice.js';
const raw = (data,mime) => decodeChatMedia({ base64: data.toString('base64'), mime });

test('photos are decoded, resized, re-encoded and stripped of location/EXIF metadata', async () => {
  const input = await sharp({ create: { width: 2400, height: 1800, channels: 3, background: '#25aa84' } }).jpeg().withExif({ IFD0: { Copyright: 'private metadata' } }).toBuffer();
  const result = await prepareChatMedia(raw(input,'image/jpeg'));
  const info = await sharp(result.data).metadata();
  assert.equal(result.meta.kind,'image'); assert.equal(result.meta.mime,'image/webp');
  assert.equal(info.width,1600); assert.equal(info.height,1200); assert.equal(info.exif,undefined);
  assert.equal(result.meta.bytes,result.data.length);
});

test('GIF animation survives validation/re-encoding', async () => {
  // Two complete 1x1 frames, different palette entries and delays.
  const gif = Buffer.from('47494638396101000100800000000000ffffff21ff0b4e45545343415045322e30030100000021f904000a0000002c000000000100010000020244010021f904000a0000002c00000000010001000002024c01003b','hex');
  assert.equal((await sharp(gif,{animated:true}).metadata()).pages,2);
  const result = await prepareChatMedia(raw(gif,'image/gif'));
  assert.equal(result.meta.kind,'gif'); assert.equal((await sharp(result.data,{animated:true}).metadata()).pages,2);
});

test('audio is really decoded and converted to universally playable MP3 with bounded duration', async () => {
  const pcm = Buffer.alloc(48000);
  for (let n=0;n<24000;n++) pcm.writeInt16LE(Math.round(Math.sin(n*440*2*Math.PI/24000)*8000),n*2);
  const result = await prepareChatMedia(raw(pcmToWav([pcm]),'audio/wav'));
  assert.equal(result.meta.kind,'audio'); assert.equal(result.meta.mime,'audio/mpeg');
  assert.equal(result.meta.duration,1); assert.ok(result.data.length > 1000 && result.data.length < 15000);
  const roundtrip = await prepareChatMedia(raw(result.data,'audio/mpeg'));
  assert.ok(roundtrip.meta.duration >= .95 && roundtrip.meta.duration < 1.2);
});

test('invalid, active, spoofed, oversized and overlong files fail before persistence', async () => {
  for (const value of [{base64:'AA==',mime:'image/svg+xml'},{base64:'!bad',mime:'image/png'}, {base64:'A'.repeat(Math.ceil(CHAT_MEDIA_MAX_BYTES/3)*4+4),mime:'image/png'}]) assert.throws(() => decodeChatMedia(value),error => error.status === 400);
  const png = await sharp({create:{width:2,height:2,channels:3,background:'red'}}).png().toBuffer();
  await assert.rejects(prepareChatMedia(raw(png,'image/jpeg')),error => error.status === 400);
  await assert.rejects(prepareChatMedia(raw(Buffer.from('<html>not audio</html>'),'audio/webm')),error => error.status === 400);
  await assert.rejects(prepareChatMedia(raw(pcmToWav([Buffer.alloc(48000*91)]),'audio/wav')),error => error.status === 400 && /90/.test(error.message));
});

test('media byte ranges support player seeking, suffixes and reject malformed/multipart requests', () => {
  assert.equal(chatMediaRange(null,100),null);
  assert.deepEqual(chatMediaRange('bytes=10-19',100),{start:10,end:19});
  assert.deepEqual(chatMediaRange('bytes=90-',100),{start:90,end:99});
  assert.deepEqual(chatMediaRange('bytes=-10',100),{start:90,end:99});
  for (const value of ['bytes=100-','bytes=20-10','bytes=-0','bytes=1-2,4-6','garbage','bytes=-']) assert.equal(chatMediaRange(value,100),false,value);
});


test('Safari MP4/AAC and Chrome WebM/Opus recordings decode through the production pipeline',async()=>{
  const pcm=Buffer.alloc(48000);
  for(let n=0;n<24000;n++)pcm.writeInt16LE(Math.round(Math.sin(n*440*2*Math.PI/24000)*8000),n*2);
  for(const [mime,args] of [['audio/mp4',['-c:a','aac','-movflags','frag_keyframe+empty_moov','-f','mp4']],['audio/webm',['-c:a','libopus','-f','webm']]]){
    const generated=spawnSync(ffmpeg,['-hide_banner','-loglevel','error','-f','s16le','-ar','24000','-ac','1','-i','pipe:0',...args,'pipe:1'],{input:pcm,maxBuffer:1024*1024});
    assert.equal(generated.status,0,generated.stderr.toString());
    const result=await prepareChatMedia(raw(generated.stdout,mime));
    assert.equal(result.meta.mime,'audio/mpeg');assert.ok(result.meta.duration>.9&&result.meta.duration<1.3,mime);
  }
});
