// Run with TEC's configured voice provider. Only these three synthetic examples are sent.
import assert from 'node:assert/strict';
import {createCoachVoiceProvider} from '../src/coach-voice.js';

const cases = [
  {name:'objeción',text:'Si el cliente quiere pensarlo, pregúntale qué duda le gustaría resolver primero.',words:['cliente','duda']},
  {name:'precio',text:'El precio del ejemplo es de doscientos cincuenta dólares. Revisa el total con el cliente.',words:['precio','cliente'],amount:/250|doscientos cincuenta/},
  {name:'anticipo',text:'Con un anticipo de cincuenta dólares, quedan doscientos dólares por cubrir.',words:['anticipo'],amount:/200|doscientos/}
];
const voice = createCoachVoiceProvider();
for (const item of cases) {
  const chunks = [];
  const start = performance.now();
  let first;
  const wav = await voice.speak(item.text,'tec-synthetic-voice-check',{
    onChunk(chunk) { first ??= performance.now(); chunks.push(chunk); }
  });
  assert.equal(wav.toString('ascii',8,12),'WAVE');
  assert.equal(wav.readUInt32LE(24),24000);
  assert.deepEqual(Buffer.concat(chunks),wav.subarray(44));
  const duration = (wav.length-44)/48000;
  assert.ok(duration > 1 && duration < 30,'Duración inesperada');
  const transcript = (await voice.transcribe(wav,'audio/wav', {prompt:'Transcribe fielmente este breve ejemplo de ventas en español mexicano.'})).toLowerCase();
  for (const word of item.words) assert.ok(transcript.includes(word), 'Falta '+word+' en '+item.name);
  if (item.amount) assert.match(transcript,item.amount);
  console.log(item.name, 'OK', duration.toFixed(1)+' s de voz', ((first-start)/1000).toFixed(1)+' s al primer audio', transcript);
}
