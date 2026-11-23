// Voz bilingüe a prueba: solo la oyen las cuentas indicadas y, si Azure no la acepta, se lee con Jorge.
// También se comprueba que un archivo de la app no se publique con la versión anterior.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {createCoachVoiceProvider, jorgeSpeechDocument, trialVoice, JORGE_VOICE} from '../src/coach-voice.js';

const TRIAL='es-MX-JorgeMultilingualNeural';

test('la voz de prueba es solo para las cuentas listadas y con un nombre válido',()=>{
  const env={COACH_VOICE_TRIAL_EMAILS:' A@example.invalid , b@example.invalid'};
  assert.equal(trialVoice(env,'a@example.invalid'),TRIAL);
  assert.equal(trialVoice(env,'c@example.invalid'),'');
  assert.equal(trialVoice({},'a@example.invalid'),'');
  assert.equal(trialVoice({...env,COACH_VOICE_TRIAL_NAME:'"><voice name="x'},'a@example.invalid'),'');
  assert.equal(trialVoice({...env,COACH_VOICE_TRIAL_NAME:'es-MX-DaliaMultilingualNeural'},'b@example.invalid'),'es-MX-DaliaMultilingualNeural');
});

test('la voz bilingüe dice en inglés calles y ciudades; Jorge conserva su estilo y sus alias',()=>{
  const text='Vive en 2301 W 127th St, Blue Island, IL. Le interesa la Power Blender.';
  const trial=jorgeSpeechDocument(text,TRIAL);
  assert.match(trial,/<voice name="es-MX-JorgeMultilingualNeural">/);
  assert.match(trial,/<lang xml:lang="en-US">Blue Island<\/lang>/);
  assert.match(trial,/<lang xml:lang="en-US">Street<\/lang>/);
  assert.doesNotMatch(trial,/express-as/);
  const jorge=jorgeSpeechDocument(text);
  assert.match(jorge,/<voice name="es-MX-JorgeNeural"><mstts:express-as style="chat">/);
  assert.match(jorge,/<sub alias="Blú Áiland">Blue Island<\/sub>/);
  assert.doesNotMatch(jorge,/xml:lang="en-US"/);
});

test('si Azure rechaza la voz de prueba, la respuesta se lee con Jorge',async()=>{
  const voices=[];
  const fetcher=async(url,init)=>{
    const voice=/<voice name="([^"]+)"/.exec(init.body)[1];
    voices.push(voice);
    if (voice!==JORGE_VOICE) return {ok:false,status:400,body:{cancel:async()=>{}}};
    return {ok:true,status:200,body:(async function*(){yield Buffer.alloc(4);})()};
  };
  const provider=createCoachVoiceProvider({env:{AZURE_SPEECH_KEY:'k',AZURE_SPEECH_REGION:'eastus'},fetcher,jorgeOnly:true});
  const wave=await provider.speak('Hola',owner(),{voice:TRIAL});
  assert.deepEqual(voices,[TRIAL,JORGE_VOICE]);
  assert.equal(wave.length,48);
  voices.length=0;
  await provider.speak('Hola',owner());
  assert.deepEqual(voices,[JORGE_VOICE]);
});
function owner() { return '000000000000000000000411'; }

test('cada archivo de la app que carga el teléfono tiene su huella anotada con la versión vigente',()=>{
  const stamps=JSON.parse(fs.readFileSync(new URL('../scripts/coach-asset-stamps.json',import.meta.url),'utf8'));
  const worker=fs.readFileSync(new URL('../public/coach/sw.js',import.meta.url),'utf8');
  assert.equal(stamps.cache,/const TEC_CACHE_VERSION = "([^"]+)"/.exec(worker)[1]);
  for (const name of ['/coach/app.js','/coach/coach-voice.js','/coach/styles.css','/coach/property-map.js']) {
    assert.match(stamps.files[name].sha,/^[a-f0-9]{16}$/);
    assert.ok(stamps.files[name].versions.some(Boolean));
  }
  const script=fs.readFileSync(new URL('../scripts/coach-release-assets.mjs',import.meta.url),'utf8');
  assert.match(script,/sin cambiar su versión/);
});

test('el mapa conserva abierta la casa elegida al redibujar y atiende en orden los toques seguidos',()=>{
  const map=fs.readFileSync(new URL('../public/coach/property-map.js',import.meta.url),'utf8');
  assert.match(map,/const reopen = openPin;/);
  assert.match(map,/again\.openPopup\(\);/);
  assert.match(map,/toggleQueue = toggleQueue\.then\(run, run\);/);
});
