import test from 'node:test';
import assert from 'node:assert/strict';
import {evaluatePilot} from '../scripts/coach-agent-eval.mjs';

const off=()=>{throw Error('No se debe llamar al modelo en vista previa');};
test('comparador usa 20 expresiones, marca la procedencia y exige optar por llamadas reales',async()=>{
  const lines=[];
  const outcome=await evaluatePilot([],{interpret:off,key:'',emit:l=>lines.push(JSON.parse(l))});
  assert.equal(outcome,0);
  assert.equal(lines[0].calls,40);
  assert.equal(lines[0].cases.filter(c=>c.source==='captura de Agustín').length,1);
  assert.equal(lines[0].cases.filter(c=>c.source.includes('sintética')).length,19);
  await assert.rejects(evaluatePilot(['--run'],{interpret:off,key:''}),/OPENAI_API_KEY/);
  for(const args of [['--limit','0'],['--model','other'],['--case','other'],['--limit','21']]) {
    await assert.rejects(evaluatePilot(args,{interpret:off,key:''}));
  }
});

test('una expresión y un modelo ejecutan un turno y reportan tiempo y estimación de tokens',async()=>{
  const output=[],calls=[];
  const mock=async(message,{model,onUsage})=>{
    calls.push({model,message});
    onUsage({inputTokens:1000,cachedInputTokens:200,cacheWriteTokens:200,longCacheWriteTokens:100,outputTokens:100,tool:'consultar_agenda'});
    return {kind:'agenda'};
  };
  const options={interpret:mock,key:'ficticia',emit:l=>output.push(JSON.parse(l))};
  assert.equal(await evaluatePilot(['--run','--case','agenda-manana','--model','gpt-6.2-sol'],options),0);
  assert.equal(calls.length,1);
  assert.equal(calls[0].model,'gpt-6.2-sol');
  assert.match(calls[0].message,/Revisa si tengo citas/);
  assert.equal(output[1].estimatedUsd,0.00297);
  assert.equal(output[1].passed,1);
  assert.equal(output[1].total,1);
  calls.length=0;
  assert.equal(await evaluatePilot(['--run','--limit','2'],options),0);
  assert.equal(calls.length,4);
});

test('una gestión combinada incompleta falla aunque el agente eligió preparar un plan',async()=>{
  const lines=[];
  const result=await evaluatePilot(['--run','--case','tres-herramientas','--model','gpt-6-luna'],{
    interpret:async()=>({kind:'plan',operations:['list'],leadKind:'referrals'}),key:'ficticia',emit:line=>lines.push(JSON.parse(line))});
  assert.equal(result,1);
  assert.equal(lines[1].passed,0);
  assert.equal(lines[1].estimatedUsd,null);
});
