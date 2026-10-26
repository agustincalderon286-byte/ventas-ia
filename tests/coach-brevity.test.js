import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { applyCoachResponseStyle, COACH_RESPONSE_STYLE, COACH_PROMPT_VERSION, COACH_CONSULTATION_PROMPT, coachKnowledgeCacheKey, getCoachKnowledgeDirectReply } from '../src/knowledge/coach-knowledge.js';

test('brief response contract follows legacy context without changing model, history or token budget', () => {
  const messages = [{role:'system',content:'contexto de demo'}, {role:'user',content:'la licuadora'}];
  const base = {model:'gpt-5-mini',reasoning_effort:'low',stream:true,max_completion_tokens:1800,messages};
  const result = applyCoachResponseStyle(base);
  assert.equal(result.verbosity,'low');
  assert.equal(result.model,base.model);
  assert.equal(result.reasoning_effort,base.reasoning_effort);
  assert.equal(result.stream,true);
  assert.equal(result.max_completion_tokens,1800);
  assert.deepEqual(result.messages.slice(0,-1),messages);
  assert.equal(result.messages.at(-1).content,COACH_RESPONSE_STYLE);
  assert.equal(base.messages.length,2);
  assert.equal(base.verbosity,undefined);
});
test('clarify first, no unsolicited sales coaching, but preserve necessary detail', () => {
  for (const text of ['UNA sola pregunta','sin explicar cada una','no vuelvas a preguntarlo','No añadas objeciones','Amplía solo','Nunca omitas','No imites respuestas largas']) {
    assert.ok(COACH_CONSULTATION_PROMPT.includes(text),text);
  }
  assert.ok(getCoachKnowledgeDirectReply('Como uso RoyalOne').split(/\s+/).length < 35);
});
test('brief style invalidates cached verbose replies and is wired before streaming', () => {
  assert.equal(COACH_PROMPT_VERSION,'tec-consultation-v7-product-aliases');
  assert.notEqual(coachKnowledgeCacheKey({question:'licuadora'}),coachKnowledgeCacheKey({question:'licuadora',prompt:'tec-consultation-v4'}));
  const server = fs.readFileSync(new URL('../server.js',import.meta.url),'utf8');
  assert.match(server,/if \(modoChat === "coach"\) \{\s*Object.assign\(openAiPayload, applyCoachResponseStyle\(openAiPayload\)\)/);
  assert.doesNotMatch(server,/- enfocate en objeciones, seguimiento, demo, cierre/);
});
