import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const ctx={};vm.runInNewContext(readFileSync(new URL('../public/chef/chef-appointment-flow.js',import.meta.url),'utf8'),ctx);
const flow=ctx.TecChefAppointmentFlow;
test('the local chat wizard validates contact details and produces a review, never an automatic send',()=>{
 let state=flow.create('synthetic-request');
 state=flow.answer(state,'Contacto de prueba');assert.equal(state.step,1);
 assert.throws(()=>flow.answer(state,'mal correo'),/teléfono/);assert.equal(state.step,1);
 state=flow.answer(state,'(708) 555-0100');assert.equal(state.data.phone,'7085550100');assert.equal(state.data.email,'');
 state=flow.answer(state,'Hillside, IL');state=flow.answer(state,'Power Blender Max');state=flow.answer(state,'Viernes por la tarde');
 assert.equal(state.step,5);assert.equal(state.data.appointmentRequested,true);assert.equal(state.data.consentGiven,undefined);
 assert.equal(state.data.appointmentStatus,undefined);assert.equal(flow.summary(state).length,5);
 assert.throws(()=>flow.answer(state,'Enviar'),/confirma/);
});
test('email contact and length limits are preserved without silently changing the person',()=>{
 let state=flow.answer(flow.create('synthetic-request'),'Nombre prueba');
 state=flow.answer(state,'Persona@Example.invalid');assert.equal(state.data.email,'persona@example.invalid');assert.equal(state.data.phone,'');
 assert.throws(()=>flow.answer(state,'X'.repeat(121)),/caracteres/);
 const source=readFileSync(new URL('../public/chef/tec-chef.js',import.meta.url),'utf8');
 assert.ok(source.indexOf('if(appointment){answerAppointment(question);return;}')<source.indexOf("api('/api/chef/admin/chat'"));
 assert.doesNotMatch(source,/innerHTML|OPENAI_API_KEY/);
});
