import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {coachSessionCredentialsMatch} from '../src/coach-password-recovery.js';
import {inDemoClosePilot} from '../src/coach-demo-close.js';

const source = readFileSync(new URL('../server.js', import.meta.url),'utf8');
function section(start,end) {
  const from = source.indexOf(start), to = source.indexOf(end,from+start.length);
  assert.ok(from >= 0 && to > from);
  return source.slice(from,to);
}
const wiring = [
  section('function resolverCoachOwnerUserId(', 'async function getCoachTeamChatKey('),
  section('function construirCoachProgramSheetWorkspaceQuery(', 'function isOwnCaptureSession('),
  section('async function liveSellerSessionCurrent(', 'registerCoachAnnouncementRead(app,')
].join('\n');
const own = { _id:'a', name:'Vendedor de prueba',email:'agustincalderon286@gmail.com',accountType:'owner',credentialVersion:'v1' };
const same = {user:own,session:{_id:'session-a',tokenHash:'example',credentialVersion:'v1',accessMode:'direct'}};

function createHarness(){
  let readDeps, liveDeps, session={...same.session,userId:{...own}};
  const queries=[];
  const dbQuery = value=>({ select(){return this},populate(){return this},sort(){return this},limit(){return this},lean:async()=>value });
  const models={CoachPersonalGmail:{},CoachProgramSheet:{},CoachPersonalProgramSheet:{}};
  vm.runInNewContext(wiring,{
    app:{},mongoose:{models,connection:{collection:()=>({})}},...models,personalConnectionsConfig:{},liveHistory:{collection:{}},
    COACH_RETURN_SESSION_COOKIE:'return',requireCoachActivo(){},construirContextoEstaticoCoach(){},
    createLiveRecordReader:deps=>{readDeps=deps;return ()=>{};},createLiveCoach:deps=>{liveDeps=deps;return {};},
    registerLiveJorge(){},createLiveLedger(){},inDemoClosePilot,coachSessionCredentialsMatch,
    normalizarEmail:e=>e.trim().toLowerCase(),normalizarCoachAccountType:v=>v,
    coachTieneAccesoOperativo:user=>!user.disabled,
    CoachSession:{findOne(q){queries.push(q);return dbQuery(session);}},
    CoachCrmRecord:{find(q){queries.push(q);return dbQuery([{leadName:'Prospecto de prueba',nextAction:'Llamar',nextActionAt:'2027-01-01'}]);}}
  });
  return {readDeps,liveDeps,models,queries,setSession(s){session=s;}};
}

test('Jorge usa ambos almacenes de 4 en 14 y conserva la consulta CRM de la cuenta activa',async()=>{
  const {readDeps,liveDeps,models,queries}=createHarness();
  assert.equal(readDeps.gmailStore,models.CoachPersonalGmail);
  assert.equal(readDeps.programSheets,models.CoachProgramSheet);
  assert.equal(readDeps.personalProgramSheets,models.CoachPersonalProgramSheet);
  assert.deepEqual(JSON.parse(JSON.stringify(readDeps.programQuery(own))),{programType:'4_en_14',generatedByUserId:{$in:['a',null]},ownerUserId:'a'});
  assert.deepEqual(JSON.parse(JSON.stringify(readDeps.programQuery({...own,accountType:'leader',teamOwnerUserId:'other'}))),
    {programType:'4_en_14',generatedByUserId:'a',ownerUserId:'other'});
  const context=JSON.parse(await liveDeps.personalContext(own,'Mis seguimientos en el CRM'));
  assert.equal(queries[0].ownerUserId,'a');
  assert.equal(context.primerosSeguimientosCRM[0].nombre,'Prospecto de prueba');
});

test('la lectura privada vuelve a verificar sesión, credenciales, acceso y vendedor antes de entregar resultados',async()=>{
  const {readDeps,queries,setSession}=createHarness();
  assert.equal(await readDeps.sessionCurrent(same),true);
  assert.equal(queries[0].tokenHash,same.session.tokenHash);
  assert.equal(queries[0].userId,own._id);
  assert.equal(queries[0]._id,same.session._id);
  assert.ok(Number.isFinite(queries[0].expiresAt.$gt.getTime()));
  for (const session of [null,
    {...same.session,accessMode:'team_visit',userId:own},
    {...same.session,userId:{...own,credentialVersion:'v2'}},
    {...same.session,userId:{...own,disabled:true}},
    {...same.session,userId:{...own,_id:'b'}},
    {...same.session,userId:{...own,name:'Otro vendedor'}},
    {...same.session,userId:{...own,accountType:'leader',teamOwnerUserId:'new-team'}}]) {
    setSession(session);assert.equal(await readDeps.sessionCurrent(same),false);
  }
});
