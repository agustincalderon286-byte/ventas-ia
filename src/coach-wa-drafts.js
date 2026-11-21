import {createHash, randomBytes} from 'node:crypto';
import mongoose from 'mongoose';
import {openToken, sealToken} from './coach-hycite-gmail.js';

const ttl = 30 * 60_000;
const schema = new mongoose.Schema({
  _id:String, code:{type:String,required:true,unique:true}, owner:{type:String,required:true,index:true},
  sessionKey:String, state:String, content:String, expiresAt:{type:Date,expires:0}
}, {versionKey:false,writeConcern:{w:'majority'}});
const Model = mongoose.models.CoachPersonalWaDraft || mongoose.model('CoachPersonalWaDraft',schema);
const code = () => randomBytes(6).toString('hex').toUpperCase();
const unavailable = {status:'unavailable'};

export function validWaBody(body) {
  return typeof body==='string' && Boolean(body.trim()) && body.length<=4096 &&
    !/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(body);
}
export function createWaDrafts({key,store=Model,now=Date.now}) {
  const scope = ({owner,session}) => ({owner:String(owner),sessionKey:createHash('sha256').update(session).digest('hex')});
  const aad = d => JSON.stringify(['personal-whatsapp',d._id,d.owner,d.sessionKey]);
  const seal = (d,message) => sealToken(JSON.stringify(message),key,aad(d));
  const open = d => JSON.parse(openToken(d.content,key,aad(d)));
  const find = input => typeof input.id==='string' && /^[a-f0-9]{12}$/i.test(input.id)
    ? store.findOne({...scope(input),code:input.id.toUpperCase(),expiresAt:{$gt:new Date(now())}}).lean() : null;
  const awaiting = d => ({_id:d._id,code:d.code,state:'pending',expiresAt:{$gt:new Date(now())}});
  const result = d => !d ? unavailable : d.state==='cancelled' ? {status:'cancelled'} : {status:d.state,id:d.code,...open(d)};
  return {
    async prepare(input) {
      if (!/^\+[1-9]\d{6,14}$/.test(input.to) || !validWaBody(input.body)) throw Error('Número o mensaje no válido.');
      const d={...scope(input),_id:randomBytes(16).toString('hex'),code:code(),state:'pending',expiresAt:new Date(now()+ttl)};
      const fields={to:input.to,name:String(input.name||'').slice(0,150),body:input.body};
      if (typeof input.senderPhone==='string' && /^\+[1-9]\d{6,14}$/.test(input.senderPhone)) fields.senderPhone=input.senderPhone;
      await store.create({...d,content:seal(d,fields)});
      return {status:'pending',id:d.code,...fields};
    },
    async view(input) {return result(await find(input));},
    async revise(input) {
      if (!validWaBody(input.body)) throw Error('Mensaje no válido.');
      const d=await find(input);
      if (d?.state!=='pending') return result(d);
      const message={...open(d),body:input.body};
      const next=code();
      const changed=await store.updateOne(awaiting(d),{$set:{code:next,content:seal(d,message),expiresAt:new Date(now()+ttl)}});
      return changed.matchedCount ? {status:'pending',id:next,...message} : result(await find(input));
    },
    async cancel(input) {
      const d=await find(input);
      if (d?.state!=='pending') return result(d);
      const changed=await store.updateOne(awaiting(d),{$set:{state:'cancelled'},$unset:{content:1}});
      return changed.matchedCount ? {status:'cancelled'} : result(await find(input));
    },
    async confirm(input) {
      const d=await find(input);
      if (d?.state!=='pending') return result(d);
      // Correction and confirmation contend for the displayed revision. An old
      // button cannot produce a link containing a different revision or number.
      const changed=await store.updateOne(awaiting(d),{$set:{state:'ready'}});
      return changed.matchedCount ? {status:'ready',id:d.code,...open(d)} : result(await find(input));
    }
  };
}
