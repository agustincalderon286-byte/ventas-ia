import { createHash } from 'node:crypto';
import { isGmailAddress } from './coach-mail-address.js';
import { structuredResult } from './coach-mail-intent.js';

const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const fold = value => String(value || '').normalize('NFD').replace(/\p{M}/gu,'').toLowerCase().trim();
const command = /^TECPERSON ([a-f0-9]{24}) (create|update|cancel) (\d{1,2})$/;
const reply = (respuesta, revision = '', actions = []) => ({mode:'coach',coachMailAgent:true,coachContactsAgent:true,
  coachContactsPending:revision,coachHelpActions:actions,respuesta});
const connect = () => reply('Conecta Contactos de Google con el correo de tu propia cuenta del piloto.',
  '', [{type:'connections',label:'Conectar mis Contactos de Google'}]);
const direct = auth => Boolean(auth?.session?.tokenHash && auth.user?._id &&
  (auth.session.accessMode === 'direct' || (!auth.session.accessMode && auth.user.accountType !== 'seat')));
const personLabel = p => [p.name,...(p.phones || []),...(p.emails || [])].filter(Boolean).join(' · ');
const stale = () => reply('Esa propuesta cambió o caducó. Pídeme de nuevo que revise el contacto.');
const contactsMention = t => /\b(?:mis contactos|(?:google|mis) contacts|contactos (?:de |en )?google|agenda (?:de contactos|telefonica)|libreta (?:de contactos|de direcciones)|como (?:un )?contacto|contacto (?:nuevo|de google))\b/.test(t);
const phonePattern = /(?<!\d)(?:\+\d[\d(). -]{6,19}\d|(?:1[ .-]?)?\(?\d{3}\)?[ .-]?\d{3}[ .-]?\d{4})(?!\d)/gu;
const mailPattern = /[\w.!#$%&'*+/=?^_`{|}~-]+@[\w.-]+\.[a-z]{2,}/gi;
function exactName(value, text) {
  return typeof value==='string' && value.length<=110 && /^[\p{L}\p{M}][\p{L}\p{M} .’'-]*$/u.test(value)
    && text.toLowerCase().includes(value.toLowerCase()) ? value.trim() : '';
}
async function extract(text, infer) {
  const phones = [...new Set(text.match(phonePattern) || [])];
  const emails = [...new Set(text.match(mailPattern) || [])];
  if (phones.length>1 || emails.length>1 || emails.some(e=>!isGmailAddress(e))) return {ambiguous:true};
  const masked=text.replace(phonePattern,'[teléfono]').replace(mailPattern,'[correo]');
  const folded=fold(masked);
  let fields;
  try {
    fields=await infer({name:'tec_contacto_google',timeout:9000,instruction:masked,
      system:'Extrae solo fragmentos literales del mensaje del vendedor. name es la persona que se va a buscar o guardar, host la persona que la refirió. No inventes ni corrijas nombres. Sin dato usa cadena vacía. sheet indica si menciona boleta, referido, su hoja o un lead. readOnly si pide únicamente buscar o consultar. No ejecutas nada.',
      properties:{name:{type:'string'},host:{type:'string'},sheet:{type:'boolean'},readOnly:{type:'boolean'}}});
  } catch { fields={}; }
  const fallback=masked.match(/(?:gu[aá]rda(?:me)?|agrega(?:me)?|agr[eé]ga(?:me)?|a[nñ]ad[eí](?:me)?|busca(?:me)?|actualiza(?:me)?|se llama|nombre(?: es)?)\s+(?:(?:en|a) mis contactos\s+)?(?:al?\s+)?(?:(?:la|el|compa|señora?)\s+)?([\p{L}][\p{L} ’'-]*?)(?=\s+(?:en|a|el tel[eé]fono|con|referid[oa]|por|de la boleta|de mis contactos)|[,.?;:]|\s*\[|$)/iu);
  const host=masked.match(/(?:refiri[oó]|referencia de|referid[oa] (?:de|por))\s+([\p{L}][\p{L} ’'-]*?)(?=\s+(?:en|a mis)|[,.?;:]|$)/iu);
  return {name:exactName(fields.name,masked) || exactName(fallback?.[1]?.trim(),masked),
    host:exactName(fields.host,masked) || exactName(host?.[1]?.trim(),masked),
    phone:phones[0]?.trim() || '',email:emails[0] || '',
    sheet:fields.sheet===true || /\b(boleta|referencia|referid[oa]|hoja|lead|rifa)\b/.test(folded),
    readOnly:fields.readOnly===true || /^(?:[¿ ]*)?(?:busca|consulta|revisa|muestrame|tengo a)/.test(folded) && !/\b(?:guarda|agrega|actualiza)/.test(folded)};
}
const snapshot = people => people.map(p=>[p.resourceName,p.etag]).sort((a,b)=>a[0].localeCompare(b[0]));
export function createCoachContactsAgent({config,contacts,plans,readSession,switched,sheets,sheetTools,infer=structuredResult}) {
  return async ({req,auth,text,sessionId}) => {
    const body=req.body || {}, t=fold(text), cmd=command.exec(text), focus=body.coachQuestionFocus==='contacts' && body.coachContactsPending;
    const starts=contactsMention(t);
    if (!starts && !cmd && !focus) return null;
    if (!starts && !cmd && focus && (/^tec(?:plan|route)/i.test(text) || /\b(?:gmail|calendar|prepara un correo|ho[rR]arios|proxima llamada|postventa|whats\s?app|a quien (?:llamo|marco))\b/.test(t))) return null;
    const email=fold(auth?.user?.email);
    if (!direct(auth) || switched(req) || String(auth.user._id)!==body.accountId || !sessionId ||
        !(config.contactsPilot ?? config.planPilot)?.some(v=>v==='*' || v===email)) return reply('Abre Contactos desde tu propia sesión del piloto.');
    const owner=auth.user._id, session=auth.session.tokenHash;
    const scope=digest(['people-chat',String(owner),session,sessionId]);
    const current=async()=>{const live=await readSession(req);return direct(live) && !switched(req) &&
      String(live.user._id)===String(owner) && live.session.tokenHash===session;};
    const sheetContext=async()=>{
      if (!config.sheetsPilot?.some(v=>v==='*' || v===email)) throw new Error('sheetsPilot');
      const {token,grant,...workbook}=await sheets.access(owner);
      return {owner,session,grant,workbook,access:token,sellerName:auth.user.name,
        isCurrent:async()=>await current() && await sheets.current(owner,grant)};
    };
    const action=(revision,op,index,label)=>({type:'contactsAction',revision,op,index,label});
    try {
      if (!await current()) return stale();
      if (!config.contactsReady) return connect();
      const profile=await contacts.profile(owner);
      if (!profile?.grant) return connect();
      const grant=profile.grant;
      const record=await plans.load(scope), old=record?.data;
      const valid=old?.grant===grant && body.coachContactsPending===record?.revision;
      if ((cmd || (!starts && focus)) && (!valid || (cmd && cmd[1]!==record.revision))) return stale();
      if (cmd) {
        const [,revision,op,num]=cmd, index=Number(num);
        if (old.stage!=='preview') return stale();
        if (op==='cancel') {
          if (!await plans.save(scope,revision,{grant,stage:'closed'})) return stale();
          return reply('Propuesta de contacto descartada.');
        }
        const expected=op==='update' ? old.matches[index-1] : null;
        if (op==='update' ? !expected : index!==0) return stale();
        if (old.source) {
          const ctx=await sheetContext();
          if (ctx.grant!==old.source.grant || !await sheetTools.revalidate(ctx,old.source.identityKey)) return stale();
        }
        const fresh=await contacts.search(owner,grant,old.person);
        if (JSON.stringify(snapshot(fresh))!==JSON.stringify(snapshot(old.matches))) return reply('La agenda cambió desde la vista previa. Vuelve a pedirme que revise las coincidencias.');
        if (!await current()) return stale();
        const claimed=await plans.save(scope,revision,{grant,stage:'writing'});
        if (!claimed) return stale();
        // An uncertain create must never be repeated automatically, even from a second chat.
        const writeKey=digest(['people-write',String(owner),grant,op,expected?.resourceName || '',expected?.etag || '',fold(old.person.name),old.person.phone.replace(/\D/g,''),fold(old.person.email)]);
        const guard=await plans.load(writeKey);
        if (guard?.data || !await plans.save(writeKey,guard?.revision,{stage:'claimed'})) return reply('Esta operación ya se confirmó o está en proceso. Revisa tus Contactos de Google antes de repetirla.');
        if (!await current()) return stale();
        let saved;
        try {
          saved=expected ? await contacts.update(owner,grant,expected,old.person) : await contacts.create(owner,grant,old.person);
        } catch (err) {
          if (err.reauthorize) return connect();
          return reply(err.status===409 || err.status===400 ? 'La ficha cambió. Vuelve a buscar el contacto y revisa los datos.' :
            'Google no confirmó el resultado. Revisa tu agenda antes de preparar otra operación.');
        }
        if (!await current()) return stale();
        if (expected && saved?.changed?.length===0) return reply('La ficha de Google ya tenía esos datos.');
        return reply((expected?'Contacto actualizado':'Contacto creado')+' en tu cuenta de Google.');
      }
      if (focus && !starts && old.stage==='preview') {
        if (/^(?:s[ií]|confirmo|guardar?|actualiza(?:lo)?|crear?|cancela(?:r)?)[.! ]*$/.test(t)) return reply('Elige el botón de la ficha que quieres guardar o actualizar.',record.revision,buttons(old,record.revision));
        if (!text.match(phonePattern) && !text.match(mailPattern) && !/\b(?:cambia|corrige|mejor|tel[eé]fono|correo|nombre)\b/i.test(text)) return null;
      }
      phonePattern.lastIndex=mailPattern.lastIndex=0;
      if (focus && !starts && old.stage==='details' && !text.match(phonePattern) && !text.match(mailPattern) &&
          (old.person?.name || !exactName(text.trim(),text) || /[¿?]|\b(?:quiero|dime|explica|ayuda|puedo|puedes|necesito|revisa)\b/.test(t))) return null;
      let input=await extract(text,infer);
      if (input.ambiguous) return reply('¿Qué único teléfono y correo corresponden a la persona que quieres revisar?');
      if (!starts && valid && old.person) input={...input,name:input.name || old.person.name || exactName(text.trim(),text),phone:input.phone || old.person.phone,email:input.email || old.person.email};
      const save=async(state,message)=>{
        if (!await current()) return stale();
        const revision=await plans.save(scope,record?.revision,{grant,...state});
        return revision?reply(message,revision,state.stage==='preview'?buttons(state,revision):[]):stale();
      };
      let person={name:input.name,phone:input.phone,email:input.email};
      let source=!starts && valid ? old.source : null;
      if (input.sheet && (input.host || input.name) && sheets && sheetTools) {
        const ctx=await sheetContext();
        const {contacts:leads,total}=await sheetTools.search(ctx,{name:input.name,host:input.host,withPhone:true});
        if (total!==1) return reply(total?'Hay varias fichas de tu hoja con esos datos. Indica el nombre completo y el anfitrión.':'No encontré esa referencia en tus filas. Dime el nombre y el teléfono o correo.');
        source={grant:ctx.grant,identityKey:leads[0].identityKey};
        person={name:leads[0].fullName,phone:person.phone || leads[0].phone || '',email:person.email || leads[0].email || ''};
      }
      if (!person.name && !person.phone && !person.email) return save({stage:'details',person,source},'¿Cómo se llama la persona que quieres buscar en tus contactos?');
      const matches=await contacts.search(owner,grant,person);
      if (!await current()) return stale();
      if (matches.length>10) return reply('Hay más de diez coincidencias. Dime el nombre completo, teléfono o correo para distinguir la ficha.');
      const lines=matches.map((p,i)=>(i+1)+'. '+personLabel(p));
      if (input.readOnly) return reply(lines.length?'Encontré estas coincidencias en tu agenda:\n'+lines.join('\n'):'No encontré coincidencias en tus contactos.');
      if (!person.name || (!person.phone && !person.email)) return save({stage:'details',person,source},
        [...lines,'Para preparar el cambio necesito '+(!person.name?'el nombre':'el teléfono o correo que quieres guardar')+'.'].join('\n'));
      const state={stage:'preview',person,matches,source};
      return save(state,['Datos para tu agenda: '+[person.name,person.phone,person.email].filter(Boolean).join(' · '),
        matches.length?'Coincidencias en Google:\n'+lines.join('\n'):'No encontré coincidencias en Google.',
        'Revisa la ficha y elige qué guardar.'].join('\n\n'));
    } catch(error) {
      if (error.code==='contactsTooMany') return reply('El nombre coincide con demasiadas fichas. Dime el teléfono o correo completo para distinguirlas.');
      if (error.status===409) return stale();
      if (error.reauthorize) return connect();
      return reply('No pude verificar los datos de la agenda o de la hoja. Revisa Mis conexiones y vuelve a intentarlo.','',[{type:'connections',label:'Mis conexiones'}]);
    }
    function buttons(state,rev) {
      return [action(rev,'create',0,'Crear contacto nuevo'),...state.matches.map((p,i)=>action(rev,'update',i+1,'Agregar los datos a '+p.name+' ('+(i+1)+')')),action(rev,'cancel',0,'Descartar')];
    }
  };
}
