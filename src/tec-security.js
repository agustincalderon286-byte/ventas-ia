import crypto from 'node:crypto';
const unsafe = new Set(['POST','PUT','PATCH','DELETE']);
const pendingPath = /^\/api\/coach\/(?:leads|program-4-in-14|health-surveys|recruitment-applications|demo-outcomes|property-route|property-visits(?:\/lead)?)\/?$/;
export const scopeCoachChat = (userId, clientId) => 'coach-private-' + crypto.createHash('sha256').update(String(userId)+'\n'+String(clientId)).digest('hex');
export function createCoachRequestGuard({ requireActive, publicOrigin = 'https://totalelitecoaching.com', now = Date.now }) {
  const origins = new Set(['https://totalelitecoaching.com','https://www.totalelitecoaching.com','https://cmwf-crm-api.onrender.com']);
  try { origins.add(new URL(publicOrigin).origin); } catch {}
  const rates = new Map();
  function consume(key, max, res) {
    const at=now();if(rates.size>=2000)for(const[k,v]of rates)if(v.until<=at)rates.delete(k);
    if(!rates.has(key)&&rates.size>=2000){res.status(429).json({error:'Espera unos minutos antes de registrarte.'});return false;}
    const previous=rates.get(key),value=previous?.until>at?previous:{count:0,until:at+15*60000};value.count++;rates.set(key,value);
    if(value.count>max){res.set('Retry-After',String(Math.ceil((value.until-at)/1000)));res.status(429).json({error:'Demasiados intentos de registro. Intenta nuevamente después.'});return false;}return true;
  }
  return async(req,res,next)=>{
    res.set('Cache-Control','private, no-store');res.vary('Cookie');
    if(!unsafe.has(req.method))return next();
    const pathname=(req.originalUrl||req.url||'').split('?')[0];
    const origin=req.get('Origin'),site=req.get('Sec-Fetch-Site');
    if(site==='cross-site'||(origin&&!origins.has(origin))){return res.status(403).json({error:'Abre esta acción desde TEC.'});}
    const json=req.is('application/json'),binary=req.is('audio/*')||req.is('video/webm')||req.is('application/octet-stream');
    // No-origin non-browser/native clients must use a non-simple content type or
    // header. A foreign browser cannot submit these without an allowed preflight.
    if(!origin&&!['same-origin','none'].includes(site)&&!json&&!binary&&!req.get('X-TEC-Request-ID'))return res.status(403).json({error:'Vuelve a abrir TEC antes de continuar.'});
    if(['/api/coach/login','/api/coach/signup-checkout'].includes(pathname)){
      if(!json)return res.status(415).json({error:'Inicia sesión desde el formulario de TEC.'});
      if(typeof req.body?.email!=='string'||req.body.email.length>254||!/^\S+@\S+\.\S+$/.test(req.body.email.trim())||typeof req.body?.password!=='string'||req.body.password.length>128)return res.status(400).json({error:'Revisa el correo y la contraseña (máximo 128 caracteres).'});
      if(pathname.endsWith('signup-checkout')&&(!consume('ip:'+req.ip,60,res)||!consume('email:'+req.body.email.toLowerCase().trim(),5,res)))return;
    }
    if(pendingPath.test(pathname)){
      const requested=req.get('X-TEC-Account-ID')||req.body?.accountId;
      const requestId=req.get('X-TEC-Request-ID');
      if(requestId||requested){
        const auth=await requireActive(req,res);if(!auth)return;
        if(!requested||[req.get('X-TEC-Account-ID'),req.body?.accountId].filter(Boolean).some(value=>String(value)!==String(auth.user._id||auth.user.id)))return res.status(403).json({error:'Este pendiente pertenece a otra cuenta o necesita la actualización de seguridad. Tu copia local se conserva; cierra y vuelve a abrir TEC cuando termines tu trabajo.',code:'sync_account_mismatch'});
        // Keep the same authenticated principal through receipt + actual write.
        req.tecRequestAuth=auth;
      }
    }
    return next();
  };
}
export function securityHeaders(req,res,next){
  if(req.secure||req.get('X-Forwarded-Proto')==='https')res.set('Strict-Transport-Security','max-age=31536000');
  res.set('Content-Security-Policy',"object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'self'");
  // Report-only while verifying legacy tools, PDF workers, maps, AR and native UI.
  res.set('Content-Security-Policy-Report-Only',"default-src 'self'; script-src 'self' 'unsafe-inline' https://www.googletagmanager.com https://www.google-analytics.com https://maps.googleapis.com https://www.youtube.com; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; img-src 'self' data: blob: https:; font-src 'self' https://fonts.gstatic.com data:; connect-src 'self' https:; media-src 'self' blob: https:; frame-src https:; worker-src 'self' blob:; object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'self'");
  next();
}
export function securityErrorHandler(error,req,res,next){
  if(res.headersSent)return next(error);
  const status=Number(error.status||error.statusCode);
  // Do not echo input, stack, request headers or third-party service messages.
  if([400,413,415].includes(status))return res.status(status).json({error:status===413?'El archivo o la solicitud es demasiado grande.':'La solicitud no es válida. Revisa los datos e inténtalo de nuevo.'});
  console.error('TEC request failed',{name:error.name||'Error',code:error.code||''});
  res.status(503).json({error:'No pude completar la solicitud en este momento. Conserva tu copia e inténtalo de nuevo.'});
}
