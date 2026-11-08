import express from 'express';
import {fileURLToPath} from 'node:url';
import {BADGES} from '../public/audiolibros/logros-core.js';
import {badgeShareInfo} from '../public/audiolibros/insignias-share-core.js';
import {badgeSvg} from '../public/audiolibros/insignias-art.js';
const escape=value=>String(value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

export function badgeSharePage(id,origin){
  const info=badgeShareInfo(id,'preview',origin),b=info.badge;
  const target=b.bookId?'/audiolibros/?libro='+encodeURIComponent(b.bookId):'/audiolibros/#catalogo';
  return `<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>${escape(b.name)} · Audiolibros TEC</title><meta name="description" content="Un logro compartido de Audiolibros TEC: ${escape(b.name)}. Descubre clásicos en español y escucha una muestra gratis."><meta name="robots" content="noindex,follow"><meta name="theme-color" content="#102421"><link rel="canonical" href="${escape(info.url)}"><meta property="og:type" content="website"><meta property="og:site_name" content="Audiolibros TEC"><meta property="og:title" content="${escape(b.name)} · Un logro de escucha en TEC"><meta property="og:description" content="Cada escucha deja huella. Descubre tu próxima historia y escucha gratis el primer 10 % de cada libro."><meta property="og:url" content="${escape(info.url)}"><meta property="og:image" content="${escape(info.image)}"><meta property="og:image:width" content="1200"><meta property="og:image:height" content="630"><meta property="og:image:type" content="image/png"><meta property="og:image:alt" content="Insignia ${escape(b.name)} de Audiolibros TEC"><meta name="twitter:card" content="summary_large_image"><meta name="twitter:title" content="${escape(b.name)} · Audiolibros TEC"><meta name="twitter:image" content="${escape(info.image)}"><link rel="icon" href="/audiolibros/identidad/favicon-96.png"><link rel="stylesheet" href="/audiolibros/estilos.css?v=6"><link rel="stylesheet" href="/audiolibros/insignias-compartir.css?v=2"></head><body class="badge-public"><header class="wrap top"><a class="brand" href="/audiolibros/"><strong>Audiolibros TEC<small>Historias que van contigo</small></strong></a><a href="/audiolibros/">Explorar biblioteca →</a></header><main class="wrap"><article class="badge-public-hero"><div class="badge-public-art">${badgeSvg(id,true)}</div><div><p class="eyebrow">Un logro compartido</p><h1>${escape(b.name)}</h1><p class="intro">${escape(b.description)}</p><p>Cada escucha deja huella. Descubre los clásicos en español de TEC, sigue el texto mientras suenan y comienza tu propia colección de insignias.</p><div class="actions"><a class="button primary" href="${escape(target)}">Escuchar una muestra gratis</a><a class="button outline" href="/audiolibros/#logros">Conocer las 25 insignias</a></div><p class="fine">El primer 10 % de cada libro es gratis y no requiere registro. El catálogo completo está disponible con membresía.</p><p class="fine">Esta página muestra el diseño del logro compartido; no expone nombres, cuentas ni el historial de escucha de una persona.</p></div></article></main><footer class="wrap"><span>Audiolibros TEC · Total Elite Coaching</span><a href="/audiolibros/contacto/">Contacto y soporte</a><a href="/audiolibros/privacidad/">Privacidad</a></footer></body></html>`;
}

export function registerBadgeSharePages(app,{publicOrigin='https://totalelitecoaching.com'}={}){
  const origin=new URL(publicOrigin).origin,pages=new Map(BADGES.map(b=>[b.id,badgeSharePage(b.id,origin)]));
  app.use('/audiolibros/insignias/v2',express.static(fileURLToPath(new URL('../public/audiolibros/insignias/v2/',import.meta.url)),{maxAge:'365d',immutable:true,dotfiles:'deny'}));
  app.get(/^\/audiolibros\/logros\/([a-z0-9-]+)\/?$/, (req,res)=>{
    const id=req.params[0],page=pages.get(id);
    res.set({'X-Robots-Tag':'noindex, follow','Cache-Control':'public, max-age=300'});
    if(!page)return res.status(404).type('html').send('<!doctype html><html lang="es"><title>Insignia no encontrada · TEC</title><h1>No encontramos esa insignia</h1><a href="/audiolibros/#logros">Ver las insignias de TEC</a></html>');
    if(!req.path.endsWith('/'))return res.redirect(301,badgeShareInfo(id).path);
    res.set('Content-Security-Policy',"default-src 'none'; img-src 'self'; style-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'self'");
    return res.type('html').send(page);
  });
}
