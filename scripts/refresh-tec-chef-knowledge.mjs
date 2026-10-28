import fs from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import path from 'node:path';
import { buildProductsDocument, buildWarrantyDocument, buildHelpfulHintsDocument, fetchText, stripHtml, extractSupportHeader } from './fetch-royalprestige-site-knowledge.js';
const clean = text => stripHtml(String(text || '')).replace(/&reg;|&trade;/g, '').trim();
const items = value => [...String(value || '').matchAll(/<li[^>]*>([\s\S]*?)<\/li>/gi)].map(m=>clean(m[1])).filter(Boolean);
export function normalizeChefRecipe(row) {
  const [ingredients, tools = ''] = String(row.ingredients || '').split(/Utensilios\s+Royal\s+Prestige[^<]*/i);
  return { id:row.id, slug:row.urlName, title:clean(row.title || row.text1), description:clean(row.text2),
    ingredients:items(ingredients), steps:items(row.instructions),
    utensils:[...new Set([...items(tools), ...(row.recipes || []).map(r=>clean(r.text))])],
    categories:(row.categories || []).map(r=>clean(r.text)), minutes:String(row.actualCookingTime || ''), servings:String(row.actualFoodPortions || ''),
    url:new URL(row.recipeDetailUrl, 'https://www.royalprestige.com').href };
}
async function recipesPage(pageNumber) {
  const response=await fetch('https://www.royalprestige.com/api/rp/recipes',{method:'POST',headers:{'Content-Type':'application/json'},signal:AbortSignal.timeout(45000),body:JSON.stringify({language:'es',siteId:'77368a0b-d1cc-4d0e-983a-6b5231c7ff85',filterOptions:{pageNumber},filters:[{name:'ingredients',values:[],displayExpanded:true}],isDesktop:true})});
  if(!response.ok)throw Error('Recipe source unavailable');
  const result=await response.json();if(result.errorDetail?.isError)throw Error('Recipe source error');return result.data;
}
export async function refreshChefKnowledge(output) {
  const first=await recipesPage(1);const all=[...first.recipes];
  for(let n=2;n<=first.paging.totalPages;n+=3){const batch=await Promise.all(Array.from({length:Math.min(3,first.paging.totalPages-n+1)},(_,i)=>recipesPage(n+i)));all.push(...batch.flatMap(p=>p.recipes));}
  const [products,warranty,care,returnsHtml]=await Promise.all([buildProductsDocument(),buildWarrantyDocument(),buildHelpfulHintsDocument(),fetchText('https://www.royalprestige.com/apoyo/politca-de-devolucion')]);
  const returnText=clean(extractSupportHeader(returnsHtml,'<h2>Política de Devolución</h2>'));
  const recipes=[...new Map(all.map(r=>[r.id,normalizeChefRecipe(r)])).values()];
  if(recipes.length<200 || recipes.some(r=>!r.title||!r.ingredients.length||!r.steps.length)||products.productos.length<50||!warranty.secciones.length||!care.consejos.length)throw Error('Incomplete Chef public sources');
  // Keep support/marketing CTAs out of the assistant's factual index.
  const doc={metadata:{updatedAt:new Date().toISOString(),country:'US',recipeCountry:'MX',recipeSource:'https://www.royalprestige.com/api/rp/recipes'},recipes,
    products:products.productos.map(r=>({id:r.id,title:r.titulo,description:r.descripcion_corta,details:r.descripcion_general,benefits:r.por_que_te_encantara,url:r.url})),
    warranty:{intro:warranty.pagina?.introduccion || '',sections:warranty.secciones.map(r=>({title:r.titulo,text:r.contenido})),url:warranty.pagina?.url || 'https://www.royalprestige.com/apoyo/garantia'},
    care:care.consejos.map(r=>({title:r.pregunta,text:r.respuesta,url:r.url || care.pagina?.url || 'https://www.royalprestige.com/apoyo/consejos-utiles'})),
    returns:returnText.length>300?{title:'Devoluciones en Estados Unidos',text:returnText,url:'https://www.royalprestige.com/apoyo/politca-de-devolucion'}:null};
  // If the HTML boundary changed, preserve the previously verified US policy with its own date.
  if(!doc.returns){const old=JSON.parse(await fs.readFile(new URL('../src/data/tec_returns_us.json',import.meta.url),'utf8'));doc.returns={title:old.titulo,text:old.contenido,url:old.metadata.source,verifiedAt:old.metadata.fetched_at};}
  await fs.writeFile(output,JSON.stringify(doc,null,2)+'\n');
  console.log(JSON.stringify({recipes:recipes.length,products:doc.products.length,warranty:doc.warranty.sections.length,care:doc.care.length,updatedAt:doc.metadata.updatedAt}));
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href)refreshChefKnowledge(process.argv[2]||new URL('../src/data/tec_chef_public_knowledge.json',import.meta.url)).catch(e=>{console.error(e.message);process.exitCode=1;});
