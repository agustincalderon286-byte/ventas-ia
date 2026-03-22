import fs from "fs/promises";
import path from "path";

const BASE_URL = "https://www.royalprestige.com";
const SITEMAP_URL = `${BASE_URL}/sitemap/sitemap.xml`;
const DATA_DIR = path.join(process.cwd(), "src/data");
const OUTPUTS = {
  products: path.join(DATA_DIR, "royalprestige_productos_site.json"),
  warranty: path.join(DATA_DIR, "royalprestige_garantia_site.json"),
  payments: path.join(DATA_DIR, "royalprestige_pagos_site.json"),
  helpfulHints: path.join(DATA_DIR, "royalprestige_consejos_site.json"),
  classes: path.join(DATA_DIR, "royalprestige_clases_site.json"),
  magazine: path.join(DATA_DIR, "royalprestige_revista_site.json"),
  productMatch: path.join(DATA_DIR, "royalprestige_producto_ideal_site.json")
};

const SUPPORT_URLS = {
  warranty: `${BASE_URL}/apoyo/garantia`,
  payments: `${BASE_URL}/apoyo/opciones-de-pago`,
  helpfulHints: `${BASE_URL}/apoyo/consejos-utiles`
};

const INSPIRATION_URLS = {
  classes: `${BASE_URL}/clases-de-cocina`,
  magazine: `${BASE_URL}/inspiracion/revista-royal-prestige`,
  productMatch: `${BASE_URL}/inspiracion/tu-producto-ideal`
};

function decodeHtmlEntities(text) {
  return String(text || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&frac12;/gi, "1/2")
    .replace(/&frac14;/gi, "1/4")
    .replace(/&frac34;/gi, "3/4")
    .replace(/&deg;/gi, "°")
    .replace(/&aacute;/gi, "á")
    .replace(/&eacute;/gi, "é")
    .replace(/&iacute;/gi, "í")
    .replace(/&oacute;/gi, "ó")
    .replace(/&uacute;/gi, "ú")
    .replace(/&ntilde;/gi, "ñ")
    .replace(/&uuml;/gi, "ü")
    .replace(/&Aacute;/gi, "Á")
    .replace(/&Eacute;/gi, "É")
    .replace(/&Iacute;/gi, "Í")
    .replace(/&Oacute;/gi, "Ó")
    .replace(/&Uacute;/gi, "Ú")
    .replace(/&Ntilde;/gi, "Ñ")
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => String.fromCharCode(parseInt(code, 16)));
}

function stripHtml(html) {
  return decodeHtmlEntities(
    String(html || "")
      .replace(/<sup[^>]*>.*?<\/sup>/gi, "")
      .replace(/<iframe[\s\S]*?<\/iframe>/gi, "")
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/p>/gi, "\n")
      .replace(/<\/div>/gi, "\n")
      .replace(/<\/li>/gi, "\n")
      .replace(/<li[^>]*>/gi, "- ")
      .replace(/<\/h[1-6]>/gi, "\n")
      .replace(/<[^>]+>/g, "")
  )
    .replace(/\u00a0/g, " ")
    .replace(/\s+\n/g, "\n")
    .replace(/\n{2,}/g, "\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

function slugify(text) {
  return decodeHtmlEntities(String(text || ""))
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function unique(values) {
  return Array.from(new Set(values.filter(Boolean)));
}

function extractFirst(text, regex, index = 1) {
  const match = text.match(regex);
  return match ? match[index] : "";
}

function absolutize(url) {
  if (!url) {
    return "";
  }

  try {
    return new URL(url, BASE_URL).toString();
  } catch {
    return url;
  }
}

async function fetchText(url) {
  const response = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "text/html,application/xml;q=0.9,*/*;q=0.8"
    }
  });

  if (!response.ok) {
    throw new Error(`No se pudo descargar ${url}: ${response.status}`);
  }

  return await response.text();
}

async function mapWithConcurrency(items, limit, mapper) {
  const results = new Array(items.length);
  let cursor = 0;

  async function worker() {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;
      results[index] = await mapper(items[index], index);
    }
  }

  const workers = Array.from({ length: Math.min(limit, items.length || 1) }, () => worker());
  await Promise.all(workers);

  return results;
}

function parseJsonLd(html) {
  const scripts = Array.from(html.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi));

  for (const script of scripts) {
    const raw = decodeHtmlEntities(script[1]).trim();

    try {
      const parsed = JSON.parse(raw);
      const entries = Array.isArray(parsed) ? parsed : [parsed];
      const candidate = entries.find(entry => entry && typeof entry === "object" && (entry["@type"] === "Product" || entry.name));

      if (candidate) {
        return candidate;
      }
    } catch {
      continue;
    }
  }

  return null;
}

function extractProductBenefits(html) {
  return Array.from(html.matchAll(/<div class="product-detail__benefit-content">([\s\S]*?)<\/div>/gi))
    .map(match => stripHtml(match[1]))
    .filter(Boolean);
}

function extractTitleFromPage(html) {
  const h1 = stripHtml(extractFirst(html, /<h1 class="product-detail__product-heading"\s*>[\s\S]*?<\/h1>/i, 0));

  if (h1) {
    return h1;
  }

  const title = decodeHtmlEntities(extractFirst(html, /<title>\s*([\s\S]*?)\s*<\/title>/i));
  return title.replace(/\s*\|\s*Royal Prestige.*$/i, "").trim();
}

function extractSupportHeader(html, heading) {
  const start = html.indexOf(heading);
  const endCandidates = [
    html.indexOf('<form action="', start),
    html.indexOf('<div class="distributorContactWrapper">', start)
  ].filter(index => index > start);

  const end = endCandidates.length ? Math.min(...endCandidates) : html.length;
  return start >= 0 ? html.slice(start, end) : "";
}

function extractSection(html, regex) {
  const match = String(html || "").match(regex);
  return match ? match[1] : "";
}

function extractParagraphs(html) {
  return Array.from(String(html || "").matchAll(/<p[^>]*>([\s\S]*?)<\/p>/gi)).map(match => match[1]);
}

function extractMetaDescription(html) {
  return decodeHtmlEntities(extractFirst(html, /<meta name="description" content="([^"]*)"/i));
}

function extractOgTitle(html) {
  return decodeHtmlEntities(extractFirst(html, /<meta property="og:title" content="([^"]*)"/i));
}

function parseWarrantySections(html) {
  const block = extractSupportHeader(html, "<h2>Garantía limitada</h2>");
  const paragraphs = extractParagraphs(block);
  const intro = [];
  const sections = [];
  let currentSection = null;

  for (const paragraphHtml of paragraphs) {
    const headingMatches = Array.from(paragraphHtml.matchAll(/<b><u>([\s\S]*?)<\/u><\/b>/gi))
      .map(match => stripHtml(match[1]))
      .filter(Boolean);
    const text = stripHtml(paragraphHtml);

    if (!text) {
      continue;
    }

    if (headingMatches.length) {
      const title = headingMatches.join(" ").replace(/\s+/g, " ").trim();
      let content = text;

      if (content.startsWith(title)) {
        content = content.slice(title.length).trim();
      }

      content = content.replace(/^[:：.\s-]+/, "").trim();

      currentSection = {
        id: slugify(title),
        titulo: title,
        contenido: content
      };

      sections.push(currentSection);
      continue;
    }

    if (currentSection) {
      currentSection.contenido = `${currentSection.contenido}\n${text}`.trim();
    } else {
      intro.push(text);
    }
  }

  return {
    introduccion: intro.join("\n\n").trim(),
    secciones: sections.filter(section => section.titulo && section.contenido)
  };
}

function parsePaymentMethods(html) {
  const listHtml = extractSupportHeader(html, '<div class="pm">');
  const starts = Array.from(listHtml.matchAll(/<li class="pm__item">/gi)).map(match => match.index);

  return starts
    .map((start, index) => {
      const end = index + 1 < starts.length ? starts[index + 1] : listHtml.indexOf("</ul>", start);
      const itemHtml = listHtml.slice(start, end > start ? end : undefined);
      const title = stripHtml(extractFirst(itemHtml, /<div class="pm__title">([\s\S]*?)<\/div>/i));
      const content = stripHtml(extractFirst(itemHtml, /<div class="pm__inner-content">([\s\S]*?)<\/div>/i));
      const links = Array.from(itemHtml.matchAll(/<a[^>]+href="([^"]+)"[^>]*>/gi))
        .map(linkMatch => absolutize(linkMatch[1]))
        .filter(link => !link.startsWith("#"));

      return {
        id: slugify(title || `metodo-${index + 1}`),
        nombre: title || `Método ${index + 1}`,
        instrucciones: content,
        enlaces: unique(links)
      };
    })
    .filter(item => item.nombre && item.instrucciones);
}

function parseHelpfulHints(html) {
  const block = extractSupportHeader(html, '<div class="hh">');
  const items = Array.from(block.matchAll(/<li class="hh__item">([\s\S]*?)<\/li>/gi));

  return items
    .map((match, index) => {
      const itemHtml = match[1];
      const question = stripHtml(extractFirst(itemHtml, /<div class="hh__title">([\s\S]*?)<\/div>/i));
      const answer = stripHtml(extractFirst(itemHtml, /<div class="hh__inner-content">([\s\S]*?)<\/div>/i));

      return {
        id: slugify(question || `consejo-${index + 1}`),
        pregunta: question,
        respuesta: answer
      };
    })
    .filter(item => item.pregunta && item.respuesta);
}

function parseProductCards(html) {
  return Array.from(String(html || "").matchAll(/<a\s+href="([^"]+)" class="product-card">([\s\S]*?)<\/a>/gi))
    .map((match, index) => {
      const cardHtml = match[2];
      const title = stripHtml(extractFirst(cardHtml, /<div class="product-card__title">([\s\S]*?)<\/div>/i));
      const description = stripHtml(extractFirst(cardHtml, /<div class="product-card__description">([\s\S]*?)<\/div>/i));

      return {
        id: slugify(title || `producto-${index + 1}`),
        titulo: title || `Producto ${index + 1}`,
        descripcion: description,
        url: absolutize(match[1])
      };
    })
    .filter(card => card.titulo);
}

function parseClasses(html) {
  const introStart = html.indexOf("Acompáñanos en este viaje");
  const introEnd = html.indexOf('<div id="MainContent_C035_Col00"', introStart);
  const introSlice = introStart >= 0 ? html.slice(introStart, introEnd > introStart ? introEnd : html.length) : "";
  const introduccion = Array.from(introSlice.matchAll(/<h5[^>]*>([\s\S]*?)<\/h5>/gi))
    .map(match => stripHtml(match[1]))
    .filter(Boolean)
    .join("\n\n");

  const rows = Array.from(
    String(html || "").matchAll(/<div class="row(?: row-reverse)?" data-sf-element="Row">([\s\S]*?)<\/div>\s*<\/div>\s*<\/div>/gi)
  );

  const clases = rows
    .map(match => match[1])
    .filter(block => /recipe-card-class/i.test(block))
    .map((block, index) => {
      const title = stripHtml(extractFirst(block, /<h2[^>]*>([\s\S]*?)<\/h2>/i));
      const paragraphs = extractParagraphs(block)
        .map(stripHtml)
        .filter(Boolean);
      const description = paragraphs.find(paragraph =>
        !/^Descarga/i.test(paragraph) &&
        !/(Lunes|Martes|Mi[eé]rcoles|Jueves|Viernes|S[aá]bado|Domingo)/i.test(paragraph) &&
        !/Los [ÁA]ngeles|Madison|New York/i.test(paragraph)
      ) || "";
      const date = paragraphs.find(paragraph =>
        /(Lunes|Martes|Mi[eé]rcoles|Jueves|Viernes|S[aá]bado|Domingo)/i.test(paragraph)
      ) || "";
      const schedule = paragraphs.find(paragraph => /Los [ÁA]ngeles|Madison|New York/i.test(paragraph)) || "";
      const imageUrl = absolutize(extractFirst(block, /<img[^>]+src="([^"]+)"/i));
      const recipesPdfUrl = absolutize(extractFirst(block, /href="([^"]*recipe-card-class[^"]*)"/i));
      const certificateUrl = absolutize(extractFirst(block, /href="([^"]*clases-magistrales-certificado[^"]*)"/i));
      const productLinks = unique(
        Array.from(block.matchAll(/<a[^>]+href="([^"]*\/productos\/detalle\/[^"]+)"/gi)).map(link => absolutize(link[1]))
      );

      return {
        id: slugify(title || `clase-${index + 1}`),
        titulo: title || `Clase ${index + 1}`,
        descripcion: description,
        fecha: date,
        horarios: schedule,
        recetas_pdf_url: recipesPdfUrl,
        certificado_url: certificateUrl,
        imagen_url: imageUrl,
        productos_relacionados: productLinks
      };
    })
    .filter(item => item.titulo && item.recetas_pdf_url);

  return {
    introduccion,
    clases
  };
}

function parseMagazine(html) {
  const block = extractSection(html, /<section class="featured-magazine">([\s\S]*?)<\/section>/i);

  if (!block) {
    return [];
  }

  const kicker = stripHtml(extractFirst(block, /<div class="featured-magazine__kicker">([\s\S]*?)<\/div>/i));
  const season = stripHtml(extractFirst(block, /<div class="featured-magazine__season">([\s\S]*?)<\/div>/i));
  const title = stripHtml(extractFirst(block, /<div class="featured-magazine__title">([\s\S]*?)<\/div>/i));
  const issue = stripHtml(extractFirst(block, /<div class="featured-magazine__issue">([\s\S]*?)<\/div>/i));
  const description = stripHtml(extractFirst(block, /<div class="featured-magazine__des">([\s\S]*?)<\/div>/i));
  const imageUrl = absolutize(extractFirst(block, /<img[^>]+class="featured-magazine__image"[^>]+src="([^"]+)"/i));
  const downloadUrl = absolutize(extractFirst(block, /<a href="([^"]+)"[^>]*>Descargar<\/a>/i));
  const readOnlineUrl = absolutize(extractFirst(block, /<a href="([^"]+)"[^>]*>Leer en l/i));

  return [
    {
      id: slugify(`${season} ${title}` || "revista-destacada"),
      tipo: kicker,
      temporada: season,
      titulo: title,
      encabezado: issue,
      descripcion: description,
      descarga_pdf_url: downloadUrl,
      leer_online_url: readOnlineUrl,
      imagen_url: imageUrl,
      url: INSPIRATION_URLS.magazine
    }
  ].filter(item => item.titulo);
}

async function parseProductMatchPage(url) {
  const html = await fetchText(url);
  const start = html.indexOf('<h1 class="results__heading">');
  const endMarker = html.indexOf('href="/inspiracion/tu-producto-ideal"', start);
  const content = start >= 0 ? html.slice(start, endMarker > start ? endMarker : html.length) : html;
  const cardsStart = html.indexOf('<div class="product-cards">', start);
  const cardsEndCandidates = [
    html.indexOf("Volver a intentar", cardsStart > start ? cardsStart : start),
    html.indexOf("Try Again", cardsStart > start ? cardsStart : start)
  ].filter(index => index > cardsStart);
  const cardsEnd = cardsEndCandidates.length ? Math.min(...cardsEndCandidates) : -1;
  const cardsBlock = cardsStart > start ? html.slice(cardsStart, cardsEnd > cardsStart ? cardsEnd : html.length) : "";
  const audienceMatch = content.match(/Productos Perfectos Para<\/b><\/p><h2[^>]*>([\s\S]*?)<\/h2>/i);
  const audienceEnd = audienceMatch ? audienceMatch.index + audienceMatch[0].length : 0;
  const afterAudience = audienceEnd ? content.slice(audienceEnd) : content;
  const featuredMatch = afterAudience.match(/<div ><p[^>]*><b>([\s\S]*?)<\/b><\/p><p>([\s\S]*?)<\/p><\/div>/i);
  const featuredProduct = featuredMatch ? stripHtml(featuredMatch[1]) : "";
  const featuredDescription = featuredMatch ? stripHtml(featuredMatch[2]) : "";
  const afterFeatured = featuredMatch
    ? afterAudience.slice(featuredMatch.index + featuredMatch[0].length)
    : afterAudience;
  const recommendationBlock = extractFirst(afterFeatured, /<div >((?:<h2[^>]*>[\s\S]*?<\/h2>)+[\s\S]*?)<\/div>/i, 1);
  const recommendationTitle = Array.from(String(recommendationBlock || "").matchAll(/<h2[^>]*>([\s\S]*?)<\/h2>/gi))
    .map(match => stripHtml(match[1]))
    .filter(Boolean)
    .at(-1) || "";
  const recommendationText = stripHtml(recommendationBlock)
    .replace(new RegExp(`^${recommendationTitle.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s*`), "")
    .trim();
  const recommendedProducts = parseProductCards(cardsBlock);

  return {
    id: slugify(new URL(url).pathname.split("/").pop() || extractOgTitle(html)),
    nombre: extractOgTitle(html) || stripHtml(extractFirst(content, /<h1 class="results__heading">([\s\S]*?)<\/h1>/i)),
    titulo_resultado: stripHtml(extractFirst(content, /<h1 class="results__heading">([\s\S]*?)<\/h1>/i)),
    publico_ideal: stripHtml(extractFirst(content, /Productos Perfectos Para<\/b><\/p><h2[^>]*>([\s\S]*?)<\/h2>/i)),
    producto_destacado: featuredProduct || recommendedProducts[0]?.titulo || "",
    descripcion_producto_destacado: featuredDescription || recommendedProducts[0]?.descripcion || "",
    recomendacion_titulo: recommendationTitle,
    recomendacion_texto: recommendationText,
    productos_recomendados: recommendedProducts,
    agenda_demo_disponible: /Agenda tu Demo/i.test(html),
    url: absolutize(url)
  };
}

async function parseProductPage(url) {
  const html = await fetchText(url);
  const jsonLd = parseJsonLd(html) || {};
  const title = extractTitleFromPage(html);
  const metaDescription = decodeHtmlEntities(extractFirst(html, /<meta name="description" content="([^"]*)"/i));
  const description = stripHtml(jsonLd.description || metaDescription);
  const benefits = extractProductBenefits(html);
  const usageCareTitle = stripHtml(extractFirst(html, /<div class="content-and-media__heading[^"]*">([\s\S]*?)<\/div>/i));
  const youtubeId = extractFirst(html, /youtube\.com\/embed\/([^"?&]+)/i);
  const productCode = extractFirst(html, /data-bv-product-id="([^"]+)"/i);
  const whatsappUrl = absolutize(extractFirst(html, /<a href="(https:\/\/wa\.me\/[^"]+)"/i));

  return {
    id: slugify(new URL(url).pathname.split("/").pop() || title),
    titulo: title,
    descripcion_corta: metaDescription,
    descripcion_general: description,
    por_que_te_encantara: benefits,
    uso_y_cuidados_titulo: usageCareTitle,
    video_youtube_id: youtubeId || "",
    codigo_producto: productCode || "",
    agenda_demo_disponible: /Agenda tu Demo/i.test(html),
    whatsapp_chat_disponible: Boolean(whatsappUrl),
    whatsapp_url: whatsappUrl,
    opciones_financiamiento: /Opciones de financiamiento disponibles/i.test(html),
    url
  };
}

function extractLocs(xml) {
  return Array.from(xml.matchAll(/<loc>(.*?)<\/loc>/gi)).map(match => decodeHtmlEntities(match[1]));
}

async function buildProductsDocument() {
  const sitemap = await fetchText(SITEMAP_URL);
  const urls = unique(
    extractLocs(sitemap)
      .filter(url => url.includes("/productos/detalle/"))
      .filter(url => !url.includes("/en-us/"))
      .map(absolutize)
  );
  const productos = await mapWithConcurrency(urls, 4, parseProductPage);

  return {
    metadata: {
      coleccion: "royalprestige_productos_site",
      descripcion: "Productos estructurados descargados desde páginas públicas de Royal Prestige US en español.",
      total_productos: productos.length,
      fetched_at: new Date().toISOString(),
      source: SITEMAP_URL,
      campos_por_chunk: [
        "titulo",
        "descripcion_general",
        "por_que_te_encantara",
        "uso_y_cuidados_titulo",
        "opciones_financiamiento"
      ]
    },
    productos
  };
}

async function buildWarrantyDocument() {
  const html = await fetchText(SUPPORT_URLS.warranty);
  const title = decodeHtmlEntities(extractFirst(html, /<title>\s*([\s\S]*?)\s*<\/title>/i));
  const metaDescription = decodeHtmlEntities(extractFirst(html, /<meta name="description" content="([^"]*)"/i));
  const { introduccion, secciones } = parseWarrantySections(html);

  return {
    metadata: {
      coleccion: "royalprestige_garantia_site",
      descripcion: "Secciones de garantía extraídas desde la página pública de apoyo de Royal Prestige US en español.",
      total_secciones: secciones.length,
      fetched_at: new Date().toISOString(),
      source: SUPPORT_URLS.warranty,
      campos_por_chunk: [
        "titulo",
        "contenido"
      ]
    },
    pagina: {
      titulo: title,
      descripcion: metaDescription,
      introduccion,
      url: SUPPORT_URLS.warranty
    },
    secciones
  };
}

async function buildPaymentsDocument() {
  const html = await fetchText(SUPPORT_URLS.payments);
  const title = decodeHtmlEntities(extractFirst(html, /<title>\s*([\s\S]*?)\s*<\/title>/i));
  const metaDescription = decodeHtmlEntities(extractFirst(html, /<meta name="description" content="([^"]*)"/i));
  const introBlock = extractSupportHeader(html, "<h2>Opciones de Pago</h2>");
  const intro = stripHtml(extractFirst(introBlock, /<h2>Opciones de Pago<\/h2>([\s\S]*?)<\/div>/i));
  const metodos_pago = parsePaymentMethods(html);

  return {
    metadata: {
      coleccion: "royalprestige_pagos_site",
      descripcion: "Métodos de pago extraídos desde la página pública de apoyo de Royal Prestige US en español.",
      total_metodos: metodos_pago.length,
      fetched_at: new Date().toISOString(),
      source: SUPPORT_URLS.payments,
      campos_por_chunk: [
        "nombre",
        "instrucciones",
        "enlaces"
      ]
    },
    pagina: {
      titulo: title,
      descripcion: metaDescription,
      introduccion: intro,
      url: SUPPORT_URLS.payments
    },
    metodos_pago
  };
}

async function buildHelpfulHintsDocument() {
  const html = await fetchText(SUPPORT_URLS.helpfulHints);
  const title = decodeHtmlEntities(extractFirst(html, /<title>\s*([\s\S]*?)\s*<\/title>/i));
  const metaDescription = decodeHtmlEntities(extractFirst(html, /<meta name="description" content="([^"]*)"/i));
  const introBlock = extractSupportHeader(html, "<h2>Enorgullécete");
  const intro = stripHtml(extractFirst(introBlock, /<div ><h2>[\s\S]*?<\/div>/i, 0));
  const consejos = parseHelpfulHints(html);

  return {
    metadata: {
      coleccion: "royalprestige_consejos_site",
      descripcion: "Consejos útiles extraídos desde la página pública de apoyo de Royal Prestige US en español.",
      total_consejos: consejos.length,
      fetched_at: new Date().toISOString(),
      source: SUPPORT_URLS.helpfulHints,
      campos_por_chunk: [
        "pregunta",
        "respuesta"
      ]
    },
    pagina: {
      titulo: title,
      descripcion: metaDescription,
      introduccion: intro,
      url: SUPPORT_URLS.helpfulHints
    },
    consejos
  };
}

async function buildClassesDocument() {
  const html = await fetchText(INSPIRATION_URLS.classes);
  const title = decodeHtmlEntities(extractFirst(html, /<title>\s*([\s\S]*?)\s*<\/title>/i));
  const metaDescription = extractMetaDescription(html);
  const { introduccion, clases } = parseClasses(html);

  return {
    metadata: {
      coleccion: "royalprestige_clases_site",
      descripcion: "Clases magistrales públicas de cocina de Royal Prestige con recetas descargables y horarios.",
      total_clases: clases.length,
      fetched_at: new Date().toISOString(),
      source: INSPIRATION_URLS.classes,
      campos_por_chunk: [
        "titulo",
        "descripcion",
        "fecha",
        "horarios",
        "recetas_pdf_url"
      ]
    },
    pagina: {
      titulo: title,
      descripcion: metaDescription,
      introduccion,
      url: INSPIRATION_URLS.classes
    },
    clases
  };
}

async function buildMagazineDocument() {
  const html = await fetchText(INSPIRATION_URLS.magazine);
  const title = decodeHtmlEntities(extractFirst(html, /<title>\s*([\s\S]*?)\s*<\/title>/i));
  const metaDescription = extractMetaDescription(html);
  const ediciones = parseMagazine(html);

  return {
    metadata: {
      coleccion: "royalprestige_revista_site",
      descripcion: "Ediciones destacadas de la revista pública Royal Prestige con resumen y enlaces de lectura.",
      total_ediciones: ediciones.length,
      fetched_at: new Date().toISOString(),
      source: INSPIRATION_URLS.magazine,
      campos_por_chunk: [
        "temporada",
        "titulo",
        "descripcion",
        "descarga_pdf_url",
        "leer_online_url"
      ]
    },
    pagina: {
      titulo: title,
      descripcion: metaDescription,
      url: INSPIRATION_URLS.magazine
    },
    ediciones
  };
}

async function buildProductMatchDocument() {
  const [sitemap, landingHtml] = await Promise.all([
    fetchText(SITEMAP_URL),
    fetchText(INSPIRATION_URLS.productMatch)
  ]);
  const urls = unique(
    extractLocs(sitemap)
      .filter(url => url.includes("/inspiracion/tu-producto-ideal/"))
      .filter(url => !url.includes("/en-us/"))
      .map(absolutize)
  );
  const perfiles = (await mapWithConcurrency(urls, 3, async url => {
    try {
      return await parseProductMatchPage(url);
    } catch (error) {
      console.warn(`Omitiendo perfil con error: ${url} -> ${error.message}`);
      return null;
    }
  })).filter(Boolean);

  return {
    metadata: {
      coleccion: "royalprestige_producto_ideal_site",
      descripcion: "Perfiles públicos de Tu Producto Ideal de Royal Prestige para recomendar productos por estilo de vida.",
      total_perfiles: perfiles.length,
      fetched_at: new Date().toISOString(),
      source: INSPIRATION_URLS.productMatch,
      campos_por_chunk: [
        "nombre",
        "publico_ideal",
        "producto_destacado",
        "recomendacion_texto",
        "productos_recomendados"
      ]
    },
    pagina: {
      titulo: decodeHtmlEntities(extractFirst(landingHtml, /<title>\s*([\s\S]*?)\s*<\/title>/i)),
      descripcion: extractMetaDescription(landingHtml),
      url: INSPIRATION_URLS.productMatch
    },
    perfiles
  };
}

async function writeJson(filePath, payload) {
  await fs.writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

async function main() {
  const [
    productsDoc,
    warrantyDoc,
    paymentsDoc,
    helpfulHintsDoc,
    classesDoc,
    magazineDoc,
    productMatchDoc
  ] = await Promise.all([
    buildProductsDocument(),
    buildWarrantyDocument(),
    buildPaymentsDocument(),
    buildHelpfulHintsDocument(),
    buildClassesDocument(),
    buildMagazineDocument(),
    buildProductMatchDocument()
  ]);

  await Promise.all([
    writeJson(OUTPUTS.products, productsDoc),
    writeJson(OUTPUTS.warranty, warrantyDoc),
    writeJson(OUTPUTS.payments, paymentsDoc),
    writeJson(OUTPUTS.helpfulHints, helpfulHintsDoc),
    writeJson(OUTPUTS.classes, classesDoc),
    writeJson(OUTPUTS.magazine, magazineDoc),
    writeJson(OUTPUTS.productMatch, productMatchDoc)
  ]);

  console.log(
    JSON.stringify(
      {
        outputs: OUTPUTS,
        summary: {
          productos: productsDoc.productos.length,
          garantia_secciones: warrantyDoc.secciones.length,
          metodos_pago: paymentsDoc.metodos_pago.length,
          consejos: helpfulHintsDoc.consejos.length,
          clases: classesDoc.clases.length,
          revista_ediciones: magazineDoc.ediciones.length,
          producto_ideal_perfiles: productMatchDoc.perfiles.length
        }
      },
      null,
      2
    )
  );
}

main().catch(error => {
  console.error("Error descargando conocimiento general de Royal Prestige:", error.message);
  process.exit(1);
});
