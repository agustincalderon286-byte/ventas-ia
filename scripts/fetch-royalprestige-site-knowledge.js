import fs from "fs/promises";
import path from "path";

const BASE_URL = "https://www.royalprestige.com";
const SITEMAP_URL = `${BASE_URL}/sitemap/sitemap.xml`;
const DATA_DIR = path.join(process.cwd(), "src/data");
const OUTPUTS = {
  products: path.join(DATA_DIR, "royalprestige_productos_site.json"),
  warranty: path.join(DATA_DIR, "royalprestige_garantia_site.json"),
  payments: path.join(DATA_DIR, "royalprestige_pagos_site.json"),
  helpfulHints: path.join(DATA_DIR, "royalprestige_consejos_site.json")
};

const SUPPORT_URLS = {
  warranty: `${BASE_URL}/apoyo/garantia`,
  payments: `${BASE_URL}/apoyo/opciones-de-pago`,
  helpfulHints: `${BASE_URL}/apoyo/consejos-utiles`
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

function extractParagraphs(html) {
  return Array.from(String(html || "").matchAll(/<p[^>]*>([\s\S]*?)<\/p>/gi)).map(match => match[1]);
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

async function writeJson(filePath, payload) {
  await fs.writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

async function main() {
  const [productsDoc, warrantyDoc, paymentsDoc, helpfulHintsDoc] = await Promise.all([
    buildProductsDocument(),
    buildWarrantyDocument(),
    buildPaymentsDocument(),
    buildHelpfulHintsDocument()
  ]);

  await Promise.all([
    writeJson(OUTPUTS.products, productsDoc),
    writeJson(OUTPUTS.warranty, warrantyDoc),
    writeJson(OUTPUTS.payments, paymentsDoc),
    writeJson(OUTPUTS.helpfulHints, helpfulHintsDoc)
  ]);

  console.log(
    JSON.stringify(
      {
        outputs: OUTPUTS,
        summary: {
          productos: productsDoc.productos.length,
          garantia_secciones: warrantyDoc.secciones.length,
          metodos_pago: paymentsDoc.metodos_pago.length,
          consejos: helpfulHintsDoc.consejos.length
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
