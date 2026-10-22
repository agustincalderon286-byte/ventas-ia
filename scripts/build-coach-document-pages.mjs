// Read-only PDF rendering: original PDFs are never modified.
// Requires Poppler's pdfinfo and pdftoppm in PATH.
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const base = "/coach/assets/documents";
const output = path.join(root, "public", base);
const inputs = {
  catalogo: "novel-catalog-digital-3.pdf",
  carpeta: "novel-sales-presentation-2024.pdf"
};
const manifest = { version: 1, documents: {} };

function jpegSize(buffer) {
  let offset = 2;
  while (offset + 8 < buffer.length) {
    if (buffer[offset++] !== 0xff) throw new Error("Invalid JPEG marker");
    while (buffer[offset] === 0xff) offset++;
    const marker = buffer[offset++];
    const size = buffer.readUInt16BE(offset);
    if ([0xc0, 0xc1, 0xc2].includes(marker)) {
      return { width: buffer.readUInt16BE(offset + 5), height: buffer.readUInt16BE(offset + 3) };
    }
    offset += size;
  }
  throw new Error("JPEG dimensions missing");
}

for (const [id, file] of Object.entries(inputs)) {
  const source = path.join(root, "public/coach/resources", file);
  const count = Number(execFileSync("pdfinfo", [source], { encoding: "utf8" }).match(/^Pages:\s+(\d+)/m)?.[1]);
  if (!count) throw new Error(`Missing PDF page count: ${file}`);
  const directory = path.join(output, `${id}-v1`);
  fs.mkdirSync(directory, { recursive: true });
  execFileSync("pdftoppm", ["-jpeg", "-scale-to", "1600", "-jpegopt", "quality=88,optimize=y", source, path.join(directory, "page")], { stdio: "inherit" });
  const pages = [];
  for (let page = 1; page <= count; page++) {
    const name = `page-${String(page).padStart(String(count).length, "0")}.jpg`;
    const bytes = fs.readFileSync(path.join(directory, name));
    pages.push({ url: `${base}/${id}-v1/${name}`, ...jpegSize(bytes), bytes: bytes.length, sha256: crypto.createHash("sha256").update(bytes).digest("hex") });
  }
  manifest.documents[id] = {
    sourceSha256: crypto.createHash("sha256").update(fs.readFileSync(source)).digest("hex"), pages
  };
  console.log(`${id}: ${pages.length} pages, ${(pages.reduce((sum, page) => sum + page.bytes, 0) / 1024 / 1024).toFixed(1)} MiB`);
}
fs.writeFileSync(path.join(output, "manifest-v1.json"), `${JSON.stringify(manifest, null, 2)}\n`);
