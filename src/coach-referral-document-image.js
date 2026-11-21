import sharp from 'sharp';
import { decodeChefPhoto } from './tec-chef-photo.js';

const uploadFormats = new Set(['jpeg', 'png', 'webp']);
const readOptions = { limitInputPixels: 16000000, failOn: 'warning' };
const jpegOptions = { quality: 90, chromaSubsampling: '4:4:4' };

function imagePart(bytes) {
  return { type: 'image_url', image_url: { url: 'data:image/jpeg;base64,' + bytes.toString('base64'), detail: 'high' } };
}

export async function prepareReferralDocument(base64) {
  const bytes = decodeChefPhoto(base64);
  const input = sharp(bytes, readOptions);
  const meta = await input.metadata();
  if (!uploadFormats.has(meta.format) || !meta.width || !meta.height || (meta.pages || 1) !== 1) {
    throw new Error('invalid_document');
  }
  // Apply EXIF before choosing any crop coordinates. Working with raw pixels
  // also strips GPS, comments and thumbnails without another JPEG generation.
  const { data, info } = await input.autoOrient()
    .resize(3200, 3200, { fit: 'inside', withoutEnlargement: true })
    .flatten({ background: '#fff' }).toColourspace('srgb').removeAlpha().raw().toBuffer({ resolveWithObject: true });
  const { width, height, channels } = info;
  if (Math.min(width, height) < 64 || Math.max(width, height) < 200) throw new Error('thumbnail');
  const pixels = () => sharp(data, { raw: { width, height, channels } });
  const overview = await pixels().resize(1700, 1700, { fit: 'inside', withoutEnlargement: true })
    .jpeg(jpegOptions).toBuffer();
  const content = [
    { type: 'text', text: 'Vista general de la única fotografía. Usa esta vista para contar y situar los recuadros o boletas.' },
    imagePart(overview)
  ];
  const lowResolution = Math.min(width, height) < 500 || Math.max(width, height) < 1000;
  // Even a 1200×1600 sheet can contain ten handwritten cards. The detail
  // views give the vision model a separate view of each area at native scale.
  if (Math.max(width, height) >= 900 && Math.min(width, height) >= 450) {
    const cropWidth = Math.ceil(width * 0.59);
    const cropHeight = Math.ceil(height * 0.6);
    for (const [topLabel, top] of [['superior', 0], ['inferior', height - cropHeight]]) {
      for (const [sideLabel, left] of [['izquierdo', 0], ['derecho', width - cropWidth]]) {
        const rect = { top, left, width: cropWidth, height: cropHeight };
        const crop = await pixels().extract(rect).resize(1650, 1650, { fit: 'inside', withoutEnlargement: true })
          .jpeg(jpegOptions).toBuffer();
        const pct = (value, total) => Math.round(100 * value / total);
        content.push({ type: 'text', text: `Detalle ${topLabel} ${sideLabel} de la MISMA foto, x ${pct(left, width)}–${pct(left + cropWidth, width)}%, y ${pct(top, height)}–${pct(top + cropHeight, height)}% desde la esquina superior izquierda. Hay solapamiento con otros detalles.` }, imagePart(crop));
      }
    }
  }
  return { content, lowResolution };
}
