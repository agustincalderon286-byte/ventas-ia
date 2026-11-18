import sharp from 'sharp';
import ffmpeg from 'ffmpeg-static';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';

export const CHAT_MEDIA_MAX_BYTES = 5 * 1024 * 1024;
const audioFormats = new Map([['audio/webm','matroska'],['video/webm','matroska'],['audio/mp4','mov'],['audio/x-m4a','mov'],['audio/mpeg','mp3'],['audio/mp3','mp3'],['audio/wav','wav'],['audio/x-wav','wav'],['audio/ogg','ogg']]);
const invalid = message => Object.assign(new Error(message), { status: 400 });
export function decodeChatMedia(value) {
  if (!value || typeof value !== 'object' || typeof value.base64 !== 'string' || value.base64.length > Math.ceil(CHAT_MEDIA_MAX_BYTES / 3) * 4 || (value.base64.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(value.base64))) throw invalid('El archivo no es válido o supera 5 MB.');
  const data = Buffer.from(value.base64, 'base64');
  if (!data.length || data.length > CHAT_MEDIA_MAX_BYTES) throw invalid('El archivo no es válido o supera 5 MB.');
  const mime = String(value.mime || '').split(';')[0].toLowerCase();
  if (!['image/jpeg','image/png','image/webp','image/gif',...audioFormats.keys()].includes(mime)) throw invalid('Adjunta una foto, un GIF o un audio.');
  return { data, mime, hash: createHash('sha256').update(mime).update(data).digest('hex') };
}
function convertAudio(data, format, output = false) {
  return new Promise((resolve, reject) => {
    const args = output
      ? ['-f','s16le','-ar','24000','-ac','1','-i','pipe:0','-c:a','libmp3lame','-b:a','48k','-f','mp3','pipe:1']
      : ['-protocol_whitelist','pipe','-f',format,'-i','pipe:0','-vn','-t','91','-ac','1','-ar','24000','-f','s16le','pipe:1'];
    const child = spawn(ffmpeg, ['-hide_banner','-loglevel','error','-threads','1',...args], { stdio: ['pipe','pipe','pipe'] });
    const chunks = []; let bytes = 0, settled = false;
    const finish = error => { if (settled) return; settled = true; clearTimeout(timer); if (error) { child.kill('SIGKILL'); reject(error); } else resolve(Buffer.concat(chunks)); };
    const timer = setTimeout(() => finish(invalid('No pude procesar el audio. Usa una grabación de hasta 90 segundos.')), 20000);
    child.stdout.on('data', chunk => { bytes += chunk.length; if (bytes > 4500000) finish(invalid('El audio es demasiado largo.')); else chunks.push(chunk); });
    child.stderr.resume(); child.stdin.on('error', () => {});
    child.on('error', () => finish(Object.assign(new Error('El procesador de audio no está disponible.'), { status: 503 })));
    child.on('close', code => finish(code === 0 && bytes ? null : invalid('No pude leer este audio. Vuelve a grabarlo.')));
    child.stdin.end(data);
  });
}
export async function decodeChatAudioToPcm(data, mime) {
  const format = audioFormats.get(mime);
  if (!format) throw invalid('Usa un audio WebM, M4A, MP3, WAV u Ogg.');
  const pcm = await convertAudio(data, format);
  const duration = pcm.length / 48000;
  if (duration < .35 || duration > 90) throw invalid('Graba entre medio segundo y 90 segundos.');
  return pcm;
}
export async function prepareChatMedia(raw) {
  const { data, mime, hash } = raw;
  if (audioFormats.has(mime)) {
    const pcm = await decodeChatAudioToPcm(data, mime);
    const duration = pcm.length / 48000;
    const audio = await convertAudio(pcm, '', true);
    return { data: audio, meta: { kind: 'audio', mime: 'audio/mpeg', bytes: audio.length, duration, hash } };
  }
  try {
    const image = sharp(data, { animated: mime === 'image/gif', limitInputPixels: 24000000 });
    const metadata = await image.metadata();
    const expected = { 'image/jpeg': 'jpeg', 'image/png': 'png', 'image/webp': 'webp', 'image/gif': 'gif' }[mime];
    if (metadata.format !== expected || !metadata.width || !metadata.height) throw new Error('format');
    if (mime === 'image/gif') {
      if (data.length > 2 * 1024 * 1024 || (metadata.pages || 1) > 120 || metadata.width * metadata.height > 24000000) throw new Error('gif limits');
      // Decode and re-encode all frames, retaining animation and discarding metadata.
      const encoded = await image.resize({ width: 800, withoutEnlargement: true }).gif().toBuffer();
      if (encoded.length > 2 * 1024 * 1024) throw new Error('gif size');
      const size = await sharp(encoded,{animated:true}).metadata();
      return { data: encoded, meta: { kind: 'gif', mime: 'image/gif', bytes: encoded.length, width: size.width, height: size.pageHeight || size.height, hash } };
    }
    const encoded = await image.rotate().resize({ width: 1600, height: 1600, fit: 'inside', withoutEnlargement: true }).webp({ quality: 80 }).toBuffer();
    if (encoded.length > 2 * 1024 * 1024) throw new Error('photo size');
    const size = await sharp(encoded).metadata();
    return { data: encoded, meta: { kind: 'image', mime: 'image/webp', bytes: encoded.length, width: size.width, height: size.height, hash } };
  } catch { throw invalid('No pude leer la imagen. Usa una foto de hasta 5 MB o un GIF de hasta 2 MB y 120 cuadros.'); }
}
export function chatMediaRange(value, length) {
  if (!value) return null;
  const match = /^bytes=(\d*)-(\d*)$/.exec(value);
  if (!match || (!match[1] && !match[2])) return false;
  const start = match[1] ? Number(match[1]) : Math.max(0, length - Number(match[2]));
  const end = match[1] && match[2] ? Math.min(length - 1, Number(match[2])) : length - 1;
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start > end || start >= length) return false;
  return { start, end };
}
