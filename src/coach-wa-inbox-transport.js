import { jorgeSpeechDocument, speechText, VOICE_MAX_BYTES } from './coach-voice.js';

const api = 'https://api.twilio.com';
const types = new Set(['audio/ogg', 'audio/mpeg', 'audio/mp4']);
const failure = () => new Error('WhatsApp media or messaging request failed');

async function readAudio(response) {
  if (!response.ok || !response.body) throw failure();
  const mime = (response.headers.get('content-type') || '').split(';')[0];
  // WhatsApp may return an octet stream. Only accept real Ogg in that case.
  if (!types.has(mime) && mime !== 'application/octet-stream') throw failure();
  const reader = response.body.getReader();
  const chunks = [];
  let size = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.length;
    if (size > VOICE_MAX_BYTES) {
      await reader.cancel();
      throw failure();
    }
    chunks.push(value);
  }
  const buffer = Buffer.concat(chunks, size);
  if (!size || (mime === 'application/octet-stream' && buffer.subarray(0, 4).toString() !== 'OggS')) throw failure();
  return { buffer, mime: mime === 'application/octet-stream' ? 'audio/ogg' : mime };
}

export function coachWaInboxTransport(config, { env = process.env, fetcher = fetch, storeAudio } = {}) {
  const authorization = 'Basic ' + Buffer.from(config.accountSid + ':' + config.authToken).toString('base64');
  const accountPath = '/2010-04-01/Accounts/' + config.accountSid;
  const from = 'whatsapp:+' + config.phone;
  function request(url, init = {}, authenticate = true) {
    return fetcher(url, {
      ...init, redirect: 'manual', signal: AbortSignal.timeout(12000),
      headers: { ...(authenticate ? { Authorization: authorization } : {}), ...init.headers }
    });
  }
  async function json(response) {
    if (!response.ok) throw failure();
    return response.json();
  }

  return {
    async lineStatus() {
      const info = await json(await request('https://messaging.twilio.com/v2/Channels/Senders?Channel=whatsapp&PageSize=100'));
      const sender = info.senders?.find(item => item.sender_id === from);
      if (!sender) return 'not-registered';
      if (!['ONLINE', 'ONLINE:UPDATING'].includes(sender.status)) return 'offline';
      try {
        const callback = new URL(sender.webhook?.callback_url);
        const hosts = ['totalelitecoaching.com', 'www.totalelitecoaching.com', 'cmwf-crm-api.onrender.com'];
        return callback.protocol === 'https:' && !callback.port && hosts.includes(callback.hostname)
          && callback.pathname === '/webhooks/twilio/whatsapp'
          && (sender.webhook.callback_method || 'POST').toUpperCase() === 'POST' ? 'online' : 'webhook-pending';
      } catch { return 'webhook-pending'; }
    },
    async audio(media) {
      if (!/^(?:SM|MM)[a-f0-9]{32}$/i.test(media.message) || !/^ME[a-f0-9]{32}$/i.test(media.id)) throw failure();
      let response = await request(api + accountPath + '/Messages/' + media.message + '/Media/' + media.id);
      if (response.status >= 300 && response.status < 400) {
        const url = new URL(response.headers.get('location'));
        if (url.protocol !== 'https:' || url.port || url.username || url.password ||
          !/(?:^|\.)(?:twilio\.com|twiliocdn\.com)$/.test(url.hostname)) throw failure();
        response = await request(url, {}, false);
      }
      return readAudio(response);
    },
    async voice(text) {
      const region = env.AZURE_SPEECH_REGION;
      if (!env.AZURE_SPEECH_KEY || !/^[a-z0-9]+$/.test(region || '')) return null;
      const audio = await readAudio(await fetcher('https://' + region + '.tts.speech.microsoft.com/cognitiveservices/v1', {
        method: 'POST', redirect: 'error', signal: AbortSignal.timeout(12000),
        headers: {
          'Ocp-Apim-Subscription-Key': env.AZURE_SPEECH_KEY,
          'Content-Type': 'application/ssml+xml',
          'X-Microsoft-OutputFormat': 'ogg-24khz-16bit-mono-opus',
          'User-Agent': 'TEC-Coach'
        },
        body: jorgeSpeechDocument(speechText(text))
      }));
      return storeAudio(audio.buffer);
    },
    async send(to, text, mediaUrl = null) {
      const form = new URLSearchParams({ From: from, To: 'whatsapp:+' + to });
      if (mediaUrl) form.set('MediaUrl', mediaUrl);
      else form.set('Body', text.slice(0, 1600));
      await json(await request(api + accountPath + '/Messages.json', { method: 'POST', body: form }));
    }
  };
}
