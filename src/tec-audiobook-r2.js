import { GetObjectCommand, HeadObjectCommand, S3Client } from '@aws-sdk/client-s3';

export function createAudiobookR2(env = process.env) {
  const accountId = env.TEC_AUDIO_R2_ACCOUNT_ID?.trim();
  const accessKeyId = env.TEC_AUDIO_R2_ACCESS_KEY_ID?.trim();
  const secretAccessKey = env.TEC_AUDIO_R2_SECRET_ACCESS_KEY?.trim();
  if (!/^[a-f0-9]{32}$/.test(accountId || '') || !accessKeyId || !secretAccessKey) return null;

  const client = new S3Client({
    endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
    region: 'auto',
    credentials: { accessKeyId, secretAccessKey },
    forcePathStyle: true,
    maxAttempts: 1,
    requestHandler: { connectionTimeout: 2000, socketTimeout: 12000 }
  });
  const checks = new Map();
  const object = audio => ({ Bucket: 'tec-audiolibros', Key: audio.r2Key, IfMatch: audio.r2ETag });

  return {
    ready(audio) {
      const cached = checks.get(audio.r2Key);
      if (cached?.until > Date.now()) return cached.result;
      const check = { until: Date.now() + 5000 };
      check.result = client.send(new HeadObjectCommand(object(audio)), { abortSignal: AbortSignal.timeout(3500) })
        .then(result => {
          const ready = result.ContentLength === audio.bytes && result.ETag === audio.r2ETag;
          check.until = Date.now() + (ready ? 60000 : 10000);
          return ready;
        }).catch(() => {
          check.until = Date.now() + 10000;
          return false;
        });
      checks.set(audio.r2Key, check);
      return check.result;
    },
    async open(audio, range, signal) {
      const contentRange = range ? `bytes ${range.start}-${range.end}/${audio.bytes}` : undefined;
      const length = range ? range.end - range.start + 1 : audio.bytes;
      const result = await client.send(new GetObjectCommand({
        ...object(audio),
        Range: range ? `bytes=${range.start}-${range.end}` : undefined
      }), { abortSignal: signal });
      if (!result.Body || result.ETag !== audio.r2ETag || result.ContentLength !== length || result.ContentRange !== contentRange) {
        result.Body?.destroy();
        throw new Error('Unexpected R2 audiobook response');
      }
      return result.Body;
    }
  };
}
