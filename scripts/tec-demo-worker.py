#!/usr/bin/env python3
"""Process one TEC demonstration at a time with a local MLX Whisper model."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class Queue:
    def __init__(self, origin, key):
        self.root = origin.rstrip('/') + '/api/coach/demo-worker'
        self.key = key
        self.opener = urllib.request.build_opener(NoRedirect)

    def request(self, path, *, payload=None, lease=None, maximum=8 * 1024 * 1024, timeout=35):
        headers = {'Authorization': f'Bearer {self.key}'}
        data = None
        if lease:
            headers['X-TEC-Demo-Lease'] = lease
        if payload is not None:
            headers['Content-Type'] = 'application/json'
            data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        request = urllib.request.Request(self.root + path, data=data, headers=headers)
        with self.opener.open(request, timeout=timeout) as response:
            result = response.read(maximum + 1)
            if len(result) > maximum:
                raise ValueError('Respuesta demasiado grande')
            return result

    def run_one(self, transcribe):
        claimed = self.request('/claim', payload={}, maximum=16_384)
        if not claimed:
            return False
        job = json.loads(claimed)
        job_path = '/' + urllib.parse.quote(job['id'], safe='')
        lease = job['lease']
        try:
            chunks = []
            for chunk in sorted(job['chunks'], key=lambda item: item['index']):
                index = int(chunk['index'])
                audio = self.request(f'{job_path}/audio/{index}', lease=lease)
                text, language = transcribe(audio)
                chunks.append({'index': index, 'text': text, 'language': language})
            result = {'chunks': chunks}
        except urllib.error.HTTPError as error:
            result = {'expiredAudio': True} if error.code == 404 else {'failed': True}
        except Exception as error:
            print(f'Falló la transcripción local ({type(error).__name__}).', file=sys.stderr, flush=True)
            result = {'failed': True}
        try:
            self.request(job_path + '/result', payload=result, lease=lease, maximum=256, timeout=260)
        except urllib.error.HTTPError as error:
            if error.code != 409:  # Another lease took over, or the owner deleted this job.
                raise
        return True


def local_transcriber(model, ffmpeg):
    os.environ['HF_HUB_OFFLINE'] = '1'
    os.environ['TRANSFORMERS_OFFLINE'] = '1'
    import numpy as np
    from mlx_whisper import transcribe

    def process(audio):
        # MP4 from Safari can keep the index at the end, so the decoder must be able to seek.
        with tempfile.NamedTemporaryFile(prefix='tec-demo-') as source:
            source.write(audio)
            source.flush()
            command = [ffmpeg, '-nostdin', '-v', 'error', '-i', source.name, '-map', '0:a:0', '-vn',
                       '-t', '365', '-ac', '1', '-ar', '16000', '-f', 's16le', 'pipe:1']
            pcm = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                 check=True, timeout=45).stdout
        if not pcm:
            raise ValueError('Sin audio decodificable')
        samples = np.frombuffer(pcm, dtype='<i2').astype(np.float32) / 32768
        if not np.any(samples):
            return '', ''
        result = transcribe(samples, path_or_hf_repo=str(model), verbose=None, condition_on_previous_text=False,
                            temperature=(0.0, 0.2, 0.4))
        text = ' '.join(segment['text'].strip() for segment in result['segments']
                        if segment['no_speech_prob'] < 0.85 and segment['compression_ratio'] < 2.8)
        language = result.get('language') or ''
        return text[:16_000], language

    return process


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', type=Path, required=True)
    parser.add_argument('--origin', default='https://totalelitecoaching.com')
    parser.add_argument('--ffmpeg', default=shutil.which('ffmpeg'))
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    key = os.environ.get('COACH_DEMO_LOCAL_WORKER_KEY', '').strip()
    origin = urllib.parse.urlsplit(args.origin)
    if len(key) < 32 or not origin.hostname or origin.username or origin.password or origin.query or origin.fragment or origin.path not in ('', '/') or (
        origin.scheme != 'https' and (origin.scheme != 'http' or origin.hostname not in ('localhost', '127.0.0.1', '::1'))
    ):
        parser.error('Configura COACH_DEMO_LOCAL_WORKER_KEY y el origen HTTPS de TEC.')
    model = args.model.expanduser().resolve()
    if not (model / 'config.json').is_file() or not any((model / f).is_file() for f in ('weights.safetensors', 'weights.npz')):
        parser.error('Indica el directorio del modelo MLX instalado en la Mac.')
    if not args.ffmpeg or not Path(args.ffmpeg).is_file():
        parser.error('Indica la ruta local de ffmpeg.')

    transcribe = local_transcriber(model, args.ffmpeg)
    queue = Queue(args.origin, key)
    while True:
        try:
            ran = queue.run_one(transcribe)
        except (OSError, ValueError) as error:
            code = f' HTTP {error.code}' if isinstance(error, urllib.error.HTTPError) else ''
            print(f'Cola de TEC temporalmente inaccesible ({type(error).__name__}{code}).', file=sys.stderr, flush=True)
            if args.once:
                return 1
            time.sleep(30)
            continue
        if args.once:
            return 0
        if not ran:
            time.sleep(20)


if __name__ == '__main__':
    sys.exit(main())
