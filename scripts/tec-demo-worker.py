#!/usr/bin/env python3
"""Process one TEC demonstration at a time with a local MLX Whisper model."""

import argparse
import ctypes
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


RNNOISE_LIBRARY = Path('/Volumes/Modelos_IA/Voz_IA/RNNoise/librnnoise.dylib')


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


def create_rnnoise_filter(path):
    import numpy as np

    library = ctypes.CDLL(os.fspath(path))
    library.rnnoise_get_frame_size.argtypes = []
    library.rnnoise_get_frame_size.restype = ctypes.c_int
    library.rnnoise_create.argtypes = [ctypes.c_void_p]
    library.rnnoise_create.restype = ctypes.c_void_p
    library.rnnoise_destroy.argtypes = [ctypes.c_void_p]
    library.rnnoise_destroy.restype = None
    frame_pointer = np.ctypeslib.ndpointer(dtype=np.float32, ndim=1, flags='C_CONTIGUOUS')
    library.rnnoise_process_frame.argtypes = [ctypes.c_void_p, frame_pointer, frame_pointer]
    library.rnnoise_process_frame.restype = ctypes.c_float
    frame_size = library.rnnoise_get_frame_size()
    if frame_size != 480:
        raise ValueError('RNNoise requiere cuadros de 480 muestras a 48 kHz')

    def reduce_noise(pcm):
        original = np.frombuffer(pcm, dtype='<i2')
        if not original.size:
            return pcm
        # Flush the overlap and lookahead so each chunk keeps its original duration.
        delay = 2 * frame_size
        padded_size = ((original.size + frame_size - 1) // frame_size) * frame_size + delay
        source = np.zeros(padded_size, dtype=np.float32)
        source[:original.size] = original
        cleaned = np.empty_like(source)
        state = library.rnnoise_create(None)
        if not state:
            raise RuntimeError('No se pudo iniciar RNNoise')
        try:
            for raw, filtered in zip(source.reshape(-1, frame_size), cleaned.reshape(-1, frame_size)):
                library.rnnoise_process_frame(state, filtered, raw)
        finally:
            library.rnnoise_destroy(state)

        mixed = cleaned[delay:delay + original.size]
        if not np.isfinite(mixed).all():
            raise ValueError('RNNoise devolvió muestras inválidas')
        # Keep some original signal so quiet words and consonants remain audible.
        mixed *= 0.6
        mixed += source[:original.size] * 0.4
        return np.clip(mixed, -32768, 32767).astype('<i2').tobytes()

    return reduce_noise


def decode_for_whisper(audio, ffmpeg, reduce_noise=None):
    def pcm(args, sample_rate, source_bytes=None):
        command = [ffmpeg, '-nostdin', '-v', 'error', *args, '-map', '0:a:0', '-vn',
                   '-t', '365', '-ac', '1', '-ar', str(sample_rate), '-f', 's16le', 'pipe:1']
        return subprocess.run(command, input=source_bytes, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                              check=True, timeout=45).stdout

    # MP4 from Safari can keep its index at the end and needs a seekable input.
    with tempfile.NamedTemporaryFile(prefix='tec-demo-') as source:
        source.write(audio)
        source.flush()
        args = ['-i', source.name]
        if reduce_noise:
            try:
                cleaned = reduce_noise(pcm(args, 48000))
                if cleaned:
                    converted = pcm(['-f', 's16le', '-ar', '48000', '-ac', '1', '-i', 'pipe:0'], 16000, cleaned)
                    if converted:
                        return converted
            except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
                print(f'RNNoise omitido para este segmento ({type(error).__name__}).', file=sys.stderr, flush=True)
        return pcm(args, 16000)


def local_transcriber(model, ffmpeg, rnnoise=None):
    os.environ['HF_HUB_OFFLINE'] = '1'
    os.environ['TRANSFORMERS_OFFLINE'] = '1'
    import numpy as np
    from mlx_whisper import transcribe

    reduce_noise = None
    if rnnoise:
        try:
            reduce_noise = create_rnnoise_filter(rnnoise)
            print('Reducción de ruido RNNoise activa.', file=sys.stderr, flush=True)
        except (OSError, AttributeError, ValueError) as error:
            print(f'RNNoise no se pudo cargar ({type(error).__name__}).', file=sys.stderr, flush=True)

    def recognize(pcm):
        if not pcm:
            raise ValueError('Sin audio decodificable')
        samples = np.frombuffer(pcm, dtype='<i2').astype(np.float32) / 32768
        if not np.any(samples):
            return '', '', float('-inf')
        result = transcribe(samples, path_or_hf_repo=str(model), verbose=None, condition_on_previous_text=False,
                            temperature=(0.0, 0.2, 0.4))
        segments = [item for item in result['segments'] if item['text'].strip() and
                    item['no_speech_prob'] < 0.85 and item['compression_ratio'] < 2.8]
        if not segments:
            return '', result.get('language') or '', float('-inf')
        durations = [max(0.01, item['end'] - item['start']) for item in segments]
        score = np.average([item['avg_logprob'] for item in segments], weights=durations)
        return ' '.join(item['text'].strip() for item in segments)[:16_000], result.get('language') or '', score

    def process(audio):
        cleaned = decode_for_whisper(audio, ffmpeg, reduce_noise)
        text, language, score = recognize(cleaned)
        if reduce_noise and score < -0.24:
            # In uncertain passages compare the original before discarding quiet words or digits.
            try:
                original = decode_for_whisper(audio, ffmpeg)
                if original != cleaned:
                    raw_text, raw_language, raw_score = recognize(original)
                    if raw_text and raw_score > score + 0.02:
                        return raw_text, raw_language
            except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
                print(f'Comparación con audio original omitida ({type(error).__name__}).', file=sys.stderr, flush=True)
        return text, language

    return process


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', type=Path, required=True)
    parser.add_argument('--origin', default='https://totalelitecoaching.com')
    parser.add_argument('--ffmpeg', default=shutil.which('ffmpeg'))
    noise_filter = parser.add_mutually_exclusive_group()
    noise_filter.add_argument('--rnnoise', type=Path, default=RNNOISE_LIBRARY if RNNOISE_LIBRARY.is_file() else None)
    noise_filter.add_argument('--no-denoise', action='store_const', const=None, dest='rnnoise')
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

    transcribe = local_transcriber(model, args.ffmpeg, args.rnnoise)
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
