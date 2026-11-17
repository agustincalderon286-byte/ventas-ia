# Mensajes de voz del Coach

La regla se decide por mensaje: texto → respuesta escrita; audio → transcripción enviada al mismo Coach, respuesta escrita y voz generada por IA. Se conservan el contexto de la visita, las herramientas y la conversación existente. No se usa una conversación de voz diferente para contestar: el servicio de voz lee la respuesta del Coach.

## Uso

En la ventana Coach, «Grabar audio» pide acceso al micrófono y «Terminar grabación» prepara una vista previa. El usuario puede escuchar, descartar o enviar. Se admiten hasta 90 segundos o un archivo de audio de hasta 8 MB. «Adjuntar audio» permite usar archivos M4A, MP3, WebM, WAV u Ogg sin grabar. El cierre del Coach o el paso a segundo plano detienen la captura; no se graba en el fondo. Un permiso denegado muestra cómo continuar con un archivo.

El texto transcrito y el audio original aparecen en la pregunta. Solo esa respuesta recibe voz. La respuesta de voz tiene controles de reproducción y la indicación «Voz generada por IA». La reproducción automática se intenta mientras la ventana está visible; si el navegador la bloquea, el usuario pulsa reproducir. La entrada de audio no abre el teclado al terminar. Cerrar el Coach pausa la reproducción.

Si falla la transcripción, la grabación queda disponible para reintentar. Si falla el chat, se conserva el borrador de audio. Si falla la voz, el texto de respuesta permanece y «Reintentar audio» no repite la pregunta al modelo del Coach. Grabaciones y reproducciones se mantienen en memoria en esta primera versión; recargar descarta esos archivos locales. No hay envío automático sin conexión.

## Servidor y proveedores

`src/coach-voice.js` registra `/api/coach/voice/transcribe` (audio binario) y `/api/coach/voice/speech` (JSON, audio WAV de respuesta). Ambos exigen sesión activa y cuenta coincidente, usan la protección de origen existente y respuestas privadas. Hay límites por cuenta (40 operaciones cada 15 minutos) y una operación simultánea. No se aceptan URLs de audio del cliente. La pregunta transcrita utiliza `/chat`, con los mismos límites diarios del plan y un control de cuenta para pestañas antiguas.

La clave de OpenAI permanece en el servidor. La transcripción usa `COACH_VOICE_TRANSCRIPTION_MODEL`, o `COACH_DEMO_TRANSCRIPTION_MODEL`, o `gpt-transcribe`. La lectura utiliza Realtime por WebSocket con `gpt-realtime-2.1-mini` y voz `cedar`; `COACH_VOICE_SPEECH_MODEL` y `COACH_VOICE_NAME` permiten configurar alternativas. Se usa el WebSocket de la dependencia `undici` ya instalada. La voz recibe solo el texto de respuesta y la instrucción de leerlo; no tiene herramientas ni el historial de la visita. Los fragmentos PCM a 24 kHz se ensamblan en un WAV completo; no se entrega un audio incompleto como si hubiera terminado correctamente.

Referencias oficiales revisadas el 17 de noviembre de 2026: [transcripción](https://developers.openai.com/api/docs/guides/speech-to-text), [voz y Realtime](https://developers.openai.com/api/docs/guides/realtime-conversations), [WebSocket](https://developers.openai.com/api/docs/guides/voice-websockets?voice-api=realtime). Se evita iniciar una dependencia de los modelos Speech API que tienen cierre anunciado para enero de 2027.

## Validación

51 pruebas cubren la regla por mensaje, respuesta escrita ante fallo de voz, autorización/cuenta/origen, MIME/tamaño, silencio, límites de uso, contratos de los proveedores, WAV completo y regresiones del chat, tarjetas, boletines, navegación y formularios sin conexión.

La vista local usa la plantilla real y el código real del compositor, con un micrófono simulado mediante AudioContext, proveedores ficticios y archivos sintéticos generados con la voz del sistema. Se comprobaron grabación/vista previa/envío, archivo adjunto, texto después de audio, cierre con captura, permiso denegado, conservación del audio tras fallo, reintento de voz sin nuevo turno del Coach y vista móvil de 390×844. Ninguna prueba local captura micrófonos o conversaciones reales.
