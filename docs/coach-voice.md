# Mensajes de voz del Coach

La regla se decide por mensaje: texto → respuesta escrita; audio → transcripción enviada al mismo Coach, respuesta escrita y voz generada por IA. Se conservan el contexto de la visita, las herramientas y la conversación existente. No se usa una conversación de voz diferente para contestar: el servicio de voz lee la respuesta del Coach.

## Uso

En la ventana Coach hay un único micrófono junto al campo vacío. Mantenerlo presionado graba; soltarlo envía automáticamente. Deslizar al menos 64 px a la izquierda muestra la papelera activa y soltar borra sin enviar. Volver hacia el micrófono antes de soltar restaura el envío. Mientras graba se muestran ondas animadas, tiempo y la indicación para cancelar. Al escribir, el botón de texto reemplaza al micrófono. Se retiraron los botones de adjuntar, terminar y confirmar del flujo normal.

Los toques inferiores a 350 ms no envían. Hasta 90 segundos y 8 MB; al llegar al límite se detiene el micrófono y se espera que el usuario suelte para enviar o deslice para borrar. Soltar antes de conceder el permiso invalida la captura: cualquier micrófono tardío se detiene. El cierre del Coach, segundo plano, pérdida de foco, cancelación del puntero o Escape descartan la captura sin enviar. La primera vez el navegador puede pedir permiso; luego hay que mantener presionado de nuevo.

Con teclado se mantiene Espacio o Enter y se suelta para enviar; Escape borra. La activación de tecnología de asistencia sin eventos de teclado/puntero permite iniciar y finalizar con el mismo botón. Si un envío falla aparecen la reproducción del audio pendiente, reintentar y borrar; esos controles solo se muestran para recuperar el error.

El texto transcrito y el audio original aparecen en la pregunta. Solo esa respuesta recibe voz. La respuesta de voz tiene controles de reproducción y la indicación «Voz generada por IA». La reproducción automática se intenta mientras la ventana está visible; si el navegador la bloquea, el usuario pulsa reproducir. La entrada de audio no abre el teclado al terminar. Cerrar el Coach pausa la reproducción.

Si falla la transcripción, la grabación queda disponible para reintentar. Si falla el chat, se conserva el borrador de audio. Si falla la voz, el texto de respuesta permanece y «Reintentar audio» no repite la pregunta al modelo del Coach. Grabaciones y reproducciones se mantienen en memoria en esta primera versión; recargar descarta esos archivos locales. No hay envío automático sin conexión.

## Servidor y proveedores

`src/coach-voice.js` registra `/api/coach/voice/transcribe` (audio binario) y `/api/coach/voice/speech` (JSON, audio WAV de respuesta). Ambos exigen sesión activa y cuenta coincidente, usan la protección de origen existente y respuestas privadas. Hay límites por cuenta (40 operaciones cada 15 minutos) y una operación simultánea. No se aceptan URLs de audio del cliente. La pregunta transcrita utiliza `/chat`, con los mismos límites diarios del plan y un control de cuenta para pestañas antiguas.

La clave de OpenAI permanece en el servidor. La transcripción usa `COACH_VOICE_TRANSCRIPTION_MODEL`, o `COACH_DEMO_TRANSCRIPTION_MODEL`, o `gpt-transcribe`. La lectura utiliza Realtime por WebSocket con `gpt-realtime-2.1-mini` y voz `ash`; `COACH_VOICE_SPEECH_MODEL` y `COACH_VOICE_NAME` permiten configurar alternativas. Se usa el WebSocket de la dependencia `undici` ya instalada. La voz recibe solo el texto de respuesta y la instrucción de leerlo; no tiene herramientas ni el historial de la visita. Los fragmentos PCM a 24 kHz se ensamblan en un WAV completo; no se entrega un audio incompleto como si hubiera terminado correctamente.

Referencias oficiales revisadas el 17 de noviembre de 2026: [transcripción](https://developers.openai.com/api/docs/guides/speech-to-text), [voz y Realtime](https://developers.openai.com/api/docs/guides/realtime-conversations), [WebSocket](https://developers.openai.com/api/docs/guides/voice-websockets?voice-api=realtime). Se evita iniciar una dependencia de los modelos Speech API que tienen cierre anunciado para enero de 2027.

## Validación

53 pruebas cubren la regla por mensaje, respuesta escrita ante fallo de voz, autorización/cuenta/origen, MIME/tamaño, silencio, límites de uso, contratos de los proveedores, WAV completo y regresiones del chat, tarjetas, boletines, navegación y formularios sin conexión.

La vista local usa la plantilla real y el código real del compositor, con un micrófono simulado mediante AudioContext, proveedores ficticios y archivos sintéticos generados con la voz del sistema. Se comprobaron grabación/vista previa/envío, archivo adjunto, texto después de audio, cierre con captura, permiso denegado, conservación del audio tras fallo, reintento de voz sin nuevo turno del Coach y vista móvil de 390×844. Ninguna prueba local captura micrófonos o conversaciones reales.

## Voz natural y menor espera

El usuario aclaró que busca una voz claramente masculina, natural como las narraciones BF16, sin exigir la identidad exacta de Damián y con poca espera. Se usa la voz conversacional `ash` del servicio en línea; la Mac no genera la voz de los usuarios ni necesita permanecer encendida. La muestra de la voz del sistema era solo de la prueba local. Una muestra BF16 de Damián quedó aislada como referencia de calidad y no se conectó al servicio.

Las peticiones nuevas de voz reciben PCM por NDJSON y el navegador reproduce los primeros fragmentos con Web Audio mientras se completa el resto. La reproducción se habilita desde el gesto de soltar el micrófono, conserva la detección de segundo plano y ofrece «Detener voz». Al finalizar se conserva un WAV completo para escuchar de nuevo; un flujo incompleto no se presenta como archivo terminado. Los clientes anteriores siguen recibiendo WAV. Las respuestas solicitadas por audio usan un estilo conversacional breve y una clave de caché distinta; el texto mantiene su estilo existente.

La primera prueba real con la voz anterior tardó unos 13,6 segundos hasta el archivo completo. Esa cifra no es una garantía; la optimización se verifica por el comienzo de reproducción, no por cuánto tarda en terminar el archivo.

## Interacción mínima · 17 noviembre 2026

94 pruebas pasan en conjunto, incluidas diez nuevas de mantener/soltar, cancelar y volver desde la papelera, permisos tardíos o denegados, interrupciones, teclado, límite y reintento. La revisión visual con la plantilla real y micrófono sintético confirmó un solo envío al soltar, ningún envío al deslizar y la interfaz de 390×844. Las pruebas iniciales del apartado de validación corresponden a la interfaz anterior; sus flujos de adjunto y confirmación fueron reemplazados.

## Jorge, español de México

En Render, configurar juntas `AZURE_SPEECH_KEY` y `AZURE_SPEECH_REGION` con los valores del mismo recurso de Azure Speech. El servidor lee cada respuesta con `es-MX-JorgeNeural` en estilo de conversación. Envía a Microsoft el texto que debe leer. La grabación original continúa por el servicio de transcripción existente. El teléfono recibe PCM a 24 kHz y puede reproducirlo mientras llega.

Con ambas variables vacías sigue activo el proveedor anterior. Una configuración parcial o un fallo de Azure conserva la respuesta escrita y permite reintentar con Jorge. Para validar con el servicio real, ejecutar `node scripts/coach-voice-smoke.mjs` en el entorno configurado y probar una pregunta nueva por micrófono en el Coach.
