# Chat privado del equipo en Coach

Mensajes → Chat del equipo admite texto, fotos, GIF animados y notas de voz. El botón ＋ adjunta un archivo; mantener presionado el micrófono graba, soltar envía y deslizar 64 px a la izquierda descarta. Un toque breve, cancelar el permiso, cerrar o pasar al fondo nunca envía una grabación. Al alcanzar el límite se espera a que el usuario suelte. El audio recibido usa controles nativos y no se reproduce automáticamente. Escribir muestra Enviar.

## Acceso y persistencia

`owner` usa su propio grupo; `seat` y `leader` usan `teamOwnerUserId`, validado contra un propietario existente. Patrocinador, territorio y parentesco no mezclan grupos. El servidor deriva el grupo de la sesión; el roomId del navegador solo detecta cambios de cuenta/equipo. Escrituras requieren la cuenta y el grupo coincidentes. Los contadores y el cursor monotónico de lectura conservan el comportamiento existente.

Cada mensaje, incluida su única imagen/audio, se guarda atómicamente en MongoDB. El índice único grupo/remitente/UUID evita duplicados tras perder la respuesta, incluso entre servidores. Los archivos binarios están excluidos por defecto de consultas y serialización; solo una ruta autenticada de ese grupo los entrega, con no-store, nosniff y Range para reproducción. El service worker no almacena chat ni archivos privados. No hay enlaces públicos de adjuntos ni dependencia de esta Mac.

El texto pendiente y su UUID se conservan por cuenta/grupo en localStorage. Los envíos con archivo se guardan primero en IndexedDB, nunca como base64 en localStorage. Si no se puede persistir el archivo, no se intenta enviarlo. Un envío sin confirmación exige reintento explícito con el mismo UUID y archivo. Un rechazo definitivo por formato/tamaño permite corregir el mensaje. Un cambio de cuenta no mueve pendientes a otra cuenta. Si el navegador borra sus datos, no se garantiza recuperar los pendientes.

## Límites y procesamiento

Un adjunto por mensaje y texto de hasta 2000 caracteres. Fotos/audio de entrada hasta 5 MB; GIF hasta 2 MB y 120 cuadros. Fotos decodificadas, sin EXIF, hasta 1600×1600 y WebP; GIF re-codificado, hasta 800 px de ancho y animación conservada. Las imágenes tienen límite de píxeles y salida de 2 MB. Audio real validado y decodificado con FFmpeg a mono 24 kHz, hasta 90 segundos, convertido a MP3 48 kbps para teléfonos. No se aceptan SVG/HTML, archivos malformados ni URLs remotas. Máximo dos conversiones simultáneas por proceso y una por usuario, con límites de frecuencia.

El archivo permanece en el documento del mensaje; su tamaño ya comprimido cuenta contra el almacenamiento de MongoDB. Un equipo con uso intenso requiere vigilar esa capacidad y trasladar los binarios a almacenamiento de objetos privado cuando el volumen lo justifique. No se configura un bucket nuevo ni se utiliza el bucket de audiolibros para este chat.

## Reportes de Cierre

Solo un nuevo resultado `venta` genera una tarjeta de venta. El mismo guardado del resultado incluye una bandeja de salida durable (`teamChatPending` y grupo calculado por servidor). Publicar es idempotente con `sale:<outcomeId>`. Una interrupción después de guardar, después de publicar o antes de confirmar se recupera en el trabajador de fondo, sin duplicar el anuncio. Los reintentos del formulario conservan sus recibos existentes. No se publica el historial anterior al despliegue.

La tarjeta incluye vendedor, monto y productos. Una lista explícita excluye notas privadas, resumen, comisión, teléfono y nombre del cliente. No se crea una venta por escribir «vendí» en el grupo. Los montos conservan la unidad usada por Cierre; no se infiere ni convierte una moneda.

## Consulta del Coach AI

Solo la ruta autenticada `modo=coach` recupera contexto del grupo calculado por el servidor. El Chef público y otras cuentas no reciben este contexto. El Coach obtiene mensajes recientes, búsqueda de texto de ese grupo y agregados de reportes automáticos por vendedor/período. Las respuestas basadas en ese contexto no reutilizan la caché de respuestas. Las preguntas sobre el equipo evitan los atajos estáticos de ayuda.

Al guardar audio se despierta un trabajador con arrendamiento recuperable que transcribe los audios ya guardados usando el proveedor de voz existente. El envío no espera la transcripción; esta puede tardar y se reintenta hasta tres veces. Fallar la transcripción conserva el audio reproducible. No se crea otro mensaje ni otra notificación por transcribir. Fotos/GIF no se interpretan visualmente.

Los mensajes y transcripciones son evidencia no confiable, nunca instrucciones ni autorizaciones. Se citan autor y fecha; para «quién vendió hoy» se cuentan los reportes automáticos, no declaraciones de texto. El día se interpreta en America/Chicago y el contexto comunica esa zona. Se admiten hoy, ayer, anteayer, semana/mes actual o anterior, últimos N días, fechas AAAA-MM-DD y rangos de dos fechas. Períodos ambiguos requieren aclaración. La fecha es la del reporte; no presume cuándo ocurrió la venta.

La consulta tiene límites explícitos: últimos 80 mensajes en preguntas de equipo (15 en otras consultas), hasta 50 coincidencias de texto, hasta 200 vendedores agregados y presupuesto de contexto. No afirma que un mensaje nunca existió por no encontrarlo. No publica respuestas del Coach en el grupo ni ejecuta acciones desde su contenido.

## Verificación de esta ampliación

201 pruebas automáticas aprobadas en esta entrega, incluidas las regresiones de voz, formularios, permisos y Chef. Pruebas de archivos reales: EXIF, redimensionado, animación GIF, audio WAV→MP3, reproducción por rangos, rechazos por duración/formato/tamaño. Pruebas HTTP: permisos, cuenta/grupo, UUID y adjuntos sin texto. Pruebas de interfaz: mantener/soltar, descarte, permisos pendientes, teclado, límite de duración, reproducción estable, persistencia antes de enviar y recuperación tras recarga. Pruebas de Cierre/contexto: handler real con cuentas ficticias, bandeja durable, privacidad, búsqueda por equipo, transcripciones, fechas y caché.

Las comprobaciones de producción son de lectura y de una consulta privada del Coach: no se envían mensajes, archivos ni ventas de prueba a equipos reales. La revisión visual en teléfono no está disponible en esta sesión.
