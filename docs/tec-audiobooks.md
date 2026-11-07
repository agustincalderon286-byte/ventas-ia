# Audiolibros TEC

## El Kybalión — 7 de noviembre de 2026

Se añade *El Kybalión*, de Tres Iniciados, en español actual: epígrafe y dedicatoria, introducción y quince capítulos; duración total 13.055,77 segundos. La lectura conserva exactamente el maestro cotejado mediante 633 pasajes sincronizados, SHA-256 `dbae31de57200c1b4066b095e5037c51656df77062a4d8d7eb9671eac6697401`.

La copia web completa se sirve desde el objeto privado R2 `El-Kybalion-BF16-solo-voz-web.m4b`, de 255.751.084 bytes, SHA-256 `824cd816d8b01334825f8fb7503958d6c54855cecf92f97d4eb9cee6bf306cd6` y ETag `6060d571e6b358d0fb49341398408ca3`, verificado desde el servicio. Conserva los 17 capítulos y la duración del maestro del SSD; el maestro original permanece intacto. El catálogo y la muestra pública usan la misma edición. La muestra comprende el primer 10 %: 1.305,58 segundos, sin capítulos incrustados. Incluye portada ilustrada y arte para los controles multimedia.

La incorporación es aditiva y conserva los siete títulos anteriores, sus cuentas, pagos, reseñas y avances. Las comprobaciones técnicas y automáticas no sustituyen una escucha editorial íntegra.

La pestaña Audiolibros usa las sesiones y el acceso operativo existente de TEC. El enlace directo es `/coach/app/#audiolibros`. Incluye catálogo, ficha, reproducción por capítulos, velocidad, avance privado por cuenta y una reseña de 1–5 estrellas por cuenta y libro. La función no crea suscripciones ni cobros adicionales.

El catálogo editorial está fijado en `src/knowledge/tec-audiobooks.json`. El primer título es *Como un hombre piensa*, de James Allen: nueve partes, 48:11, español, voz BF16 fija y sin fondos. El SHA-256 del M4B es `d614e63ebc3d519e98a2ed55574a63da6a30d5870821bc425bc5d872ab8905b8`. El archivo original y la revisión técnica se conservan en el SSD. No se regeneró audio ni se cambió la biblioteca de pruebas de Sites.

El audio se guarda una sola vez en MongoDB GridFS, bucket `tec_audiobooks`, separado de las pruebas privadas de usuarios. No se incluye el binario en Git ni en la carpeta pública. `TecAudiobookMedia` publica únicamente archivos cuyo hash coincide con el manifiesto. La descarga exige una cuenta activa, admite HEAD y rangos HTTP para móviles y no se almacena en la caché compartida de la PWA.

Las entradas con `audio.r2Key` se leen del bucket privado `tec-audiolibros` de Cloudflare R2. En Render se configuran `TEC_AUDIO_R2_ACCOUNT_ID`, `TEC_AUDIO_R2_ACCESS_KEY_ID` y `TEC_AUDIO_R2_SECRET_ACCESS_KEY` con una credencial Object Read only limitada a ese bucket. TEC verifica el tamaño y ETag fijados en el catálogo y sirve los rangos desde el propio dominio después de verificar el acceso. Los archivos completos de estas entradas se gestionan en R2.

Las colecciones `TecAudiobookReview` y `TecAudiobookProgress` usan identificadores canónicos de libro y usuario para evitar duplicados concurrentes. La identidad siempre viene de la sesión. Las respuestas de reseñas no exponen IDs de usuario ni correos. Los comentarios se insertan como texto, nunca HTML. Las escrituras rechazan orígenes ajenos.

La instalación inicial usa un permiso aleatorio temporal cuyo secreto se conserva exclusivamente en el SSD. El servidor conoce sólo su hash y caducidad; permite importar exclusivamente los bytes y hash del libro fijado y no puede reemplazar un libro listo, leer cuentas ni modificar el catálogo. Se elimina ese permiso del despliegue después de la carga. La ruta de importación restante requiere administración de TEC y sigue verificando tamaño y SHA-256. Una carga fallida no publica el audio ni borra las ediciones locales.

Para la pantalla de bloqueo en `/audiolibros/`, añade al libro un campo `mediaArtwork` con su portada cuadrada PNG de 512 × 512 píxeles en `public/audiolibros/portadas/`. La página envía esa imagen al teléfono durante la reproducción de la muestra y del libro completo.

Verificación: `npm test`. Las pruebas de integración cubren autenticación, rangos/HEAD, permisos, carga corrupta/repetida, reseñas concurrentes y ajenas, paginación y progreso. Se verificó la interfaz con Chrome aislado, el M4B real, navegación por capítulos, velocidad, reseñas con HTML inerte y pantalla móvil. Las reseñas de esas pruebas existen sólo en memoria/local y nunca se publican en TEC. La escucha editorial completa sigue pendiente.

Reversión: revertir el commit de interfaz/rutas. Conservar las colecciones y GridFS para no perder reseñas, avance o audio; no ejecutar migraciones destructivas. El cambio es aditivo.

## Lectura y Acres de diamantes — 3 de noviembre de 2026

La biblioteca incluye Acres de diamantes (Russell H. Conwell), versión completa en español de 1:31:35 con narrador, personajes y música instrumental. El audio musical se conserva en una producción independiente del SSD; no se regeneraron ni cambiaron las voces. La ficha pública usa créditos editoriales de TEC, sin notas de producción o estado interno. La procedencia y los límites de revisión permanecen en los informes internos del SSD.

Ambos títulos incluyen texto completo autenticado en `/api/coach/audiobooks/:id/text`. Los pasajes conservan exactamente el maestro español y se alinean a los segmentos de reconocimiento de las tomas originales dentro de la línea de tiempo del libro. La sincronización es por pasajes, no una afirmación de alineación exacta de cada fonema. Al escuchar se resalta y sigue el pasaje; al desplazarse manualmente se desactiva el seguimiento hasta volver a marcarlo. Tocar un pasaje lleva el reproductor a ese punto. La velocidad, capítulos y avance por cuenta usan el mismo audio y reloj; no hay un segundo reproductor. Los textos no entran en la caché compartida de la PWA y la ruta exige el mismo acceso que el audio.

Verificación: 782 pruebas completas correctas, además de revisión de interfaz con archivos de audio reales, salto desde un pasaje, velocidad 1.5×, seguimiento, texto íntegro y pantalla móvil. No se escriben reseñas de prueba en producción.
