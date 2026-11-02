# Audiolibros TEC

La pestaña Audiolibros usa las sesiones y el acceso operativo existente de TEC. El enlace directo es `/coach/app/#audiolibros`. Incluye catálogo, ficha, reproducción por capítulos, velocidad, avance privado por cuenta y una reseña de 1–5 estrellas por cuenta y libro. La función no crea suscripciones ni cobros adicionales.

El catálogo editorial está fijado en `src/knowledge/tec-audiobooks.json`. El primer título es *Como un hombre piensa*, de James Allen: nueve partes, 48:11, español, voz BF16 fija y sin fondos. El SHA-256 del M4B es `d614e63ebc3d519e98a2ed55574a63da6a30d5870821bc425bc5d872ab8905b8`. El archivo original y la revisión técnica se conservan en el SSD. No se regeneró audio ni se cambió la biblioteca de pruebas de Sites.

El audio se guarda una sola vez en MongoDB GridFS, bucket `tec_audiobooks`, separado de las pruebas privadas de usuarios. No se incluye el binario en Git ni en la carpeta pública. `TecAudiobookMedia` publica únicamente archivos cuyo hash coincide con el manifiesto. La descarga exige una cuenta activa, admite HEAD y rangos HTTP para móviles y no se almacena en la caché compartida de la PWA.

Las colecciones `TecAudiobookReview` y `TecAudiobookProgress` usan identificadores canónicos de libro y usuario para evitar duplicados concurrentes. La identidad siempre viene de la sesión. Las respuestas de reseñas no exponen IDs de usuario ni correos. Los comentarios se insertan como texto, nunca HTML. Las escrituras rechazan orígenes ajenos.

La instalación inicial usa un permiso aleatorio temporal cuyo secreto se conserva exclusivamente en el SSD. El servidor conoce sólo su hash y caducidad; permite importar exclusivamente los bytes y hash del libro fijado y no puede reemplazar un libro listo, leer cuentas ni modificar el catálogo. Se elimina ese permiso del despliegue después de la carga. La ruta de importación restante requiere administración de TEC y sigue verificando tamaño y SHA-256. Una carga fallida no publica el audio ni borra las ediciones locales.

Verificación: `npm test`. Las pruebas de integración cubren autenticación, rangos/HEAD, permisos, carga corrupta/repetida, reseñas concurrentes y ajenas, paginación y progreso. Se verificó la interfaz con Chrome aislado, el M4B real, navegación por capítulos, velocidad, reseñas con HTML inerte y pantalla móvil. Las reseñas de esas pruebas existen sólo en memoria/local y nunca se publican en TEC. La escucha editorial completa sigue pendiente.

Reversión: revertir el commit de interfaz/rutas. Conservar las colecciones y GridFS para no perder reseñas, avance o audio; no ejecutar migraciones destructivas. El cambio es aditivo.
