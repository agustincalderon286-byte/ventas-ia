# Audiolibros TEC

## Don Quijote, entrega parcial — 11 de noviembre de 2026

Publicación expresamente solicitada por Agustin del audio disponible. Primera parte, capítulos 1–10 de los 126 de la obra, en español actual. No se presenta como obra completa; no incluye todavía preliminares ni capítulos posteriores. Una ficha y un objeto privado versionado de R2, con diez capítulos, portada existente de TEC, muestra del primer diez por ciento y lectura sincronizada exacta. Duración: 7615.35 segundos; 704 pasajes.

Objeto: Don-Quijote-capitulos-001-010-es-actual-v1.m4b. SHA-256: d605b1e03618cf91c36d2bbe907f1137da7ea1d15a1c83a997a9d44945afc36c. Verificado en R2 antes de publicar. Las ampliaciones conservarán el ID y los capítulos iniciales, usando otro objeto versionado y actualizando duración, muestra, índice y lectura en conjunto. Voces nuevas en pausa y fuera de esta publicación. Las comprobaciones técnicas y de transcripción no equivalen a audición humana integral de actuación.

## La epopeya de Gilgamesh — 10 de noviembre de 2026

Publicación expresamente autorizada por Agustin. Doce tablillas, 6112,59 segundos y 629 pasajes sincronizados. Versión propia TEC al español desde la edición inglesa de Thompson de 1928, con lagunas conservadas; no traducción directa del acadio o sumerio. Portada existente de TEC, muestra del primer diez por ciento, índice completo, ficha editorial y acceso de miembros. Audio privado R2: Gilgamesh-Completo-TEC-v1.m4b. SHA-256: ed23de258f21c2dfafe767d4a1e077b6b37c329d93a5983816678ca2d803b8d3. Objeto y portada verificados en R2 antes del catálogo. No se sustituye ninguna grabación ni se modifican cuentas o pagos. La verificación técnica no sustituye escucha humana integral de actuación.

## Viaje al Oeste — 10 de noviembre de 2026

Corte expresamente autorizado: publicar solo los capítulos 1–5 con audio terminado. El capítulo 6 queda pausado con sus fragmentos conservados; los demás textos y planes se conservan para reanudar después. Esta entrega se identifica como cinco de cien capítulos, nunca como obra completa. ID estable: viaje-al-oeste. Una ficha, un audio privado de R2, cinco capítulos navegables, 1151 pasajes sincronizados y muestra del primer diez por ciento. Traducción propia directa del chino, incluidos los poemas.

Objeto versionado: Viaje-al-Oeste-capitulos-001-005-v1.m4b. SHA-256: f37bf8ca66eef8dc358c9014ca492b98419d234f09d6f464b4b13b84e80c6429. R2 verificado antes de incorporar al catálogo. Las ampliaciones deben usar nuevos objetos, conservar el ID y los capítulos iniciales, y actualizar en conjunto duración, índice, muestra y lectura. No reemplazar objetos que atienden sesiones existentes. Las comprobaciones técnicas y de transcripción no equivalen a una escucha humana integral de interpretación.


## Mitos y héroes de Grecia — 9 de noviembre de 2026

Primera entrega autorizada: tres de ocho capítulos en una sola ficha y un solo audio privado de R2. Duración 3.309,38 segundos, 204 pasajes sincronizados y muestra del primer 10 %. La descripción identifica expresamente los capítulos disponibles y los cinco que se añadirán. Selección de Thomas Bulfinch traducida por TEC; no es todo The Age of Fable.

Objeto: `Mitos-y-heroes-de-Grecia-capitulos-01-03-v1.m4b`, 43319690 bytes, SHA-256 `12d234f817a2a98d1554fd5d4d60e54e892c902be29533c4821c753fd24dca4f`. Verificado en R2 antes de publicar. La ampliación debe conservar el ID `mitos-y-heroes-de-grecia`, los capítulos iniciales y el avance de las cuentas; usar un nuevo objeto versionado, verificarlo primero y luego actualizar audio, duración, muestra y lectura. No cambiar el objeto anterior mientras atiende sesiones existentes. La revisión técnica no equivale a una escucha editorial íntegra.


## Guías y lecturas — 7 de noviembre de 2026

Se publica `/audiolibros/guias/` con cuatro artículos originales: elegir un audiolibro de desarrollo personal; comenzar por Génesis o Éxodo; comparar a James Allen y Samuel Smiles; y usar la lectura sincronizada. El contenido está en `src/knowledge/audiobook-guides.js` y se prepara como HTML al iniciar el servidor, sin nuevas solicitudes de IA, audio o base de datos. Cada artículo tiene índice de secciones, lectura estimada, fecha visible y atribución editorial a Audiolibros TEC, sin inventar una firma personal.

Las páginas enlazan a las fichas, muestras y colecciones pertinentes; las fichas y colecciones incluyen enlaces de regreso a sus guías. El inicio incorpora acceso visible en navegación, cabecera móvil, sección editorial y pie. Se usan portadas de los libros correspondientes. Las guías son públicas; los libros completos y sus textos siguen usando las autorizaciones existentes.

El marcado `BlogPosting` incluye título, descripción, imágenes, fecha de publicación/modificación fija, autor/editor como organización, idioma y página canónica. El índice utiliza `CollectionPage` e `ItemList`; cada artículo incluye migas de pan. El sitemap y `llms.txt` incorporan el índice y los cuatro artículos: 18 páginas canónicas en total. No se generan fechas nuevas en cada visita ni se promete indexación. Referencia: [Google: marcado de artículos](https://developers.google.com/search/docs/appearance/structured-data/article).

Analytics mantiene su consentimiento y protección anteriores. La lista `GUIDE_IDS` admite únicamente estos cuatro slugs públicos y añade los tipos genéricos «guias» y «guia». No se envían el texto leído, selecciones, consultas ni fragmentos de URL. Al añadir una futura guía se deben revisar el contenido, sus identificadores de libros/colecciones y la lista pública de Analytics.

Pruebas: 866 correctas. Se comprobaron los cuatro artículos, enlaces recíprocos, índices internos, datos estructurados/atribución, canonical, rutas 200/301/404, sitemap sin duplicados, privacidad y consentimiento en las nuevas páginas. Se revisó el diseño en marcos de 320 y 768 píxeles y el flujo guía → ficha → reproductor. La reversión consiste en revertir interfaz/datos editoriales/rutas; no requiere modificar cuentas, pagos, grabaciones ni archivos de audio.

## Fichas editoriales, colecciones y Analytics — 7 de noviembre de 2026

Las ocho fichas incorporan introducciones originales basadas en los maestros españoles y los índices de TEC: resumen en dos párrafos, tres ideas, público, propuesta de escucha y tres preguntas específicas por obra. `src/knowledge/audiobook-editorial.js` conserva ese material separado del audio y del texto completo de miembros. No se solicitan respuestas a OpenAI al visitar una ficha. Las notas describen las ideas de cada autor sin presentar sus tesis filosóficas como hechos científicos ni atribuir una identidad individual confirmada a Tres Iniciados.

Cuatro colecciones indexables en `/audiolibros/colecciones/:id/` reúnen desarrollo personal, Biblia en audio, clásicos en español y filosofía/estrategia. Cada una tiene contexto, recorrido sugerido, tarjetas de sus libros y enlaces a otras colecciones. La colección bíblica indica que contiene Génesis y Éxodo, no la Biblia completa. Las fichas enlazan de vuelta a las colecciones. El sitemap y la guía pública incluyen ahora 13 páginas canónicas y mantienen las ocho fichas existentes.

Analytics usa la propiedad existente `562322027`, flujo `16271808920`, identificador público `G-DZCX3RMST7`. Se comprobó en el panel que Enhanced Measurement está desactivado; no se crean secretos API ni nuevas cuentas. `public/audiolibros/analytics.js` solo carga Google después de consentimiento específico para la biblioteca y en el dominio de producción. No comparte la decisión con el Coach. Las cookies llevan prefijo `tec_audio`, ruta `/audiolibros` y duración de 30 días; su eliminación se limita a `tec_audio_ga*`, preservando expresamente `tec_audio_session` y las cookies del Coach.

Los eventos permitidos son `page_view`, `audio_sample_play`, `audio_membership_click` y `audio_share_open`. Las rutas y los identificadores de libro proceden de listas públicas cerradas; se excluyen consultas, fragmentos, búsquedas, correos, contenido de citas, posiciones y audios. Las referencias de buscadores/redes reconocidos se reducen a su origen. No se registran pagos con un clic, ni se añaden estadísticas a las colas de sincronización. Se respeta GPC/DNT y se omiten contenedores nativos detectados y el modo sin conexión. La privacidad específica está en `/audiolibros/privacidad/`; rechazar no afecta escuchar ni la membresía.

### Atribución de anuncios — 8 de noviembre de 2026

Los dos anuncios activos ya incluyen UTMs en Meta. La integración anterior eliminaba la consulta de `page_location` por privacidad, pero no trasladaba las etiquetas de campaña a los campos de GA4. Además, la elección inicial de estadísticas aparecía sólo al final de la biblioteca. No se puede recuperar retroactivamente la atribución que no se envió a Google.

`safeCampaign` conserva sólo estas combinaciones revisadas como `campaign_source`, `campaign_medium`, `campaign_name` y, cuando corresponde, `campaign_content`:

| Fuente | Medio | Campaña | Pieza |
| --- | --- | --- | --- |
| meta | paid_social | audiolibros_chicago_es | clasicos_15s |
| instagram | paid_social | audiolibros_instagram_chicago_es | clasicos_15s |
| facebook | organic_group | clasicos_es | Se omite el identificador del grupo |
| tec_test | qa | verificacion_analytics | conexion_20261108 |

La última combinación identifica exclusivamente la comprobación técnica, no visitas reales de anuncios. Las campañas nuevas requieren revisar y ampliar esta lista. No se transmiten `fbclid`, otros identificadores de clic, términos de búsqueda, valores desconocidos, consultas completas ni fragmentos. `page_location` sigue siendo la ruta pública canónica. Estas etiquetas sólo salen después del consentimiento, y tampoco se envían para cuentas de prueba, GPC/DNT o usuarios que rechacen estadísticas.

El aviso inicial ahora se muestra bajo la cabecera, en el flujo normal, sin superponer controles. La decisión sigue siendo opcional y puede cambiarse desde el pie. La página pública Nuestra historia también tiene un contexto permitido. Las cuentas de prueba conservan su exclusión.

Validación: `npm test` pasa 906 pruebas, incluidas dos nuevas sobre atribución de ambos anuncios, datos privados, parámetros desconocidos/duplicados y rechazo/exclusión. Revisado el aviso a 390 × 844 y su cierre al elegir. Para verificar producción, abrir una visita anónima deliberada etiquetada `tec_test / qa`, aceptar estadísticas y comprobar `/audiolibros/` en Google Realtime. En informes de adquisición usar fuente/medio y campaña de la sesión; el área del Coach comparte propiedad pero tiene rutas distintas. Los números de Meta y GA no tienen por qué coincidir, por consentimiento, bloqueadores y procesamiento.

Pruebas: 862 correctas. Se amplió la cobertura para datos editoriales de las ocho obras, enlaces recíprocos y cobertura real de colecciones, índices/canonical, escape HTML y rutas inexistentes, consentimiento, carga fallida de Google, almacenamiento bloqueado, carreras de rechazo durante la carga, señales de privacidad y ausencia de eliminación de cookies de acceso. En navegador se comprobó la navegación ficha → muestra, las preguntas desplegables, reproducción activa tras rechazar estadísticas y diseños en marcos de 390 y 768 px sin desbordamiento. La cifra de clics no sustituye las suscripciones verificadas por Stripe.

Referencias de implementación: [configuración GA4](https://developers.google.com/analytics/devguides/collection/ga4/reference/config), [privacidad de Google tag](https://developers.google.com/tag-platform/security/concepts/privacy) y [mediciones automáticas](https://support.google.com/analytics/answer/9216061?hl=en).

## Descubrimiento y SEO — 7 de noviembre de 2026

`registerAudiobookSeo` prepara una vez al iniciar el servidor la portada HTML completa y una ficha pública por libro en `/audiolibros/libros/:id/`. El catálogo, las descripciones, las muestras y los enlaces se pueden leer sin ejecutar JavaScript. Las fichas incluyen capítulos, créditos, portada, muestra pública sin precarga de audio y enlace al reproductor existente. El precio de 20 USD al mes se identifica como membresía; nunca como compra individual ni libro completo gratuito. La sesión y Stripe siguen resolviéndose únicamente mediante los endpoints privados existentes.

Se generan canonical, Open Graph/Twitter, JSON-LD `CollectionPage`, `ItemList`, `Audiobook`, muestra `AudioObject` y `BreadcrumbList`; `/sitemap.xml` incluye únicamente la biblioteca y las ocho fichas canónicas, sin inventar fechas de actualización. `/robots.txt` permite el catálogo y sus portadas, y excluye las rutas internas de Coach/control/API del rastreo. Esto no sustituye la autorización del servidor. `/llms.txt` y `/audiolibros/llms.txt` ofrecen una guía pública complementaria con los libros y condiciones reales, sin textos completos, rutas de R2 ni información de cuentas. No se agregan trackers ni se modifican los consentimientos de Analytics.

Los archivos públicos `googlee19e7e608fd653e9.html` y `BingSiteAuth.xml` permiten verificar la propiedad en las cuentas de Google/Bing autorizadas por Agustin. Deben conservarse para mantener la verificación. Son comprobantes públicos de propiedad, no credenciales de sesión. Las verificaciones y envíos del sitemap se completan después de que el despliegue esté activo. Las variables opcionales `GOOGLE_SITE_VERIFICATION` y `BING_SITE_VERIFICATION` sirven si se elige el método de metaetiqueta; no se requieren con los archivos actuales.

Pruebas: 855 correctas. La nueva cobertura verifica HTML sin JavaScript, las ocho fichas, muestras e imágenes existentes, datos estructurados válidos como JSON, límites de exposición, escape de HTML, sitemap/canonical y redirecciones sin bucles, respuestas 404 y ausencia de cookies en páginas públicas. El registro en buscadores permite supervisar el rastreo; no garantiza indexación, posición ni citas de una IA. Referencias: [Google: funciones de IA y SEO](https://developers.google.com/search/docs/appearance/ai-features), [sitemaps](https://developers.google.com/search/docs/crawling-indexing/sitemaps/build-sitemap), [Schema.org Audiobook](https://schema.org/Audiobook).

Reversión: revertir únicamente este cambio de interfaz/SEO; no requiere cambios en datos, pagos, archivos de audio ni membresías. Conservar los archivos de verificación si se desea mantener los registros externos.

## Compartir citas y audio — 7 de noviembre de 2026

La biblioteca pública incluye «Compartir cita o audio» en el reproductor y «Compartir» en la lectura de pantalla completa. Se puede usar el pasaje actual o una selección de pasajes consecutivos, ampliarla o acortarla y elegir tarjeta 4:5 o 9:16. Las citas conservan texto canónico y autor, con un máximo de seis pasajes y 600 caracteres. El audio admite 15, 30 o 45 segundos y se exporta como MP3 o como video vertical H264/AAC para Instagram y TikTok. La aplicación prepara el archivo antes del gesto que abre el menú nativo de compartir. También ofrece descarga, enlace, Facebook y SMS. Instagram y TikTok deciden si admiten texto/enlace junto con el archivo; la tarjeta incorpora un QR al fragmento público en TEC.

`TecAudiobookShare` guarda únicamente el identificador aleatorio, libro, texto canónico, intervalo y fecha. Crear fragmentos fuera de la muestra gratuita exige la membresía existente. Los receptores acceden al fragmento mediante un enlace público no enumerable; no reciben sesiones, identificadores de usuarios, claves de R2 ni acceso al libro completo. El enlace «Seguir escuchando» abre el libro y su posición si la cuenta tiene acceso, o la muestra si todavía no tiene membresía.

Los archivos se generan con Sharp y FFmpeg, sin llamadas adicionales a IA ni nuevas credenciales de R2. El renderizado permite un trabajo de audio/video simultáneo, deduplica solicitudes en curso y conserva una caché temporal limitada. FFmpeg lee por rangos a través de un servidor efímero accesible sólo por loopback, con ruta aleatoria, límite de transferencia de 32 MB y tiempo máximo de 90 segundos. Las creaciones tienen límite por IP. El almacenamiento original del libro y la reproducción habitual permanecen independientes.

Pruebas: 850 pruebas correctas, incluidas autorización real de membresía/muestra, origen, parámetros y límites, atribución, HTML seguro, rangos HTTP, tarjetas PNG, extracción MP3 y video MP4 con audio real, caché y lectura parcial. Revisión visual del cierre y del desbordamiento en seis tamaños desde 320×568 hasta tableta horizontal. La selección de aplicaciones concretas en el menú nativo debe comprobarse en el teléfono del usuario. La reversión no requiere borrar `TecAudiobookShare` ni los libros.

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
