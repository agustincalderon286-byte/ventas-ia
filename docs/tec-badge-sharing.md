# Insignias compartibles — 8 de noviembre de 2026

Las 25 insignias tienen símbolos vectoriales originales con variantes de marco y color. Los ID y las reglas de desbloqueo v1 se conservan, al igual que el estado local y de cuenta. Audible fue referencia de variedad y presentación; no se incorporaron sus imágenes, logotipos ni código.

Solo las insignias ganadas muestran Compartir insignia. El cuadro permite guardar PNG, copiar un enlace y usar Facebook, Instagram, TikTok y Mensajes. Web Share se invoca desde el clic con el archivo ya cargado para conservar activación de usuario. Si el sistema permite compartir archivos, el lector elige la app. Si la app no aparece o el navegador no lo admite, se ofrece guardar y subir manualmente. No se usaron SDK ni cuentas nuevas de redes sociales. No se publica ni envía nada automáticamente.

Facebook: enlace de sharer con página pública y Open Graph. Instagram/TikTok: PNG más QR; una foto no proporciona por sí sola un enlace pulsable. Para Instagram Stories se indica usar la etiqueta Enlace. Mensajes: menú nativo para imagen más texto, o SMS con enlace y adjuntar PNG manualmente. La compatibilidad final de la app receptora depende del teléfono y su menú de compartir.

## Archivos públicos

`/audiolibros/logros/:id/` sirve una ficha pública, sin cookies, base de datos ni acceso a historial. Solo admite 25 ID. Cualquier persona puede abrir la ficha y escuchar una muestra; el audio completo conserva el control de membresía. Las fichas llevan noindex,follow y no se agregan al sitemap editorial. No contienen Analytics ni identificadores privados. La tarjeta es una celebración simbólica, no una certificación de identidad o escucha.

`/audiolibros/insignias/v2/` contiene 25 SVG y 75 PNG precomputados: publicación 1080×1080, historia 1080×1920 y vista previa 1200×630. Los QR apuntan a la ficha pública correspondiente, sin parámetros de usuario. Se generan localmente con `node scripts/generate-audiobook-badge-cards.mjs`, sin llamadas de IA. El servidor entrega archivos estáticos con caché inmutable; no genera imágenes por cada solicitud. Si se cambia un archivo publicado, hay que incrementar el directorio de versión y las referencias en share-core y servidor.

## Verificación

11 pruebas específicas cubren 25 símbolos únicos, los tamaños de 75 PNG, enlaces sin datos privados, vista pública independiente de cookies, formatos/cancelación/fallos, archivo nativo y alternativas de Instagram, TikTok y Mensajes. Se descifraron los 75 QR de los PNG finales con jsQR 1.4.0 usado solo para QA temporal, sin añadir dependencias al proyecto. Todos coincidieron con su URL esperada.

En el navegador: tarjeta PNG real y formato vertical, copia de enlace, audio que sigue reproduciéndose durante el cuadro, y cierre en 320, 390, 768 y 1024 px, además de horizontal 844×390. El cierre mide 48×48 y permaneció visible; no se detectó desbordamiento. Las pruebas nativas de envío usaron dobles de navegador: no se enviaron publicaciones o mensajes reales. Las apps receptoras en teléfonos físicos todavía requieren prueba por el usuario.
