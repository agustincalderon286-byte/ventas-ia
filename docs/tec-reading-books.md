# Libros para leer — Audiolibros TEC

La sección `/audiolibros/#libros` reúne 16 ediciones completas en español con las portadas existentes del SSD. Pertenece exclusivamente a la web de Audiolibros, con la misma membresía de 20 USD mensuales; no modifica el Coach ni su suscripción.

Los diez audiolibros publicados reutilizan íntegramente sus textos aprobados. Las seis ediciones adicionales son Hamlet (Moratín), El monte de las ánimas (1885), Apología de Sócrates y Critón (Azcárate, 1871), Oráculo manual (300 aforismos) y El camino a la riqueza (traducción histórica de 1825). Cada ficha identifica su edición y procedencia. No se publican reservas en otros idiomas ni OCR pendiente de revisar.

## Acceso y persistencia

- Las fichas y portadas son públicas y aparecen en el sitemap. Los textos se conservan en `src/knowledge/reading-texts`, fuera del directorio público.
- Los endpoints `/api/audiolibros/lecturas/:id` comprueban la sesión y membresía existentes. Los capítulos completos se sirven individualmente con caché privada desactivada.
- El lector inserta texto mediante `textContent`, conserva tamaño de letra y fondo, y guarda capítulo/párrafo en `TecReadingProgress`, aislado por cuenta y libro. También guarda una copia local y reintenta su envío al recuperar la conexión.
- El capítulo abierto permanece visible si se pierde la conexión. Esta versión no descarga libros completos para lectura offline.
- El acceso por correo, las cuentas explícitas de prueba y el regreso de Stripe devuelven al libro solicitado. Cerrar sesión borra el texto visible en otras pestañas.

## Verificación

`NODE_ENV=test node --test` pasó 914 pruebas. Las pruebas de lectura verifican integridad respecto a los textos aprobados, portadas, 50 capítulos de Génesis, 40 de Éxodo, 300 aforismos, cinco actos de Hamlet, páginas públicas sin texto protegido, acceso, revocación, aislamiento del avance y rechazo de escrituras entre orígenes.

Se comprobó en navegador la búsqueda sin acentos, ingreso y retorno al lector, cambio de capítulo, letra, fondos y recuperación del avance, con tamaños de teléfono, tableta y escritorio. Las fuentes originales del SSD y los audios no se modifican.

## Pestañas y reproductor — 9 de noviembre

Audiolibros y Libros para leer tienen pestañas con paneles independientes. Los enlaces `#catalogo` y `#libros` siguen funcionando, al igual que volver/avanzar y el teclado. El cambio se realiza sin recargar la página ni tocar el audio activo; el minirreproductor permanece disponible. Las fichas públicas y el sitemap se conservan.

El reproductor agrupa Leer, Capítulos, Compartir, Dormir y Más en una fila de herramientas con iconos y nombres breves. Leer abre la vista completa; Capítulos cierra la lista al elegir uno; Dormir muestra el temporizador y un indicador de la pausa elegida. Más reúne velocidad y marcadores. El texto sincronizado se carga como antes, sin desplegarse automáticamente debajo del reproductor.

Se verificaron manualmente reproducción, continuidad al cambiar de pestaña, compartir y volver, lectura completa y regreso, temporizador/cancelación, velocidad, selección de capítulo y guardado de marcadores. Se probaron escritorio, teléfono y orientación horizontal. Tres pruebas adicionales cubren navegación, enlaces directos/teclado/historial y pausa por tiempo o fin de capítulo, junto con las 914 pruebas existentes.
