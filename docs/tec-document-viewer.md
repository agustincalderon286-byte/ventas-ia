# Documentos dentro de TEC · 2026-10-22

Los enlaces de catálogo y carpeta abren un diálogo propio, no el visor PDF nativo de una pestaña/blob. «Volver al Coach» permanece fuera del área desplazable del documento, incluso durante cargas/errores. El cierre restaura el foco y la posición sin recargar el formulario ni detener la grabación. Atrás/Escape también cierran el diálogo.

## Compatibilidad con iPhone X: páginas ligeras

La primera publicación usaba PDF.js 6.4.299 también para los documentos públicos. Esa distribución requiere Safari 18+ y no cubre el iPhone X con iOS 16. El mensaje genérico de conexión ocultaba esa incompatibilidad.

Desde `20261022-pages1`, los documentos públicos no importan PDF.js ni crean workers/canvas: se muestran páginas JPEG previamente renderizadas desde los PDF originales con Poppler. Solo hay una imagen en la pantalla, con un máximo de 1600 píxeles en su lado mayor (menos de 2.56 megapíxeles). Catálogo: 42 páginas, aproximadamente 8.2 MiB. Carpeta: 50 páginas, aproximadamente 10 MiB. Se conservan anterior/siguiente, número de página, ampliar/ajustar, volver y reintento de página.

`scripts/build-coach-document-pages.mjs` reproduce las páginas y el índice público `assets/documents/manifest-v1.json`; los hashes y conteos se comprueban en las pruebas. No modifica los PDF originales. La revisión visual de hojas de contacto verificó las 92 páginas y una página interior a tamaño completo. Las páginas son las originales completas, sin inferir ni sustituir productos.

Los enlaces públicos tienen como respaldo `/coach/document.html?document=catalogo` o `carpeta`; esa página conserva un enlace HTML de retorno aunque falle JavaScript. El servidor y el service worker redirigen navegaciones antiguas a los PDF a ese visor. Las solicitudes fetch/range siguen recibiendo bytes PDF.

«Guardar sin internet» guarda secuencialmente el índice y todas las páginas del documento público en `tec-coach-documents-v1`. Solo confirma el documento completo después de terminar todas las escrituras y registrar su marcador de finalización. Una carga cancelada conserva lo ya descargado, pero no anuncia finalización. El service worker guarda las páginas vistas y conserva esa caché entre actualizaciones. Un fallo de almacenamiento no bloquea mostrar páginas con red. Ya no se precargan 192 archivos del motor PDF en todos los teléfonos: no se necesitan para los catálogos públicos.

Los archivos privados mantienen el POST autenticado y el PIN del servidor. Solo se entregan a PDF.js en memoria y no ofrecen guardado offline ni URLs blob/nuevas ventanas. Esta corrección de iPhone X cubre los dos documentos públicos; los PDF privados arbitrarios aún requieren el navegador compatible con PDF.js 6. No se cambian permisos, cuentas, membresías o datos de clientes.

Distribución vendorizada sin modificar desde `pdfjs-dist@6.4.299` de npm, con su licencia Apache 2.0 y licencias de fuentes/decodificadores en `public/coach/vendor/pdfjs/6.4.299`, ahora cargada únicamente al abrir un PDF privado. No es una dependencia del servidor Node 20.

Referencia de integración: https://mozilla.github.io/pdf.js/examples/ (renderizado y control de tareas). La distribución legacy actual documenta Safari 18+; un fallo de compatibilidad no elimina el botón de regreso.

Pruebas automatizadas: cierre durante carga, PIN inválido, página/zoom/memoria, renderizado público sin APIs de PDF/Worker, reintento de imagen, guardado secuencial completo, los 92 archivos y sus hashes, Atrás, carrera cierre/apertura, rangos 206, lectura offline y redirección de enlaces antiguos. Las pruebas visuales locales y de producción no sustituyen una prueba física de PWA instalada en iPhone X; esa confirmación la debe hacer el usuario.
