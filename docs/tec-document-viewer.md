# Documentos dentro de TEC · 2026-10-22

Los enlaces de catálogo y carpeta abren un diálogo propio, no el visor PDF nativo de una pestaña/blob. «Volver al Coach» permanece fuera del área desplazable del documento, incluso durante cargas/errores. El cierre restaura el foco y la posición sin recargar el formulario ni detener la grabación. Atrás/Escape también cierran el diálogo.

Una página y un canvas, con resolución máxima de 6 megapíxeles, limitan el consumo de memoria en teléfonos. Hay anterior/siguiente, número de página y ampliar/ajustar. Se cancelan cargas/renders al cerrar y se ignoran respuestas antiguas.

Los enlaces públicos tienen como respaldo `/coach/document.html?document=catalogo` o `carpeta`; esa página conserva un enlace HTML de retorno aunque falle JavaScript. El servidor y el service worker redirigen navegaciones antiguas a los PDF a ese visor. Las solicitudes fetch/range siguen recibiendo bytes PDF.

«Guardar sin internet» guarda únicamente los dos documentos públicos completos en `tec-coach-documents-v1`. No se almacena una respuesta parcial 206. El service worker precarga la distribución local legacy de Mozilla PDF.js 6.4.299, fuentes y decodificadores, y conserva su caché entre actualizaciones. Los PDF grandes requieren espacio y completar la descarga; el mensaje distingue guardado confirmado de error.

Los archivos privados mantienen el POST autenticado y el PIN del servidor. Solo se entregan al visor en memoria y no ofrecen guardado offline ni URLs blob/nuevas ventanas. No se cambian permisos, cuentas, membresías o datos de clientes.

Distribución vendorizada sin modificar desde `pdfjs-dist@6.4.299` de npm, con su licencia Apache 2.0 y licencias de fuentes/decodificadores en `public/coach/vendor/pdfjs/6.4.299`. `offline-assets.js` enumera los archivos necesarios para precarga. No es una dependencia del servidor Node 20.

Referencia de integración: https://mozilla.github.io/pdf.js/examples/ (renderizado y control de tareas). La distribución legacy actual documenta Safari 18+; un fallo de compatibilidad no elimina el botón de regreso.

Pruebas automatizadas: cierre durante carga, PIN inválido, página/zoom/memoria, Atrás, carrera cierre/apertura, rangos 206, lectura offline y redirección de enlaces antiguos. Las pruebas visuales locales y de producción no sustituyen una prueba física de PWA instalada en iPhone.
