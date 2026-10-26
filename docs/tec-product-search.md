# Búsqueda de productos TEC — 25 octubre 2026

La calculadora, las tres selecciones de la encuesta y la recuperación del Coach usan el mismo índice local. No se añade una llamada de IA ni una consulta a MongoDB por cada búsqueda.

## Ejemplos

- Licuadora / licuadira: Power Blender Max y Go.
- Batidora de mano / licuadora inalámbrica / licuar en la olla: Power Blender Go.
- Moler café / triturar semillas: Power Blender Max.
- Aparato que corta verduras: máquina para ensaladas.
- Sartén que no se pega de 10 pulgadas: Easy Release de 10 pulgadas.
- Olla express: opciones de presión de 6 y 10 litros.
- Hacer palomitas: Perfect Pop.

El selector muestra fotos y conserva la identidad elegida (código, nombre y precio). La encuesta no muestra precios. No se elige automáticamente el primer resultado ambiguo. La búsqueda del selector funciona sin conexión cuando el catálogo ya fue descargado; el Coach conversacional sigue necesitando internet.

## Repuestos

Se agregó al conocimiento del Coach la lista interna existente con 465 entradas de códigos y descripciones. Incluye repuestos y otros artículos internos: no son 465 repuestos diferentes. Solo se incorporan nombre y código; los costos internos no forman parte del contexto de IA ni del catálogo del cliente.

Ejemplos: aro de silicona Innove de 20 cm (SP0430), aro de la nueva olla de presión (RP2287), aro de silicona de tapa de vidrio Easy Release de 20 cm (RP9714). La descripción no equivale a una tabla completa de compatibilidad. El Coach debe pedir modelo/medida cuando falten; no promete existencias, precios ni realiza pedidos.

No se identificó un PDF independiente de repuestos; se utilizó la lista interna de códigos que ya estaba en el proyecto. Un catálogo interno actualizado podrá incorporarse después.

## Mantenimiento y límites

`public/coach/product-search.js` contiene normalización, apodos por familia, tolerancia de una errata y reglas de medidas. Cada identidad del catálogo tiene términos alternativos; esto no significa comprensión garantizada de cualquier frase. Las descripciones verificadas de la web oficial complementan el índice. Nombres incompletos o funciones compartidas pueden producir varias opciones; el Coach debe pedir confirmación breve.

`src/knowledge/product-catalog.js` proyecta únicamente datos permitidos y prepara los índices una vez. Las listas de precios no se modificaron. Un código con fichas contradictorias (como WF0654) conserva la advertencia; nunca se fusiona arbitrariamente.

Al cambiar estas reglas: actualizar versión de búsqueda/PWA y prompt, ejecutar `npm test`, realizar las verificaciones sintéticas `node scripts/verify-tec-brevity.mjs --live`, y publicar la biblioteca con `node scripts/publish-tec-knowledge.mjs` en el entorno autorizado. No borrar el almacenamiento offline ni forzar la activación del service worker durante una demostración.
