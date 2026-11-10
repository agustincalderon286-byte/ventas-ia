# Modelos 3D en la calculadora del Coach

Preparado el 10 de noviembre de 2026 sobre producción `8d140fc`. Estado: implementación y 933 pruebas automáticas completas; pendiente revisión visual en navegador y AR en dispositivos físicos. No se ha publicado esta integración.

## Comportamiento

Al elegir un producto con código confirmado, aparece «Ver en 3D / AR» debajo de su imagen, junto al botón de videos. Se abre un diálogo dentro del Coach con giro, zoom, centrar, reintentar y cierre visible fuera del área desplazable. Cerrar devuelve el foco sin cambiar productos, importes ni posición de la calculadora.

El visor de Google model-viewer 4.4.0 se sirve localmente y se importa únicamente al abrir un modelo. No se descargan GLB durante la carga normal de la calculadora. Se limita la caché de modelos del visor a una pieza y se retira el visor al cerrar. No se inicia micrófono ni cámara automáticamente.

En dispositivos compatibles, «Ver en mi espacio» inicia AR mediante WebXR o Quick Look. Se excluye Scene Viewer porque no puede reenviar la sesión privada del Coach. Quick Look genera el USDZ desde el GLB cargado. No es un modo de gafas VR. La apariencia, la entrada a AR y el regreso desde la app nativa necesitan verificación física; no se consideran probados por pasar las pruebas automatizadas.

## Productos

| Código | Modelo |
|---|---|
| CO9550 | Olla NOVEL 1½ cuartos |
| CO9555 | Olla NOVEL 2 cuartos |
| CO9560 | Olla NOVEL 3 cuartos |
| CO9565 | Olla NOVEL 4 cuartos |
| CO9570 | Olla NOVEL 6 cuartos |
| CO9575 | Olla NOVEL 8 cuartos |
| CO9660 | Olla grande 12 cuartos |
| CO9675 | Olla grande 20 cuartos |
| CO9144 | Olla grande 60 cuartos; parrilla no representada |
| CO9777 | Easy Release 2 cuartos |
| CO9778 | Easy Release 3 cuartos |
| CO9779 | Easy Release 3½ cuartos |
| CO1253 | Olla de presión 6 litros v02 |
| CO1258 | Olla de presión 10 litros v02 |
| CO9598 | Paellera NOVEL 14 pulgadas |
| CO6589 | Pavera ovalada |
| ES0082 | Power Blender Max v08 web |
| CO0101 | Chocolatera v03 |

Se reutilizan los modelos publicados del prototipo y se añaden copias privadas de Power Blender v08 y presión v02 del SSD. Los originales permanecen intactos. Los modelos son recreaciones visuales sin medidas de fábrica verificadas.

Se omiten la olla de 30 cuartos (apariencia de colección distinta al código INNOVE disponible), la paellera NOVEL de 10 pulgadas (sin código inequívoco en esta lista), la cacerola individual (la lista contiene un juego de cuatro) y el antiguo ejemplo MultiPan de Quick Look. No se sustituyen por modelos parecidos ni se vinculan por nombres genéricos.

## Acceso y límites

`/api/coach/product-models/:id/model.glb` y `/poster` requieren `requireCoachActivo`. Solo aceptan modelos y archivos incluidos en el manifiesto; admiten HEAD/rangos y envían `private, no-store`. La protección por PIN del prototipo original permanece intacta. El service worker no almacena los modelos ni precarga el motor 3D; solo incorpora los archivos pequeños de integración.

El documento autenticado del Coach permite cámara y seguimiento espacial propios para que AR pueda solicitar permiso al pulsar su botón. Las páginas de acceso conservan su política restrictiva. GPS, micrófono, grabación y cálculos existentes no se modifican.

## Validación y revisión pendiente

Pasaron 933 pruebas: códigos/nombres exactos y negativos entre familias, integridad SHA256 y estructura GLB autocontenida, archivos/pósteres existentes, sesión obligatoria, rangos y rutas no autorizadas, carga diferida, cierre antes de cargar, limpieza, errores, reintentos, desconexión y regresiones generales del Coach.

Vista local: `http://127.0.0.1:4198/`. Usa el componente real de calculadora y un servidor independiente ligado solo a loopback, con una sesión sintética local. No lee ni modifica cuentas de producción. Se verificaron HTTP, tipos de archivo y sintaxis del JavaScript servido; no se afirma revisión visual.

Para continuar: revisar los 18 modelos, especialmente transparencia de Power Blender v08 y asas/cierre de presión v02; comprobar teléfono vertical/horizontal, cerrar y regresar sin mover la calculadora; probar Quick Look en iPhone/iPad y WebXR en Android. Después integrar la última producción y publicar.
