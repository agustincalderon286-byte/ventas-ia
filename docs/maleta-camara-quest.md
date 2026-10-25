# Maleta: cámara interactiva y Meta Quest 2

- iPhone/Safari: https://totalelitecoaching.com/prototipo-agustin20-chef-ar/camara/
- Tarjeta: https://totalelitecoaching.com/prototipo-agustin20-chef-ar/camara/referencia.html
- Quest: https://totalelitecoaching.com/prototipo-agustin20-chef-ar/vr/

Las rutas conservan el PIN existente. La vista Quick Look anterior sigue disponible para colocar el conjunto completo sin tarjeta.

## iPhone

Usa la tarjeta **V2, blanca y negra**, que reemplaza la fotografía de la maleta. Muestra la tarjeta en otra pantalla o imprímela. Abre la cámara interactiva en Safari, permite la cámara y apunta a toda la tarjeta. Abre Ajustes y elige «En pantalla / vertical» o «Sobre una mesa» según su ubicación. Selecciona o toca una pieza, usa Sacar pieza, arrastra para mover/girar y usa Guardar para devolverla. También se encaja al soltarla cerca de su compartimento. La carcasa se puede trasladar o girar con las piezas que siguen guardadas.

La tarjeta debe permanecer visible: se oculta el modelo y se pausa la manipulación si se pierde el seguimiento, conservando las posiciones para recuperarlas. El tamaño depende del tamaño visible de la tarjeta y del control de escala. Esta vista no realiza seguimiento libre del suelo. No graba ni transmite fotogramas; ArUco procesa la cámara localmente en CPU. Parar cámara, abandonar la página u ocultarla detiene las pistas de video.

«Probar sin cámara» es una vista 3D explícita para revisar las interacciones, no una simulación de una sesión AR verificada.

## Meta Quest 2

Abre la página en Quest Browser y toca Entrar a VR. Apunta a una pieza y mantén el gatillo para moverla y girarla con el control; suelta para dejarla. Con manos, acerca pulgar e índice a la pieza y pellizca. Al soltar cerca del lugar correcto se encaja. Toma la carcasa para mover la maleta con las piezas guardadas. Las piezas que ya están fuera permanecen donde se dejaron. Novel se conserva como producto independiente en la isla.

Los botones flotantes dentro de la escena permiten guardar la pieza seleccionada, recolocar todo y salir de VR. No dependen de que el visor admita superposición HTML. Al perder seguimiento de la mano/control o salir de la sesión se libera el objeto manteniendo su posición.

## Validación y reproducción

- `node --test tests/ar-maleta.test.js tests/case-interaction.test.js tests/square-marker.test.js`: 20 pruebas, con Three.js 0.160.1 y la geometría GLB real. Las pruebas de transformaciones omiten decodificación de texturas; el navegador valida la apariencia.
- Verificadas salida y devolución exacta de las siete piezas en navegador, vista móvil de 390 píxeles, carga del visor de Quest, conservación de Novel y controles de guardar/recolocar.
- Pruebas de control y mano mediante eventos WebXR simulados; no equivalen a una sesión en un visor físico.
- Pendiente la comprobación física de seguimiento de la tarjeta en el iPhone y agarre en Quest 2. No hay acceso a esos dispositivos desde esta tarea.
- No se ha añadido gravedad ni colisión dinámica: las piezas conservan la pose donde se sueltan; el regreso al compartimento es asistido.

## Corrección de cámara y cocina · V2

El usuario confirmó cámara visible y tarjeta sin detección. MindAR reconoció la tarjeta antigua en la prueba de escritorio, por lo que no se confirmó la causa en su dispositivo. Existe un fallo coincidente de shaders de MindAR en iOS 27: https://github.com/hiukim/mind-ar-js/issues/581. Se sustituyó por js-aruco2 2.0.0 (MIT), detector CPU y estimación POSIT con el marcador ARUCO_MIP_36h12 ID 23. El sitio ya no importa MindAR ni TensorFlow al abrir la cámara. Los archivos anteriores se conservan por compatibilidad con revisiones anteriores.

La tarjeta V2 debe tener el borde negro y el margen blanco completos. La cámara se procesa como máximo a 640 píxeles y 20 detecciones por segundo; el render 3D conserva la resolución de pantalla. La transformación compensa el recorte de video en pantallas verticales. Se muestran estados de búsqueda/detección, se ocultan controles de manipulación mientras se busca y se detiene el video al salir. La focal es aproximada: no se ofrece una medición métrica calibrada.

Validación adicional: cinco proyecciones sintéticas del SVG real, inclinación hasta 0.8 radianes, rotación hasta 180°, formatos vertical/horizontal, rechazo de imagen vacía y coincidencia entre proyección de cámara y CSS cover. En navegador, video sintético de la tarjeta en movimiento atravesó el detector real y mostró la maleta; las siete piezas salieron y regresaron con seguimiento activo. Esto no sustituye la validación física en el iPhone del usuario.

La cocina incorpora gabinetes y cajones individuales, encimera de cuarzo, piso de piedra, nogal PBR, horno y parrillas, fregadero y grifo, zócalos, lámparas y detalles a escala. Texturas y HDRI de Poly Haven CC0 alojados localmente; fuentes en assets/kitchen/sources.json. Un mapa de reflexión capturado dentro de la propia cocina mejora el acero. Las sombras usan una sola luz direccional; los botones flotantes aparecen exclusivamente en VR. Foto genera una captura de 1920 × 1080 de la escena. No se promete fotorealismo absoluto ni una tasa de cuadros medida en Quest 2: falta prueba física. Los originales de Royal Prestige y MultiPan siguen sin modificarse.
