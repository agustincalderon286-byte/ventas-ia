# Maleta: cámara interactiva y Meta Quest 2

- iPhone/Safari: https://totalelitecoaching.com/prototipo-agustin20-chef-ar/camara/
- Tarjeta: https://totalelitecoaching.com/prototipo-agustin20-chef-ar/camara/referencia.html
- Quest: https://totalelitecoaching.com/prototipo-agustin20-chef-ar/vr/

Las rutas conservan el PIN existente. La vista Quick Look anterior sigue disponible para colocar el conjunto completo sin tarjeta.

## iPhone

Muestra la tarjeta en otra pantalla o imprímela. Abre la cámara interactiva en Safari, permite la cámara y apunta a toda la tarjeta. Elige «En pantalla / vertical» o «Sobre una mesa» según su ubicación. Selecciona o toca una pieza, usa Sacar pieza, arrastra para mover/girar y usa Guardar para devolverla. También se encaja al soltarla cerca de su compartimento. La carcasa se puede trasladar o girar con las piezas que siguen guardadas.

La tarjeta debe permanecer visible: se oculta el modelo y se pausa la manipulación si se pierde el seguimiento, conservando las posiciones para recuperarlas. El tamaño depende del tamaño visible de la tarjeta y del control de escala. Esta vista no realiza seguimiento libre del suelo. No graba ni transmite fotogramas; MindAR procesa la cámara localmente. Parar cámara, abandonar la página u ocultarla detiene las pistas de video.

«Probar sin cámara» es una vista 3D explícita para revisar las interacciones, no una simulación de una sesión AR verificada.

## Meta Quest 2

Abre la página en Quest Browser y toca Entrar a VR. Apunta a una pieza y mantén el gatillo para moverla y girarla con el control; suelta para dejarla. Con manos, acerca pulgar e índice a la pieza y pellizca. Al soltar cerca del lugar correcto se encaja. Toma la carcasa para mover la maleta con las piezas guardadas. Las piezas que ya están fuera permanecen donde se dejaron. Novel se conserva como producto independiente en la isla.

Los botones flotantes dentro de la escena permiten guardar la pieza seleccionada, recolocar todo y salir de VR. No dependen de que el visor admita superposición HTML. Al perder seguimiento de la mano/control o salir de la sesión se libera el objeto manteniendo su posición.

## Validación y reproducción

- `node --test tests/ar-maleta.test.js tests/case-interaction.test.js`: 13 pruebas, con Three.js 0.160.1 y la geometría GLB real. Las pruebas de transformaciones omiten decodificación de texturas; el navegador valida la apariencia.
- Verificadas salida y devolución exacta de las siete piezas en navegador, vista móvil de 390 píxeles, carga del visor de Quest, conservación de Novel y controles de guardar/recolocar.
- Pruebas de control y mano mediante eventos WebXR simulados; no equivalen a una sesión en un visor físico.
- Pendiente la comprobación física de seguimiento de la tarjeta en el iPhone y agarre en Quest 2. No hay acceso a esos dispositivos desde esta tarea.
- No se ha añadido gravedad ni colisión dinámica: las piezas conservan la pose donde se sueltan; el regreso al compartimento es asistido.

MindAR 1.2.5 (MIT) está alojado con la página en `vendor/mindar-1.2.5/`; la licencia se conserva. La tarjeta compilada contiene un objetivo de 1000 × 750 píxeles, diez escalas de detección y dos niveles de seguimiento. Los modelos originales de Royal Prestige y MultiPan siguen sin modificarse.
