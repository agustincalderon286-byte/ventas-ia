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

El panel lateral a la izquierda permite guardar la pieza seleccionada, abrir/cerrar la maleta, encender/apagar LED, desplazarse entre estaciones, activar estufa y grifo, recolocar todo y salir de VR. Apunta con el control y pulsa/suelta el gatillo; con manos apunta y mantén un pellizco unos 0.4 segundos antes de soltar. Los paneles no reaccionan al contacto de las manos. Recolocar todo requiere dos pulsaciones completas en menos de siete segundos, con confirmación visible. No dependen de que el visor admita superposición HTML. Al perder seguimiento de la mano/control o salir de la sesión se libera el objeto manteniendo su posición.

## Validación y reproducción

- `node --test tests/ar-maleta.test.js tests/case-interaction.test.js tests/square-marker.test.js tests/vr-controls.test.js`: 27 pruebas, con Three.js 0.160.1 y la geometría GLB real. Las pruebas de transformaciones omiten decodificación de texturas; el navegador valida la apariencia.
- Verificadas salida y devolución exacta de las siete piezas en navegador, vista móvil de 390 píxeles, carga del visor de Quest, conservación de Novel y controles de guardar/recolocar.
- Pruebas de control y mano mediante eventos WebXR simulados; no equivalen a una sesión en un visor físico.
- Pendiente la comprobación física de seguimiento de la tarjeta en el iPhone y agarre en Quest 2. No hay acceso a esos dispositivos desde esta tarea.
- No se ha añadido gravedad ni colisión dinámica: las piezas conservan la pose donde se sueltan; el regreso al compartimento es asistido.

## Corrección de cámara y cocina · V2

El usuario confirmó cámara visible y tarjeta sin detección. MindAR reconoció la tarjeta antigua en la prueba de escritorio, por lo que no se confirmó la causa en su dispositivo. Existe un fallo coincidente de shaders de MindAR en iOS 27: https://github.com/hiukim/mind-ar-js/issues/581. Se sustituyó por js-aruco2 2.0.0 (MIT), detector CPU y estimación POSIT con el marcador ARUCO_MIP_36h12 ID 23. El sitio ya no importa MindAR ni TensorFlow al abrir la cámara. Los archivos anteriores se conservan por compatibilidad con revisiones anteriores.

La tarjeta V2 debe tener el borde negro y el margen blanco completos. La cámara se procesa como máximo a 640 píxeles y 20 detecciones por segundo; el render 3D conserva la resolución de pantalla. La transformación compensa el recorte de video en pantallas verticales. Se muestran estados de búsqueda/detección, se ocultan controles de manipulación mientras se busca y se detiene el video al salir. La focal es aproximada: no se ofrece una medición métrica calibrada.

Validación adicional: cinco proyecciones sintéticas del SVG real, inclinación hasta 0.8 radianes, rotación hasta 180°, formatos vertical/horizontal, rechazo de imagen vacía y coincidencia entre proyección de cámara y CSS cover. En navegador, video sintético de la tarjeta en movimiento atravesó el detector real y mostró la maleta; las siete piezas salieron y regresaron con seguimiento activo. Esto no sustituye la validación física en el iPhone del usuario.

La cocina incorpora gabinetes y cajones individuales, encimera de cuarzo, piso de piedra, nogal PBR, horno y parrillas, fregadero y grifo, zócalos, lámparas y detalles a escala. Texturas y HDRI de Poly Haven CC0 alojados localmente; fuentes en assets/kitchen/sources.json. Un mapa de reflexión capturado dentro de la propia cocina mejora el acero. Las sombras usan una sola luz direccional; los botones flotantes aparecen exclusivamente en VR. Foto genera una captura de 1920 × 1080 de la escena. No se promete fotorealismo absoluto ni una tasa de cuadros medida en Quest 2: falta prueba física. Los originales de Royal Prestige y MultiPan siguen sin modificarse.


## Cocina interactiva · V3

- Joystick izquierdo: desplazamiento a 0.9 m/s relativo a la mirada. Se bloquea el paso a través de la isla, gabinetes y límites de la habitación. Joystick derecho: giros de 30 grados. El movimiento físico del visor no se restringe por software.
- Sin controles: apunta al panel lateral y pellizca brevemente para elegir Isla, Estufa o Fregadero. Los saltos conservan la altura y orientación de la cabeza. Las piezas agarradas viajan contigo.
- La tapa se anima entre 0 y 108 grados; no abre/cierra mientras sostienes una pieza. Hay que abrir para sacar o guardar piezas. Los LED pueden encenderse y apagarse desde el panel o el interruptor de la maleta.
- Cada perilla de la estufa controla un quemador. Apunta y pulsa/suelta el gatillo, o pellizca cerca de la perilla unos 0.4 segundos y suelta. El panel tiene un interruptor general para los cuatro.
- La palanca del grifo responde igual: abre/cierra un chorro con ondas animadas en el fregadero. El agua y fuego son efectos visuales; no hay temperatura, llenado de ollas ni física de fluidos.
- La ventana muestra un panorama HDR de campo al amanecer, de Poly Haven (CC0), alojado con el proyecto. Los reflejos de la cocina incluyen la ventana.
- En computadora están los mismos controles de tapa, LED, estaciones, estufa y grifo en el panel HTML.

Validación V3: geometría real del cierre y retorno a su ángulo original, bloqueo de extracción con tapa cerrada, LEDs, confirmación con caducidad, pulsación deliberada por mano/control, colisiones de desplazamiento, giro alrededor de la cabeza y destinos del visor. Revisión visual en navegador del cierre, apertura, panorama, llama y agua. Esta versión nueva aún requiere comprobar ergonomía y rendimiento en el Quest físico del usuario.

## Juego de cocina con manos · V4

El usuario confirmó que el panel fijo quedaba inaccesible después de viajar al fregadero. Se sustituye por un botón **Menú**, abajo a la izquierda de la mirada, que acompaña al usuario. Al activarlo aparece un panel completo orientado hacia la vista, también al mirar hacia abajo. El panel permanece quieto durante la pulsación. Viajar lo oculta automáticamente; Menú lo vuelve a abrir desde cualquier estación. Las pestañas son Viajar, Cocinar y Maleta. Hay destinos para isla, estufa, fregadero, refrigerador y cajones, y giro de 90 grados sin controles físicos.

1. Retira la tapa correspondiente y toma la olla de 3 o 6 cuartos, o el recipiente MultiPan sin sus piezas superiores.
2. Suéltala cerca del centro del fregadero: se acomoda bajo el grifo. Abre la palanca con un pellizco breve y suelta, o usa Cocinar > Grifo. El nivel sube en unos siete segundos. También se puede sostener bajo el chorro. Una olla inclinada o tapada no se llena.
3. Lleva la olla a un quemador y suéltala cerca de la parrilla. Se acomoda en posición vertical; no se superponen dos ollas sobre el mismo quemador. Enciende su perilla o el interruptor general del menú. El agua genera vapor después de un calentamiento breve de juego, y se enfría al retirarla o apagar el fuego.
4. Abre el refrigerador mediante la puerta, su asa o el menú Cocinar. Hay nueve alimentos individuales: tomates, manzanas y zanahorias. Se pueden tomar, mover y soltar dentro de una olla; acompañan al recipiente y se pueden volver a sacar. Cambian de tono gradualmente al calentar.
5. Cuatro cajones traseros abren/cierra individualmente al pellizcar y soltar su frente o asa. El menú también los abre/cierra juntos. Contienen bandejas y cubiertos decorativos que se desplazan con el cajón.

Para facilitar la colocación hay acciones Olla bajo grifo, Olla a estufa y Olla a la isla en Cocinar; actúan sobre la olla seleccionada. La última pieza que se toma queda seleccionada. El agua se conserva al moverla y se vacía al inclinarla. Recolocar todo sigue exigiendo confirmación y devuelve también los alimentos a su estante, vacía el agua y reinicia el calentamiento.

Se trata de una simulación de juego: los tiempos de llenado/calentamiento no son tiempos de cocina real, y no hay fluidos volumétricos ni gravedad general. Las colocaciones en parrillas, fregadero, olla y encimeras son asistidas; los cubiertos de los cajones son decoración. Esta actualización no cambia la cámara AR del iPhone ni los archivos originales del catálogo.

Validación V4: 33 pruebas con geometría GLB real y eventos simulados, más revisión de navegador del agua, vapor, puertas, cajones y menú desde distintas estaciones. Se comprueban agua solo bajo chorro abierto, recipiente tapado/inclinado, enfriamiento, quemador ocupado, alimentos dentro/fuera de la olla, puertas, bloqueo de alimentos con refrigerador cerrado, menú tras teleportación/giro y panel quieto durante pulsación. La ergonomía final del botón con seguimiento de manos todavía requiere prueba física en Quest.
