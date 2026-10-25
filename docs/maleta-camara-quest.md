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

El menú frontal permite guardar la pieza seleccionada, abrir/cerrar la maleta, encender/apagar LED, desplazarse entre estaciones, activar estufa y grifo, recolocar todo y salir de VR. Apunta con el control y pulsa/suelta el gatillo; con manos apunta y mantén un pellizco unos 0.4 segundos antes de soltar. Los paneles no reaccionan al contacto de las manos. Recolocar todo requiere dos pulsaciones completas en menos de siete segundos, con confirmación visible. No dependen de que el visor admita superposición HTML. Al perder seguimiento de la mano/control o salir de la sesión se libera el objeto manteniendo su posición.

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
- Sin controles: apunta al menú y pellizca brevemente para elegir Isla, Estufa o Fregadero. Los saltos conservan la altura y orientación de la cabeza. Las piezas agarradas viajan contigo.
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


## Estabilización del menú · V5 (histórica)

Esta revisión sustituye el comportamiento de navegación del menú V4. El usuario reportó paneles inclinados, un botón periférico difícil de apuntar y pérdida del menú después de viajar. No se incorporan nuevas mecánicas de cocina.

- Panel y botón Menú permanecen verticales respecto del suelo; solo usan el giro horizontal de la cabeza. La altura al abrir se ajusta a la mirada con límites, sin copiar inclinación ni roll. Se mantienen anclados en el mundo mientras se apunta.
- Al viajar a otra estación el menú permanece abierto frente al usuario. Ocultar menú deja un único botón Menú centrado, ligeramente abajo. No hay botón lateral ni superposición del botón de recuperación sobre el panel.
- Si el panel o botón queda lejos, demasiado cerca o fuera del campo frontal durante 650 ms, se coloca de nuevo delante. No se recoloca durante una pulsación ni mientras hay un objeto agarrado. Los pequeños movimientos de cabeza no desplazan los blancos.
- La cámara se sincroniza con la pose WebXR del cuadro actual antes de navegar y ubicar los controles; las matrices del menú se actualizan antes del raycast. Al entrar se espera la primera actualización, evitando ubicarlo con la cámara de escritorio.
- Los botones se iluminan al apuntarlos con las manos/controles. Para activar: apuntar, juntar índice y pulgar medio segundo, soltar sobre el mismo botón. La pérdida de seguimiento cancela pulsaciones pendientes. Recolocar todo conserva su confirmación de dos pasos.
- Texturas de texto con proporción correspondiente al botón evitan letras estiradas. Los módulos modificados usan versión de URL nueva para renovar la caché.

Validación: 37 pruebas aprobadas, incluidas inclinación/roll, rig girado, viajes repetidos, botón inmóvil al apuntar, recuperación con espera, bloqueo durante selección y pérdida de seguimiento. En navegador se comprobó Isla → Fregadero → ocultar → abrir Menú → Isla mediante los botones 3D. Estas verificaciones no sustituyen una sesión de seguimiento de manos en Quest físico.

Prueba de aceptación en el visor: salir de VR, recargar la página, entrar; ir al fregadero y regresar a Isla; ocultar y recuperar Menú; girarse y esperar un segundo con las manos relajadas; apuntar/pellizcar/soltar; comprobar que no se inclina ni reinicia al pasar las manos. No declarar cerrada la ergonomía física hasta confirmar esa prueba.


## Manos, traslado y recuperación · V6 (vigente)

Esta versión reemplaza el botón flotante y la reapertura automática de V5 por un gesto explícito. No añade nuevas mecánicas de cocina ni cambia los modelos originales.

### Uso

1. Entra a VR con las manos abiertas. La escena comienza sin menú. Gira una mano libre con la palma hacia arriba, dedos extendidos, a la altura del pecho y visible frente al visor. Mantenla quieta unos 0.7 segundos: un pequeño círculo sobre la muñeca indica el progreso y abre el menú. Sirve cualquiera de las manos. No usa el pellizco de palma reservado al sistema de Meta. El menú queda fijo; Ocultar menú elimina toda la interfaz flotante. El mismo gesto permite recuperarlo desde otro lugar.
2. Con el menú cerrado, apunta la mano libre hacia el piso y pellizca. Aparecen un arco y un círculo: ámbar durante la preparación, verde tras 0.35 segundos si el destino es válido, rojo si está bloqueado o fuera de la habitación. Suelta para trasladarte. Apuntar fuera del piso o perder seguimiento cancela el traslado. Los destinos próximos a una estación se ajustan dentro de 40 cm. Se conserva la altura del visor.
3. Pellizca cerca de una pieza para agarrarla; la mano ocupada no puede iniciar otro traslado ni invocar el menú. La otra mano puede navegar. La olla, el agua y los ingredientes viajan contigo. Separar los dedos durante 0.14 segundos libera la pieza; los picos más cortos no la sueltan.
4. Ayuda > Recuperar controles cancela pulsaciones y traslados pendientes, libera piezas en su ubicación actual sin encajarlas ni recolocarlas y permite volver a agarrarlas después de abrir la mano. No resetea agua, comida, estufa, grifo ni muebles.
5. Ayuda > Recargar página muestra un aviso y requiere una segunda pulsación en siete segundos. Sale de la sesión inmersiva antes de recargar. Reinicia la cocina; hay que pulsar Entrar a VR otra vez. La versión de escritorio también tiene Recuperar controles y Recargar página.

### Estabilidad y límites

Los soportes de agarre están dentro del grupo de desplazamiento del usuario; ya no dependen de recibir un nuevo cuadro de seguimiento para viajar. Las posiciones y rotaciones de articulaciones se convierten al espacio local de ese grupo. Se tolera hasta 1.2 segundos de ausencia de seguimiento, manteniendo la pose relativa al desplazamiento. Si la ausencia dura más, la pieza queda estacionada en su pose actual y se muestra un aviso; no se intenta inventar la ubicación de una mano invisible. Tras recuperar controles se exige abrir los dedos antes de permitir un nuevo agarre. Al salir de VR se liberan las piezas antes de restablecer el grupo de cámara.

El gesto se calcula con articulaciones WebXR, no depende de microgestos nativos de OVRPlugin. Requiere normal de palma hacia arriba, dedos extendidos, mano frente al visor a una altura razonable y permanencia estable. Un gesto sostenido dispara una sola apertura; debe abandonarse antes de rearmarlo. Las pulsaciones UI, agarres cercanos y navegación tienen prioridades exclusivas. Una mano sin articulaciones válidas no puede iniciar una acción ni confirmar traslado.

El traslado usa los mismos límites del suelo y zonas bloqueadas que el movimiento existente; no es un motor de colisiones volumétricas para todos los muebles/objetos. Las instrucciones de la página y los colores del arco explican el flujo sin agregar un botón fijo frente al usuario.

### Validación

50 pruebas con GLB real y eventos/juntas simulados. Incluyen ambas palmas, una apertura por gesto, rechazo de pellizco/mano cerrada/inclinación incorrecta/articulaciones ausentes; menú oculto durante viajes; arco válido/bloqueado, confirmación por liberación, cancelación por seguimiento y propietario exclusivo; olla real con agua y alimento viajando con cada mano mientras la contraria navega, incluso con una interrupción de seguimiento durante el salto; giro del usuario, liberación deliberada, recuperación sin reiniciar y salida de XR antes de recarga.

En navegador se comprobó apertura de la previsualización, pestaña Ayuda, recuperación manteniendo quemadores encendidos, cierre sin botón flotante, confirmación y recarga completa sin errores. Las pruebas automatizadas y de navegador no equivalen a seguimiento físico en Quest 2: falta la verificación del usuario para confirmar comodidad del gesto y rendimiento en su visor.
