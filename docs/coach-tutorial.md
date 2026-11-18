# Tutorial y ayuda

Mi cuenta incluye un botón que abre un diálogo propio con un recorrido de tres minutos, nueve guías con capturas y quince preguntas frecuentes (incluidas dos sobre compartir el Chef y su página de clientes) buscables. No cambia el área activa ni envía formularios. Cerrar o Escape pausa el video y devuelve el foco a Mi cuenta. También se pausa al ocultar la página; un evento de reproducción tardío no puede reiniciarlo con el diálogo cerrado.

El video es la versión vigente de TEC Coach con Power Blender Max, de 180 segundos y 1920×1080, sin recodificar. SHA256: `99be143ac51c8faaeac1ec873537f61932a4ccef8c3fb6455c16a312b8b60086`. No se generaron nuevas voces. Las capturas de inicio, orden, encuesta, cálculo, audiolibros, actividad y mapa se extrajeron del recorrido. Las de voz y mensajes corresponden a la interfaz publicada el 17 de noviembre. El recorrido no enseña el gesto nuevo de voz; la guía específica sí lo explica. Los datos e importes de las capturas son ejemplos, no cotizaciones vigentes.

Todos los medios permanecen en `private/coach-tutorial/`. `/api/coach/tutorial/media/:asset` exige acceso activo, permite solo la lista explícita de archivos y devuelve `private, no-store`, `Vary: Cookie` y `noindex`. Express conserva los rangos de bytes para adelantar el MP4. El service worker excluye estos medios de sus cachés; solo se añaden el CSS y JS genéricos al shell. El video usa controles nativos, no se reproduce automáticamente y tiene texto alternativo mediante las guías. Las imágenes se abren en otra pestaña para ampliarlas.

104 pruebas pasan en conjunto, incluidas diez nuevas sobre autenticación de medios, rangos, rutas ajenas, referencias, carga perezosa, cierre/foco, búsqueda con acentos, error de video y pausa en segundo plano. La revisión local con plantilla real y cuenta ficticia comprobó reproducción a 180 s/1080p, pausa al cerrar, menú, búsqueda sin resultados y lectura móvil de 390×844 sin desbordamiento.

## Acceso desde iPhone y Android · 17 noviembre 2026

El tutorial añade dos videos verticales dentro de guías desplegables: iPhone/Safari (64 s) y Android/Chrome (67.5 s). Son los MP4 originales de las grabaciones de los simuladores, con voz BF16 local, música y subtítulos; se copiaron sin recodificar. Android muestra Create shortcut y un icono que abre Chrome, no una instalación WebAPK verificada. Los medios nuevos mantienen el mismo acceso activo, caché privada y soporte de rangos. Cada video móvil se carga al abrir su guía; cerrar, plegar o abandonar el tutorial detiene la reproducción, y reproducir un video pausa los otros.

La entrada pública `/coach/` y el registro `/coach/planes/` comparten `coach-public.css`, limitado a `coach-public-body`. Conservan el flujo de cuenta, planes, precios y pago existente; usan los colores azul/rojo y superficies claras de la aplicación.

## Bienvenida y capítulos · 17 noviembre 2026

`TecCoachOnboarding.init({ user })` se llama después de verificar el acceso y revelar la app. La guía tiene tres pasos informativos: comenzar en Cierre, ubicar herramientas y abrir Tutorial y ayuda. No activa visitas ni envía formularios. Se puede repetir desde Mi cuenta → Guía de bienvenida. Guarda «completado» o «omitido» por `user.id` en este navegador solamente al pulsar Listo, Ahora no o Terminar y ver tutorial. Escape cierra sin marcar la guía como completada. No se abre automáticamente si hay otro diálogo, una edición activa o medios reproduciéndose; sin almacenamiento funciona y puede volver a aparecer. Cambiar de cuenta elimina los manejadores anteriores.

El recorrido existente tiene un índice desplegable de catorce capítulos. Los tiempos exactos provienen de la línea de tiempo de la producción y están registrados, junto con los hashes de procedencia, en `coach-tutorial-chapters.json`. Elegir capítulo pausa los reproductores y prepara el punto elegido, sin reproducir automáticamente. Si aún faltan los metadatos, conserva la última selección; cerrar cancela un salto pendiente. Los videos y audios originales permanecen intactos.
