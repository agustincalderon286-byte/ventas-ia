# Acceso a Mensajes desde la cabecera

La navegación compacta oculta la pestaña Mensajes porque ofrece un botón junto a Nuevo cliente. Antes, tanto el selector programático como el controlador de áreas rechazaban esa pestaña oculta: el botón no hacía nada aunque tuviera mensajes pendientes.

El botón ahora permite abrir esa única área auxiliar en el portal normal cuando su acceso de cabecera está disponible. Las demás áreas ocultas y el portal telemarketing conservan sus restricciones. El título cambia a Mensajes, se indica el estado activo del botón y se vuelve al inicio para mostrar el centro interno.

Cada apertura consulta de nuevo `GET /api/coach/messages/overview`, con los mismos permisos de cuenta y alcance de boletines que ya aplica el servidor. No envía avisos ni marca nada como leído. Los errores se muestran sin cerrar el área y se limpian al reintentar. La versión del script y del service worker incluye la corrección.

Validación: tres pruebas de navegación cubren el botón con pestaña oculta, actualización de avisos, regreso a Cierre, bloqueo de áreas restringidas/telemarketing y recuperación tras error. Pasaron junto a las 32 pruebas existentes de tarjetas, navegación y formularios sin conexión. La plantilla real se probó con avisos ficticios antes de desplegar.
