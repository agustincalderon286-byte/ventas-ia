# Boletines leídos al abrir Mensajes

Al abrir Mensajes en una pestaña visible, después de cargar y presentar los avisos, el cliente confirma los identificadores recibidos con `POST /api/coach/announcements/read`. La respuesta confirma qué avisos fueron registrados. Solo entonces se actualizan los rótulos y el contador. Cargar Coach en Cierre o en segundo plano no registra lecturas.

El servidor valida sesión activa, cuenta coincidente (para pestañas antiguas), origen, formato, duplicados y un máximo de 20 avisos, igual al límite actual del listado. Vuelve a obtener los avisos accesibles para esa cuenta y rechaza identificadores ajenos. Persiste recibos por usuario y boletín usando la colección y el índice único existentes. Repetir una confirmación no duplica recibos.

Los chats privados y soporte conservan sus pendientes. El botón «Marcar todo como leído» mantiene su alcance explícito. Ante fallo de conexión o escritura el contador conserva los avisos pendientes y se puede reintentar abriendo Mensajes. Los avisos que no estaban en la lista enviada no se marcan, aunque llegue uno nuevo durante la petición. Este cambio sustituye el comportamiento anterior documentado en `coach-messages-navigation.md`, que no registraba lecturas al abrir.

Validación: pruebas del endpoint para persistencia, aislamiento de cuentas, boletines ajenos, origen, sesión, listas inválidas, reintentos y almacenamiento fallido; pruebas de navegación para apertura, lectura confirmada, preservación de chats/soporte y pestañas en segundo plano; regresiones de tarjetas, navegación y formularios sin conexión.
