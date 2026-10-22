# Mapa de propiedades de TEC — primera versión

## Uso

En el Coach privado, abrir la pestaña **Mapa**. Escribir número y calle en Dirección y la ciudad de Cook County en el campo separado. Elegir 100, 200 o 300 metros. Si hay varias unidades o propiedades para la misma búsqueda, seleccionar la coincidencia correcta.

La dirección de la cita aparece en rojo; las propiedades cercanas aparecen en azul. La lista muestra dirección, propietario registrado disponible y distancia en línea recta. Seleccionar hasta cuatro direcciones y abrir la ruta en Google Maps. Es un recorrido a pie desde la cita, en orden de selección, no una ruta optimizada dentro de TEC.

La búsqueda y el mapa de calles requieren conexión. No se crean leads automáticamente ni se deduce el idioma o el origen étnico de los propietarios. El botón de cliente activo copia una dirección ya seleccionada en el CRM; conviene revisar que calle y ciudad estén separadas antes de buscar.

## Datos y límites

- Fuentes: [Assessor - Parcel Addresses](https://datacatalog.cookcountyil.gov/d/3723-97qp) y [Assessor - Parcel Universe (Current)](https://datacatalog.cookcountyil.gov/d/pabr-t5kh).
- La búsqueda utiliza el año más reciente disponible para la ciudad. Las coordenadas provienen del conjunto Current, relacionadas por PIN completo de 14 dígitos.
- El propietario se obtiene de `owner_address_name`, sin sustituirlo por el contribuyente `mail_address_name`. No se descargan direcciones postales privadas ni datos de contacto del propietario.
- Una coincidencia sin coordenadas no se ubica artificialmente. Las direcciones que no se encuentran se informan al usuario.
- Se consultan hasta 500 parcelas del rectángulo alrededor de la cita, se filtran por radio real y se toman hasta las 100 primeras por distancia dentro de esa muestra. Se consultan sus direcciones y se muestran hasta 60 direcciones distintas. Cuando cualquiera de los límites recorta el resultado, la pantalla advierte que no se muestra toda la zona. En áreas densas no se garantiza incluir todas las propiedades más cercanas del radio.
- Parcelas con la misma dirección se agrupan y se muestran sus propietarios disponibles. Una parcela no equivale necesariamente a una vivienda o entrada física.
- Los nombres pueden estar desactualizados y no indican quién reside en la propiedad.
- Endpoints privados: `GET /api/coach/properties/search` y `GET /api/coach/properties/nearby`; requieren sesión con acceso activo al Coach. Las respuestas no se guardan en caché HTTP pública.
- Límite de 30 peticiones entre ambos endpoints por cuenta cada cinco minutos. Caché de resultados públicos en memoria por cinco minutos, máximo 100 entradas, con un máximo de ocho consultas simultáneas a la fuente por proceso.

## Mapa y navegación

Leaflet 1.9.4 y su licencia están incluidos en `public/coach/vendor/leaflet`. El código del mapa se inicializa después de validar la sesión. La biblioteca se carga cuando hay resultados para mostrar.

El fondo cartográfico usa `https://tile.openstreetmap.org/{z}/{x}/{y}.png`, con atribución visible y las cabeceras de caché del navegador. No se descargan áreas masivamente ni se precargan teselas para uso offline. Este servidor es apropiado para el piloto de bajo volumen; no ofrece un SLA y se debe revisar un proveedor de teselas si crece el uso. Referencia: [Tile Usage Policy](https://operations.osmfoundation.org/policies/tiles/).

Las rutas se abren mediante [Maps URLs](https://developers.google.com/maps/documentation/urls/get-started), sin clave de Google Maps. Se limita a cuatro destinos para no superar tres puntos intermedios en navegadores móviles. Abrir la ruta transmite a Google Maps únicamente las direcciones de la cita y las paradas elegidas, no los nombres de los propietarios.

## Verificación realizada

- 50 pruebas automáticas del proyecto aprobadas, incluyendo ocho nuevas de validación, relación de registros, campos faltantes, agrupación de parcelas, límites, fallos de la fuente, autenticación y límites por cuenta.
- Búsqueda y mapa con datos públicos reales de Hillside desde el mismo módulo de servidor utilizado en producción.
- Selección de cuatro paradas, bloqueo de paradas adicionales y enlace con origen, destino y tres puntos intermedios.
- Cuenta local de prueba sin permisos de administrador: se ven Cierre y Mapa; la Torre permanece oculta; cambiar de pestaña muestra únicamente la sección elegida.
- Diseño comprobado en escritorio y a un ancho móvil observado de 375 px, sin desbordamiento horizontal.
- Manejo de dirección inexistente y recuperación con una búsqueda válida.
- Vista previa sin errores de JavaScript. La previsualización usa un servidor temporal local con datos de cuenta ficticios y no forma parte del código publicado.

No se contrató ninguna suscripción ni se enviaron leads a un servicio externo durante las pruebas.

## Fichas de seguimiento por casa

La versión `20261022-visits1` añade el botón **Seguimiento** en cada propiedad, en su marcador y en la dirección de la cita. Permite guardar No contestaron, Nos atendieron, Volver después, Cita agendada o No volver a visitar, junto con notas de hasta 2000 caracteres. Los dos estados con fecha requieren día y hora. Estas fechas son datos de seguimiento; no generan por sí solas notificaciones ni eventos externos de calendario.

**Mis seguimientos por casa** permite volver a abrir las fichas recientes sin repetir la búsqueda, incluso cuando no hay conexión. Las fichas se identifican por dirección, ciudad y estado normalizados, no por el nombre del propietario; un cambio de PIN o un ZIP faltante no separa las notas de una misma dirección. Las unidades con direcciones distintas conservan fichas separadas. Los estados aparecen en las tarjetas y con colores en los marcadores; los resultados y la selección de rutas se conservan.

Las notas son privadas del usuario autenticado que las registra, no del propietario del inmueble ni del administrador de su equipo. `CoachPropertyVisit` guarda cada cambio como un evento, con índice único por usuario e identificador de petición. La ficha muestra el evento con fecha de captura más reciente, aunque otro evento más antiguo llegue después desde un dispositivo sin conexión. El historial se conserva en el servidor; esta versión muestra el estado actual, no un visor del historial completo.

Cada escritura se confirma primero en la bandeja de IndexedDB y después se intenta enviar. La interfaz distingue protegido en el teléfono, pendiente de sincronizar y sincronizado. Las transacciones locales sólo se consideran guardadas cuando se confirma su commit, incluyendo detección de abortos por almacenamiento. Se reutilizan los mecanismos de reconexión y sincronización de la PWA. La caché de fichas está separada por ID y correo de cuenta; el servidor rechaza peticiones pendientes que no correspondan a la cuenta que inició sesión.

En **Crear lead con datos de contacto**, el vendedor introduce nombre y teléfono o correo de quien lo atendió. No se presupone que el propietario registrado sea esa persona. La creación es manual y reutiliza el CRM y los destinos que el usuario ya tenga configurados en TEC. Se conserva la dirección, las notas y la fecha de seguimiento, y se guarda el identificador del lead para no crear otro al reintentar. No se añadió ninguna conexión con Konverta.

Endpoints privados adicionales:

- `GET /api/coach/property-visits`: hasta 100 fichas recientes de la propia cuenta.
- `POST /api/coach/property-visits/lookup`: consulta de hasta 61 claves de propiedades de la propia cuenta.
- `POST /api/coach/property-visits`: nuevo evento idempotente de seguimiento.
- `POST /api/coach/property-visits/lead`: creación manual y enlace del lead.

Verificación de esta ampliación: pruebas de aislamiento entre usuarios, idempotencia, registros offline fuera de orden, fechas requeridas, límites de notas, conservación del enlace al lead, recuperación desde la bandeja local, confirmación de transacciones y orden de sincronización. En la vista previa con almacenamiento de servidor simulado se guardó una nota con fecha sin conexión simulada, se recargó la página, se recuperó la ficha y se sincronizó al reconectar. Se creó un único lead ficticio y se comprobó el diseño a 375 px sin desbordamiento. No se escribieron notas ni leads de prueba en la base de producción.
