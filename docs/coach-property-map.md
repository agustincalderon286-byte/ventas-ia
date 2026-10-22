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
