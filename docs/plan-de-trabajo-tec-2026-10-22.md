# Plan de correcciones de Total Elite Coaching

El objetivo es estabilizar la primera fase del Coach antes de ampliar el piloto. Las correcciones se publicarán en bloques comprobables, conservando los datos, las cuentas de prueba, los permisos de la Torre y el visor de catálogos que ya funciona en el iPhone X.

La [auditoría del 22 de octubre](auditoria-tec-y-plan-de-correccion-2026-10-22.md) contiene la evidencia de cada hallazgo. Este plan distingue cambios terminados de trabajo pendiente; no implica que toda la aplicación esté certificada.

## Orden de trabajo

| Etapa | Alcance | Criterio para darla por terminada | Estado |
| --- | --- | --- | --- |
| 1 Lectura y calculadora | A01, A04, A12. Contraste de ofertas y resultados, importes válidos y explicación de estimaciones. | Textos legibles en los estados revisados; errores explícitos para porcentajes, negativos y pagos que superan el pedido; pantalla y Coach usan el mismo cálculo; pruebas móvil y escritorio. | Publicada y verificada |
| 2 Protección de información | A02, A03, A05. Borradores, cola sin conexión, persistencia incremental de audio y recuperación ante red inestable. | Guardado confirmado por almacenamiento local; sincronización sin duplicados; separación por cuenta; pruebas de cierre y reapertura, reconexión y rechazo de sesión. | Formularios publicados; audio y sesión pendientes |
| 3 Navegación y coherencia | A06 a A11, A13 a A15. Documentos privados en iPhone antiguo, ofertas por sesión, cuentas de prueba, agua, etiquetas, accesibilidad, marca, carga inicial y ayuda de acceso. | Cada control anuncia lo que realmente hace; navegación y permisos correctos; compatibilidad y estados de error comprobados. | Pendiente |
| 4 Validación integral del piloto | Recorridos de representante, líder y administrador con datos ficticios. | Leads, 4 en 14, formularios, audios y mapa reabren correctamente; el líder ve solo su equipo; catálogos y offline funcionan en dispositivos acordados. | Pendiente |

## Primera entrega

1. Cambiar el amarillo casi invisible de regalo y descuento por texto oscuro sobre las tarjetas claras. Revisar también perfil del cliente, balance de decisiones, avisos de calculadora y placeholders. No cambiar la paleta del portal de telemarketing.
2. Validar los números antes de mostrar totales o enviarlos al Coach. Aceptar porcentajes entre 0 y 100; rechazar negativos, números no finitos y más de dos decimales. No convertir un escenario inválido en un balance de cero.
3. Comprobar que anticipo más descuentos no excedan el importe, tanto por producto como en el resumen. Conservar los valores escritos para que la persona pueda corregirlos.
4. Calcular en centavos y usar una sola función para la pantalla y el escenario del Coach. Separar «Descuento aplicado» de «Anticipo» y mostrar el desglose de impuesto y envío.
5. Explicar las fórmulas vigentes: impuesto estimado de 10%, envío de 5% y mensualidad estimada de 5% del balance; semanal = mensual / 4 y diario = semanal / 7. No cambiar términos comerciales ni presentarlos como crédito aprobado.
6. Ejecutar pruebas automatizadas y visuales, actualizar la versión de la PWA, publicar y comprobar la entrega en el dominio. La caché de catálogos y los pendientes locales se conservan.

## Siguiente entrega de protección de información

La entrega de formularios protege encuesta de salud, aplicación de negocio y resultado de demostración. Los borradores se separan por cuenta y formulario. El paso de borrador a pendiente se realiza en una sola transacción local; la confirmación del servidor y la eliminación del pendiente también se guardan juntas. La interfaz distingue borrador, pendiente, confirmado y error, y permite recuperar rechazos de validación para corregirlos. No promete almacenamiento local antes de confirmar la escritura.

El servidor conserva un comprobante por cuenta, ruta e identificador de solicitud. Un reintento con los mismos datos devuelve la respuesta confirmada; datos diferentes con el mismo identificador se rechazan. Si una operación del servidor terminó con un fallo parcial o se interrumpió antes de confirmar su resultado, requiere revisión y conserva la copia local: no se ejecuta otra vez automáticamente. Esto evita una segunda venta, pero no equivale a recuperar automáticamente cualquier fallo interno del servidor. Las acciones externas de reclutamiento mantienen su comportamiento previo y no se repiten al reproducir una respuesta ya confirmada.

Después se protegerá el segmento de audio mientras se graba. La persistencia debe permitir reconstruir un archivo reproducible tras una interrupción; no se tratará cada fragmento del grabador como si fuera un archivo independiente. La recuperación no debe mezclar grabaciones entre cuentas. Sigue siendo necesario el consentimiento de quienes participan.

Por último se distinguirá una caída de red de un rechazo de autenticación. Un error de conexión no debe borrar el trabajo local; un rechazo de permisos no debe abrir información usando una sesión guardada. La vigencia del acceso offline se definirá y probará explícitamente.

## Límites y verificación final

Las pruebas ordinarias se harán con datos ficticios en una copia local. No requieren crear actividad en las cuentas de David o Lorena, enviar mensajes, grabar clientes ni efectuar cobros.

Las pruebas de micrófono, interrupción real en iPhone X y análisis de audio requieren una sesión de prueba acordada. Las pruebas de membresía se harán en el entorno de pruebas de Stripe, no mediante un cargo real. Konverta permanece como integración aparte mientras su proveedor responde.

No se borrarán funciones desactivadas ni registros existentes. Cada entrega tendrá pruebas, una revisión de los archivos modificados, una versión identificable y un respaldo idéntico en el SSD externo.

## Registro de entregas

- Versión `20261022-audit2`: implementada y probada localmente. Las 84 pruebas automatizadas pasan, incluidas 11 nuevas de cálculo, integración de interfaz y contraste. Comprobados los códigos 305 y 14407, balance y perfil; un 150% se rechaza y un escenario de $1000 con anticipo de $200 y descuento de 10% devuelve balance de $835 y descuento de $115. Revisión responsive con anchos efectivos de 376 y 1029 píxeles sin desbordamiento horizontal en la muestra. La prueba responsive no sustituye una prueba física del iPhone X.
- Publicación confirmada en `https://totalelitecoaching.com`, código del commit `e058265`. Login, aplicación, estilos, función de cálculo y service worker públicos coinciden exactamente con los archivos locales. La página privada autenticada carga `20261022-audit2`; se comprobó el rechazo del 150% y la recuperación al ejemplo válido sin guardar pedidos ni enviar un escenario al Coach. Los colores calculados de oferta, perfil y balance corresponden a las correcciones. David conserva Cierre, Mapa y Equipo, sin Torre.
- En la primera entrega no se realizaron grabaciones, análisis pagados, cobros, envíos de mensajes ni cambios de cuentas.
- Entrega de formularios `20261022-forms1`: 100 pruebas automatizadas aprobadas, incluidas 16 nuevas sobre borradores, transacciones y comprobantes del servidor. En un servidor local aislado con registros ficticios se recuperaron los tres borradores tras reabrir la página; sus pendientes se conservaron al impedir escrituras tanto desde la página como desde el service worker. La cuenta B no mostró los pendientes de A; al volver a A se confirmó un registro de cada formulario. Una encuesta adicional con respuesta perdida terminó con una sola creación adicional. No se escribieron registros ni se hicieron envíos en producción durante estas pruebas.
- Formularios publicados en `https://totalelitecoaching.com`, código `ae61c23`. Login, aplicación, módulo de formularios, estilos y service worker coinciden exactamente con los archivos locales. En la página autenticada de David se comprobó la carga de los cuatro controles de estado, incluidos los dos espacios de aplicación de negocio, y la apertura de la encuesta. Conserva Cierre, Mapa y Equipo, sin Torre. Las pruebas de escritura, recuperación y sincronización se limitaron al servidor local con datos ficticios.
- La interrupción física en iPhone X, el almacenamiento incremental del audio y la recuperación de sesión ante Wi-Fi sin internet siguen pendientes. Las etapas 3 y 4 tampoco están cerradas.
