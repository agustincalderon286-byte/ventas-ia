# Plan de correcciones de Total Elite Coaching

El objetivo es estabilizar la primera fase del Coach antes de ampliar el piloto. Las correcciones se publicarán en bloques comprobables, conservando los datos, las cuentas de prueba, los permisos de la Torre y el visor de catálogos que ya funciona en el iPhone X.

La [auditoría del 22 de octubre](auditoria-tec-y-plan-de-correccion-2026-10-22.md) contiene la evidencia de cada hallazgo. Este plan distingue cambios terminados de trabajo pendiente; no implica que toda la aplicación esté certificada.

## Orden de trabajo

| Etapa | Alcance | Criterio para darla por terminada | Estado |
| --- | --- | --- | --- |
| 1 Lectura y calculadora | A01, A04, A12. Contraste de ofertas y resultados, importes válidos y explicación de estimaciones. | Textos legibles en los estados revisados; errores explícitos para porcentajes, negativos y pagos que superan el pedido; pantalla y Coach usan el mismo cálculo; pruebas móvil y escritorio. | Publicada y verificada |
| 2 Protección de información | A02, A03, A05. Borradores, cola sin conexión, persistencia incremental de audio y recuperación ante red inestable. | Guardado confirmado por almacenamiento local; sincronización sin duplicados; separación por cuenta; pruebas de cierre y reapertura, reconexión y rechazo de sesión. | Formularios, audio y sesión publicados; falta validación física |
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

El audio se escribe incrementalmente en el dispositivo conforme el navegador entrega datos. El indicador de tiempo protegido avanza solo después de confirmar la escritura. Al terminar cada segmento de hasta cuatro minutos, una transacción sustituye sus partes por un archivo completo antes de intentar enviarlo. La captura no espera a la red para empezar el siguiente segmento. Sigue siendo necesario el consentimiento de quienes participan.

Un cierre inesperado conserva los datos ya escritos, pero el último segmento puede no ser reproducible. La interfaz permite descargarlo para revisión, sin enviarlo automáticamente al análisis. La recuperación manual puede analizar solo los segmentos completos y conserva la parte interrumpida por separado. Si no hay segmentos completos conocidos, no ofrece analizar bytes parciales. La separación por cuenta y, donde está disponible, un bloqueo entre ventanas impiden recuperar una captura activa de otra ventana. En navegadores sin ese bloqueo es necesario cerrar las demás ventanas antes de recuperar.

Si falla el almacenamiento, la aplicación detiene la captura y ofrece una descarga de emergencia cuando aún conserva los bytes en memoria. No garantiza recuperar datos que el sistema no entregó, que nunca se guardaron, ni que el navegador eliminó. El indicador mide la persistencia confirmada, no una copia en el servidor.

Una caída de red, tiempo de espera o error temporal del servidor puede utilizar el acceso local verificado por un máximo de 24 horas. No se prolonga ese plazo con lecturas de caché y una membresía ordinaria vencida no lo mantiene. Una respuesta anónima o un rechazo 401 o 403 invalida el acceso guardado sin borrar los pendientes. La interfaz avisa del modo local; las operaciones del servidor siguen exigiendo sesión y permisos válidos.

## Actualizaciones con usuarios activos

La nueva PWA no fuerza la activación del service worker ni recarga la pantalla mientras hay ventanas del Coach abiertas. Un aviso permite seguir trabajando; la versión nueva se activa al cerrar todas esas ventanas. La instalación rechaza una actualización si faltan archivos esenciales. Conserva una versión anterior de interfaz y recursos para permitir el regreso sin conexión, además de los catálogos y documentos guardados.

El servidor distingue disponibilidad de arranque y cierre: el chequeo de salud devuelve 503 si la base de datos no está conectada o el proceso está terminando. Ante una señal de cierre deja de tomar trabajos nuevos y conserva la base abierta hasta terminar las solicitudes y el trabajo en curso, con un límite. La configuración del repositorio solicita 300 segundos de margen en Render; ese valor solo aplica si el servicio adopta la configuración y debe comprobarse en su panel antes de depender de él.

Render admite despliegues con cambio de tráfico cuando la instancia nueva está lista, pero los servicios con disco persistente tienen una excepción. Los fallos de infraestructura, las solicitudes que exceden el plazo y los cambios incompatibles de base de datos siguen requiriendo medidas adicionales. Las próximas entregas deben conservar compatibilidad con clientes anteriores y usar cambios de esquema aditivos; las migraciones destructivas requieren un procedimiento aparte. Véase [despliegues de Render](https://render.com/docs/deploys).

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
- Entrega de audio y actualizaciones `20261022-audio3`: 114 pruebas automatizadas aprobadas. Cubren el orden de bytes, confirmación transaccional, errores de espacio, copia de emergencia, recuperación por cuenta, bloqueo de captura entre ventanas, acceso local de 24 horas, rechazo de sesión, espera de actualizaciones, conservación de la versión anterior y cierre ordenado del servidor.
- Prueba local con MediaRecorder nativo y una señal de audio sintética, sin micrófono real: dos grabaciones finalizadas llegaron al servidor ficticio como WebM con cabecera válida; dos cierres simulados conservaron la parte interrumpida sin analizarla automáticamente. En la versión nueva se comprobó el botón desactivado cuando no había un segmento completo. Con el servidor devolviendo 503 y el navegador todavía conectado, la página reabrió con la cuenta local y mostró el aviso de acceso limitado. No hubo registros, transcripciones pagadas ni cobros de producción en estas pruebas.
- Audio y actualizaciones publicados en `https://totalelitecoaching.com`, código `1c5d857`. Login, aplicación, módulo de seguridad, estilos y service worker coinciden por SHA-256 con los archivos locales; el chequeo de salud responde 200. La página autenticada de David carga la versión nueva y el panel de recuperación sin errores de consola en la muestra. Mantiene Cierre, Mapa y Equipo, sin Torre. Se dejó intacta su ventana anterior, sin forzar su actualización. Respaldo idéntico verificado en el SSD, carpeta `TEC/audio-safe-updates-20261022`.
- La interrupción física, bloqueo de pantalla y falta de espacio en iPhone X, así como el análisis real tras una desconexión, requieren una sesión de prueba. Las etapas 3 y 4 no están cerradas. No se ha certificado disponibilidad continua ni modificado el plan comercial de Render.
