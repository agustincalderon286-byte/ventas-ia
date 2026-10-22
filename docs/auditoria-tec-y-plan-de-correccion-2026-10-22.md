# Auditoría de TEC y plan de corrección

Fecha: 22 de octubre de 2026. Versión revisada: `a960905`, interfaz `20261022-pages1`.

TEC ya permite recorrer el cierre, consultar productos y revisar Equipo. Antes de cerrar la primera fase conviene resolver problemas de legibilidad, validación de descuentos y protección de datos cuando falla la conexión. El texto amarillo de regalo y descuento es un problema confirmado, pero no es el único.

Esta auditoría deja 15 observaciones priorizadas. Distingue fallos reproducidos, limitaciones confirmadas en el código y riesgos que necesitan una prueba de dispositivo. No se modificó ni publicó código de la aplicación, no se crearon registros en cuentas reales, no se grabó audio y no se efectuaron pagos.

## Alcance de la revisión

- Cuenta real de prueba de David: navegación, Equipo, expediente de Lorena, pestañas de actividad, Mapa y búsqueda de productos. Sin cambiar accesos, contraseñas, mensajes o registros.
- Copia local del mismo código: apertura de herramientas, colores calculados, formularios, calculadora con datos ficticios y fallos de conexión simulados.
- Pantalla de teléfono de 375 píxeles y escritorio. El usuario ya confirmó que el catálogo público funciona en su iPhone X; esta auditoría no sustituye una prueba física completa de grabación o de todos los formularios en ese teléfono.
- Código de autenticación, modo sin conexión, ofertas, documentos privados, grabación y membresías.
- 73 pruebas automatizadas aprobadas. Incluyen funciones, rutas y simulaciones; no son una certificación de todos los botones ni de todas las operaciones en producción.
- Comprobación HTTP sin sesión de las páginas públicas y comprobación local de 24 recursos estáticos referenciados en las páginas revisadas.

## Inventario por prioridad

Alta significa que puede impedir leer una instrucción importante, presentar importes inválidos o dejar información sin protección. Media significa que puede confundir al usuario o interrumpir un flujo. Baja corresponde a consistencia de presentación.

| ID | Prioridad | Área | Observación |
| --- | --- | --- | --- |
| A01 | Alta | Legibilidad | Amarillos y azules muy claros sobre tarjetas claras en oferta, perfil y balance. |
| A02 | Alta | Formularios sin conexión | Encuesta, reclutamiento y resultado no tienen el guardado local de leads y 4 en 14. |
| A03 | Alta | Grabación | El fragmento en curso permanece en memoria hasta que se cierra, normalmente cada cuatro minutos. |
| A04 | Alta | Calculadora | Permite descuentos superiores al 100 % y muestra importes incoherentes sin advertirlo. |
| A05 | Alta | Acceso con red inestable | El acceso local depende de que el navegador declare que está sin conexión. |
| A06 | Media | PDF privados | Conservan el motor incompatible con el iPhone X; la solución ligera cubre los dos documentos públicos. |
| A07 | Media | Oferta especial | El estado no está ligado a una casa o cuenta; reclamar no aplica automáticamente una promoción al pedido. |
| A08 | Media | Cuenta de prueba | David ve botones de suscripción aunque una cuenta de prueba sin cliente Stripe no necesita ese portal. |
| A09 | Media | Prueba de agua | Abre una web después de una operación asíncrona y anuncia apertura sin comprobarla. |
| A10 | Media | Primer paso | «Iniciar práctica» cambia el paso del Coach, pero no inicia un ejercicio de práctica. |
| A11 | Media | Accesibilidad | Faltan nombres accesibles en controles del balance y el orden del documento no coincide con el visual. |
| A12 | Media | Explicación de importes | La calculadora no desglosa claramente sus supuestos y llama ahorro a una suma que incluye el anticipo. |
| A13 | Baja | Marca y lenguaje | Permanecen textos de Agustín y botones en inglés dentro de TEC. |
| A14 | Media | Inicio de la interfaz | Se muestran pestañas fuera de alcance antes de terminar de cargar la cuenta. |
| A15 | Media | Acceso | No hay recuperación de contraseña desde el formulario de entrada. |

## Hallazgos y criterios de corrección

### A01 Textos que no se distinguen del fondo

Confirmado visualmente y con estilos calculados en la copia local del código publicado. Las tarjetas tienen fondo `#f6f8fb` y conservan colores pensados para la interfaz oscura anterior:

| Texto | Color | Contraste aproximado |
| --- | --- | --- |
| Oferta, instrucciones «Cómo usarla» | `#fff0c6` | 1.07:1 |
| Oferta, título «Descuento especial» | `#ffe5c7` | 1.14:1 |
| Perfil principal del cliente | `#d8ebff` | 1.14:1 |
| Porcentaje del balance | `#ffd8a9` | 1.26:1 |

Son textos de aproximadamente 16 píxeles. La referencia de [W3C para contraste mínimo](https://www.w3.org/WAI/WCAG21/Understanding/contrast-minimum.html) establece 4.5:1 para texto normal y 3:1 para texto grande. Estas mediciones corresponden a los fondos sólidos indicados, no a una certificación completa de accesibilidad.

Fuentes: `public/coach/styles.css:4760`, `:4962`, `:5125`, `:5134` y fondo claro en `:7126`.

Corrección: definir colores de texto de resultados y estados que pertenezcan al tema claro. Revisar también mensajes de éxito, advertencia, error, seleccionado y deshabilitado, no solo las instrucciones de la oferta.

Aceptación: oferta 305 y 14407, perfil y balance se leen antes y después de calcular, a 375 píxeles y en escritorio, con contraste suficiente.

### A02 Guardado sin conexión incompleto

Reproducido únicamente en la copia local: al simular falta de red, «Guardar encuesta», «Guardar aplicación» y «Guardar resultado» muestran error, no una confirmación de guardado protegido. Los campos permanecen en pantalla, pero esos envíos no entran a la cola local. No debe confundirse conservar el formulario abierto con guardar un borrador duradero.

Fuentes: POST de encuesta en `public/coach/app.js:4282`, reclutamiento en `:4535` y resultado en `:15104`. No solicitan `offlineQueue`, a diferencia de leads, 4 en 14 y seguimiento de propiedades. El indicador general de modo sin conexión puede decir «guardado local activo» sin distinguir estos formularios.

Corrección: borradores por cuenta y formulario, cola local con identificador idempotente, estado visible de pendiente/enviado/error y recuperación después de cerrar. No anunciar «guardado» hasta confirmar la escritura local.

Aceptación: completar cada formulario sin red, guardarlo, cerrar y reabrir, recuperar la información y sincronizar una sola vez. Cambiar de cuenta no debe mostrar ni enviar los pendientes de otra persona.

### A03 Último fragmento de audio todavía en memoria

Confirmado en el código; el cierre abrupto o suspensión del iPhone no se reprodujo en esta auditoría. `MediaRecorder` produce datos cada segundo, pero el evento los acumula en `segmentParts`. El archivo se escribe localmente cuando se detiene el segmento, normalmente cada cuatro minutos o al finalizar manualmente.

Fuentes: `public/coach/app.js:15733`, `:15770`, `:15850` y `:15874`.

Riesgo: si el sistema termina la página durante el segmento, la parte más reciente puede no haber llegado a IndexedDB. El aviso de salida no sustituye esa escritura. Los segmentos anteriores ya guardados tienen otra situación y no deben describirse como perdidos.

Corrección: reducir la ventana sin protección o persistir subfragmentos de forma compatible con el formato de audio; mantener orden, recuperación e idempotencia de subida. Mostrar hasta dónde está realmente protegido el audio y avisar sobre bloqueo o salida de la aplicación.

Aceptación: pruebas físicas de interrupción, llamada entrante, bloqueo, cierre y recuperación, tanto con red como sin ella. El informe debe identificar con precisión cualquier intervalo no recuperable.

### A04 Descuentos e importes fuera de rango

Reproducido en la copia local. Precio base 1000, anticipo 200 y descuento 10 % producen bruto 1150, descuento 115, saldo 835 y mensual 41.75 según la fórmula actual. Al cambiar el descuento a 150 %, la interfaz muestra descuento 1725 y saldo cero. No presenta advertencia ni límite superior en ese campo.

Fuentes: entrada porcentual en `public/coach/app.js:1775`, porcentaje global en `private/coach-app.html:4888`, cálculo en `public/coach/app.js:2244` y siguientes.

Corrección: validar límites en el cálculo, además de los atributos del campo. Rechazar porcentajes fuera de 0 a 100, importes negativos y combinaciones de anticipo/descuentos que excedan lo permitido por la regla comercial. Evitar que un saldo limitado a cero oculte la entrada inválida.

Aceptación: casos 0, 100, 150, negativos, anticipo mayor al total y descuento individual combinado con global. La interfaz debe explicar qué valor corregir antes de compartir la cotización.

### A05 Red conectada sin salida a internet

Riesgo identificado en código, no reproducido con una red física intermitente. `fetchViewer` permite usar la cuenta almacenada solo si `navigator.onLine` es falso. Una red Wi-Fi puede seguir conectada aunque falle el acceso al servidor; en ese caso la interfaz puede devolver al login pese a tener una sesión local guardada.

Fuente: `public/coach/app.js:8713`, especialmente `:8720`.

Corrección: distinguir errores de transporte de respuestas de autenticación. La alternativa local debe contemplar fallos reales de red sin ignorar un 401/403, una cuenta revocada o el vencimiento de acceso que se defina.

Aceptación: comprobar modo avión, Wi-Fi sin internet y servidor temporalmente inaccesible. No confundir una sesión válida sin red con una sesión rechazada por el servidor.

### A06 PDF privados en teléfonos antiguos

Limitación confirmada en el código. Catálogo público y carpeta pública usan las páginas ligeras que ya funcionaron en el iPhone X. Un PDF que suba un distribuidor sigue pasando al motor PDF.js 6.4.299, cuya distribución legacy documenta Safari 18 o posterior.

Fuentes: `public/coach/document-viewer.js:288` y [compatibilidad publicada por Mozilla](https://github.com/mozilla/pdf.js/wiki/Frequently-Asked-Questions#which-browsersenvironments-are-supported). No se abrió un archivo privado real durante la auditoría.

Corrección: decidir un mecanismo compatible de visualización privada o mostrar una limitación explícita y una alternativa segura. Cualquier conversión debe conservar autenticación, PIN y privacidad; no usar la carpeta pública para estos archivos.

Aceptación: subir un PDF ficticio a una cuenta de prueba y abrirlo con PIN correcto e incorrecto en iPhone X. Siempre debe existir salida al Coach.

### A07 Alcance real de la oferta

Confirmado en código. El estado se guarda bajo una clave global del navegador, sin identificar casa o cuenta. «Reclamar oferta» registra estado y un evento de la demostración; no aplica por sí mismo un importe de descuento, valida el umbral del regalo contra un pedido ni envía una promoción a RoyalOne.

Fuentes: `public/coach/app.js:81`, `:631`, `:2879` y el controlador de reclamar dentro de `initDailyPrizeTool`.

Corrección: ligar la activación a la visita o pedir confirmación al reutilizarla. Aclarar si es un apoyo para negociar una promoción aplicada manualmente o si se quiere convertir en una regla automática. Esa decisión comercial debe preceder una integración nueva.

Aceptación: cambiar de casa o cuenta no debe reutilizar silenciosamente una oferta anterior. El texto debe explicar exactamente qué queda reservado o aplicado al pulsar reclamar.

### A08 Botones de pago en acceso de prueba

Confirmado visualmente con David: aparecen «Suscripción» y «Administrar suscripción», aunque el estado es de prueba. El servidor contempla responder que una cuenta de prueba sin cliente Stripe no necesita portal. No se pulsó ninguno de esos botones.

Fuentes: `private/coach-app.html:52`, `:5680` y `server.js:32562`, especialmente `:32577`.

Corrección: mostrar «Acceso de prueba sin vencimiento» cuando corresponda y ocultar o explicar las acciones de cobro que no aplican. No interpretar esto como un cobro realizado: no se observó ni efectuó ninguno.

Aceptación: prueba gratuita permanente, subcuenta, suscriptor activo y cuenta sin membresía deben ver acciones distintas y apropiadas.

### A09 Apertura de la consulta de agua

Riesgo en código pendiente de prueba física. El botón espera a copiar el ZIP al portapapeles y después llama a `window.open`. Además, el mensaje anuncia que la página se abrió sin comprobar si la apertura fue bloqueada. La herramienta abre EWG y requiere consultar allí; no realiza por sí misma un análisis dentro de TEC.

Fuente: `public/coach/app.js:17185`, `:17200` y `:17219`.

Corrección: ofrecer un enlace de apertura claramente identificado, conservar la acción directa del usuario y proporcionar un enlace de respaldo si no se abre. Usar un nombre como «Consultar agua en EWG» para describir su función real.

Aceptación: Safari normal y PWA instalada, permiso de portapapeles denegado y ventanas emergentes bloqueadas. En ninguno de esos casos debe anunciarse una apertura inexistente.

### A10 El botón de práctica no inicia una práctica

Confirmado en código. «Iniciar práctica» es el mismo activador de etapa que los botones «Estoy aquí». Cambia la etapa y puede mostrar un mensaje fijo del Coach; no inicia un ejercicio con cliente simulado. Si la etapa ya estaba seleccionada, el cambio visible puede ser mínimo.

Fuentes: `private/coach-app.html:1236` y controlador `demoStageButtons` en `public/coach/app.js:16694`.

Corrección: renombrar el botón según su acción actual, o especificar una función de práctica distinta antes de implementarla.

Aceptación: el usuario puede predecir qué ocurrirá por el nombre del botón y recibe una señal visible de que su acción se registró.

### A11 Etiquetas y orden accesible

Confirmado en DOM y código. Los selectores de peso del balance no tienen un nombre accesible asociado a su razón u objeción. Los campos de texto dependen del placeholder. Además, el paso 6 está antes que el 3 y el 5 en el documento, aunque CSS los coloca visualmente en otro orden.

Fuentes: creación de filas en `public/coach/app.js:2430`, orden de tarjetas en `private/coach-app.html` y `public/coach/styles.css:5529`.

Corrección: etiquetas explícitas como «Peso de razón a favor 1» y orden de contenido coherente con el recorrido visual. Mantener foco visible y apertura/cierre accesibles.

Aceptación: recorrer con teclado y VoiceOver debe seguir la secuencia del cierre y anunciar el propósito de cada campo, no solo «Peso 0».

### A12 Supuestos de la calculadora y etiqueta de ahorro

Confirmado en código e interfaz. La fórmula fija agrega 10 % de impuesto y 5 % de envío, y estima el mensual como 5 % del saldo. La tarjeta de producto llama «Ahorro aplicado» a una suma que incluye el anticipo, aunque un anticipo no es un descuento.

Fuente: `public/coach/app.js:2250` y siguientes. La fórmula del servidor coincide en `server.js:25055`.

Corrección: separar precio, impuesto, envío, descuento y anticipo; llamar a cada concepto por su nombre. Mostrar que los pagos son estimaciones bajo la fórmula interna y qué condiciones se deben confirmar al hacer el pedido. Verificar las reglas comerciales con el responsable, sin asumir que los porcentajes actuales están aprobados para cualquier caso.

Aceptación: una persona puede explicar por qué un precio base de 1000 se convierte en 1150 y distinguir los 200 de anticipo de los 115 de descuento del ejemplo.

### A13 Marca y lenguaje mezclados

Confirmado: el resultado de la demo aún dice «Agustín tomará esta demo», y Equipo muestra «Reset password». También quedan referencias antiguas en funciones temporalmente ocultas.

Fuentes: `private/coach-app.html:5442`, `public/coach/app.js:14916` y `:9149`.

Corrección: unificar TEC, Total Elite Coaching y terminología en español. Usar «Restablecer contraseña» y decidir si se conservará «seguimiento» en lugar de «Follow up». No cambiar nombres propios de las personas.

Aceptación: revisar textos de pantallas, avisos, reportes y enlaces compartidos, no solo el encabezado.

### A14 Pestañas visibles durante la carga inicial

Observado al abrir una pantalla nueva: antes de resolver la cuenta aparecen pestañas como Prospección, Marketing, CRM y Agenda, que luego se ocultan. No se observó acceso a información restringida por este destello; es una inconsistencia del estado de carga.

Fuentes: `initCoachAppPage` espera a la cuenta y configuración desde `public/coach/app.js:16223`; los permisos visuales se aplican en `:16255`.

Corrección: un estado de carga inicial que no muestre controles fuera del alcance y que comunique un fallo de carga sin dejar botones aparentemente inertes.

Aceptación: en una conexión lenta no aparecen módulos temporalmente cerrados ni controles de administrador para un usuario no autorizado.

### A15 Recuperación de acceso

Confirmado en el formulario de entrada: no existe «Olvidé mi contraseña». Hay un enlace a Contacto y el líder tiene una acción de restablecimiento para subcuentas; no son una recuperación autónoma desde el login.

Fuente: `public/coach/login/index.html:100` y pie de página. No se solicitó ningún restablecimiento real.

Corrección: definir un flujo seguro de recuperación o señalar claramente cómo solicitar ayuda mientras se mantiene el procedimiento manual.

Aceptación: quien pierde su contraseña sabe qué hacer sin tener que adivinar a quién escribir ni compartir su contraseña.

## Elementos que sí pasaron la revisión

- David ve Cierre, Mapa y Equipo. No aparece la Torre. La navegación directa a `/control/` vuelve a `/coach/app/`. El código protege además las rutas de la Torre con su autorización específica; esta observación no equivale a una auditoría de penetración.
- Equipo muestra a Lorena y abre su expediente. Las pestañas Resumen, Leads, 4 en 14, Audios y Formularios responden y muestran estados vacíos apropiados. No había registros para comprobar aperturas de expedientes con contenido real.
- En la cuenta de David, buscar «olla» devuelve cinco tamaños con cinco miniaturas distintas y cargadas. No se seleccionó ni guardó un pedido.
- Se abren y cierran las herramientas de calculadora, balance, perfil, hoja, encuesta, oferta y aplicación en la copia local.
- La navegación de Mapa muestra sus controles. Las búsquedas y escrituras de notas no se repitieron en la cuenta real para no crear actividad de prueba.
- En la muestra móvil con herramientas abiertas no apareció desbordamiento horizontal general. No se certifica aquí cada tabla futura con datos largos.
- Inicio, login, planes, privacidad, términos, cancelaciones, contacto y visor público respondieron HTTP 200. La raíz del dominio redirige al login. Los 24 recursos estáticos únicos referenciados en las páginas analizadas existen en el proyecto.
- Las 73 pruebas automatizadas terminaron sin fallos.

## Plan propuesto

### Etapa 1 Lectura e importes seguros

Corregir A01 y A04 primero: contraste de todos los estados y límites de la calculadora. Añadir los desgloses y avisos de A12. Son cambios verificables sin rehacer la aplicación ni alterar las membresías.

Salida de etapa: comparación visual en móvil/escritorio, casos de importes válidos e inválidos y pruebas de regresión. Los textos de oferta deben ser legibles antes y después de activar ambos códigos.

### Etapa 2 Protección real de la información

Resolver A02, A03 y A05 en cambios separados: borradores y cola de formularios, persistencia del audio y comportamiento frente a red inestable. Mantener la separación por cuenta y los identificadores que evitan duplicados.

Salida de etapa: una matriz de pruebas con modo avión, Wi-Fi sin internet, reconexión, cierre/reapertura y cambio de cuenta. No debe darse por terminado un guardado solo porque apareció un mensaje verde.

### Etapa 3 Botones y estados coherentes

Resolver A06 a A11 y A13 a A15: documentos privados en equipos antiguos, contexto de ofertas, acciones de pago adecuadas a cada cuenta, consulta de agua, nombre del botón de práctica, accesibilidad, marca, carga inicial y recuperación.

La decisión sobre si oferta y práctica se vuelven funciones nuevas debe quedar explícita. Una corrección de texto no debe anunciar una integración que todavía no existe.

### Etapa 4 Prueba completa antes de ampliar el piloto

Con cuentas y datos ficticios acordados, comprobar:

1. Representante, líder y administrador: navegación y permisos de interfaz y servidor.
2. Crear y reabrir un lead, una hoja 4 en 14, una encuesta, una aplicación y un resultado.
3. Acceso del líder solo a los representantes que correspondan.
4. Guardado offline, recuperación y sincronización sin duplicados de cada formulario.
5. Audio con consentimiento, permisos denegados, interrupciones, transcripción y reporte. Este paso usa servicios y necesita una prueba expresamente acordada, no una grabación de clientes reales.
6. Catálogos públicos y PDF privado ficticio en iPhone X, tableta y computadora.
7. Mapa, notas por casa y conversión a lead sin mezclar cuentas.
8. Búsqueda de productos, imágenes, ampliación, casos límite de importes y paso al Coach.
9. Portal y membresías en un entorno de prueba de Stripe, con renovación, cancelación y webhook. No se autoriza ni requiere un cobro real para esta auditoría.
10. Enlaces de salida, formularios con errores, recuperación y mensajes comprensibles en español.

## Pruebas que quedan pendientes

No se ejecutaron escrituras reales, envíos de mensajes, restablecimientos de contraseña, creación o pausa de cuentas, análisis pagados de audio ni transacciones de Stripe. Tampoco se certificó una integración con Konverta; la conversación con su proveedor es un trabajo aparte. Las observaciones anteriores no deben presentarse como si esas operaciones ya hubieran pasado una prueba completa.

La prioridad recomendada es estabilizar lo que ya existe y comprobarlo con datos de prueba antes de ampliar funciones o usuarios.
