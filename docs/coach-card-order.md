# Orden personal de las tarjetas de Cierre

Cada cuenta empieza con las 14 tarjetas en el orden estándar del HTML. «Editar orden» abre una lista compacta con flechas Subir/Bajar y guardado explícito. Cancelar o Escape descarta el borrador; «Restablecer estándar» prepara el orden original y requiere Guardar. Los controles permanecen visibles mientras la lista se desplaza en móvil.

## Persistencia e integración

- `CoachUser.coachCardOrder` almacena identificadores estables. `[]` significa seguir el orden estándar actual, sin migración para cuentas existentes.
- `/api/coach/me` entrega solo el orden de la cuenta autenticada. `PUT /api/coach/card-order` requiere acceso activo y un `accountId` coincidente con la sesión para impedir escrituras desde una pestaña de otra cuenta. No se utiliza el propietario del equipo.
- La protección existente de origen, cookies y respuestas privadas sigue vigente. El endpoint valida IDs conocidos, longitud y duplicados antes de actualizar exclusivamente al usuario autenticado.
- La lista guardada se combina con el HTML actual: se quitan tarjetas retiradas y se añaden tarjetas nuevas al final. Mantener IDs estables al cambiar títulos; registrar IDs nuevos en `src/coach-card-order.js`.
- Se mueven los nodos originales mediante `TecUi.alignFlow`; no se reconstruyen formularios, botones ni herramientas. Las tarjetas permanecen antes del panel de cuenta. Solo los rótulos «Paso» se renumeran; se conservan IDs de navegación y etapas de la visita.
- El servidor es la fuente de verdad en línea. Una copia local por ID de usuario permite mostrar el último orden confirmado sin conexión; no se encolan cambios. Un error conserva el borrador en el editor para reintentar, sin anunciar éxito.
- El editor se inicia solo en el portal normal, dentro de Cierre. El nuevo CSS respeta el atributo `hidden`. La versión del service worker incluye los nuevos recursos sin cambiar las bases de datos ni catálogos sin conexión.

## Validación (17 de noviembre de 2026)

32 pruebas pasaron: orden estándar y conciliación, separación de cuentas del mismo equipo, restauración, sesión/origen/cambio de cuenta, entradas inválidas, fallo de almacenamiento y regresiones de navegación/formularios/sin conexión.

```sh
NODE_ENV=test node --test tests/coach-card-order.test.js tests/coach-ui-coherence.test.js tests/coach-compact-header.test.js tests/coach-native-offline.test.js tests/coach-offline-forms.test.js
```

La vista local usó la plantilla real y dos cuentas ficticias. Se comprobó mover hacia arriba/abajo (incluida Prueba de agua), Cancelar, Guardar, recargar, alternar cuentas, restablecer estándar, conservar valores y manejadores existentes, y mantener el borrador ante error de guardado. En 390×844 el diálogo cabe en pantalla, la lista se desplaza y las acciones quedan accesibles. Las preferencias de usuarios reales no se alteran durante estas pruebas.
