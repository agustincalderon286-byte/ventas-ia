# Las 25 insignias de Audiolibros TEC

1. **Primer minuto** — Escucha 1 minuto de contenido distinto.
2. **Cinco para ti** — Escucha 5 minutos de contenido distinto.
3. **Un cuarto de hora** — Escucha 15 minutos de contenido distinto.
4. **Media hora de historias** — Escucha 30 minutos de contenido distinto.
5. **Una hora de ideas** — Escucha 1 hora de contenido distinto.
6. **Oídos curiosos** — Escucha 3 horas de contenido distinto.
7. **Gran escucha** — Escucha 5 horas de contenido distinto.
8. **Diez horas contigo** — Escucha 10 horas de contenido distinto.
9. **Primera exploración** — Escucha al menos el 80 % de una muestra.
10. **Tres descubrimientos** — Escucha al menos el 80 % de 3 muestras diferentes.
11. **Explorador de muestras** — Escucha al menos el 80 % de 5 muestras diferentes.
12. **Primera historia completa** — Completa tu primer audiolibro.
13. **Trilogía personal** — Completa 3 audiolibros diferentes.
14. **Biblioteca en marcha** — Completa 5 audiolibros diferentes.
15. **Coleccionista TEC** — Completa 8 audiolibros diferentes.
16. **Mente en movimiento** — Completa Como un hombre piensa.
17. **Descubre tus diamantes** — Completa Acres de diamantes.
18. **Estratega** — Completa El arte de la guerra.
19. **En el principio** — Completa Génesis.
20. **Un nuevo camino** — Completa Éxodo.
21. **Espíritu de iniciativa** — Completa ¡Ayúdate!.
22. **Pensamiento y acción** — Completa La ciencia de hacerse rico.
23. **Buscador de principios** — Completa El Kybalión.
24. **Lee y escucha** — Escucha 5 minutos con la lectura sincronizada abierta y la página visible.
25. **Tres mundos** — Escucha 5 minutos de tres temas: desarrollo personal, estrategia, Biblia o filosofía.

## Conteo y guardado

Se registran intervalos realmente reproducidos (tiempo de contenido, no tiempo de reloj). Una posición guardada o adelantar la barra no prueban escucha. Se unen intervalos solapados entre muestras, libros completos, pestañas y dispositivos para no duplicar tiempo. Un libro requiere al menos 90 % reproducido y haber escuchado su final. Los logros son simbólicos: no otorgan descuentos ni cambian acceso o cobros. No es una medición antifraude ni comprueba atención humana.

Los logros comienzan con esta versión; las posiciones anteriores se conservan, pero no se convierten en horas ni libros completados. El catálogo actual permite alcanzar las 25 insignias.

Invitado: estado local separado. Cuenta: caché local por identificador opaco y colección MongoDB TecAudioAchievements, con revisión para mezcla atómica e idempotente. No se mezclan automáticamente los logros de invitados con cuentas. El usuario autenticado ve sus logros incluso con membresía vencida; solo una membresía activa agrega crédito de libros completos en el servidor. Los endpoints privados son GET/PUT /api/audiolibros/logros, con el mismo control de origen que membresías. Se validan IDs, tipos, límites de intervalo y payload de hasta 200 KB. No se guarda correo en el navegador ni se envían logros a Analytics.

Guardado local cada 10 segundos de eventos de reproducción, pausa, cambio de libro y salida. Sincronización al entrar, cada 30 segundos de eventos, pausa, salida y vuelta de conexión; las respuestas tardías de otra cuenta se descartan. Se conserva el estado local si falla una solicitud y se vuelve a intentar. Un cierre forzado puede perder los últimos segundos desde el último guardado; no habilita por sí mismo la biblioteca ni los audios sin internet.

Diseño: 25 medallones SVG propios, estado desbloqueado en dorado, condiciones legibles y progreso; acceso superior, desde Cuenta y en navegación móvil. No hay popups que pausen audio ni sonidos adicionales.

Pruebas: reglas de 25 logros, cobertura real, seek/replay, 1x/2x, lectura visible, recuperación offline, varias cuentas/pestañas, carreras de escritura, autorización y muestras frente a libros completos. Verificación visual en 320, 390, 768 y 1024 px.
