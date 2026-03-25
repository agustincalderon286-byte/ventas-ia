# Agustin 2.0 Lead Memory Field Map

Este documento define que campos conviene conservar, limpiar y derivar de estas dos fuentes:

- [4-en-14-cnl.csv](/Users/monse/Documents/New%20project/tmp_exports/4-en-14-cnl.csv)
- [esmeralda-rifa-digital.csv](/Users/monse/Documents/New%20project/tmp_exports/esmeralda-rifa-digital.csv)

Objetivo:

- meter solo lo que suma
- evitar ruido de hojas crudas
- convertir listas operativas en memoria util para Agustin 2.0
- dejar una base lista para Mongo o CSV maestro

## Principios

- No meter filas completas como texto libre al prompt.
- Separar `perfil`, `estado de contacto`, `insights` y `referidos`.
- Guardar notas crudas solo como respaldo; la IA debe trabajar sobre resumenes y etiquetas.
- Excluir pruebas como `Pedro Prueba`, `Test1`, `Agustin prueba` y variantes.

## Fuente 1: 4 En 14

Muestra actual:

- 13 filas
- 1 anfitrion de prueba detectado: `Test1`

Esta hoja sirve mejor como memoria de programa y referidos.

### Tabla destino 1: `programa_4en14_eventos`

Una fila por anfitrion / evento.

| Campo final | Tipo | Viene de | Regla |
| --- | --- | --- | --- |
| `source` | string | fijo | `4_en_14` |
| `source_row_id` | string | derivado | hash o id interno de fila |
| `created_at_raw` | string | `Timestamp` | conservar valor original |
| `created_at` | datetime | `Timestamp` | normalizar a ISO si es posible |
| `host_name` | string | `Nombre De Anfitrion` | trim |
| `host_phone_raw` | string | `Telefono de anfitrión` | conservar original |
| `host_phone` | string | `Telefono de anfitrión` | normalizar a 10 digitos o E.164 si aplica |
| `gift_selected` | string | `Regalo Elejido` | trim, corregir ortografia si conviene |
| `rep_name` | string | `Nombre De Representante` | trim |
| `rep_phone_raw` | string | `Telefono De Representante` | conservar original |
| `rep_phone` | string | `Telefono De Representante` | normalizar |
| `program_window_raw` | string | `Feha De Inicio & Vencimiento` | conservar valor original |
| `program_start_date` | date | `Feha De Inicio & Vencimiento` | derivar si la fecha viene clara |
| `program_end_date` | date | `Feha De Inicio & Vencimiento` | derivar si la fecha viene clara |
| `is_test` | boolean | derivado | nombre o rep con `test`, `prueba` |
| `raw_snapshot` | object/string | fila completa | opcional para auditoria |

### Tabla destino 2: `programa_4en14_referidos`

Una fila por referido individual.

| Campo final | Tipo | Viene de | Regla |
| --- | --- | --- | --- |
| `source` | string | fijo | `4_en_14` |
| `event_source_row_id` | string | derivado | referencia a `programa_4en14_eventos.source_row_id` |
| `slot_index` | number | derivado | 1, 2, 3... segun posicion en la fila |
| `referral_name` | string | `Nombre D Referido` y bloques `Nombre` | trim |
| `referral_phone_raw` | string | `Telefono` | conservar original |
| `referral_phone` | string | `Telefono` | normalizar |
| `referral_note_raw` | string | `Notas` | conservar original |
| `relationship_hint` | string | `Notas` | derivado si aparece `hermano`, `tia`, `vecina`, `amigo`, etc. |
| `interest_hint` | string | `Notas` | derivado si aparece `quiere comprar`, `quiere credito`, `tiene royal` |
| `location_hint` | string | `Notas` | derivado si aparece ciudad o zona |
| `marital_hint` | string | `Notas` | derivado si aparece `casada`, `soltero`, etc. |
| `children_hint` | string | `Notas` | derivado si aparece `hijos`, `sin hijos` |
| `employment_hint` | string | `Notas` | derivado si aparece `trabaja`, `no trabaja` |
| `is_test` | boolean | derivado | nombre o nota con `test`, `prueba` |

### Campos del 4 En 14 que si se quedan

- `Timestamp`
- `Nombre De Anfitrion`
- `Telefono de anfitrión`
- `Regalo Elejido`
- `Nombre De Representante`
- `Telefono De Representante`
- `Feha De Inicio & Vencimiento`
- todos los bloques de `Nombre`, `Telefono`, `Notas`

### Campos del 4 En 14 que no se deben usar directo en prompt

- fila completa como CSV crudo
- columnas vacias
- texto repetido del mismo anfitrion
- pruebas

## Fuente 2: Rifa digital

Muestra actual:

- 79 filas exportadas
- 76 filas reales con lead
- 1 prueba clara detectada: `Pedro Prueba`

Estados observados:

- `No contesto1`
- `llamar mas adelnate`
- `No interesado`
- `Cita agendada`
- `SIN SERVICIO`
- `no califica`
- `Cosinado`
- `NO LLAMAR`

Esta hoja sirve para perfiles de lead, estado de contacto, scoring y patrones reales de telemarketing.

### Tabla destino 1: `lead_profiles`

Una fila por lead.

| Campo final | Tipo | Viene de | Regla |
| --- | --- | --- | --- |
| `source` | string | fijo | `rifa_digital` |
| `lead_id` | string | `Lead ID` | clave principal |
| `created_at_raw` | string | `Timestamp` | conservar original |
| `created_at` | datetime | `Timestamp` | normalizar |
| `lead_name` | string | `Nombre` | trim |
| `email` | string | `Email` | lowercase, trim |
| `phone_raw` | string | `Telefono` | conservar original |
| `phone` | string | `Telefono` | normalizar |
| `water_source_raw` | string | `Toma Agua` | conservar original |
| `best_call_window_raw` | string | `Mejor hora para llamar` | trim |
| `scheduled_call_window_raw` | string | `Hora Para Llamar` | trim |
| `product_interest_raw` | string | `A cual producto le daria mas uso` | conservar original |
| `knows_brand` | string/boolean | `Conose` | normalizar `si/no` si viene claro |
| `has_royal_prestige` | string/boolean | `Tiene Royal Prestige` | normalizar |
| `products_owned_raw` | string | `Que productos tiene?` | trim |
| `rep_name` | string | `Nombre del representante` | trim |
| `address_raw` | string | `Direccion` | trim |
| `home_type_raw` | string | `Que es?` | trim |
| `notes_raw` | string | `Cita/Notas` | conservar |
| `event_source` | string | `Donde Participo?` | trim |
| `prize_won_raw` | string | `Que se gano?` | trim |
| `marital_status_raw` | string | `Casado?` | trim |
| `followup_flag_raw` | string | `Cita` | conservar si aporta |
| `source_date_raw` | string | `Fecha` | conservar |
| `is_test` | boolean | derivado | nombre con `test`, `prueba` |

### Tabla destino 2: `lead_contact_state`

Una fila por lead con el ultimo estado operativo conocido.

| Campo final | Tipo | Viene de | Regla |
| --- | --- | --- | --- |
| `lead_id` | string | `Lead ID` | relacion |
| `call_status_raw` | string | `Estado de llamada` | conservar |
| `call_status_normalized` | string | `Estado de llamada` | ver mapa de normalizacion |
| `call_log_raw` | string | `Hora de primera llamada` | aqui hay historial real, no solo hora |
| `response_time_raw` | string | `Tiempo D Respuesta` | conservar si viene |
| `next_step` | string | derivado | `reagendar`, `cerrar`, `descartar`, etc. |
| `appointment_detected` | boolean | derivado | si hay `cita` o hora concreta |
| `do_not_call` | boolean | derivado | si aparece `NO LLAMAR` |
| `has_voicemail_pattern` | boolean | derivado | si se repite `buzon` |
| `attempt_count_estimated` | number | derivado | contar marcas en `call_log_raw` |
| `last_contact_note_summary` | string | derivado | resumen corto de `notes_raw` + `call_log_raw` |

### Tabla destino 3: `lead_insights`

Una fila por lead con etiquetas para Agustin.

| Campo final | Tipo | Como sale |
| --- | --- | --- |
| `lead_id` | string | `Lead ID` |
| `lead_temperature` | string | `hot`, `warm`, `cold`, `dead` |
| `interest_water` | boolean | `Toma Agua` o notas |
| `interest_filter` | boolean | producto interes o notas |
| `interest_cooking_system` | boolean | producto interes o notas |
| `interest_extractor` | boolean | producto interes o notas |
| `interest_health` | boolean | notas con salud, filtro, agua, cocina saludable |
| `owns_competitor` | boolean | notas o `Que productos tiene?` |
| `needs_replacement_parts` | boolean | notas como `aros rojos`, `valvula`, etc. |
| `requires_spouse_present` | boolean | notas |
| `works_late` | boolean | notas |
| `travelling_or_unavailable` | boolean | notas |
| `callback_recommended` | boolean | estado y notas |
| `conversion_signal` | boolean | cita o alto interes |
| `primary_objection` | string | tiempo, interes bajo, no contesta, etc. |
| `best_script_angle` | string | salud, agua, cocina, regalo, reemplazo |

### Tabla opcional 4: `lead_contact_attempts`

Solo si despues quieres granularidad.

Una fila por intento de llamada extraido del historial crudo.

| Campo final | Tipo | Viene de |
| --- | --- | --- |
| `lead_id` | string | `Lead ID` |
| `attempt_index` | number | derivado |
| `attempt_datetime_raw` | string | `Hora de primera llamada` |
| `attempt_outcome_raw` | string | `Hora de primera llamada` |
| `attempt_outcome_normalized` | string | derivado |
| `attempt_rep_name` | string | derivado si se menciona |

## Mapa de normalizacion de estado

| Valor original | Valor limpio |
| --- | --- |
| `No contesto1` | `no_contesto` |
| `llamar mas adelnate` | `reagendar` |
| `No interesado` | `no_interesado` |
| `Cita agendada` | `cita_agendada` |
| `SIN SERVICIO` | `sin_servicio` |
| `no califica` | `no_califica` |
| `Cosinado` | `cocinando_o_ocupado` |
| `cosinado` | `cocinando_o_ocupado` |
| `NO LLAMAR` | `no_llamar` |

## Columnas que se deben descartar

### 4 En 14

- slots completamente vacios
- filas de prueba
- telefono sin contexto cuando el nombre esta vacio

### Rifa digital

- `Semana`
- `Mes`
- `Column 28`
- `Intento de contacto` si sigue vacio
- cualquier fila de prueba

## Minimal version para importar primero

Si quieres empezar rapido, importaria solo esto:

### 4 En 14 minimal

- `source_row_id`
- `created_at`
- `host_name`
- `host_phone`
- `gift_selected`
- `rep_name`
- `program_window_raw`
- `referral_name`
- `referral_phone`
- `referral_note_raw`

### Rifa digital minimal

- `lead_id`
- `created_at`
- `lead_name`
- `email`
- `phone`
- `water_source_raw`
- `best_call_window_raw`
- `product_interest_raw`
- `has_royal_prestige`
- `products_owned_raw`
- `rep_name`
- `event_source`
- `call_status_normalized`
- `call_log_raw`
- `last_contact_note_summary`
- `lead_temperature`
- `best_script_angle`

## Como usaria esto Agustin

### Coach

- recordar el ultimo estado del lead
- sugerir el siguiente paso
- elegir mejor script de llamada
- detectar si vale la pena insistir
- reconocer patron de objecion o de baja respuesta

### Chef

- detectar si el gancho debe entrar por salud, agua o cocina
- ofrecer recetas o explicaciones segun interes real
- usar contexto del hogar sin tocar cierres privados

## Orden recomendado

1. limpiar pruebas
2. normalizar telefonos
3. convertir `4 En 14` a filas individuales por referido
4. normalizar estados de `Rifa digital`
5. resumir notas largas a etiquetas y resumen corto
6. importar primero la version minimal
7. despues agregar `lead_contact_attempts`

## Nota

Esto no reemplaza el conocimiento de Agustin. Lo complementa.

- conocimiento = que decir
- memoria = a quien le dijimos que
- analytics = que patron se repite

Ese cruce es el que realmente vuelve mas fuerte al sistema.
