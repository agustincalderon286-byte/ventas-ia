# Auditoria de Codigos Internos 2026

- Fuente interna: `src/data/Lista_Precios_2026`
- Fuente publica de catalogo: `src/data/lista_de_precios.json`
- Objetivo: conservar codigos utiles sin usar precios internos para cotizacion.

## Resumen

- Codigos unicos en lista interna: 465
- Codigos tipo producto: 85
- Codigos tipo parte o interno: 380
- Codigos que tambien existen en el catalogo publico: 141
- Codigos solo internos: 324

## Hallazgo clave

- `CO0101` (Chocolatera) aparece con dos valores muy distintos:
  - Catalogo curado: `599`
  - Lista interna 2026: `76.28`
- Conclusion: la lista interna no debe usarse para cotizar al cliente final.

## Archivo recomendado

- `src/data/codigos_productos_internos_2026.json`
- Ese archivo ya no trae precios; solo codigos, descripcion, categoria y una bandera heuristica para separar producto vs parte/interno.

## Regla recomendada

- Para precios del Coach: solo `lista_de_precios.json`.
- Para codigos y piezas internas: `codigos_productos_internos_2026.json`.

