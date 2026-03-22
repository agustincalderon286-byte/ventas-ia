# Knowledge Vector Setup

## Flujo nuevo

1. Agrega o edita archivos dentro de `src/data/`
2. Corre:

```bash
npm run knowledge:import
```

Si necesitas crear el indice vectorial desde codigo:

```bash
npm run knowledge:index
```

3. Eso sube la informacion a:
   - `knowledge_sources`
   - `knowledge_chunks`

## Importar un solo archivo

```bash
node scripts/import-knowledge.js src/data/recetas_royal_prestige
```

## Buscar en la base vectorial

```bash
npm run knowledge:search -- "recomiendame una receta saludable con pollo"
```

## Variables recomendadas

```env
KNOWLEDGE_EMBEDDING_MODEL=text-embedding-3-small
KNOWLEDGE_VECTOR_INDEX=knowledge_embedding_index
ENABLE_VECTOR_SEARCH=false
```

## Atlas Vector Search

Despues de importar, crea un indice de Atlas Vector Search en la coleccion `knowledge_chunks` sobre el campo `embedding`.

Configuracion sugerida:

```json
{
  "fields": [
    {
      "type": "vector",
      "path": "embedding",
      "numDimensions": 1536,
      "similarity": "cosine"
    },
    {
      "type": "filter",
      "path": "sourceType"
    },
    {
      "type": "filter",
      "path": "sourceKey"
    },
    {
      "type": "filter",
      "path": "tags"
    }
  ]
}
```

Usa `1536` si mantienes `text-embedding-3-small`.

## Activar en el server

Cuando ya exista el indice y hayas importado tus datos:

```env
ENABLE_VECTOR_SEARCH=true
```

Si falla el vector search, el server sigue usando el contexto estatico actual como fallback.
