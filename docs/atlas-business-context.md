# Atlas Business Context Bridge

This bridge lets Atlas query Chicago Metal Works and Agustin 2.0 business knowledge without pulling in unrelated knowledge-store material.

## Command

```bash
npm run metalworks:knowledge -- "What should we prioritize on Thumbtack leads?"
```

## What it is for

Use this when Atlas needs business context such as:

- lead priority rules
- Thumbtack patterns
- follow-up timing
- qualifying questions
- what information to capture first
- how Agustin 2.0 was trained to think about Chicago Metal Works leads

## Good questions

```bash
npm run metalworks:knowledge -- "What should we prioritize on Thumbtack leads?"
npm run metalworks:knowledge -- "How fast should we reply to a new lead?"
npm run metalworks:knowledge -- "What should we ask first on a new railing lead?"
npm run metalworks:knowledge -- "Which jobs are low fit for Chicago Metal Works?"
npm run metalworks:knowledge -- "What follow-up patterns matter most for gate repair leads?"
```

## How Atlas should use it

1. Use `metalworks:knowledge` for general business playbook context.
2. Use `metalworks:crm` for a specific lead, phone number, or CRM record.
3. When both matter, combine them:
   - CRM tells Atlas who this lead is.
   - knowledge tells Atlas how your business usually handles this kind of lead.

## Current source scope

This bridge is intentionally filtered to Chicago Metal Works training material:

- `src/data/entrenamiento/agustin20-thumbtack-chicago-metal-works-2022-2024.md`
- `src/data/entrenamiento/agustin20-thumbtack-message-patterns-sample.md`
- `src/data/entrenamiento/agustin20-thumbtack-chicago-metal-works-2022-2024.json`
- `src/data/entrenamiento/agustin20-thumbtack-message-patterns-sample.json`

The live query prefers Mongo vector search when those sources are present in the knowledge store and falls back to the local files if needed.
