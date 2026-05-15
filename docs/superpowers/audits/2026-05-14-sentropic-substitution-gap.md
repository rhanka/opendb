# Sentropic substitution gap audit — 2026-05-14

État OpenDB : Sprint 12 + 12.1 livrés. Tous les probes POC smoke
PASS (A1..A5, B1, C1, C2) — un Drizzle client se connecte et
exécute `SELECT * / SELECT col WHERE eq(...)` end-to-end.

État cible utilisateur : substitution complète de la DB d'sentropic,
**zéro modification côté sentropic**. Drizzle + `pg` + 50 tables +
toutes les routes HTTP.

## Audit chiffré sur `/home/antoinefa/src/sentropic/api/`

### Schéma DDL

- **50 tables** déclarées via `pgTable(...)` dans `api/src/db/schema.ts`.
- **Aucun `CREATE TYPE / FUNCTION / TRIGGER / VIEW / EXTENSION /
  MATERIALIZED VIEW / SEQUENCE`** dans `api/drizzle/*.sql` (grep
  exhaustif). Pas d'enum PostgreSQL natif, pas de fonction stockée.
- **Pas de types PG exotiques** : pas de `uuid` natif, pas de
  `numeric`, pas de `varchar`, pas de `serial`, pas de `bytea`, pas de
  `int[]`/`text[]`, pas de `interval`, pas de `date`/`time` indépendant
  (les "date" / "time" trouvés sont des préfixes de noms de colonnes
  `created_at`, `updated_at` typés `timestamp`).
- Types réellement utilisés : `text`, `timestamp`, `jsonb`, `boolean`,
  `integer` (déjà tous supportés par OpenDB).
- **2 primary keys composites** sur 50 tables : `workflow_run_state`
  (col `runId`), `workflow_task_result_pk` (lignes 911, 938 de
  `schema.ts`).

### DML Drizzle (sites comptés dans `api/src/`)

| Verbe / pattern         | Sites | OpenDB actuel                                   |
|-------------------------|-------|-------------------------------------------------|
| `.select(...)`          | 445   | ✅ `SELECT * / col1, col2` simple + JOIN        |
| `.where(and(...))`      | 256   | ❌ pas de WHERE composé AND/OR                  |
| `.where(eq(...))`       | 242   | ⚠️ seulement WHERE pk = literal                  |
| `.delete(...)`          | 142   | ⚠️ seulement DELETE WHERE pk = literal           |
| `.update(...)`          | **126** | ❌ pas d'UPDATE                                |
| `.where(inArray(...))`  | 15    | ❌ pas de `IN (…)`                              |
| `.where(lt(...))`       | 6     | ❌ pas de `<` / `>` / `<=` / `>=`               |
| GROUP BY / agrégats     | **201** | ❌ pas de `GROUP BY`, `count()`, `sum()`, etc. |
| `.returning(...)`       | 19    | ❌ pas de RETURNING                             |
| `db.transaction(...)`   | 15    | ⚠️ no-op skeleton (tags seuls, pas d'isolation) |

### Synthèse priorisée (top-10 features manquantes par impact)

1. **UPDATE table SET col=val WHERE …** (126 sites) — bloque ~25 %
   des routes HTTP qui modifient l'état.
2. **WHERE composé `and()/or()`** (256 sites) — quasi toutes les
   queries non-trivial. Combiné avec eq/lt/gt/inArray.
3. **`.where(eq(col, X))` sur n'importe quel `col`** (pas seulement
   PK, 242 sites) — bloque tout filtre simple non-PK.
4. **GROUP BY + agrégats `count(*)/sum/max/min/avg`** (201 sites) —
   tous les endpoints de stats / pagination cachée.
5. **`IN (…)` / `inArray(...)`** (15 sites) — fetch par batch d'IDs.
6. **Comparateurs `<, >, <=, >=`** (~10 sites) — filtres date/temps.
7. **`.returning(...)`** (19 sites) — Drizzle s'en sert pour
   récupérer les IDs auto / les rows fraîchement insérées.
8. **`DELETE FROM t WHERE col = …`** non-PK (~30-50 sites
   estimés sur les 142 deletes) — soft-delete patterns.
9. **Transactions atomiques réelles** (15 sites, dont seed et
   bulk-import critiques). Le no-op suffit en lecture, pas en
   écriture.
10. **PK composites** (2 tables, `workflow_*`) — petit volume mais
    schéma déjà déployé.

### Hors scope confirmé

- Pas de `CREATE EXTENSION`, donc pas de `pgcrypto`, `pg_trgm`,
  `postgis`, `tsvector`.
- Pas de JSON ops `-> / ->> / @> / ?` côté SQL — sentropic parse les
  JSONB en TypeScript après lecture.
- Pas de subqueries dans la première lecture, à reconfirmer pendant
  l'implémentation.

## Conséquence pour la roadmap

Le drumbeat Sprint 6→12.1 a couvert ~30 % du gap utile (DDL complet,
types, JOINs simples, INSERT/DELETE de base, pgwire Extended). Les
~70 % restants pour une **substitution complète** :

- Sprint 13 — UPDATE (parser + executor + parity + bench).
- Sprint 14 — WHERE composé `AND/OR` + opérateurs `< > <= >= IN` +
  WHERE sur colonnes non-PK + DELETE multi-ligne.
- Sprint 15 — GROUP BY + agrégats (count, sum, max, min, avg).
- Sprint 16 — `.returning(...)` + PK composites.
- Sprint 17 — Transactions atomiques (snapshot read + buffered
  writes + COMMIT/ROLLBACK).
- Sprint 18 — POC sentropic read-only complet (toutes les routes
  GET).
- Sprint 19 — POC sentropic write (routes POST/PATCH/DELETE).
- Sprint 20 — Migration full replay + UAT sentropic HTTP end-to-end
  + bench vs PostgreSQL.

Estimation honnête : **8 sprints, ~30-50 jours-homme actifs** à la
cadence du drumbeat précédent. Le checkpoint utilisateur Sprint 18
(POC read-only sentropic sur HTTP réel) est l'étape qui mérite une
validation interactive avant Sprint 19.
