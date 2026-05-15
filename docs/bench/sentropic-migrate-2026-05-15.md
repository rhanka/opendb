# Sentropic migration replay (Sprint 18.A) — 2026-05-15

Source: `/home/antoinefa/src/entropiq/api/drizzle` (read-only, no modification to sentropic repo).

## Per-migration verdict

| Migration | Outcome | Statements | Detail |
|-----------|---------|------------|--------|
| 0000_luxuriant_natasha_romanoff.sql | PASS | 10 | — |
| 0001_jazzy_microbe.sql | PASS | 9 | — |
| 0002_ordinary_beast.sql | PASS | 7 | — |
| 0003_cultured_puma.sql | PASS | 6 | — |
| 0004_strong_nova.sql | PASS | 1 | — |
| 0005_youthful_goblin_queen.sql | PASS | 1 | — |
| 0006_low_riptide.sql | PASS | 1 | — |
| 0007_handy_morlocks.sql | PASS | 3 | — |
| 0008_clumsy_luminals.sql | FAIL | 0 ran | `-- Étape 1 : Migrer toutes les données vers data JSONB AVANT de supprimer les colonnes -- Cette migr` → error: sql error: UPDATE requires SET |
| 0009_closed_sir_ram.sql | PASS | 16 | — |
| 0010_ambitious_silvermane.sql | PASS | 9 | — |
| 0011_past_drax.sql | PASS | 27 | — |
| 0012_aberrant_swarm.sql | FAIL | 1 ran | `-- Ensure the default workspace row exists before adding any FK constraints. INSERT INTO "workspaces` → error: sql error: INSERT requires VALUES |
| 0013_absent_midnight.sql | PASS | 2 | — |
| 0014_uneven_vision.sql | PASS | 3 | — |
| 0015_chat_generation_traces.sql | PASS | 8 | — |
| 0016_organizations.sql | FAIL | 0 ran | `-- Organizations refactor: -- - Rename companies -> organizations -- - Migrate organization profile ` → error: sql error: unsupported SQL: IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'companies'
  ) AND NOT EXISTS (
    SELECT 1 FROM informa |
| 0017_context_documents.sql | PASS | 11 | — |
| 0018_workspace_collaboration.sql | FAIL | 0 ran | `-- Workspace collaboration (Lot 1): -- - Add workspace_memberships table for multi-user workspace sh` → error: invalid input: table workspace_memberships requires exactly one primary key column |
| 0019_chat_message_feedback.sql | PASS | 7 | — |
| 0020_add_comments.sql | PASS | 7 | — |
| 0021_extension_tool_permissions.sql | PASS | 4 | — |
| 0022_settings_user_scope.sql | PASS | 8 | — |
| 0023_todo_steering_workflow_core.sql | PASS | 90 | — |
| 0024_workspace_types_initiatives.sql | FAIL | 41 ran | `-- 12. Backfill: create one neutral workspace per existing user who doesn't have one INSERT INTO "wo` → error: sql error: INSERT requires VALUES |
| 0025_workflow_runtime_state.sql | FAIL | 7 ran | `CREATE TABLE IF NOT EXISTS "workflow_task_results" ( "run_id" text NOT NULL, "workspace_id" text NOT` → error: invalid input: table workflow_task_results requires exactly one primary key column |
| 0026_google_drive_connector_accounts.sql | PASS | 12 | — |

**Verdict: 21/27 migrations PASS (78%)**

→ Décision : ping user pour arbitrage. Une partie des migrations passe ; la suite (seed, route HTTP) sera bloquée par les gaps restants.
