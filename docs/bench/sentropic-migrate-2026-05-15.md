# Sentropic migration replay (Sprint 18.A) — 2026-05-15

Source: `/home/antoinefa/src/entropiq/api/drizzle` (read-only, no modification to sentropic repo).

## Per-migration verdict

| Migration | Outcome | Statements | Detail |
|-----------|---------|------------|--------|
| 0000_luxuriant_natasha_romanoff.sql | FAIL | 7 ran | `DO $$ BEGIN ALTER TABLE "folders" ADD CONSTRAINT "folders_company_id_companies_id_fk" FOREIGN KEY ("` → error: sql error: REFERENCES columns |
| 0001_jazzy_microbe.sql | FAIL | 0 ran | `CREATE TABLE IF NOT EXISTS "magic_links" ( "id" text PRIMARY KEY NOT NULL, "token_hash" text NOT NUL` → error: sql error: unsupported column type: "magic_links_token_hash_unique" |
| 0002_ordinary_beast.sql | FAIL | 0 ran | `CREATE INDEX IF NOT EXISTS "magic_links_expires_at_idx" ON "magic_links" USING btree ("expires_at");` → error: not found: table not found: magic_links |
| 0003_cultured_puma.sql | FAIL | 0 ran | `CREATE TABLE IF NOT EXISTS "email_verification_codes" ( "id" text PRIMARY KEY NOT NULL, "code_hash" ` → error: sql error: unsupported column type: "email_verification_codes_verification_token_unique" |
| 0004_strong_nova.sql | FAIL | 0 ran | `ALTER TABLE "use_cases" ADD COLUMN "model" text DEFAULT 'gpt-5';` → error: not found: table not found: use_cases |
| 0005_youthful_goblin_queen.sql | FAIL | 0 ran | `ALTER TABLE "use_cases" ALTER COLUMN "model" DROP DEFAULT;` → error: sql error: unsupported ALTER TABLE clause: ALTER COLUMN "model" DROP DEFAULT |
| 0006_low_riptide.sql | FAIL | 0 ran | `ALTER TABLE "folders" ADD COLUMN "executive_summary" text;` → error: not found: table not found: folders |
| 0007_handy_morlocks.sql | FAIL | 0 ran | `ALTER TABLE "use_cases" ADD COLUMN "data" jsonb DEFAULT '{}'::jsonb NOT NULL;` → error: not found: table not found: use_cases |
| 0008_clumsy_luminals.sql | FAIL | 0 ran | `-- Étape 1 : Migrer toutes les données vers data JSONB AVANT de supprimer les colonnes -- Cette migr` → error: sql error: unsupported SQL: -- Étape 1 : Migrer toutes les données vers data JSONB AVANT de supprimer les colonnes
-- Cette migration est idempotente : elle ne migre que les données qui ne sont |
| 0009_closed_sir_ram.sql | FAIL | 0 ran | `ALTER TABLE "webauthn_credentials" ALTER COLUMN "created_at" SET NOT NULL;` → error: sql error: unsupported ALTER TABLE clause: ALTER COLUMN "created_at" SET NOT NULL |
| 0010_ambitious_silvermane.sql | FAIL | 0 ran | `ALTER TABLE "companies" ALTER COLUMN "created_at" SET NOT NULL;` → error: sql error: unsupported ALTER TABLE clause: ALTER COLUMN "created_at" SET NOT NULL |
| 0011_past_drax.sql | FAIL | 5 ran | `DO $$ BEGIN ALTER TABLE "chat_contexts" ADD CONSTRAINT "chat_contexts_session_id_chat_sessions_id_fk` → error: sql error: REFERENCES columns |
| 0012_aberrant_swarm.sql | FAIL | 0 ran | `CREATE TABLE IF NOT EXISTS "workspaces" ( "id" text PRIMARY KEY NOT NULL, "owner_user_id" text, "nam` → error: sql error: unsupported column type: "workspaces_owner_user_id_unique" |
| 0013_absent_midnight.sql | FAIL | 0 ran | `-- Tenancy for queue: each job belongs to a workspace (private-by-default). ALTER TABLE "job_queue" ` → error: sql error: unsupported SQL: -- Tenancy for queue: each job belongs to a workspace (private-by-default).
ALTER TABLE "job_queue" ADD COLUMN IF NOT EXISTS "workspace_id" text DEFAULT '00000000-00 |
| 0014_uneven_vision.sql | FAIL | 0 ran | `ALTER TABLE "chat_sessions" ADD COLUMN "workspace_id" text;` → error: not found: table not found: chat_sessions |
| 0015_chat_generation_traces.sql | FAIL | 0 ran | `-- Chat tracing (debug/audit): store the exact OpenAI payloads + tool calls per iteration. -- Retent` → error: sql error: unsupported SQL: -- Chat tracing (debug/audit): store the exact OpenAI payloads + tool calls per iteration.
-- Retention is enforced at the application level (purge > 7 days).
CREATE |
| 0016_organizations.sql | FAIL | 0 ran | `-- Organizations refactor: -- - Rename companies -> organizations -- - Migrate organization profile ` → error: sql error: unsupported SQL: -- Organizations refactor:
-- - Rename companies -> organizations
-- - Migrate organization profile fields into JSONB data
-- - Rename FK columns company_id -> organ |
| 0017_context_documents.sql | FAIL | 0 ran | `-- Context documents (Chatbot Lot B): -- Attach documents to a business context (organization/folder` → error: sql error: unsupported SQL: -- Context documents (Chatbot Lot B):
-- Attach documents to a business context (organization/folder/usecase)
-- and store summary + processing status.

CREATE TABLE |
| 0018_workspace_collaboration.sql | FAIL | 0 ran | `-- Workspace collaboration (Lot 1): -- - Add workspace_memberships table for multi-user workspace sh` → error: sql error: unsupported SQL: -- Workspace collaboration (Lot 1):
-- - Add workspace_memberships table for multi-user workspace sharing
-- - Add hidden_at column to workspaces for hide/unhide fun |
| 0019_chat_message_feedback.sql | FAIL | 0 ran | `-- Chat message feedback (Lot B2): -- - Add chat_message_feedback table to store per-user 👍/👎 vote` → error: sql error: unsupported SQL: -- Chat message feedback (Lot B2):
-- - Add chat_message_feedback table to store per-user 👍/👎 votes on assistant messages

CREATE TABLE IF NOT EXISTS "chat_message |
| 0020_add_comments.sql | FAIL | 0 ran | `-- Custom SQL migration file, put your code below! -- CREATE TABLE IF NOT EXISTS "comments" ( "id" t` → error: sql error: unsupported SQL: -- Custom SQL migration file, put your code below! --
CREATE TABLE IF NOT EXISTS "comments" (
  "id" text PRIMARY KEY NOT NULL,
  "workspace_id" text NOT NULL REFERE |
| 0021_extension_tool_permissions.sql | FAIL | 0 ran | `-- Custom SQL migration file, put your code below! -- CREATE TABLE IF NOT EXISTS "extension_tool_per` → error: sql error: unsupported SQL: -- Custom SQL migration file, put your code below! --
CREATE TABLE IF NOT EXISTS "extension_tool_permissions" (
  "id" text PRIMARY KEY NOT NULL,
  "user_id" text NO |
| 0022_settings_user_scope.sql | FAIL | 0 ran | `ALTER TABLE "settings" DROP CONSTRAINT IF EXISTS "settings_pkey";` → error: sql error: unsupported ALTER TABLE clause: DROP CONSTRAINT IF EXISTS "settings_pkey" |
| 0023_todo_steering_workflow_core.sql | FAIL | 1 ran | `DO $$ BEGIN ALTER TABLE "plans" ADD CONSTRAINT "plans_workspace_id_workspaces_id_fk" FOREIGN KEY ("w` → error: sql error: REFERENCES columns |
| 0024_workspace_types_initiatives.sql | FAIL | 0 ran | `-- Migration 0024: Workspace type system, initiative rename, extended objects -- BR-04 Lot 1 — Singl` → error: sql error: unsupported SQL: -- Migration 0024: Workspace type system, initiative rename, extended objects
-- BR-04 Lot 1 — Single migration file (BR04-EX1)

-- 1. Rename use_cases -> initiative |
| 0025_workflow_runtime_state.sql | FAIL | 0 ran | `-- Migration 0025: BR-04B workflow runtime state MVP -- Adds additive runtime tables for durable run` → error: sql error: unsupported SQL: -- Migration 0025: BR-04B workflow runtime state MVP
-- Adds additive runtime tables for durable run state and task results without pulling BR-23 into scope.

CREATE |
| 0026_google_drive_connector_accounts.sql | FAIL | 0 ran | `-- Migration 0026: BR-16a Google Drive connector accounts -- Adds per-user/per-workspace connector l` → error: sql error: unsupported SQL: -- Migration 0026: BR-16a Google Drive connector accounts
-- Adds per-user/per-workspace connector lifecycle state and encrypted token storage.

CREATE TABLE IF NOT  |

**Verdict: 0/27 migrations PASS (0%)**

→ Décision : STOP Sprint 18. Audit chiffré des verbes SQL manquants nécessaire avant de continuer.
