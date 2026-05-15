# Entropiq corrective probe (Sprint 15.E) — 2026-05-15

Rejeu de requêtes Drizzle copiées 1:1 depuis les routes entropiq, contre opendb-node, sans modification entropiq.

## Matrix

| Probe | Outcome | SQL | Details |
|------|---------|-----|---------|
| Q13 | PASS | `BEGIN; INSERT INTO folders (id, workspace_id, name) VALUES ('f-q13', 'w1', 'Q13 in-tx') RETURNING id; SELECT id FROM folders WHERE id = 'f-q13'; COMMIT;` | result={"inserted":[{"id":"f-q13"}],"selected":[{"id":"f-q13"}]} |
| Q14 | PASS | `BEGIN; INSERT INTO folders (id, workspace_id, name) VALUES ('f-q14', 'w1', 'Q14 rollback'); throw; ROLLBACK; SELECT id FROM folders WHERE id = 'f-q14'` | result={"rolled_back":true,"after_count":0} |
| Q1 | PASS | `select count(*) from "organizations"` | rows=[{"count":"2"}] |
| Q2 | PASS | `select count(*) from "folders"` | rows=[{"count":"4"}] |
| Q3 | PASS | `select "status", COUNT(*) as "count" from "job_queue" group by "job_queue"."status"` | rows=[{"status":"pending","count":"2"},{"status":"completed","count":"1"}] |
| Q4 | PASS | `select "id" from "folders" where "folders"."workspace_id" = $1` | rows=[{"id":"f-q13"},{"id":"f1"},{"id":"f2"},{"id":"f3"}] |
| Q5 | PASS | `select "folders"."id", "folders"."name", "folders"."status", count("initiatives"."id") from "folders" left join "initiatives" on ("initiatives"."folder_id" = "folders"."id" and "initiatives"."workspac` | rows=[{"id":"f1","name":"Folder A","status":"completed","initiativeCount":"2"},{"id":"f2","name":"Folder B","status":"completed","initiativeCount":"1"},{"id":"f3","name":"Folder C" |
| Q6 | PASS | `select "id", "type", "status", "workspace_id", "created_at" from "job_queue" order by "job_queue"."created_at" desc limit $1` | rows=[{"id":"j3","type":"use_case_detail","status":"completed","workspaceId":"w1","createdAt":"2026-05-06T02:00:00.000Z"},{"id":"j2","type":"use_case_list","status":"pending","work |
| Q7 | PASS | `select "id", "owner_user_id", "name", "type", "gate_config", "hidden_at", "created_at", "updated_at" from "workspaces" where "workspaces"."id" = $1` | rows=[{"id":"w1","ownerUserId":"u1","name":"Acme","type":"ai-ideas","gateConfig":null,"hiddenAt":null,"createdAt":"2026-05-01T00:00:00.000Z","updatedAt":"2026-05-15T00:59:02.651Z"} |
| Q8 | PASS | `select count(*) from "initiatives" where "initiatives"."status" = $1` | rows=[{"n":"2"}] |
| Q9 | PASS | `insert into "folders" ("id", "workspace_id", "name", "description", "organization_id", "matrix_config", "executive_summary", "status", "created_at") values ($1, $2, $3, default, default, default, defa` | rows=[{"id":"f-q9","workspaceId":"w1","name":"Q9 folder","description":null,"organizationId":null,"matrixConfig":null,"executiveSummary":null,"status":"completed","createdAt":"2026 |
| Q10 | PASS | `insert into "folders" ("id", "workspace_id", "name", "description", "organization_id", "matrix_config", "executive_summary", "status", "created_at") values ($1, $2, $3, default, default, default, defa` | rows=[{"id":"f-q10"}] |
| Q11 | PASS | `update "folders" set "matrix_config" = $1 where "folders"."id" = $2 returning "matrix_config"` | rows=[{"matrixConfig":"{\"x\":1}"}] |
| Q12 | PASS | `delete from "folders" where "folders"."id" = $1 returning "id", "workspace_id", "name", "description", "organization_id", "matrix_config", "executive_summary", "status", "created_at"` | rows=[{"id":"f-q10","workspaceId":"w1","name":"Q10 folder","description":null,"organizationId":null,"matrixConfig":null,"executiveSummary":null,"status":"completed","createdAt":"20 |

**Verdict: 14/14 PASS (100%)**

→ Décision : poursuivre Sprint 16 (`.returning()` + PK composite). Gaps résiduels tracés en TODO.
