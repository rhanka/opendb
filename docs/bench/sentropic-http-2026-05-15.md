# Sentropic HTTP POC (Sprint 18.C) — 2026-05-15

Replayed 22/27 migrations, seeded via Drizzle, mounted a Hono router with the exact handler logic from sentropic api/src/routes/api/folders.ts:139-198, hit two HTTP probes.

## HTTP verdict

| Probe | Outcome | Details |
|------|---------|---------|
| H1 | PASS | payload={"items":[{"id":"fld-3","name":"Folder Gamma","description":null,"organizationId":"org-poc","organizationName":"POC Org","matrixConfig":null,"status":"generating","createdAt":"2026-05-15T22:14:55.201Z","initiativeCount":1},{"id":"fl |
| H2 | PASS | payload={"items":[{"id":"fld-3","name":"Folder Gamma","description":null,"organizationId":"org-poc","organizationName":"POC Org","matrixConfig":null,"status":"generating","createdAt":"2026-05-15T22:14:55.201Z","initiativeCount":1},{"id":"fl |
| H3 | PASS | payload={"id":"fld-1","name":"Folder Alpha","description":null,"organizationId":"org-poc","organizationName":"POC Org","matrixConfig":null,"executiveSummary":null,"status":"completed","createdAt":"2026-05-15T22:14:50.716Z"} |
| H4 | PASS | status=404 |
| H5 | PASS | payload={"items":[{"id":"org-poc","workspaceId":"ws-poc","name":"POC Org","status":"completed","createdAt":"2026-05-15T22:14:46.018Z","updatedAt":"2026-05-15T22:14:46.018Z","data":{"industry":"tech"}}]} |
| H6 | PASS | payload={"items":[{"id":"i-1","workspaceId":"ws-poc","folderId":"fld-1","organizationId":null,"status":"completed","model":"gpt-5","antecedentId":null,"maturityStage":null,"gateStatus":null,"templateSnapshotId":null,"createdAt":"2026-05-15T |
| H7 | PASS | payload={"items":[{"id":"i-1","workspaceId":"ws-poc","folderId":"fld-1","organizationId":null,"status":"completed","model":"gpt-5","antecedentId":null,"maturityStage":null,"gateStatus":null,"templateSnapshotId":null,"createdAt":"2026-05-15T |
| H8 | PASS | payload={"items":[{"id":"fld-1","workspaceId":"ws-poc","name":"Folder Alpha","description":null,"organizationId":"org-poc","matrixConfig":null,"executiveSummary":null,"status":"completed","createdAt":"2026-05-15T22:14:50.716Z"},{"id":"fld-2 |
| H9 | PASS | payload={"defaults_by":"ai-ideas"} |
| H10 | PASS | payload={"defaults_by":"ai-ideas"} |

**Verdict: 10/10 HTTP probes PASS**
