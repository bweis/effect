---
"effect": patch
---

Let cluster SQL tables be managed outside the runtime. `SqlMessageStorage.migrate` and `SqlRunnerStorage.migrate` apply the cluster tables with a connection that owns them, and `migrate: false` on `make` / `layerWith` makes the storages issue no DDL, so a runtime role needs only `SELECT`, `INSERT`, `UPDATE` and `DELETE`. With `migrate: false`, `SqlMessageStorage` fails at startup when a migration it depends on has not been applied.
