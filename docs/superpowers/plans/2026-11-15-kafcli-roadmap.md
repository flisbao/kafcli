# kafcli — Roadmap de planos

Spec: `docs/superpowers/specs/2026-11-15-kafcli-design.md`

A spec cobre vários subsistemas entregáveis de forma independente. Um plano único para toda a v1 ficaria obsoleto no meio do caminho: as interfaces dos primeiros planos (config, auth, codecs) mudam o desenho dos seguintes. Por isso a v1 é dividida nos planos sequenciais abaixo. **Cada plano só é escrito depois que o anterior for mergeado**, usando as interfaces reais do código existente.

Modelo de execução para todos os planos: implementação pelo **Haiku 5.5** (um subagente por task), validação pelo **Sonnet 5.5** (um revisor por task + revisão do branch inteiro no final). Executores usam `superpowers:subagent-driven-development`.

Exigência de espaço em disco válida para todos os planos: `cargo clean` ao final de cada task (após commit e revisão), e `docker compose ... down -v` quando a task usou fixtures.

| # | Plano | Fase da spec | Entrega | Depende de |
|---|---|---|---|---|
| 01 | `2026-11-15-kafcli-01-foundation.md` | 1 + 2 (onda A) | Workspace, CI, config/perfis, segredos, auth onda A, guard, audit log, `profile list/show/add/use/test` | — |
| 00 | Spike Event Hubs (roteiro manual, **executado por humano**) | 0 | ADR `0001-event-hubs-capabilities.md` + matriz de capacidades | 01 (usa `kafcli profile test` com `azure-connection-string`) |
| 02 | `kafcli-02-produce-consume` | 3 | Codecs (raw-json, confluent-avro, confluent-json-schema, azure-avro), Schema Registry Confluent + Azure, `produce`, `consume`, envelope, templates, `--count/--rate`, `schema` | 01, 00 (formato Azure SR) |
| 03 | `kafcli-03-auth-oauth-azure` | 2 (onda B) | `oauth-oidc`, `bearer`, `azure-*` via `generate_oauth_token` com cache; auth Azure no SR | 01, 02 |
| 04 | `kafcli-04-read-operations` | 4 | `lag` (+`--watch`, `--max-lag`), `offsets show`, `groups audit` R1–R5 | 01, 00 (matriz) |
| 05 | `kafcli-05-destructive-operations` | 5 | `offsets set/restore`, `poison find/skip`, `--wait-empty` | 04 |
| 06 | `kafcli-06-distribution` | 7 | `cargo-dist`, matriz completa incl. Windows, completions, man page, README, tabela de permissões mínimas | todos |

Nota: a spec coloca o spike do Event Hubs (fase 0) primeiro. Aqui ele roda **depois do plano 01**, porque o spike precisa de um cliente que já conecte com `azure-connection-string`. O plano 01 não depende de nenhuma capacidade do Event Hubs, então a reordenação não perde nada.
