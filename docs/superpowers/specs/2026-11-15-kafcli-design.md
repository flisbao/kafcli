# kafcli — Design (v1)

Data: 2026-11-15 · Autor: Felipe Lisbão · Origem: spec "kfx" revisada em sessão de brainstorming.

Este documento substitui a spec original do `kfx`. Ele registra o que entra na v1, o que foi cortado e por quê. Itens marcados **[decisão do Claude]** foram escolhas feitas durante o design sem confirmação explícita do autor; revise-as.

## 1. Intenção e o porquê

`kafcli` é um CLI open source em Rust, binário único, para produzir, consumir e operar tópicos Kafka. O alvo principal é **Azure Event Hubs via endpoint Kafka**, mas a ferramenta é genérica (Apache Kafka, Confluent Cloud, AWS MSK, Redpanda).

Dores que motivam o projeto:

1. **kcat exige build em C** e não cobre todos os modos de autenticação (em especial OAUTHBEARER com Entra ID / Workload Identity).
2. **Não existe CLI eficiente para produzir mensagens no Event Hubs**, com autenticação Azure moderna e Avro via Azure Schema Registry.

Como o projeto é open source, o critério de escopo é: **serve a qualquer usuário de Kafka**, não só ao ambiente do autor (AKS + ~74 serviços Spring Boot). Recursos específicos desse ambiente ficam fora ou ficam genéricos.

## 2. Objetivos da v1

1. Produzir mensagens JSON e Avro (wire raw, Confluent, Azure Schema Registry) com validação local antes do envio.
2. Consumir e decodificar mensagens (substituto direto do uso principal do kcat).
3. Suportar autenticação moderna no broker e no Schema Registry, com prioridade para os modos Azure.
4. Mostrar consumer lag por grupo, tópico e partição.
5. Ajustar offsets de consumer groups com segurança (dry-run, backup, restore).
6. Tirar uma mensagem problemática do caminho de um consumer (cópia para DLT + avanço de offset).
7. Auditar consumer groups com regras genéricas.
8. Distribuir binários prontos (Linux, macOS, Windows) via GitHub Releases: ninguém precisa compilar C.

## 3. Não-objetivos (v1)

- Fuzzing / geração de payloads malformados.
- Administração de cluster (brokers, réplicas, ACLs). Criação/descrição de tópico só se trivial.
- GUI ou TUI. Saída é tabela ou `--output json`.
- **`kafcli test` (testkit de idempotência)** — adiado para v2. Ver §9.
- **`purge-before` (DeleteRecords)** — removido. Irreversível, raro, provavelmente não suportado no Event Hubs.
- **Comando `dlt` dedicado** — substituído por `consume` + `produce` com envelope (§6.2, §6.3).
- **Integração com Kubernetes (`--k8s`, crate `kube`)**.
- **`--output yaml`** — só `table` e `json`.
- **PKCS#12 no mTLS** — só PEM (documentar conversão com `openssl`).
- **Kerberos (GSSAPI)** nos binários oficiais — no máximo feature de compilação opcional, não testada no CI de release.
- **AWS MSK IAM** — v1.x.

## 4. Anti-objetivos (seria falha mesmo funcionando)

- Usuário precisar compilar C ou instalar librdkafka para usar o binário.
- Qualquer segredo aparecer em log, erro, `--output json` ou `profile show`.
- Operação destrutiva executar sem dry-run/confirmação adequada ao nível do perfil.
- `produce` registrar schema implicitamente.
- Fazer parse de schema vindo da própria mensagem.
- Recurso que só faz sentido no ambiente do autor virar comportamento padrão.

## 5. Restrições e decisões de stack

| Decisão | Escolha | Motivo |
|---|---|---|
| Linguagem | Rust estável, edition 2024, tokio | — |
| Cliente Kafka | `rdkafka` com librdkafka vendorizado (`cmake-build`, `ssl-vendored`) | Único com OAUTHBEARER custom, Admin API e SASL maduros. A dor do C é resolvida pelo CI, que compila e publica binários. |
| Distribuição | GitHub Actions + `cargo-dist`: Linux (x86_64/aarch64 musl estático), macOS (x86_64/aarch64), Windows (x86_64) | Decisão do autor. |
| Windows | Entra no pipeline de release, mas é o **último alvo** a ser estabilizado; pode sair numa release posterior se o build com SSL bloquear **[decisão do Claude]** | É o build mais difícil (OpenSSL vendorizado). |
| Licença | `MIT OR Apache-2.0` **[decisão do Claude]** | Padrão Rust, compatível com librdkafka (BSD-2) e OpenSSL 3. |
| Nome | `kafcli` — livre no crates.io e Homebrew em 2026-11-15 | `kcli` já existe (karmab/kcli) e está ocupado no crates.io. |
| CLI | `clap` derive + `clap_complete` + `clap_mangen` | — |
| Avro | `apache-avro` | — |
| JSON Schema | `jsonschema` | — |
| Schema Registry | cliente próprio sobre `reqwest` (rustls) | Confluent + Azure. |
| Azure AD | `azure_identity` / `azure_core` | — |
| Config | `figment` (TOML + env) | — |
| Segredos | referências `env:VAR`, `file:/caminho`, `keyring:serviço/conta`. **`keyring` é opcional** (feature padrão ligada, mas falha com mensagem clara em ambientes sem secret service) **[decisão do Claude]** | `keyring` quebra em CI/containers Linux sem D-Bus. |
| Saída | `comfy-table`, `serde_json`, `indicatif` | — |
| Logs/erros | `tracing`, `thiserror`, `anyhow` | — |

Restrições de código (mantidas da spec original):

- Sem `unsafe` fora do que o `rdkafka` encapsula.
- Sem `unwrap()`/`expect()` em código de produção.
- Toda opção do CLI documentada no `--help` com exemplo.
- Decisões não óbvias registradas em `docs/adr/NNNN-titulo.md`. Na ambiguidade, escolher a opção mais conservadora e anotar no ADR.

Regras de codificação (definidas pelo autor):

1. **SOLID no código e nos arquivos**: uma responsabilidade por módulo/arquivo; extensão por traits (novo auth provider ou codec = novo arquivo, sem editar os existentes além do registro); dependências por abstração (traits), não por tipos concretos.
2. **Nomes no estilo Clean Code**: funções com verbo + intenção (`fetch_committed_offsets`, `encode_confluent_avro`), sem abreviações, sem nomes genéricos (`process`, `handle`, `data`).
3. **Sem memory leak e sem CPU throttling**: nada de `Box::leak`/`mem::forget`; buffers e canais com limite (bounded); loops de `--follow`/`--watch`/`--rate` sempre aguardam (await/sleep/poll com timeout), nunca busy-wait; caches de token com tamanho fixo; clientes Kafka fechados/flushed ao terminar.

Execução: o plano de implementação é executado pelo modelo Haiku 5.5 e cada tarefa é validada pelo Sonnet 5.5.

Respostas às questões em aberto: o autor aceitou as decisões marcadas como **[decisão do Claude]**, incluindo a ordem das fases e a precedência de configuração.

### 5.1 Workspace

Dois crates no início **[decisão do Claude]** (a spec original tinha cinco):

```
kafcli/
  crates/
    kafcli/        # bin: clap, renderização (table/json), prompts do guard
    kafcli-core/   # lib: config/perfis, segredos, auth, codecs, schema registry, ops, guard, audit log
  tests/           # integração (testcontainers)
  docs/adr/
```

Dentro de `kafcli-core`, módulos: `config`, `secret`, `auth`, `codec`, `registry`, `ops::{lag,offsets,poison,groups}`, `guard`, `audit`. Extrair crates depois só se houver motivo (tempo de build, reuso).

### 5.2 Princípios de design

- Trait `AuthProvider`: devolve a config do rdkafka e, quando aplicável, gera o token OAUTHBEARER (via `ClientContext::generate_oauth_token`), com cache e refresh antes da expiração.
- Traits `Encoder`/`Decoder` com wire format explícito: `raw-json`, `confluent-avro`, `confluent-json-schema`, `azure-avro`.
- Operações que mudam estado passam pelo módulo `guard` (dry-run, confirmação, audit log).
- Funções de operação retornam structs serializáveis; renderização só no crate `kafcli`. O JSON de saída é contrato estável (versionar o schema com um campo `"kafcli_output": 1`) **[decisão do Claude]**.
- Exit codes: `0` sucesso, `1` erro/regra ERRO, `2` limite violado (`--max-lag`), `3` operação abortada pelo usuário/guard **[decisão do Claude para o 3]**.

## 6. Comandos da v1

Forma geral: `kafcli [--profile P] [--output table|json] [-v|-vv] <comando> [flags]`.

### 6.1 `kafcli profile list|add|show|test|use`

Config em `~/.config/kafcli/config.toml` (no Windows, o diretório de config padrão via `dirs`). Precedência: flags de linha de comando > perfil > variáveis `KAFCLI_*` (mantida da spec original).

```toml
default_profile = "dev-eh"

[profiles.dev-eh]
brokers = ["meu-ns.servicebus.windows.net:9093"]
auth = { type = "azure-workload-identity" }
protected = false

[profiles.prd-eh]
brokers = ["prd-ns.servicebus.windows.net:9093"]
auth = { type = "azure-sp-secret", tenant_id = "...", client_id = "...", secret_ref = "env:PRD_SP_SECRET" }
protected = true

[profiles.prd-eh.schema_registry]
type = "azure"          # ou "confluent"
url = "https://prd-ns.servicebus.windows.net"
group = "pedidos"
```

`profile test`: conecta, lista tópicos visíveis, mostra o principal autenticado (quando o mecanismo permitir) e a matriz de capacidades detectada (§8), sem vazar segredo. `profile show` mascara todo segredo.

### 6.2 `kafcli produce` — prioridade máxima

```
kafcli produce --topic pedidos --format avro --wire azure \
  --schema-subject pedidos-value --file evento.json \
  --key '{{field:pedidoId}}' --header origem=kafcli
```

- `--format json|avro`, `--wire raw|confluent|azure`. Para Avro, a entrada é JSON e o kafcli codifica.
- Origem do schema: `--schema-file`, `--schema-subject` [+ `--schema-version`], ou `--schema-id`. Sem schema só com `--format json --wire raw`.
- Validação local obrigatória; `--no-validate` pula com aviso.
- Nunca registra schema. Registro é separado: `kafcli schema register|get|check-compat`.
- Entrada: `--file` (JSON único ou NDJSON), `--stdin`, `--value`.
- **`--input envelope`**: cada linha NDJSON é um envelope `{key, value, headers, partition?}` — o mesmo formato que `consume` emite. Isso cobre o antigo `dlt replay` (`consume ... | produce --input envelope`).
- Templates em payload e key: `{{uuid}}`, `{{seq}}`, `{{now}}`, `{{field:caminho}}`, `{{env:VAR}}`.
- Volume: `--count N`, `--rate msgs/s`.
- `--dry-run`: mostra key, headers, tamanho e hex dos primeiros bytes, sem enviar.
- Producer com `acks=all` e `enable.idempotence=true` por padrão. Se a Fase 0 mostrar que o Event Hubs (algum tier) não suporta idempotência, o kafcli detecta pela matriz de capacidades, avisa e desliga **[decisão do Claude]**.
- Saída por mensagem: partição, offset, timestamp.

### 6.3 `kafcli consume` — novo, não estava na spec original

```
kafcli consume --topic pedidos [--partition 0] \
  [--from earliest|latest|offset:N|time:2026-11-15T10:00:00-03:00] \
  [--count 10] [--follow] [--format avro --wire azure] \
  [--max-bytes 2048] [--redact cpf,email] [--filter header:origem=kafcli]
```

- Não usa consumer group por padrão (assign manual, sem commit) **[decisão do Claude]**: ler nunca altera offsets de ninguém. `--group G` opcional para quem quiser.
- Decodifica com o codec/registry do perfil; falha de decode não aborta — emite o registro com erro de decode e bytes em hex/base64.
- Saída: tabela legível ou NDJSON de envelopes (`topic, partition, offset, timestamp, key, value, headers, decode_error?`).
- Decode defensivo: limites de tamanho de mensagem, profundidade JSON, tamanho de array/string Avro.

### 6.4 `kafcli schema register|get|check-compat`

Confluent e Azure. `register` é operação de escrita (guard nível Escrita).

### 6.5 `kafcli lag`

```
kafcli lag --group pedidos-service [--topic pedidos] [--watch 5s] [--max-lag 1000]
kafcli lag --all-groups --sort lag --top 20
```

- Por partição: offset commitado, log-end, lag, membro atribuído (client.id/host).
- Totais por tópico e grupo; grupo sem membros ativos marcado.
- `--watch`: redesenha e mostra taxa de consumo e tendência (subindo/caindo), calculadas entre amostras.
- `--max-lag`: exit 2 se ultrapassado.
- Critério: bate com `kafka-consumer-groups.sh --describe`.

### 6.6 `kafcli offsets show|set|restore`

```
kafcli offsets set --group G --topic T [--partition P] \
  (--to-offset N | --to-earliest | --to-latest | --to-datetime ISO8601 | --shift-by -100)
kafcli offsets restore --file backup.json
```

- `set` é dry-run por padrão: tabela antes → depois e quantas mensagens serão puladas/reprocessadas. Aplica só com `--execute`.
- Antes de aplicar, backup em `<state_dir>/kafcli/offsets-backup/<grupo>-<timestamp>.json`.
- Exige grupo sem membros ativos; explica o motivo e oferece `--wait-empty TIMEOUT`.

### 6.7 `kafcli poison find|skip`

- `find --group G --topic T`: para cada partição com lag, lê a mensagem no offset commitado e tenta decodificar. Mostra partição, offset, tamanho, erro de decode, headers. **Sem `--observe` na v1** (heurística de "partição parada" fica para v1.x) **[decisão do Claude]**.
- `skip --group G --topic T --partition P --offset O --dlt T.DLT --reason "..."`: copia key, valor e headers para o DLT adicionando `kafcli-original-topic`, `-partition`, `-offset`, `-reason`, `-operator`, `-timestamp`; depois commita `O+1`. Mesmas regras de grupo vazio do `offsets set`.
- Reprocessar o DLT: `consume --topic T.DLT | produce --topic T --input envelope`.

### 6.8 `kafcli groups audit`

Coleta grupos (ou `--group`): membros, atribuição, subscription (decodificar o metadata do protocolo consumer).

Identificação do serviço por membro, nesta ordem:

1. Regex configurável sobre `client.id` (grupo de captura = nome do serviço).
2. Host: remoção de sufixos no padrão Kubernetes ReplicaSet/pod (`pedidos-api-7d9f8c6b5-x2kqp` → `pedidos-api`). Genérico o bastante para qualquer k8s.

| Regra | Condição | Severidade |
|---|---|---|
| R1 | Membros de mais de um serviço no mesmo grupo | ERRO |
| R2 | Membros com subscriptions diferentes | ERRO |
| R3 | Grupo sem membros e com lag > 0 (na v1, sem medir "crescendo") **[decisão do Claude]** | AVISO |
| R4 | Mais membros do que partições | INFO |
| R5 | Nome fora da convenção (regex no config; regra desligada se não configurada) | AVISO |

Exit 1 se houver ERRO.

## 7. Autenticação

Triagem por custo. Tudo que é "só config do librdkafka" sai barato.

**Broker — Onda A (v1, primeiro):** `none`, `tls`, `mtls` (PEM), `sasl-plain`, `sasl-scram-256`, `sasl-scram-512`, `azure-connection-string`.

**Broker — Onda B (v1):** `oauth-oidc` (refresh pelo librdkafka), `bearer` (token estático de env/arquivo; avisar se `exp` do JWT estiver perto), e os modos Azure via callback `generate_oauth_token` com cache: `azure-sp-secret`, `azure-sp-cert`, `azure-managed-identity`, `azure-workload-identity`, `azure-cli`, `azure-default`. Scope: `https://<namespace>.servicebus.windows.net/.default`.

**Fora da v1:** `aws-msk-iam` (v1.x; avaliar crate de signer SigV4), `kerberos` (feature de compilação opcional, fora dos binários oficiais).

**Schema Registry:** `none`, `basic`, `bearer`, `mtls`, `azure` (mesmos providers Azure).

**Regras:**

- `none` recusado em perfil `protected`.
- TLS: verificação de certificado e hostname sempre ligada; `--insecure` só em perfil não protegido, com aviso.
- TOML só guarda referências a segredo. Segredos nunca em log, erro, JSON ou `profile show`. Tipos de segredo envolvidos em wrapper que não implementa `Debug`/`Display`/`Serialize` em claro (ex.: `secrecy`) **[decisão do Claude]**.

## 8. Fase 0 — Matriz de capacidades do Event Hubs

Antes de implementar comandos operacionais, um spike contra um namespace Event Hubs real (Standard e, se disponível, Premium) confirma: `ListGroups`, `DescribeGroups`, `ListConsumerGroupOffsets`, `AlterConsumerGroupOffsets` / `OffsetCommit` fora de grupo ativo, `OffsetsForTimes`, producer idempotente, e o formato Avro do Azure Schema Registry (content-type, header/prefixo com schema id). Avaliar também se o **emulador oficial do Event Hubs** (Docker) serve para testes de integração.

Resultado: ADR `0001-event-hubs-capabilities.md` + uma matriz embutida no código que os comandos consultam para avisar o usuário ("operação não suportada neste broker") em vez de falhar com erro obscuro. Se uma API crítica não existir no Event Hubs, o comando correspondente continua existindo para Kafka genérico e avisa no Event Hubs.

## 9. Guarda-corpos

| Nível | Comandos | Exigência |
|---|---|---|
| Leitura | `consume`, `lag`, `offsets show`, `groups audit`, `poison find`, `schema get/check-compat`, `profile *` | Nenhuma |
| Escrita | `produce`, `schema register` | Em `protected`: confirmação y/N ou `--yes` |
| Destrutiva | `offsets set`, `offsets restore`, `poison skip` | Dry-run padrão + `--execute`; em `protected`, digitar o nome do grupo |

- Sem TTY (CI) e sem a flag de confirmação, operação de Escrita/Destrutiva em perfil `protected` aborta (exit 3) **[decisão do Claude]**.
- Audit log local JSONL em `<state_dir>/kafcli/audit.log`: quem, quando, perfil, comando, alvo, antes/depois. Nunca segredos nem conteúdo de mensagens.
- `consume` e `poison find` truncam valor (`--max-bytes 2048`) e aceitam `--redact`.
- Documentação lista permissões mínimas por comando (ACLs Kafka; papéis Azure: Data Receiver, Data Sender, Data Owner).

## 10. Testes e critérios de aceite

**Estratégia:**

- Unitários: round-trip de cada codec/wire, parsing de config, mascaramento de segredos, regras do audit com fixtures, templates.
- Integração com testcontainers: Kafka KRaft + Schema Registry Confluent; containers separados com SASL/SCRAM e mTLS.
- Azure: mock do token endpoint; roteiro manual documentado contra Event Hubs de dev. Emulador do Event Hubs se a Fase 0 aprovar.
- Teste de vazamento: roda comandos com segredos conhecidos em `-vv` e `--output json`, faz grep pelo segredo em stdout/stderr/audit log.
- CI: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`, `cargo deny`, build de release em todos os alvos.

**Critérios de aceite:**

- [ ] `produce` envia JSON raw, Avro Confluent e Avro Azure; recusa payload inválido contra o schema com mensagem clara.
- [ ] `produce` nunca registra schema.
- [ ] `consume` decodifica as mesmas três formas e sua saída NDJSON é aceita por `produce --input envelope` preservando key e headers.
- [ ] `lag` bate com `kafka-consumer-groups.sh --describe`.
- [ ] `poison skip` copia para o DLT com headers `kafcli-original-*` e o consumer volta a avançar.
- [ ] `offsets set` sem `--execute` não altera nada; com `--execute` gera backup e `restore` volta ao estado anterior.
- [ ] `groups audit` detecta R1 com dois serviços no mesmo grupo.
- [ ] Cada modo de auth das ondas A e B tem teste automatizado ou roteiro manual, e `profile test` funciona com ele.
- [ ] Nenhum segredo aparece em log, erro, `--output json` ou audit log.
- [ ] Operações destrutivas em perfil `protected` exigem confirmação digitada.
- [ ] Binários de release rodam em máquina limpa sem librdkafka/OpenSSL instalados (Linux musl, macOS; Windows quando estabilizado).

## 11. Fases

Cada fase entrega testes passando e commit próprio.

0. **Spike Event Hubs** → ADR + matriz de capacidades (§8).
1. **Esqueleto**: workspace, clap, perfis, segredos, `--output`, tracing, guard, audit log, CI com build de release Linux/macOS desde o início (para descobrir cedo problemas do build estático).
2. **Auth onda A** + `profile test`.
3. **Produce + consume**: codecs, Schema Registry Confluent e Azure, validação, templates, NDJSON, envelope, `--count/--rate`, `schema`.
4. **Auth onda B** (OAuth/Azure). Colocado depois do produce para que o primeiro marco útil seja produzir no Event Hubs via `azure-connection-string` **[decisão do Claude — ver pergunta em aberto]**.
5. **Leitura operacional**: `lag` (com `--watch`), `offsets show`, `groups audit`.
6. **Destrutivas**: `offsets set/restore`, `poison find/skip`.
7. **Distribuição**: `cargo-dist`, Windows, completions, man page, README com exemplos por comando e tabela de permissões mínimas.

## 12. Deixado para o builder

- Estrutura interna de módulos além do esboço em §5.1.
- Formato exato das tabelas.
- Escolha entre `oauth2` crate ou chamada direta ao token endpoint.
- Biblioteca de wrapper de segredo.
- Detalhes do formato do backup de offsets (desde que `restore` o aceite e ele seja versionado).

## 13. Backlog pós-v1

- **v1.x**: `aws-msk-iam`; `poison find --observe` (partição parada); R3 com tendência de lag; PKCS#12.
- **v2**: `kafcli test` (cenários duplicate, duplicate-cross-partition, same-id-new-content, out-of-order, replay; verificadores topic/http/group; JUnit; consumer de referência). Boa parte dos cenários já é reproduzível na v1 com `produce --count` e NDJSON.
- Talvez: `--k8s` para mapear pod → deployment; Kerberos nos binários oficiais.
