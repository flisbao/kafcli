# ADR 0004: Schemas e Schema Registry

- Status: aceito
- Data: 2026-11-17
- Escopo: plano 02b (schemas, wires e Schema Registry), Tasks 1 a 9

## Contexto

O plano 02a entregou `produce` e `consume` com JSON puro. O plano 02b adiciona Avro e JSON Schema, os wires `confluent` e `azure` e o cliente do Schema Registry. Os comandos de schema (`kafcli schema register|get|check-compat`) fecham o plano.

As decisões abaixo valem para o código do branch `feat/schemas`. Onde a especificação deixava escolhas em aberto, o controlador fixou a regra durante o plano; cada regra está marcada nas seções.

## Decisão

### Mapeamento JSON e Avro

- Union sem embrulho: um valor JSON encaixado em uma union Avro vai direto, sem o objeto `{"tipo": valor}` do JSON do Avro. Na codificação, o ramo escolhido é o primeiro que aceita o valor.
- `bytes`, `fixed` e `decimal` em bytes: a string JSON é convertida caractere por caractere, cada um virando um byte de mesmo code point. Um caractere acima de U+00FF é erro de validação (`character above U+00FF in bytes`).
- Valores não finitos (`NaN`, `Infinity`, `-Infinity`) de `float` e `double` são aceitos como string no JSON. Um número JSON que estoura o tipo é erro.
- Tipos lógicos seguem o tipo de base: `date` lê um inteiro JSON, e `decimal` lê os bytes como string (`bytes` ou `fixed`), com a checagem de tamanho do `fixed`.
- Profundidade de conversão de no máximo 64 níveis de JSON (`MAX_NESTING_DEPTH`). Ver também os limites de decodificação abaixo.

### Fora do escopo

- Schema references (`references` no registry) e Protobuf ficam fora. O cliente rejeita um schema com referências (`schema registry error: schema references are not supported`) e um tipo `PROTOBUF` (`schema type 'PROTOBUF' is not supported`).
- `$ref` remoto (`http:` e `file:`) está desligado. A crate `jsonschema` é usada sem features padrão, então nenhum documento externo é buscado.

### Flags e combinações

- `--no-validate` só vale para `--format json`. Com Avro, o comando recusa com `--no-validate only applies to --format json`.
- `--schema-file` é proibido com `--wire confluent` e `--wire azure`: o id do schema tem de vir do registry. O erro diz `--schema-file cannot be used with --wire <wire>: the schema id must come from the registry`.
- `--wire raw` aceita um registry de qualquer tipo. `--wire confluent` e `--wire azure` exigem o registry do mesmo tipo.
- O registry não é construído para `--no-validate --format json --wire raw`, porque esse caminho nunca lê schema. Assim, um contexto sem `schema_registry` funciona nesse caso.
- `--dry-run` consulta o registry para buscar o schema (só GET), mas nunca abre conexão com o broker.
- O `--dry-run` em JSON sempre traz o campo `schema_id`, com `null` quando não há schema do registry. É uma mudança aditiva ao contrato do 02a.

### Schema Registry

- `kafcli schema register` é o único comando que registra schema. `produce` nunca chama `register_schema`, e a regra é verificada pelo teste `produce_never_sends_put_or_post_to_registry`.
- O schema vem sempre de `--schema-file` ou do registry, pelo id ou pelo subject. Nunca é lido de dentro de uma mensagem.
- O arquivo de schema é lido com limite de 1 MiB e validado localmente (`AvroSchema::parse` ou `JsonSchemaValidator::parse`) antes de qualquer requisição.
- `schema register` passa pelo guard de escrita (`OperationLevel::Write`), com a descrição `register schema for subject <S>` e o subject como alvo de confirmação. Em contexto protegido sem terminal e sem `--yes`, sai com código 3. O guard roda antes de construir o cliente do registry, então nenhuma requisição sai antes da aprovação.
- No Confluent, `schema register` mostra `version: null`: a API devolve só o id do schema, e a versão não é consultada em outra requisição.
- `schema get` e `schema check-compat` são leituras: não passam pelo guard e não gravam no audit log.
- O audit log de `schema register` tem contexto, comando `schema register`, `target` `subject=<S>`, `after` com `schema_id` e `version`, e o outcome `aborted`, `failed` ou `executed` (nunca `dry-run`). Nunca tem o texto do schema. Falha ao gravar o audit vira aviso no stderr.
- Schema de Avro e de JSON Schema são registrados com `schemaType` `AVRO` ou `JSON` no Confluent. No Azure, só Avro: `JSON Schema on the Azure Schema Registry is not supported by kafcli`.
- `schema check-compat` imprime o relatório e sai com código 1 quando o schema é incompatível, com a mensagem `schema is not compatible with subject '<S>' version <v>`.
- No Azure, `check-compat` não é suportado: o erro `check-compat is not supported by the Azure Schema Registry` sai antes de qualquer requisição. A compatibilidade no Azure é aplicada pelo grupo de schemas no momento do registro.
- `schema get` mostra o schema indentado e com caracteres de controle trocados por `?` na tabela. No JSON, o schema sai como string JSON, escapada pelo serde.

### Segurança e limites do cliente

- As configurações do registry são validadas sempre que um registry é conectado (`schema get`, `schema check-compat`, `schema register`, `produce` e `consume` que usam schema). Vale também a regra de contexto protegido: registry `http://` e registry sem autenticação (`none`) são recusados antes de qualquer requisição, mesmo em um arquivo de configuração editado à mão.
- Segredos do registry (senha `basic`, token `bearer`) vêm só de `SecretReference`, são resolvidos na construção do cliente e ficam em `secrecy::SecretString`. O header `Authorization` é marcado como sensível. Erros do `reqwest` passam por `without_url()`. A URL do registry não pode conter `user:pass@`.
- Texto vindo do registry (mensagens de erro, mensagens de compatibilidade, subjects e ids de headers) é neutralizado e limitado a 200 caracteres já no core (`neutralize_registry_text`). No máximo 50 mensagens de compatibilidade são mantidas.
- Corpo de resposta limitado a 8 MiB. Timeout por requisição igual a `--timeout-seconds` (1 a 3600, padrão 30).
- Subject e nome de schema vão no caminho com percent-encoding de segmento: só `A-Z a-z 0-9 - . _ ~` passam crus.
- Erros de validação e de decodificação nunca contêm valores do payload: só o caminho (JSON Pointer, raiz `/`), o nome de campo do schema e o motivo. Formato: `value does not match schema at '<pointer>': <motivo>`. Chaves de objeto do payload podem aparecer no ponteiro e em `unknown field '<nome>'`; valores nunca aparecem.
- `RegistryHttpClient` envia `Accept` conforme o tipo do registry: Confluent `application/vnd.schemaregistry.v1+json`, Azure `application/json`. O Confluent também usa esse `Content-Type` nas requisições com corpo.
- Headers do encoder substituem os de mesmo nome.

### Decodificação e limites de pilha

- `apache-avro` roda com `max_allocation_bytes` de 64 MiB. O limite também cobre itens de array e map de largura zero por datum.
- `max_decode_recursion_depth` de 256, para que o limite de 64 níveis de conversão valha também na decodificação.
- A CLI roda o trabalho na thread `kafcli-main`, com 64 MiB de stack. Assim, uma mensagem maliciosa profundamente aninhada não estoura a stack.

### Cache de schemas

- O decodificador guarda no máximo 256 schemas por execução (`SCHEMA_CACHE_CAPACITY`), incluindo falhas, para que um id ruim não gere uma requisição por mensagem. Não há cache global entre execuções.
- Falha definitiva (id inexistente, schema que não faz parse, tipo não suportado, referências, tipo divergente do esperado) fica no cache pelo resto da execução: um id 404 nunca é pedido de novo.
- Falha transitória (erro de transporte, timeout, HTTP 5xx) fica no cache por 30 segundos (`TRANSIENT_FAILURE_TTL`). Depois disso, a próxima mensagem com o mesmo id busca o schema de novo. Assim, uma oscilação do registry durante `consume --follow` não vira `decode_error` para todas as mensagens seguintes.

### Wire Azure

- Header Kafka `content-type` com `avro/binary+<schema id>` em UTF-8, e o corpo em Avro binário puro, sem prefixo. Na leitura, o header é separado no primeiro `+`, e a primeira parte tem de ser exatamente `avro/binary`.
- Mensagem sem header: `value has no content-type header with avro/binary+<schema id>; the legacy 36-byte prefix format is not supported`.
- Azure aceita autenticação `bearer` (com `token_ref`) ou `azure`, que usa um token do Entra ID herdado da credencial azure-* do broker. O modo `azure` está descrito no ADR 0005.
- Os caminhos e o `api-version` do Azure (`/$schemaGroups/<grupo>/schemas/<nome>/versions/<n>?api-version=2023-07-01`) foram verificados só contra o mock local. A rodada real fica pendente junto com a do ADR 0001.

### Wire Confluent

- Byte `0x00`, id do schema em 4 bytes big-endian, e o corpo (Avro binário ou JSON compacto).

### Falhas de decodificação no consume

- Falha de decodificação nunca aborta o `consume`, nem quando o registry está fora do ar. A mensagem sai com `decode_error`, `value: null` e `value_base64`, como no 02a.

### Testes e ferramentas

- Os testes de integração contra o Redpanda Schema Registry ficam em `crates/kafcli/tests/integration_schemas_cli.rs` com `#[ignore]`. Rodam com `docker compose -f tests/fixtures/compose.yaml up -d --wait`, depois `cargo test -p kafcli --test integration_schemas_cli -- --ignored`.
- A espera pelo registry usa `std::thread::sleep` de 500 ms, no máximo 30 s. É o único sleep do projeto, só em teste, com `allow` local e justificativa exigida pelo `clippy.toml`. Nenhum código de produção dorme.

## Consequências

- Só `schema register` escreve no registry. Um script que chama `produce` não cria schema por acidente.
- O audit log de `schema register` basta para saber quem registrou o quê, sem vazar o schema.
- Azure fica limitado: sem JSON, sem `check-compat` e sem auth Entra ID antes do plano 03.
- Caminhos e headers do Azure dependem de mock até a rodada real. Um erro nesses caminhos só aparece contra um namespace de verdade.
- Licenças pendentes no `cargo deny check`: `CDLA-Permissive-2.0` (crate `webpki-root-certs`, via `reqwest` e `rustls`) e `MIT-0` (crate `borrow-or-share`, via `jsonschema`). O `deny.toml` não foi alterado; a decisão do mantenedor está pendente.

## Fora do escopo

- Schema references e Protobuf.
- Autenticação Entra ID no Schema Registry Azure e os modos `azure-*` do broker (plano 03).
- Compatibilidade de Azure por `check-compat`.
