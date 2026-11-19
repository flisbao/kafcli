# ADR 0006: operações de leitura (`lag`, `offsets show`, `groups audit`)

- Status: aceito
- Data: 2026-11-18
- Escopo: plano 04 (fase 5 da spec), Tasks 1 a 8

## Contexto

A spec pede três comandos de leitura sobre grupos de consumo: `kafcli lag` (§6.5), `kafcli offsets show` (§6.6) e `kafcli groups audit` com as regras R1 a R5 (§6.8). Todos são do nível Leitura (§9): não pedem confirmação, não gravam no audit log e não comitam offset. O critério de aceite (§10) é o `lag` bater com `kafka-consumer-groups.sh --describe` e o `groups audit` detectar R1 com dois serviços no mesmo grupo.

O ADR 0001 é provisório para o Event Hubs. `ListGroups` e `DescribeGroups` estão "não verificado", e `OffsetFetch` e `ListOffsets` foram vistos só no emulador. A rodada real do roteiro `docs/spike/0001-event-hubs-runbook.md` ainda não aconteceu.

## Decisão

### Só `BaseConsumer`, sem `AdminClient`

- O rdkafka 0.39 não expõe `ListConsumerGroupOffsets` no `AdminClient`. As leituras usam só `BaseConsumer<KafkaClientContext>`:
  - `fetch_group_list` para `ListGroups` (sem grupo) e `DescribeGroups` (com grupo);
  - `committed_offsets` com `group.id` para `OffsetFetch`;
  - `fetch_watermarks` para `ListOffsets`;
  - `fetch_metadata` para as partições dos tópicos.
- Todo consumer de leitura tem `enable.auto.commit=false`, `enable.auto.offset.store=false` e `allow.auto.create.topics=false`, e nunca chama `subscribe`, `assign` nem `commit`.
- O `KafkaClusterReader` guarda um consumer de metadata sem `group.id`. Para cada grupo cria um consumer com `group.id` igual ao grupo, lê os offsets e o descarta antes do próximo. No máximo um consumer de grupo vivo por vez.
- Os consumers são criados por `KafkaConnection::create_consumer`, então os modos OAuth esperam o primeiro token como no ADR 0005. Sob `--watch`, o mesmo leitor serve todas as amostras, e a espera entre elas faz `poll` no consumer de metadata para que ele continue renovando o token (ver `--watch`).
- `DescribeGroups` clássico responde um grupo inexistente com estado `Dead` e sem membros, e não com `GroupIdNotFound`. O leitor descarta esses grupos, e o pedido de um grupo assim vira "não encontrado" (ver abaixo).

### De onde vêm as partições de um grupo

- Com `--topic`: todas as partições daquele tópico.
- Sem `--topic`, com membros: a união dos tópicos dos assignments e das subscriptions decodificadas.
- Sem `--topic`, sem membros (consumer parado, caso típico de incidente), ou com algum membro sem subscription nem assignment decodificados (metadata ilegível, protocolo que não é `consumer`, grupo do KIP-848): todos os tópicos do metadata, menos os internos (prefixo `__`). Sem isso, um grupo assim sairia com lag zero e o `--max-lag` não dispararia.
- Nos três casos ficam só as partições com offset commitado, mais as atribuídas a um membro ativo, que ficam sempre.
- Um `--group` que o broker não descreve e que não tem nenhum offset commitado é `consumer group '<g>' not found` (exit 1). Com offset commitado, entra como grupo sem membros, com estado `Unknown`.
- O metadata é lido uma vez por comando e compartilhado entre os grupos.

### Conta do lag

- Com offset commitado `c` ≥ 0: `lag = max(0, log_end - c)`.
- Sem offset commitado (membro novo, `auto.offset.reset` ainda não aplicado): `committed_offset` e `lag` são `null` no JSON e `-` na tabela, e a partição fica fora dos totais. É o mesmo comportamento do `kafka-consumer-groups.sh`.
- Total de tópico e de grupo = soma dos lags não nulos.
- A tabela do `lag` usa as colunas do `kafka-consumer-groups.sh --describe` e fecha cada grupo com `TOTAL <grupo> <lag>`, mais `(no active members)` quando é o caso.

### Decodificação do protocolo consumer

- `ConsumerProtocolSubscription` (versões 0 a 3) e `ConsumerProtocolAssignment` são lidos com um cursor big-endian próprio, sem dependência nova. Só os tópicos e as partições interessam; `user_data` e o resto são ignorados, e bytes que sobram no fim são aceitos.
- Limites: 10.000 tópicos, 100.000 partições por membro (somando todos os tópicos) e strings de no máximo 32.767 bytes. A contagem é checada antes de qualquer alocação, então um metadata malicioso não aloca memória grande nem causa pânico.
- Erro de decodificação nunca aborta o comando. O membro sai com `subscription`/`assignment` `None`, e um `tracing::warn!` registra o grupo, o membro e o campo, sem os bytes.
- Só grupos com `protocol_type` `consumer` são decodificados. Grupos do protocolo novo (KIP-848, `group.protocol=consumer`) aparecem pelo `DescribeGroups` clássico sem membros decodificados, o que o `--help` avisa.

### Erros e avisos do Event Hubs

- `map_broker_operation_error` traduz os erros do broker por operação (`ListGroups`, `DescribeGroups`, `OffsetFetch`, `ListOffsets`):
  - `GroupIdNotFound` com grupo conhecido vira `consumer group '<g>' not found`, nunca "não suportado";
  - `UnsupportedVersion`, `InvalidRequest`, `UnsupportedFeature` e `UnsupportedForMessageFormat` no Event Hubs, para operação sem status `Documented`, viram `<Operation> is not supported by this broker (Azure Event Hubs): <code>`; em Kafka genérico, `<Operation> is not supported by this broker: <code>`;
  - o resto continua `kafka error: ...`.
- Lista de grupos vazia, ou sem o grupo pedido, no Event Hubs adiciona o aviso `the group list may be incomplete on Azure Event Hubs (ListGroups is unverified, see ADR 0001)`. O aviso vai em `warnings` no JSON e no stderr em `table`.
- Erro de metadata de um tópico pedido só vira `TopicNotFound` com `UnknownTopicOrPartition`. `LeaderNotAvailable` e os demais saem com o código, para não esconder um tópico que existe mas está sem líder. A mesma regra vale para o probe de capacidades e o dump de mensagens.

### `--watch` sem histórico

- `lag --watch <INTERVAL>` aceita `5s`, `1m` ou um número de segundos, de 1 s a 3600 s.
- O laço guarda só a amostra anterior. A taxa de consumo é a soma dos offsets commitados ganhos dividida pelos segundos entre as amostras, só nas partições com commit nas duas. A tendência é `rising`, `falling` ou `steady` pelo delta do lag total.
- Entre amostras o comando não usa `std::thread::sleep`. Ele espera o intervalo chamando `poll` no consumer de metadata, em fatias de até 1 s, até o prazo lido do relógio. O librdkafka só entrega o pedido de renovação do token OAUTHBEARER dentro de `poll`, então um `sleep` deixaria o token do consumer de metadata vencer num `--watch` longo. O `poll` bloqueia até o fim da fatia (o consumer não tem assignment, então não recebe mensagem), e por isso não há busy-wait.
- Em `table` com stdout de terminal, cada quadro limpa a tela (`\x1b[2J\x1b[H`) e traz os avisos no fim do próprio quadro, porque um aviso escrito antes no stderr seria apagado. Fora de terminal, os quadros saem em sequência, separados por uma linha vazia, sem sequências ANSI. Em `json`, cada amostra é uma linha NDJSON com o envelope `kafcli_output: 1` e o campo `changes`.
- Pipe fechado no stdout encerra com exit 0, como no `consume`.

### `--max-lag`

- Compara o `total_lag` de cada grupo, antes do corte do `--top`, então um grupo fora da saída também dispara o limite. O primeiro grupo da ordem do `--sort` acima do limite gera `limit exceeded: group '<g>' lag <total> exceeds --max-lag <N>`, com exit 2.
- O relatório completo sai antes no stdout, via `RenderedOutput.trailing_error`, inclusive em `--output json`.
- Com `--watch`, a primeira amostra acima do limite imprime o quadro e encerra com exit 2.

### Identificação de serviço e regras do audit

- O serviço de um membro vem, nesta ordem (spec §6.8), do `service_pattern` aplicado ao `client_id` (grupo nomeado `service`, ou o primeiro grupo de captura) e do host sem o sufixo de pod do Kubernetes (`-<hash>-<id>` ou `-<id>`, no alfabeto `bcdfghjklmnpqrstvwxz2456789`).
- O broker costuma reportar o host como IP (`/10.0.0.7`). Esse host não identifica serviço. Membros sem serviço geram um `INFO` e não contam para R1, para que um grupo de pods sem `client.id` não vire ERROR por falta de informação.
- `service_pattern` e `group_name_pattern` vêm de `groups_audit` no arquivo de configuração, e as flags de mesmo nome os sobrepõem.
- R2 e R4 só rodam para `protocol_type` `consumer`, porque só esse metadata é decodificado. Grupos de outros protocolos (`connect`, por exemplo) com membros recebem um `INFO` explicando que R2 e R4 foram pulados. R4 só roda quando há ao menos um tópico de subscription decodificado.
- O lag é lido só para grupos sem membros, que é o que R3 precisa.
- Qualquer achado `ERROR` imprime o relatório e encerra com `groups audit found <n> ERROR finding(s)` (exit 1).

### Contrato JSON de `offsets show`

`offsets show --output json` devolve, dentro do envelope `kafcli_output: 1`:

```json
{
  "group": "pedidos-service",
  "state": "Empty",
  "partitions": [
    {"topic": "pedidos", "partition": 0, "committed_offset": 13, "log_start_offset": 5, "log_end_offset": 20, "lag": 7}
  ],
  "warnings": []
}
```

O plano 05 usa `partitions` como formato do backup de offsets antes de `offsets set`. Campos só podem ser acrescentados; mudar ou remover um campo exige subir `kafcli_output`.

## Consequências

- O merge do branch depende da rodada real do roteiro do Event Hubs (ADR 0001). Sem ela, os status `Unverified`/`EmulatorOnly` continuam valendo, com os avisos e erros descritos aqui.
- Os testes automatizados usam o `FakeClusterReader` (feature `test-support`) para a conta do lag e funções puras para o protocolo, as regras e a renderização. O leitor real é coberto pela fixture `kafka-plaintext` (Apache Kafka 4.1.1), que compara o `lag` com `kafka-consumer-groups.sh --describe` e o `offsets show` com `kafka-get-offsets.sh`, e detecta R1 com dois consumers de serviços diferentes no mesmo grupo.
- `--all-groups` e `groups audit` sem `--group` fazem uma leitura de offsets por grupo. Até o plano 05 ela era em sequência, para nunca manter mais de um consumer de grupo aberto. Desde o plano 05b (2026-11-19), o `lag` sem `--group` lê até 8 grupos ao mesmo tempo (`LAG_WORKERS`), com no máximo 8 consumers de grupo abertos. Os watermarks de cada partição são lidos uma vez por amostra e compartilhados entre os grupos, inclusive quando a leitura falha. A saída continua em ordem alfabética, e com um único grupo nenhuma thread extra é criada. O `groups audit` continua chamando a leitura uma vez por grupo vazio, em sequência.
- Grupos do KIP-848 ficam sem membros decodificados até o rdkafka expor o `ConsumerGroupDescribe`.

## Fora do escopo

- `offsets set`, `offsets restore`, `poison find`, `poison skip` e `--wait-empty` (plano 05).
- R3 com tendência de lag ("crescendo"), que é v1.x (spec §6.8, §13).
- Mapear pod para deployment pela API do Kubernetes (spec §3).
- Membros decodificados de grupos do protocolo novo (KIP-848).
