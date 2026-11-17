# ADR 0001: Capacidades Kafka do Azure Event Hubs

- Status: provisório
- Data: 2026-11-17
- Escopo: plano 00 (spike do Event Hubs); consumido pelos planos 02, 04 e 05

## Contexto

A spec (§8) pede um spike contra um namespace Event Hubs real antes dos comandos operacionais. O spike deveria confirmar `ListGroups`, `DescribeGroups`, `ListConsumerGroupOffsets`, `OffsetCommit` fora de grupo ativo, `OffsetsForTimes`, producer idempotente e o formato Avro do Azure Schema Registry. Também deveria avaliar se o emulador oficial serve para testes de integração.

O plano 00 entregou as ferramentas: o probe de capacidades, o exemplo `capability_probe`, o escritor Python do Schema Registry e o roteiro `docs/spike/0001-event-hubs-runbook.md`. O emulador foi avaliado em `tests/fixtures/eventhubs-emulator/`.

A rodada contra um namespace real não aconteceu: no momento não há assinatura Azure disponível. Por isso este ADR é **provisório**. Ele registra o que a documentação oficial, o código-fonte dos serializers da Microsoft e o emulador permitem afirmar, e marca o que continua sem verificação. Cada linha da matriz diz de onde veio.

### Fontes

Consultadas em 2026-11-17. Datas entre parênteses são as de atualização das páginas.

- [D1] Visão geral do Kafka no Event Hubs: <https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview> (2026-02-06).
- [D2] FAQ do Kafka no Event Hubs: <https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-frequently-asked-questions> (2025-03-06).
- [D3] Cotas: <https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-quotas> (2026-02-06).
- [D4] Comparação de tiers: <https://learn.microsoft.com/en-us/azure/event-hubs/compare-tiers> (2026-08-26).
- [D5] Conceitos do Schema Registry: <https://learn.microsoft.com/en-us/azure/event-hubs/schema-registry-concepts> (2026-08-26).
- [D6] Troca de eventos entre AMQP e Kafka: <https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-exchange-events-different-protocols> (2024-12-18; os exemplos usam clientes antigos: Kafka Java 1.1.1, Event Hubs Java 1.1.0).
- [S1] Encoder Python `azure-schemaregistry-avroencoder`, tag `azure-schemaregistry-avroencoder_1.0.0`: `_utils.py` e `_constants.py` em <https://github.com/Azure/azure-sdk-for-python/tree/azure-schemaregistry-avroencoder_1.0.0/sdk/schemaregistry/azure-schemaregistry-avroencoder/azure/schemaregistry/encoder/avroencoder>.
- [S2] Serializer Kafka Java, `KafkaAvroSerializer.java` e `KafkaAvroDeserializer.java` no `master` de <https://github.com/Azure/azure-schema-registry-for-kafka> (o repositório não publica tags). Mudança de formato no commit `9c3b5d6d` (PR #29, 2022-08-09).
- [S2b] CHANGELOG da biblioteca Java `azure-data-schemaregistry-apacheavro` (repositório Azure/azure-sdk-for-java, `sdk/schemaregistry/azure-data-schemaregistry-apacheavro/CHANGELOG.md`): a versão 1.0.0 (2022-05-13) traz "Removed old support for preamble deserialization".
- [S3] Serializer Kafka .NET, `csharp/avro/src/KafkaAvroSerializer.cs` no mesmo repositório, `master`.
- [S4] `azure-eventhub` Python, `EventData.content_type` em `sdk/eventhub/azure-eventhub/azure/eventhub/_common.py`, `main`.
- [E] Probe contra o emulador 2.2.1: `tests/fixtures/eventhubs-emulator/probe-result.json`. A Microsoft declara que o emulador só é compatível com as APIs de producer e consumer; o resultado dele não vale como evidência do serviço real para grupos.

## Decisão

### Tiers

- **Basic não é suportado pelo kafcli.** O endpoint Kafka existe só em Standard, Premium e Dedicated [D1]. O Schema Registry também não existe no Basic [D5].
- O kafcli não detecta o tier: o endpoint Kafka não o expõe, e o contexto `azure-connection-string` não tem campo para ele. Em `azure-connection-string`, o kafcli assume o menor denominador entre os tiers suportados, que é o Standard.

### Matriz de capacidades

Legenda da coluna "Status":

- **documentado**: afirmado pela documentação ou pelo código-fonte citado.
- **emulador**: só observado no emulador.
- **não verificado**: a documentação não trata do assunto; depende da rodada real.

| Capacidade | Standard | Premium / Dedicated | Status | Fonte | Uso no kafcli |
|---|---|---|---|---|---|
| Endpoint Kafka `<ns>.servicebus.windows.net:9093`, SASL_SSL, protocolo ≥ 1.0 | sim | sim | documentado | D1 | todos os comandos |
| Token SAS gerado (em vez de connection string) | não | não | documentado | D1, D2 | `azure-connection-string` usa `PLAIN` com usuário `$ConnectionString` e a connection string como senha, nunca um token SAS |
| Producer idempotente | sim | sim | documentado | D1, D2, E | plano 02, `produce` |
| Compressão | nenhuma | só `gzip` | documentado | D1 | plano 02 |
| Transações | não | preview | documentado | D1 | fora da v1 |
| Kafka Streams | não | preview | documentado | D1 | fora da v1 |
| Headers Kafka em mensagens produzidas por Kafka | — | — | emulador | E; D6 descreve headers entre AMQP e Kafka, mas com clientes antigos | plano 02 |
| Criação automática de tópico por produce | — | — | não verificado | D1 só diz que o event hub é criado à parte | o kafcli sempre usa `allow.auto.create.topics=false` |
| Consumer groups Kafka criados automaticamente, offsets guardados no serviço | sim | sim | documentado | D2 | plano 04 |
| `ListOffsets` (watermarks) | — | — | emulador | E | plano 04, `lag`, `offsets show` |
| `OffsetsForTimes` | — | — | emulador | E | plano 05, `offsets set --to-datetime` |
| `OffsetCommit` sem membro ativo (fora de grupo) | — | — | emulador | E; D2 só fala em "consumer group APIs" | plano 05, `offsets set`, `restore` |
| `OffsetFetch` (`ListConsumerGroupOffsets`) | — | — | emulador | E | plano 04 |
| `ListGroups` | — | — | não verificado | no emulador a chamada foi aceita, mas o grupo do probe não foi listado (`supported`, "probe group listed: no") | plano 04, `lag`, `groups audit` |
| `DescribeGroups` | — | — | não verificado | no emulador falhou com "probe group not found" | plano 04, plano 05 (`--wait-empty`) |
| Commit recusado com membro ativo no grupo | — | — | não verificado | o emulador **aceitou** o commit | plano 05, guarda-corpo do `offsets set` |
| Admin API (criar e apagar tópico, `DescribeConfigs`, `DeleteRecords`) | — | — | não verificado | D1 a D4 são omissos | fora da v1 |

Limites por tier [D3, D4]:

| Limite | Basic | Standard | Premium | Dedicated |
|---|---|---|---|---|
| Consumer groups Kafka por namespace | — | 1.000 | 1.000 | 1.000 |
| Partições por event hub | 32 | 32 | 100 | 1.024 |
| Tamanho máximo de mensagem | 256 KB | 1 MB | 1 MB | 20 MB |
| Retenção máxima | 1 dia | 7 dias | 90 dias | 90 dias |
| Schema groups / versões | — | 1 (além do padrão) / 25 | 100 / 1.000 | 1.000 / 10.000 |

O limite do Standard importa: um namespace Standard tem espaço para um único schema group além do padrão. As 25 versões são o total do namespace, somando todos os grupos, num registry de até 25 MB.

### Formato Avro do Azure Schema Registry (`azure-avro`)

Os três serializers da Microsoft convergem no mesmo formato. A documentação do Schema Registry [D5] não descreve os bytes; o formato vem do código-fonte.

- **Corpo da mensagem:** Avro binário puro, sem bytes de prefixo [S1, S2, S3].
- **Schema id:** vai no header Kafka `content-type`, com valor `avro/binary+<schema id>` em UTF-8 cru, sem codificação AMQP.
  - O serializer Java [S2] e o .NET [S3] gravam esse header diretamente.
  - O encoder Python [S1] devolve `content_type = "avro/binary+<schema id>"`, e o `azure-eventhub` grava esse valor na propriedade de sistema AMQP `content-type` [S4]. O serviço converte essa propriedade para o header Kafka `content-type` em UTF-8 [D6].
- **Leitura:**
  - O decoder Python [S1] faz `split("+")` e exige exatamente duas partes, com a primeira igual a `avro/binary`. Ele não confere tamanho nem caracteres do id.
  - O deserializer Java [S2] repassa o header sem analisar.
  - O kafcli separa o valor no primeiro `+` (`split_once('+')`). A primeira parte precisa ser exatamente `avro/binary`, e o resto é o schema id. É uma decisão mais tolerante que a do Python, tomada de propósito: o id é opaco e só serve para buscar o schema no registry.
- **Formato legado com prefixo:**
  - Os serializers Java gravavam um prefixo no corpo até o commit `9c3b5d6d` (agosto de 2022) [S2]. O layout, segundo as constantes mantidas no encoder Python [S1], é: 4 bytes de indicador de formato, 32 bytes de schema id e o Avro a partir do byte 36.
  - Pela biblioteca Java `azure-data-schemaregistry-apacheavro`, a versão 1.0.0 (2022-05-13) removeu a leitura desse prefixo [S2b]. O deserializer Kafka no `master` atual depende dessa biblioteca e não lê mais o formato legado.
  - O Javadoc do `KafkaAvroSerializer` ainda fala em payload "prefixed with a GUID". O texto está desatualizado: o código não grava prefixo.
  - O kafcli **não** lê o formato legado na v1. Uma mensagem sem o header `content-type` sai como erro de decode, sem tentar adivinhar o prefixo. O texto exato da mensagem fica para o plano 02.
- **Escrita:** o encoder `azure-avro` do kafcli grava o header `content-type` com `avro/binary+<id>` em UTF-8 e o corpo em Avro puro. Esse é o formato que os três serializers atuais leem.

### Como o kafcli usa a matriz

- A matriz vira uma tabela no código, como a spec pede (§8). A tabela é **criada no plano 02**, que já precisa dela para a idempotência e a compressão. Os planos 04 e 05 acrescentam as linhas que usam.
- A tabela é consultada pelos comandos que dependem de uma capacidade que não esteja como **documentada**, isto é, marcada como **emulador** ou **não verificado**. O emulador não vale como evidência do serviço real.
- Em contexto `azure-connection-string`, e enquanto a capacidade não estiver documentada, o kafcli tenta a operação. Se o broker responder com erro de "não suportado" (`UnsupportedVersion`, `InvalidRequest`, `UnsupportedFeature`), o comando falha com uma mensagem que nomeia a operação e o Event Hubs.
- Grupo não encontrado **não** é "não suportado". `GroupIdNotFound` também é a resposta normal para um grupo que nunca gravou offset ou cujos offsets expiraram. O kafcli reporta o caso como grupo não encontrado, não como operação não suportada.
- **Resultado vazio também precisa de aviso.** No emulador, `ListGroups` respondeu com sucesso, mas sem o grupo que tinha offsets gravados. Em `azure-connection-string`, enquanto `ListGroups` estiver não verificado, uma lista vazia ou sem o grupo esperado sai com aviso de que o resultado pode estar incompleto no Event Hubs.
- **Guarda-corpo do commit com grupo ativo.** O plano 05 não pode depender do broker para recusar `offsets set` com membro ativo, porque o emulador aceitou esse commit.
  - O kafcli checa antes, no cliente, via `DescribeGroups`, e distingue três estados: grupo vazio, grupo não encontrado e chamada com falha. Só o primeiro libera o commit sem pergunta.
  - `DescribeGroups` respondendo "grupo não encontrado" não prova que o grupo está vazio. Esse caso, e a falha da chamada, caem no ramo `--wait-empty` ou na confirmação explícita de que o grupo está parado.
  - A checagem no cliente não fecha a corrida com um consumer que entra no grupo logo depois. O erro real do broker, quando houver, continua sendo tratado.
  - O desenho fica para o plano 05.
- **Emulador:**
  - Serve para testes de integração de produce, consume, headers, `ListOffsets`, `OffsetsForTimes`, commit e fetch de offset fora de grupo, e metadata.
  - Não serve para nada que dependa de `ListGroups`, `DescribeGroups` ou recusa de commit com membro ativo.
  - Também não tem Schema Registry, então os testes do codec `azure-avro` usam um registry falso (mock HTTP).

### Pendente: rodada real

Este ADR passa de provisório para aceito depois da rodada do roteiro `docs/spike/0001-event-hubs-runbook.md` num namespace Standard. A rodada precisa:

1. Rodar o probe e confirmar, ou trocar o status de, cada linha marcada como **emulador** ou **não verificado**.
2. Rodar o dump do passo 5 e confirmar o header `content-type` com `avro/binary+<id>` em UTF-8 numa mensagem escrita pelo encoder Python via AMQP. Isso fecha a dependência do serviço de conversão descrita em [D6], que foi escrita para clientes antigos.

Prazo: a rodada é pré-requisito para o **merge do plano 04**. O plano 02 pode avançar com este ADR provisório, porque só depende de produce, consume e do formato `azure-avro`, que estão documentados no código-fonte.

## Consequências

- O plano 02 implementa `azure-avro` com header `content-type` e corpo Avro puro, sem suporte ao prefixo legado.
- O plano 02 deixa a compressão desligada (`compression.type=none`) em contexto `azure-connection-string`, porque o kafcli assume o Standard, que não aceita compressão. Ligar `gzip` num Premium ou Dedicated depende de uma opção de configuração que a spec ainda não prevê; fica fora da v1 até alguém pedir.
- Os planos 04 e 05 tratam toda capacidade que não esteja como documentada (marcada como **emulador** ou **não verificado**) como opcional: erro claro quando o broker recusa, aviso quando o resultado vem vazio, nunca suposição de sucesso.
- O guarda-corpo de `offsets set` contra grupo ativo fica no cliente, não no broker.
- Se a rodada real mostrar que `ListGroups` ou `DescribeGroups` não funcionam no Event Hubs, `lag` e `groups audit` continuam existindo para Kafka genérico e avisam no Event Hubs, como a spec prevê (§8).

## Fora do escopo

- Basic.
- Transações e Kafka Streams, que estão em preview e só existem em Premium e Dedicated.
- JSON Schema e Protobuf do Azure Schema Registry. [D5] lista os dois formatos, mas o formato no fio deles não foi levantado.
- Leitura do formato legado com prefixo de 36 bytes.
