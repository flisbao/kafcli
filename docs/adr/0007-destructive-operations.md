# ADR 0007: operações destrutivas (`offsets set`, `offsets restore`, `poison find`, `poison skip`)

- Status: aceito
- Data: 2026-11-18
- Escopo: plano 05 (fase 6 da spec), Tasks 1 a 11

## Contexto

A spec pede `kafcli offsets set` e `kafcli offsets restore` (§6.6), e `kafcli poison find` e `kafcli poison skip` (§6.7). `offsets set`, `offsets restore` e `poison skip` são do nível Destrutiva (§9): dry-run por padrão, `--execute` para aplicar e, em contexto `protected`, o nome do grupo digitado. `poison find` é Leitura. O §4 diz que uma operação destrutiva sem dry-run ou confirmação é falha mesmo funcionando. Os critérios do §10 que este plano cobre:

- `offsets set` sem `--execute` não altera nada;
- com `--execute` gera backup, e `restore` volta ao estado anterior;
- `poison skip` copia para o DLT com os headers `kafcli-original-*`, e o consumer volta a avançar;
- operação destrutiva em `protected` exige confirmação digitada.

O ADR 0001 continua provisório. Três linhas dele importam aqui: `OffsetsForTimes` e `OffsetCommit` sem membro ativo foram vistos só no emulador, e "Commit recusado com membro ativo no grupo" está "não verificado", porque o emulador **aceitou** esse commit. O ADR 0006 define o contrato `partitions` do `offsets show`, que o backup reaproveita.

## Decisão

### Commit com `BaseConsumer`, sem `AdminClient`

- O rdkafka 0.39 não expõe `AlterConsumerGroupOffsets` nem `DeleteConsumerGroupOffsets`. O `KafkaOffsetCommitter` cria um `BaseConsumer` com `group.id` igual ao grupo alvo, faz `assign` das partições e chama `commit(lista, CommitMode::Sync)`. O consumer nunca faz `subscribe`, então nunca entra no grupo.
- É o único tipo do projeto, além do `capability_probe`, que chama `commit`.
- O commit síncrono pode bloquear além de qualquer timeout do librdkafka. Por isso roda numa thread, e quem chama espera no máximo `--timeout-seconds` com `recv_timeout`. No timeout o erro é `offset commit did not complete within <n>s; the outcome is unknown, check with 'kafcli offsets show'`. A thread pode sobreviver até o timeout do socket.
- Offset negativo é recusado antes de criar o consumer (`offset must be zero or positive`).
- Erros passam por `map_broker_operation_error` com a nova operação `OffsetCommit`. `UnknownMemberId`, `IllegalGeneration` e `RebalanceInProgress` viram `CommitRefusedActiveGroup`: `the broker refused the offset commit: consumer group '<g>' has active members`. `OffsetsForTimes` também entrou na matriz, `EmulatorOnly` no Event Hubs e `Documented` em Kafka genérico.

### Estado do grupo e regra de liberação

`read_group_activity` descreve o grupo uma vez e devolve um de quatro estados:

| Estado | Kafka genérico | Event Hubs |
|---|---|---|
| `Empty` (descrito, sem membros) | libera | libera |
| `Active { members }` | nunca libera | nunca libera |
| `NotDescribed` (fora da resposta, ou `GroupIdNotFound`) | libera | só com `--group-stopped` |
| `DescribeFailed { error }` | só com `--group-stopped` | só com `--group-stopped` |

- Em Kafka genérico, `DescribeGroups` é `Documented` e o broker recusa commit de grupo com membros. Então um grupo que o broker não descreve está, de fato, sem membros.
- No Event Hubs, `DescribeGroups` é "não verificado", e o emulador respondeu "grupo não existe" para um grupo que tinha offsets. Uma lista vazia não prova nada, e o serviço pode aceitar o commit com membros. `--group-stopped` é a declaração do operador de que os consumers estão parados.
- `--group-stopped` não vale para `Active`: quando o broker mostra membros, não há o que declarar.
- `--wait-empty <DURATION>` (1 s a 3600 s, mesmo parser do `--watch`) lê o grupo a cada 2 s até ele não estar `Active` ou o prazo acabar. A espera faz `poll` no consumer de metadata, sem `sleep`, para continuar servindo a renovação do token OAuth. No prazo, o erro é `consumer group '<g>' still has <n> active member(s) after waiting <s>s`.
- `GroupNotEmpty`, `GroupStillActive` e `GroupStateUnknown` saem com exit 1. O exit 3 continua só para recusa do guard ou do usuário.

### Ordem com `--execute`

1. checagem do grupo, depois de `--wait-empty` quando dado;
2. `enforce_guard` (nome do grupo digitado em `protected`);
3. nova checagem do grupo, sem espera;
4. backup;
5. escrita: produce da cópia e depois commit, no `poison skip`; só commit nos outros;
6. releitura dos offsets commitados, que precisam ser os do plano;
7. audit `executed`.

- A segunda checagem cobre o consumer que sobe enquanto o operador digita o nome do grupo. Ela não fecha a corrida: um consumer pode entrar entre o passo 3 e o commit. Em Kafka genérico o broker recusa, e o erro vira `CommitRefusedActiveGroup` com o caminho do backup. No Event Hubs o commit passa, e o resultado traz o aviso `Azure Event Hubs does not refuse commits while a group has members (ADR 0001); kafcli checked the group just before committing`.
- Depois do passo 3 e antes do backup, os offsets commitados das partições do plano são lidos de novo. Um consumer que sai durante o `--wait-empty`, ou enquanto o operador digita o nome do grupo, costuma commitar a posição no shutdown. Nesse caso o plano e o backup descreveriam um estado que já não existe, e o `poison skip` rebobinaria o grupo. Por isso o comando para com `committed offsets of group '<g>' changed after the plan was made (<t>/<p>: <antes> -> <agora>); run the command again to see the new plan` (exit 3, audit `aborted`).
- Falha nos passos 1 a 3 grava audit `aborted`. Falha em 4, 5 ou 6 grava `failed`. O erro de 5 e 6 mantém a variante e ganha `; offsets before the change are in <backup>` (`CoreError::with_context`). Variantes sem texto livre viram `Kafka` com o texto completo, que também sai com exit 1.
- Um plano sem nada a mudar não escreve backup nem commita. Sai com o aviso `nothing to commit: every partition is already at its target offset` e audit `executed` com `after: []`.
- Sem `--execute`, nada vai ao cluster e o backup não é escrito. Só o audit `dry-run` é gravado. Com o grupo ativo, o dry-run avisa que o `--execute` vai recusar.

### Alvo do reset

- `--to-offset`, `--to-earliest`, `--to-latest`, `--to-datetime` e `--shift-by`, exatamente um. `--to-datetime` exige o fuso (RFC 3339). `--shift-by` aceita número negativo.
- Todo alvo é limitado a `[log_start, log_end]`. Um alvo limitado sai com `clamped: true`, `*` depois do `NEW` na tabela e o aviso `target offset of <t>/<p> was limited to <offset> (log start <low>, log end <high>)`. Isso cobre a retenção que apagou mensagens depois do backup.
- `--to-datetime` depois da última mensagem vai para o `log_end`, como o `kafka-consumer-groups.sh`. `OffsetsForTimes` é pedido uma vez por comando.
- `--shift-by` numa partição sem commit é `partition <t>/<p> has no committed offset; --shift-by needs one`. A soma usa `checked_add` e, no estouro, vai para o limite.
- Mensagens puladas e reprocessadas vêm de `max(0, new - current)` e `max(0, current - new)`, e ficam `null` numa partição sem commit.

### Backup

- Em `<state dir>/offsets-backup/<grupo>-<YYYYMMDDTHHMMSS.mmmZ>.json`. O grupo é reduzido a `[A-Za-z0-9._-]` e a 100 bytes. Um nome já usado recebe `-1` a `-9`, e o arquivo é criado com `create_new`, nunca sobrescrito.
- Diretório `0700` e arquivo `0600` em unix. `sync_all` antes de seguir. Backup que não pode ser escrito aborta antes do commit.
- Formato: `kafcli_offsets_backup: 1`, `created_at`, `context`, `group`, `command` e `partitions` com os campos do `offsets show` (ADR 0006), com o estado antes da mudança. Campos desconhecidos são aceitos na leitura, porque o contrato só cresce.
- A leitura recusa arquivo acima de 16 MiB (pelo tamanho e com `take` como segunda barreira), JSON inválido (o erro do serde não carrega o conteúdo), versão diferente de 1 e `partitions` vazio.

### `offsets restore`

- Aceita só o arquivo de backup. Um backup de outro contexto é recusado antes de conectar: `backup was taken on context '<a>', not on '<b>'`.
- Cada partição com commit no backup volta para ele, pelo mesmo limite do `set`. Um tópico que sumiu é `TopicNotFound`, e uma partição que sumiu é `InvalidInput`.
- Uma partição sem commit no backup fica como está, com aviso. O rdkafka não expõe `DeleteConsumerGroupOffsets`, então o restore não "descommita".
- O restore passa pelo mesmo fluxo e, portanto, escreve o próprio backup antes de aplicar. Ele pode ser desfeito do mesmo jeito.

### `poison find`

- Lê o lag do grupo no tópico e, para cada partição com lag, a mensagem no offset commitado. As leituras saem de um único consumer com `group.id` descartável (`kafcli-poison-<millis>`), `auto.offset.reset=error`, `assign` e `pause` da partição depois da primeira mensagem. Nunca `subscribe`, nunca `commit`.
- O veredito usa o valor inteiro: `Decodable` ou `Undecodable` com o erro do decoder. Tombstone é `Decodable`. `--max-bytes` e `--redact` limitam só o envelope mostrado.
- `BelowLogStart` quando o commit está abaixo do `log_start` (o grupo vai cair no `auto.offset.reset`), sem leitura. `NoMessage` quando nada chega dentro do timeout. Partição sem commit fica fora, com aviso. Uma mensagem em offset maior que o commitado (lacuna por compactação ou transação) gera aviso.
- Sai com exit 0 mesmo com candidatos `undecodable`.

### `poison skip`

- Antes de qualquer conexão: `--reason` não vazio e com até 1024 bytes, `--dlt` diferente de `--topic`, `--offset` não negativo.
- Antes de qualquer escrita: o offset pedido precisa ser o commitado (`run 'kafcli poison find' first`), a mensagem precisa existir nele, e o DLT precisa existir.
- A cópia mantém key, valor (tombstone continua tombstone) e headers, na ordem, menos os que já começam com `kafcli-original-`. Depois vêm `kafcli-original-topic`, `-partition`, `-offset`, `-reason`, `-operator` e `-timestamp`, este com o timestamp da mensagem original e só quando ela tem um.
- O produce vem antes do commit de `offset + 1`. Se o commit (ou a releitura) falhar depois do produce, o erro diz onde a cópia está e que rodar de novo duplica a cópia. Se o produce falhar, nada é commitado.

### Audit log

- `before` e `after` guardam só tópico, partição e offset, mais o arquivo de backup e, no `poison skip`, o DLT, a partição e o offset da cópia e o motivo. Nunca key, valor nem header de mensagem.
- Falha ao gravar o audit é aviso no stderr (`warning: audit log not written: <erro>`) e nunca falha o comando, como no `schema register`.

### Pendências do plano 04 corrigidas

- Erro por partição do `OffsetFetch` agora é erro, e não "sem commit". Sem isso, o backup gravaria `null` onde havia offset.
- No `lag --all-groups`, um grupo cujo lag não pode ser lido sai do relatório, entra em `unreadable_groups` e gera aviso. Com `--max-lag`, um relatório sem grupo acima do limite mas com grupos ilegíveis sai com `IncompleteLimitCheck` (exit 1). Com `--group`, o erro continua sendo devolvido.
- No `groups audit`, erro de lag de um grupo vazio vira aviso, e o grupo segue sem R3.

## Consequências

- A rodada real do roteiro do Event Hubs (`docs/spike/0001-event-hubs-runbook.md`, passo 4.1) continua pendente. Até lá, no Event Hubs, um grupo que o `DescribeGroups` não acha exige `--group-stopped`, e o kafcli não conta com a recusa do serviço para grupo ativo.
- Grupos do KIP-848 aparecem pelo `DescribeGroups` clássico sem membros decodificados. Para eles, a proteção depende da recusa do broker no commit.
- O restore não apaga commit. Uma partição que não tinha commit antes do `set` fica no offset novo.
- Os testes automatizados usam fakes para leitor, committer, leitor de mensagem e publisher. A fixture `kafka-plaintext` cobre dry-run, `--execute`, grupo ativo, `--wait-empty`, restore, `poison find` e `poison skip` contra `kafka-consumer-groups.sh`.

## Fora do escopo

- `poison find --observe` (v1.x, spec §6.7).
- Apagar o offset commitado de uma partição (`DeleteConsumerGroupOffsets`).
- `restore` a partir da saída de `offsets show --output json`.
- Distribuição, completions e README com permissões mínimas (plano 06).
