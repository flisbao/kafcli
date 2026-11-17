# Avaliação do emulador do Azure Event Hubs

Esta pasta sobe o emulador oficial do Event Hubs, com o Azurite que ele exige, e roda o probe de
capacidades do kafcli contra o endpoint Kafka dele. O resultado está em `probe-result.json`.

Veredito: serve para testes de integração de produção e consumo com headers, offsets (list
offsets, offsets for times, commit e fetch de offsets fora de grupo) e metadados de cluster;
não serve para validar grupos de consumo (describe/list de grupos e rejeição de commit com
membro ativo).

## Fontes e data

Consultado em 2026-11-16.

- Guia oficial: <https://learn.microsoft.com/en-us/azure/event-hubs/test-locally-with-event-hub-emulator>
  (página marcada como atualizada em 2026-08-26). Confirma o endpoint Kafka na porta `9092`,
  `SecurityProtocol` `SaslPlaintext`, `SaslMechanism` `Plain`, usuário `$ConnectionString`, o
  formato de `config.json` e a connection string com `UseDevelopmentEmulator=true`. Diz também
  que, com Kafka, só as APIs de produtor e consumidor são compatíveis.
- Instalador e README: <https://github.com/Azure/azure-event-hubs-emulator-installer>. Confirma
  que o `$Default` consumer group é criado automaticamente e não pode ser criado pelo
  `config.json`.
- EULA: <https://github.com/Azure/azure-event-hubs-emulator-installer/blob/main/EMULATOR_EULA.md>.
  O emulador é para desenvolvimento e teste, sem SLA e sem suporte oficial.
- Tags: <https://mcr.microsoft.com/v2/azure-messaging/eventhubs-emulator/tags/list> e
  <https://mcr.microsoft.com/v2/azure-storage/azurite/tags/list>.

## Imagens fixadas

| Serviço | Imagem | Motivo da tag |
|---|---|---|
| `emulator` | `mcr.microsoft.com/azure-messaging/eventhubs-emulator:2.2.1` | Maior versão publicada na lista de tags em 2026-11-16. O guia usa `latest`, que não foi fixado. |
| `azurite` | `mcr.microsoft.com/azure-storage/azurite:3.37.0` | Maior versão estável na lista de tags em 2026-11-16. |

## Como subir

A licença só é aceita na linha de comando. O compose não tem valor padrão para `ACCEPT_EULA`,
então sem ele o `docker compose` para com erro. Leia o EULA antes
(<https://github.com/Azure/azure-event-hubs-emulator-installer/blob/main/EMULATOR_EULA.md>).

A partir da raiz do repositório:

```bash
ACCEPT_EULA=Y docker compose -f tests/fixtures/eventhubs-emulator/compose.yaml up -d --wait
```

As portas ficam presas em `127.0.0.1`: Kafka em `9092`, Azurite em `10000`, `10001` e `10002`.
O `config.json` declara o namespace `emulatorNs1` e o event hub `kafcli-probe` com 2
partições. O `$Default` consumer group é criado pelo próprio emulador (o log do container
mostra `"ConsumerGroups":[{"Name":"$default"}]`).

Na primeira subida, o log do emulador mostra algumas falhas de health check do armazenamento
de metadados antes de ficar pronto. O `--wait` só termina quando o container está saudável.

## Como rodar o probe

A connection string do emulador é a documentada pelo guia, com a chave de placeholder
`SAS_KEY_VALUE`. Ela vai só para a variável de ambiente, nunca para arquivo versionado. O
contexto usa a configuração temporária, fora do repositório:

```bash
export KAFCLI_CONFIG="$(mktemp -d)/config.json"
export KAFCLI_EH_EMU_CONNECTION='Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;'

cargo run -q -p kafcli -- context add eh-emu \
  --brokers 127.0.0.1:9092 \
  --auth-type sasl-plain \
  --username '$ConnectionString' \
  --secret-ref env:KAFCLI_EH_EMU_CONNECTION \
  --no-tls

cargo run -q -p kafcli -- context test eh-emu

cargo run -q -p kafcli --example capability_probe -- \
  --context eh-emu \
  --topic kafcli-probe \
  --output json > tests/fixtures/eventhubs-emulator/probe-result.json
```

O contexto `azure-connection-string` não serve aqui: ele força TLS, e o emulador só aceita
`SASL_PLAINTEXT`. Por isso o contexto usa `sasl-plain` com `--no-tls`.

## Resultado do probe

Execução de 2026-11-16 contra a imagem `eventhubs-emulator:2.2.1`, com o tópico `kafcli-probe`.
A saída bruta está em `probe-result.json`.

| Capacidade | Resultado | Detalhe |
|---|---|---|
| `cluster_metadata` | supported | 1 broker, 2 partições |
| `idempotent_produce` | supported | 3 mensagens confirmadas na partição 0 |
| `headers_round_trip` | supported | 3 mensagens lidas com o header da execução |
| `list_offsets` | supported | low=0 high=3 |
| `offsets_for_times` | supported | offset 0 |
| `commit_offsets_outside_group` | supported | offset commitado 1 |
| `fetch_committed_offsets` | supported | offset commitado 1 |
| `list_groups` | supported | grupo da probe listado: não |
| `describe_group` | failed | `probe group not found` |
| `commit_rejected_while_group_active` | unsupported | commit aceito com membro ativo |

Leitura dos resultados. O probe só faz `assign` e commit de offset fora do grupo, sem que um
membro entre no grupo. Por isso o grupo da probe não existe como objeto no emulador, e
`list_groups` e `describe_group` não o encontram. Isso é consistente com a documentação, que
diz que só as APIs de produtor e consumidor são compatíveis. Esses dois resultados dizem
respeito à API de administração de grupos do emulador, não ao caminho de produção e consumo
do kafcli. A rejeição de commit com membro ativo não foi reproduzida: o emulador aceitou o
commit. O comportamento do Event Hubs real nessa situação não foi verificado aqui; ele fica para
o roteiro do Standard (`docs/spike/0001-event-hubs-runbook.md`).

## Limitações

- O emulador não tem integração com o Schema Registry, então o escritor Avro do roteiro não
  roda contra ele.
- Não há autenticação Microsoft Entra ID, então `DefaultAzureCredential` não funciona nele.
- A Microsoft não oferece suporte oficial ao emulador. Ele é para desenvolvimento e teste.

## Como derrubar

```bash
ACCEPT_EULA=N docker compose -f tests/fixtures/eventhubs-emulator/compose.yaml down -v
```

O compose interpola `ACCEPT_EULA` em todos os comandos, inclusive no `down`, então a variável
precisa estar definida mesmo aqui. O valor não importa para o `down`, que não inicia o
emulador. O comando remove os containers e os volumes. Rode-o mesmo quando a avaliação falhar.
