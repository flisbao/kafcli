# Roteiro do spike Event Hubs (kafcli)

Este roteiro cria recursos reais no Azure para verificar, com o kafcli, o que o Event Hubs
aceita pelo protocolo Kafka e como o Azure Schema Registry codifica eventos Avro. Ele
assume um namespace novo com acesso de rede público (sem private endpoint nem VNet).

Resultados vão para `/tmp/kafcli-spike-results`, fora do repositório. Este diretório é o que
"`results/`" significa nos passos abaixo.

## Pré-requisitos

- Rust/cargo e o repositório `kcli` na branch de trabalho.
- Python 3 com `venv`.
- Azure CLI instalado (`az`). Guia de instalação: <https://learn.microsoft.com/en-us/cli/azure/install-azure-cli>.
- Uma assinatura Azure em que você possa criar recursos e atribuir papéis (RBAC) no namespace.
- Rode todos os comandos a partir da raiz do repositório `kcli`. Os caminhos relativos, como
  `spike/eventhubs/...`, dependem disso.

Crie o diretório de resultados antes de começar:

```bash
mkdir -p /tmp/kafcli-spike-results
```

## 1. Custo estimado e limpeza

- O Standard cobra por hora por unidade de throughput (TU) e tem cobrança adicional por
  evento de ingresso e captura. Preços mudam com o tempo; consulte a tabela oficial antes de
  começar:
  <https://azure.microsoft.com/en-us/pricing/details/event-hubs/>.
- O spike, usando só Standard, deve custar pouco se for apagado no mesmo dia. Deixar o
  namespace ligado gera cobrança por hora mesmo sem tráfego.
- **Apague o resource group no fim** (passo 9). Ele remove o namespace, o event hub e o
  schema group de uma vez.

## 2. Recursos no Azure (Standard)

Antes de começar, faça login e escolha a assinatura. Substitua `<sufixo>` por algo único,
porque o nome do namespace é global no Azure:

```bash
az login
az account set -s "<nome-ou-ID-da-assinatura>"

export KAFCLI_SPIKE_RG="rg-kafcli-spike"
export KAFCLI_LOC="brazilsouth"
export KAFCLI_NS_STD="kafcli-eh-std-<sufixo>"
```

Fontes para os comandos desta seção: `az login` em
<https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli-interactively>;
`az account set -s` em <https://learn.microsoft.com/en-us/cli/azure/eventhubs/namespace>
(opção `--subscription`, "az account set -s NAME_OR_ID").

**Resource group**

```bash
az group create --name "$KAFCLI_SPIKE_RG" --location "$KAFCLI_LOC"
```

Fonte: <https://learn.microsoft.com/en-us/cli/azure/group>. Os valores de `--location` vêm de
`az account list-locations`.

**Namespace Standard** (acesso público, Kafka habilitado)

```bash
az eventhubs namespace create \
  --resource-group "$KAFCLI_SPIKE_RG" \
  --name "$KAFCLI_NS_STD" \
  --location "$KAFCLI_LOC" \
  --sku Standard \
  --enable-kafka true \
  --public-network-access Enabled
```

Fonte: <https://learn.microsoft.com/en-us/cli/azure/eventhubs/namespace>
(`az eventhubs namespace create`; de `--public-network-access`: Disabled, Enabled).

**Event hub `kafcli-probe` com 2 partições** (é o tópico Kafka)

```bash
az eventhubs eventhub create \
  --resource-group "$KAFCLI_SPIKE_RG" \
  --namespace-name "$KAFCLI_NS_STD" \
  --name kafcli-probe \
  --partition-count 2
```

Fonte: <https://learn.microsoft.com/en-us/cli/azure/eventhubs/eventhub>
(`az eventhubs eventhub create`; `--partition-count` aceita de 2 a 32 por padrão).

**Schema group `kafcli-spike` em Avro**

```bash
az eventhubs namespace schema-registry create \
  --resource-group "$KAFCLI_SPIKE_RG" \
  --namespace-name "$KAFCLI_NS_STD" \
  --name kafcli-spike \
  --schema-type Avro \
  --schema-compatibility Backward
```

Fonte: <https://learn.microsoft.com/en-us/cli/azure/eventhubs/namespace/schema-registry>
(`--name` é alias de `--schema-group-name`; valores de `--schema-type`: Avro, Json, ProtoBuf,
Unknown; de `--schema-compatibility`: Backward, Forward, None). A escolha `Backward` é minha,
não vem de um requisito do spike.

**Papéis RBAC para o usuário logado**

Dê ao usuário os dois papéis que o script precisa: `Schema Registry Contributor` para
registrar o schema e `Azure Event Hubs Data Owner` para enviar eventos. O escopo é o próprio
namespace.

```bash
export NS_ID="$(az eventhubs namespace show --resource-group "$KAFCLI_SPIKE_RG" --name "$KAFCLI_NS_STD" --query id -o tsv)"
export USER_OID="$(az ad signed-in-user show --query id -o tsv)"

az role assignment create \
  --assignee-object-id "$USER_OID" \
  --assignee-principal-type User \
  --role "Schema Registry Contributor" \
  --scope "$NS_ID"

az role assignment create \
  --assignee-object-id "$USER_OID" \
  --assignee-principal-type User \
  --role "Azure Event Hubs Data Owner" \
  --scope "$NS_ID"
```

Fontes: `az role assignment create` em <https://learn.microsoft.com/en-us/cli/azure/role/assignment>
(`--role`, `--scope`, `--assignee-object-id`, `--assignee-principal-type`; o
`--assignee-object-id` evita a consulta ao Microsoft Graph); nomes dos papéis em
<https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/analytics>
("Azure Event Hubs Data Owner" e "Schema Registry Contributor"); `az ad signed-in-user show`
em <https://learn.microsoft.com/en-us/cli/azure/ad/signed-in-user>. Os papéis podem levar
alguns minutos para valer; se o passo 5 falhar com erro de autorização, espere e tente de novo.

**Connection string em variável de ambiente (nunca em arquivo)**

```bash
export KAFCLI_EH_STD_CONNECTION="$(az eventhubs namespace authorization-rule keys list \
  --resource-group "$KAFCLI_SPIKE_RG" \
  --namespace-name "$KAFCLI_NS_STD" \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv)"
```

Fontes: <https://learn.microsoft.com/en-us/cli/azure/eventhubs/namespace/authorization-rule/keys>
(`--name` é o nome da regra; `keys list` devolve as connection strings do namespace).
`RootManageSharedAccessKey` é a regra padrão; a página do protocolo Kafka mostra essa regra
em `SharedAccessKeyName=RootManageSharedAccessKey`:
<https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview>.

A connection string fica só na variável de ambiente. Não a imprima, não a cole em arquivo e
não a coloque em comando com `echo`.

## 3. Contexto no kafcli

A variável `$KAFCLI_NS_STD` já tem o nome do namespace, sem `.servicebus.windows.net`. O endpoint
Kafka do Event Hubs é `<namespace>.servicebus.windows.net:9093`, e o nome do event hub é o tópico
Kafka (fonte: <https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview>).

```bash
cargo run -q -p kafcli -- context add eh-std \
  --brokers "$KAFCLI_NS_STD.servicebus.windows.net:9093" \
  --auth-type azure-connection-string \
  --secret-ref env:KAFCLI_EH_STD_CONNECTION

cargo run -q -p kafcli -- context test eh-std
```

Não passe `--protected` neste contexto de spike.

## 4. Probe de capacidades

Grava 3 mensagens na partição 0 e lê offsets em grupos com prefixo `kafcli-probe-`. Não cria
tópicos, então o event hub precisa existir (passo 2).

```bash
cargo run -q -p kafcli --example capability_probe -- \
  --context eh-std \
  --topic kafcli-probe \
  --output json > /tmp/kafcli-spike-results/eh-std-probe.json
```

Esta nota vale para todas as saídas de probe e dump deste roteiro: se o comando imprimir
`error:`, apague o arquivo de saída, que fica vazio ou inválido, e registre o erro exato num
arquivo de notas em `/tmp/kafcli-spike-results/`.

## 4.1 Offsets e commit (plano 05)

Confirma as três linhas do commit no ADR 0001: `OffsetsForTimes`, `OffsetCommit` sem membro ativo e commit com membro ativo. Usa o event hub `kafcli-probe` do passo 4, que já tem mensagens, e um grupo só de teste. O backup e o audit log vão para o diretório de estado do kafcli; o comando aponta o caminho do backup.

Primeiro, deixe o grupo parado com offsets commitados (o `consume` com `--group` commita ao sair):

```bash
cargo run -q -p kafcli -- --context eh-std consume --topic kafcli-probe \
  --group kafcli-reset-test --count 3 --output json > /dev/null
cargo run -q -p kafcli -- --context eh-std offsets show --group kafcli-reset-test \
  --output json > /tmp/kafcli-spike-results/eh-std-offsets-before.json
```

1. `OffsetsForTimes`, em dry-run (nada é commitado):

   ```bash
   cargo run -q -p kafcli -- --context eh-std offsets set --group kafcli-reset-test \
     --topic kafcli-probe --to-datetime "$(date -u -d '-10 minutes' +%Y-%m-%dT%H:%M:%SZ)" \
     --output json > /tmp/kafcli-spike-results/eh-std-offsets-for-times.json
   ```

   Um erro `OffsetsForTimes is not supported by this broker (Azure Event Hubs)` muda a linha para "não suportado".

2. `OffsetCommit` sem membro ativo, seguido do restore. `--group-stopped` é necessário quando o Event Hubs não descreve o grupo:

   ```bash
   cargo run -q -p kafcli -- --context eh-std offsets set --group kafcli-reset-test \
     --topic kafcli-probe --to-earliest --execute --group-stopped \
     --output json > /tmp/kafcli-spike-results/eh-std-offsets-set.json
   cargo run -q -p kafcli -- --context eh-std offsets show --group kafcli-reset-test \
     --output json > /tmp/kafcli-spike-results/eh-std-offsets-after-set.json
   cargo run -q -p kafcli -- --context eh-std offsets restore --execute --group-stopped \
     --file "$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["backup_file"])' < /tmp/kafcli-spike-results/eh-std-offsets-set.json)" \
     --output json > /tmp/kafcli-spike-results/eh-std-offsets-restore.json
   cargo run -q -p kafcli -- --context eh-std offsets show --group kafcli-reset-test \
     --output json > /tmp/kafcli-spike-results/eh-std-offsets-after-restore.json
   ```

   O `after-set` deve mostrar os offsets no início do log, e o `after-restore` deve bater com o `before`.

3. Commit com membro ativo. Deixe um consumer do grupo de pé num segundo terminal e rode o mesmo `set`:

   ```bash
   # terminal 2
   cargo run -q -p kafcli -- --context eh-std consume --topic kafcli-probe \
     --group kafcli-reset-test --follow > /dev/null
   # terminal 1, com o consumer de pé
   cargo run -q -p kafcli -- --context eh-std offsets set --group kafcli-reset-test \
     --topic kafcli-probe --to-earliest --execute --group-stopped \
     --output json > /tmp/kafcli-spike-results/eh-std-offsets-active.json 2>&1
   ```

   Registre se o kafcli recusou pelo `DescribeGroups` (`active member(s)`), se o serviço recusou o commit (`the broker refused the offset commit`) ou se o commit passou. Pare o consumer do terminal 2 com Ctrl+C.

O resultado de cada passo troca a linha correspondente da matriz do ADR 0001 (`OffsetsForTimes`, `OffsetCommit` sem membro ativo e "Commit recusado com membro ativo no grupo").

## 5. Escritor Avro com o Azure Schema Registry

O script `spike/eventhubs/azure_schema_registry_writer.py` registra o schema fixo
`kafcli.spike.ProbeEvent` no grupo `kafcli-spike`, codifica eventos com `AvroEncoder` e envia
com `EventHubProducerClient`. Ele autentica com `DefaultAzureCredential`, então rode
`az login` antes (passo 2). Ele não aceita segredo por argumento.

```bash
python3 -m venv "$HOME/kafcli-spike-venv"
. "$HOME/kafcli-spike-venv/bin/activate"
pip install -r spike/eventhubs/requirements.txt

python spike/eventhubs/azure_schema_registry_writer.py \
  --namespace "$KAFCLI_NS_STD.servicebus.windows.net" \
  --event-hub kafcli-probe \
  --schema-group kafcli-spike \
  --count 3 2>&1 | tee /tmp/kafcli-spike-results/azure-sr-writer.txt

deactivate
```

O script imprime, para cada evento, o `content_type` gerado (`avro/binary+<schema id>`) e o
schema id, para comparar com o dump Kafka. Para ver as opções, rode
`python spike/eventhubs/azure_schema_registry_writer.py --help`.

Os pacotes ficam fixados em `spike/eventhubs/requirements.txt`. Se o `pip install` falhar com
erro de autenticação do Azure, o problema é o login do passo 2, não a instalação.

## 6. Dump das mensagens

Lê até 20 mensagens do tópico. Só lê: não grava nem faz commit de offset.

```bash
cargo run -q -p kafcli --example capability_probe -- \
  --context eh-std \
  --topic kafcli-probe \
  --dump 20 \
  --output json > /tmp/kafcli-spike-results/eh-std-dump.json
```

**Nota opcional (produtores Java):** se os produtores reais usarem o serializador Kafka Java
do Azure Schema Registry (`azure-schemaregistry-kafka-avro`), o formato pode ser diferente:
o schema id pode ir em header, e não como prefixo do payload, como no script Python. Se você
tiver uma mensagem produzida assim, faça um dump dela e salve em
`/tmp/kafcli-spike-results/` (por exemplo `java-producer-dump.json`).

## 7. Emulador do Event Hubs (fora deste roteiro)

O emulador oficial foi avaliado em `tests/fixtures/eventhubs-emulator/`. O README dessa pasta
tem o passo a passo, a tabela de resultado do probe e o veredito. Este roteiro não o usa: ele
cobre só o Standard na nuvem.

Para subir o emulador é preciso aceitar a licença com `ACCEPT_EULA=Y` na linha de comando.
Leia o EULA antes: <https://github.com/Azure/azure-event-hubs-emulator-installer/blob/main/EMULATOR_EULA.md>.
O escritor do passo 5 não roda contra o emulador, porque ele não tem Schema Registry.

## 8. O que devolver para avaliação

Devolva a pasta `/tmp/kafcli-spike-results/` inteira. Antes de enviar, confira que ela não tem
connection string:

```bash
grep -ri "SharedAccessKey" /tmp/kafcli-spike-results/
grep -ri "Endpoint=sb://" /tmp/kafcli-spike-results/
```

Os dois comandos devem não imprimir nada. Se imprimirem algo, remova a linha antes de enviar.

## 9. Limpeza

Apague o resource group, o que remove todos os recursos do spike:

```bash
az group delete --name "$KAFCLI_SPIKE_RG" --yes
unset KAFCLI_EH_STD_CONNECTION KAFCLI_EH_EMU_CONNECTION
rm -rf "$HOME/kafcli-spike-venv"
```

Se o emulador ainda estiver no ar (seção 7), derrube-o com o compose da pasta de fixtures. O
compose interpola `ACCEPT_EULA` até no `down`, então a variável precisa estar definida; o valor
`N` basta, porque o `down` não inicia o emulador:

```bash
ACCEPT_EULA=N docker compose -f tests/fixtures/eventhubs-emulator/compose.yaml down -v
```

Os contextos `eh-std` e, se você os criou no arquivo de configuração padrão, `eh-emu`
continuam nesse arquivo, apontando para hosts que já foram apagados ou que param de existir.
O CLI não tem `context remove`, então remova-os editando o `config.json` no diretório de
configuração do kafcli (ou no caminho definido em `KAFCLI_CONFIG`).

Fonte: <https://learn.microsoft.com/en-us/cli/azure/group> (`az group delete --name`,
`--yes` para não pedir confirmação; `--no-wait` é opcional).
