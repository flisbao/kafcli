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

Crie o diretório de resultados antes de começar:

```bash
mkdir -p /tmp/kafcli-spike-results
```

## 1. Custo estimado e limpeza

- O Standard cobra por hora por unidade de throughput (TU) e tem cobrança adicional por
  evento de ingresso e captura. O Premium cobra por hora por unidade de processamento (PU)
  e custa bem mais. Preços mudam com o tempo; consulte a tabela oficial antes de começar:
  <https://azure.microsoft.com/en-us/pricing/details/event-hubs/>.
- O spike, usando só Standard, deve custar pouco se for apagado no mesmo dia. Deixar o
  namespace ligado gera cobrança por hora mesmo sem tráfego.
- **Apague o resource group no fim** (passo 10). Ele remove o namespace, o event hub e o
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
(`az eventhubs namespace create`; valores aceitos de `--sku`: Basic, Premium, Standard; de
`--public-network-access`: Disabled, Enabled).

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

Substitua `<ns>` pelo nome do namespace, sem `.servicebus.windows.net`. O endpoint Kafka do
Event Hubs é `<namespace>.servicebus.windows.net:9093`, e o nome do event hub é o tópico Kafka
(fonte: <https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview>).

```bash
cargo run -q -p kafcli -- context add eh-std \
  --brokers <ns>.servicebus.windows.net:9093 \
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
  --namespace "<ns>.servicebus.windows.net" \
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

## 7. Opcional: mesmo fluxo no Premium

O Premium é opcional e mais caro (cobrança por PU, por hora). Só faça se precisar comparar
com o Standard. Use outros nomes para não misturar resultados:

```bash
export KAFCLI_NS_PREM="kafcli-eh-prem-<sufixo>"

az eventhubs namespace create \
  --resource-group "$KAFCLI_SPIKE_RG" \
  --name "$KAFCLI_NS_PREM" \
  --location "$KAFCLI_LOC" \
  --sku Premium \
  --capacity 1 \
  --enable-kafka true \
  --public-network-access Enabled
```

Fontes: `--sku Premium` e `--capacity` (unidades Premium, de 0 a 10) em
<https://learn.microsoft.com/en-us/cli/azure/eventhubs/namespace>. Repita para o Premium os
comandos de event hub, schema group, papéis RBAC e connection string do passo 2, trocando
`$KAFCLI_NS_STD` por `$KAFCLI_NS_PREM` e `KAFCLI_EH_STD_CONNECTION` por
`KAFCLI_EH_PREM_CONNECTION`. Depois crie o contexto `eh-prem` com
`--secret-ref env:KAFCLI_EH_PREM_CONNECTION` e rode os passos 3 a 6 com `--context eh-prem`,
gravando em `eh-prem-probe.json`, `eh-prem-dump.json` e `azure-sr-writer-premium.txt`.

## 8. Emulador do Event Hubs (avaliação feita pelo autor)

Esta etapa é sua. Ela não faz parte do que foi executado para este roteiro. Use a
documentação oficial, que descreve o emulador como ferramenta local de desenvolvimento e
teste, e não para produção:
<https://learn.microsoft.com/en-us/azure/event-hubs/overview-emulator>.

**Licença.** O emulador é distribuído sob os termos de licença da Microsoft. Para iniciá-lo
é preciso aceitar esses termos definindo `ACCEPT_EULA=Y`. Leia o contrato antes:
<https://github.com/Azure/azure-event-hubs-emulator-installer/blob/main/EMULATOR_EULA.md>.

**Subida com Docker Compose.** Os passos e o arquivo `docker-compose.yaml` estão em
<https://learn.microsoft.com/en-us/azure/event-hubs/test-locally-with-event-hub-emulator>.
Em resumo, o `.env` precisa de `CONFIG_PATH` (caminho do `config.json`) e de
`ACCEPT_EULA="Y"` (só depois de ler o EULA). Depois:

```bash
docker compose -f <caminho-do-docker-compose.yaml> up -d
```

O guia mostra que o Kafka do emulador usa a porta `9092` (mapeamento `9092:9092`) e exige
`SecurityProtocol = SaslPlaintext` com `SaslMechanism = Plain` e usuário `$ConnectionString`.

**O que o emulador não cobre.** Pela mesma documentação, o emulador não tem integração com
o Schema Registry. Portanto o escritor do passo 5 não roda contra ele. Também não há
autenticação Microsoft Entra ID, então o `DefaultAzureCredential` não funciona nele.

**Probe contra o emulador.** Se quiser testar só o probe:

1. Defina a connection string do emulador numa variável de ambiente (por exemplo
   `KAFCLI_EH_EMU_CONNECTION`). O guia mostra o formato
   `Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<chave>;UseDevelopmentEmulator=true;`.
   O guia usa `SAS_KEY_VALUE` como marcador; confirme a chave na configuração do seu emulador.
2. Crie um contexto com `--brokers localhost:9092` e
   `--secret-ref env:KAFCLI_EH_EMU_CONNECTION`.
3. Rode o probe com `--topic eh1` (o nome do event hub do `config.json` de exemplo do guia).

**Aviso importante:** o `azure-connection-string` do kafcli sempre usa `SASL_SSL`, e o
emulador aceita só `SASL_PLAINTEXT`. Por isso, com o kafcli atual, a conexão tende a falhar
no handshake TLS. Registre o erro como ele aparecer, em
`/tmp/kafcli-spike-results/emulator-probe-notes.txt`. Corrigir isso exige mudança no código
do kafcli, que está fora deste roteiro.

Para registrar a avaliação, anote no mesmo arquivo: versão da imagem, se o compose subiu, se
o probe conectou e o erro exato.

## 9. O que devolver para avaliação

Devolva a pasta `/tmp/kafcli-spike-results/` inteira. Antes de enviar, confira que ela não tem
connection string:

```bash
grep -ri "SharedAccessKey" /tmp/kafcli-spike-results/
grep -ri "Endpoint=sb://" /tmp/kafcli-spike-results/
```

Os dois comandos devem não imprimir nada. Se imprimirem algo, remova a linha antes de enviar.

## 10. Limpeza

Apague o resource group, o que remove todos os recursos do spike (Standard e, se criado,
Premium):

```bash
az group delete --name "$KAFCLI_SPIKE_RG" --yes
unset KAFCLI_EH_STD_CONNECTION KAFCLI_EH_PREM_CONNECTION
```

Fonte: <https://learn.microsoft.com/en-us/cli/azure/group> (`az group delete --name`,
`--yes` para não pedir confirmação; `--no-wait` é opcional).
