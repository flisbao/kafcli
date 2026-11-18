# Roteiro manual de autenticação Azure (kafcli)

Este roteiro confere, contra um Entra ID e um Event Hubs reais, os modos Azure do kafcli (ADR 0005):
`azure-sp-secret`, `azure-sp-cert`, `azure-managed-identity`, `azure-workload-identity`, `azure-cli` e
`azure-default`. Os testes automatizados usam fakes locais do Entra ID; o que está aqui é o que eles
não cobrem. O namespace e o event hub são os do roteiro 0001 (`docs/spike/0001-event-hubs-runbook.md`),
e a limpeza no fim apaga o que for criado aqui.

Os comandos assumem a raiz do repositório `kcli` e o binário `kafcli` no `PATH`.

## Pré-requisitos gerais

- Namespace Event Hubs Standard com Kafka habilitado e o event hub `kafcli-probe` com 2 partições
  (passos 2 do roteiro 0001).
- Variáveis, substituindo os valores entre `<>`:

```bash
export KAFCLI_RG="rg-kafcli-spike"
export KAFCLI_NS="<namespace>.servicebus.windows.net:9093"
export KAFCLI_NS_NAME="<namespace>"
export KAFCLI_TOPIC="kafcli-probe"
export KAFCLI_TENANT="<tenant-id>"
```

- Papéis RBAC no namespace para a identidade que o modo usa (ver cada modo):
  `Azure Event Hubs Data Sender` para produzir e `Azure Event Hubs Data Receiver` para consumir.
- Para o passo de schema (opcional, no fim de cada modo): `Schema Registry Reader` para ler e
  `Schema Registry Contributor` para registrar, no schema group do registry.

Para atribuir um papel no namespace, use o id do recurso:

```bash
export KAFCLI_NS_ID="$(az eventhubs namespace show -g "$KAFCLI_RG" -n "$KAFCLI_NS_NAME" --query id -o tsv)"
az role assignment create --assignee "<principal>" --role "Azure Event Hubs Data Sender" --scope "$KAFCLI_NS_ID"
az role assignment create --assignee "<principal>" --role "Azure Event Hubs Data Receiver" --scope "$KAFCLI_NS_ID"
```

Em cada modo, execute nesta ordem. Os resultados esperados são a tabela de `context test` (com
`authenticated_principal`), o código de saída de `produce` e a mensagem que `consume` mostra:

```bash
kafcli context test --timeout-seconds 20
kafcli produce --topic "$KAFCLI_TOPIC" --value '{"n":1}'
kafcli consume --topic "$KAFCLI_TOPIC" --count 1
```

`consume` lê do início do tópico, então a primeira mensagem pode ser outra que já estava lá. O
resultado conta como correto quando a saída é uma mensagem JSON válida.

## 1. `azure-sp-secret` (service principal com client secret)

Pré-requisito: app registration no Entra ID com um client secret, e os papéis acima atribuídos à
service principal da app (`<principal>` é o `appId` da app).

```bash
az ad app credential reset --id "<app-id>" --append
# guarde o campo "password" da saída; ele é o client secret
export KAFCLI_SP_SECRET="<client-secret>"
kafcli context add eh-sp --brokers "$KAFCLI_NS" --auth-type azure-sp-secret \
  --tenant-id "$KAFCLI_TENANT" --client-id "<app-id>" --secret-ref env:KAFCLI_SP_SECRET --use
```

Resultado esperado:

- `context test`: `auth_type` = `azure-sp-secret`; `principal` = o `client_id` da app;
  `authenticated_principal` = o mesmo `appId` (claim `appid` ou `azp` do token); `brokers` >= 1;
  `topics` inclui `kafcli-probe`.
- `produce`: sai com código 0.
- `consume`: mostra `{"n":1}` ou outra mensagem JSON do tópico.

Schema (opcional): com o schema group `<grupo>` do namespace, adicione o registry ao contexto com
`--registry-type azure --registry-url https://$KAFCLI_NS_NAME.servicebus.windows.net --registry-group <grupo> --registry-auth-type azure`
e confira `kafcli schema get --id <id-de-um-schema-existente>`. A autenticação do registry usa o
mesmo token da app, então a app precisa de `Schema Registry Reader`.

## 2. `azure-sp-cert` (service principal com certificado)

Pré-requisito: certificado X.509 e chave RSA **sem senha** em um único arquivo PEM (o certificado
e a chave juntos). Crie e envie só a parte pública à app registration:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout kafcli-sp-key.pem -out kafcli-sp-cert.pem -days 30 -subj "/CN=kafcli-spike"
cat kafcli-sp-cert.pem kafcli-sp-key.pem > kafcli-sp.pem
az ad app credential reset --id "<app-id>" --cert @kafcli-sp-cert.pem --append
```

Os papéis de `Pré-requisitos gerais` valem para a app. Depois:

```bash
kafcli context add eh-cert --brokers "$KAFCLI_NS" --auth-type azure-sp-cert \
  --tenant-id "$KAFCLI_TENANT" --client-id "<app-id>" \
  --certificate-location "$PWD/kafcli-sp.pem" --use
```

Resultado esperado: o mesmo de `azure-sp-secret` (`principal` e `authenticated_principal` com o
`appId`; `produce` com código 0; `consume` com uma mensagem JSON). Se o `context test` falhar com
`invalid_client`, confira se o certificado enviado ao Entra ID é o `kafcli-sp-cert.pem`.

## 3. `azure-managed-identity` (VM ou Container App)

Este modo roda dentro da VM ou do Container App, porque o token vem do IMDS (ou do endpoint de App
Service e Container Apps) do próprio host. Rode o kafcli no host, não na sua máquina.

Pré-requisito: identidade gerenciada ativa no host e papéis atribuídos a ela.

```bash
# VM (identidade atribuída pelo sistema)
az vm identity assign -g "<rg-da-vm>" -n "<vm>"
# Container App (identidade atribuída pelo sistema)
az containerapp identity assign -g "<rg-do-app>" -n "<container-app>" --system-assigned
```

Atribua os papéis de `Pré-requisitos gerais` ao `principalId` da identidade. No host:

```bash
kafcli context add eh-mi --brokers "$KAFCLI_NS" --auth-type azure-managed-identity --use
# identidade atribuída pelo usuário: acrescente --client-id "<client-id-da-identidade>"
```

Resultado esperado: `context test` mostra `auth_type` = `azure-managed-identity` e um
`authenticated_principal` não vazio (claim `appid`, `azp` ou `oid` da identidade); `produce` e
`consume` como nos modos anteriores.

## 4. `azure-workload-identity` (pod AKS com o webhook)

Este modo roda dentro de um pod AKS. O webhook de workload identity injeta no pod as variáveis
`AZURE_CLIENT_ID`, `AZURE_TENANT_ID` e `AZURE_FEDERATED_TOKEN_FILE`, e o kafcli as lê; por isso o
`context add` não recebe tenant nem client.

Pré-requisito:

- cluster AKS com o OIDC issuer e o workload identity habilitados
  (`az aks update -g "<rg>" -n "<cluster>" --enable-oidc-issuer --enable-workload-identity`);
- identidade gerenciada com a federated credential para o service account do pod;
- papéis de `Pré-requisitos gerais` atribuídos a essa identidade;
- o pod com a label `azure.workload.identity/use: "true"` e o service account com a anotação
  `azure.workload.identity/client-id`;
- o binário `kafcli` dentro da imagem do pod.

No pod:

```bash
kafcli context add eh-wi --brokers "$KAFCLI_NS" --auth-type azure-workload-identity --use
```

Rode os comandos com `kubectl exec -it <pod> -- kafcli ...`, ou dentro do próprio pod. Resultado
esperado: o mesmo de `azure-managed-identity`, com `authenticated_principal` igual ao client id da
identidade do pod. Se `context test` falhar, confira a label e a anotação do pod e se o webhook
injetou as três variáveis (`env` dentro do pod).

## 5. `azure-cli` (login de usuário com `az`)

Pré-requisito: `az` instalado e `az login` feito com um usuário que tenha os papéis de
`Pré-requisitos gerais` no namespace.

```bash
az login
kafcli context add eh-cli --brokers "$KAFCLI_NS" --auth-type azure-cli --tenant-id "$KAFCLI_TENANT" --use
```

O `--tenant-id` é opcional. Resultado esperado: `auth_type` = `azure-cli`; `authenticated_principal`
com o claim `upn` ou `oid` do usuário; `produce` e `consume` como acima. 
## 6. `azure-default` (cadeia de credenciais)

Este modo escolhe a primeira fonte disponível no host, nesta ordem: variáveis de service principal
(`AZURE_CLIENT_SECRET` ou `AZURE_CLIENT_CERTIFICATE_PATH`), workload identity
(`AZURE_FEDERATED_TOKEN_FILE`), managed identity e Azure CLI. Para o teste, deixe ativa só uma
fonte, para que a escolha seja a esperada.

```bash
kafcli context add eh-default --brokers "$KAFCLI_NS" --auth-type azure-default --use
```

Resultado esperado: o mesmo de `azure-sp-secret` quando a fonte for o service principal, ou o da
fonte que estiver ativa no host. Anote qual fonte respondeu.

## Anotação dos resultados

Preencha uma linha por modo, com a data do teste:

| Data | Modo | `context test` | `produce` | `consume` | Observações |
|---|---|---|---|---|---|
| | azure-sp-secret | | | | |
| | azure-sp-cert | | | | |
| | azure-managed-identity | | | | |
| | azure-workload-identity | | | | |
| | azure-cli | | | | |
| | azure-default | | | | |

Se algum passo falhar, copie a mensagem de erro (ela já sai sem segredos) e a versão do kafcli
(`kafcli --version`).

## Limpeza

```bash
az role assignment delete --assignee "<principal>" --scope "$KAFCLI_NS_ID"
az ad app delete --id "<app-id>"
```

Apague o resource group do roteiro 0001 quando terminar (`az group delete -n "$KAFCLI_RG"`). Remova
também as identidades gerenciadas criadas para o teste.
