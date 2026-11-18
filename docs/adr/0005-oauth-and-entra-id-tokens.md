# ADR 0005: OAuth e tokens do Entra ID

- Status: aceito
- Data: 2026-11-17
- Escopo: plano 03 (autenticação OAuth, bearer e Azure, onda B), Tasks 1 a 10

## Contexto

O plano 02b entregou o Schema Registry com as autenticações `none`, `basic`, `bearer` e `mtls`. O plano 03 adiciona os modos de broker `bearer`, `oauth-oidc`, `azure-sp-secret`, `azure-sp-cert`, `azure-managed-identity`, `azure-workload-identity`, `azure-cli` e `azure-default`, todos por OAUTHBEARER, e o modo `azure` no Schema Registry.

A spec §5 previa o crate `azure_identity` para os tokens do Entra ID. Dois fatos impediram isso. O callback de token do rdkafka (`generate_oauth_token`) é síncrono, e o `azure_identity` é assíncrono, o que exigiria um runtime tokio. A regra do projeto também proíbe dependência direta de `tokio`. Por isso o plano 03 reimplementa a parte necessária do Entra ID com HTTP direto.

## Decisão

### HTTP direto com `reqwest::blocking`

- Os tokens do Entra ID, do IMDS e do App Service são pedidos com `reqwest::blocking`. Essa escolha foi autorizada pelo autor em 2026-11-17, como desvio da spec §5.
- Timeout de 30 s por requisição, corpo limitado a 1 MiB e redirecionamento desligado (`reqwest::redirect::Policy::none()`). Erros do `reqwest` passam por `without_url()`, para que a URL não apareça na mensagem.
- Erros do Entra ID e do IMDS mostram só `error`, `error_description` e o status HTTP, no formato `azure token request failed (HTTP <status>): <error>: <error_description>`. Antes de sair, passam pela redação de todo segredo conhecido da requisição e por `neutralize_registry_text` (200 caracteres, controle trocado por `?`).

### Assinatura com `aws-lc-rs`

- O `azure-sp-cert` monta a client assertion RS256 e a assina com `aws-lc-rs`, já presente no `Cargo.lock` como dependência do rustls. Não há crate de JWT nem `rsa`.
- O PEM do certificado e da chave é lido com a trait `PemObject` de `rustls-pki-types`. Só PEM sem senha (chave RSA não criptografada). PKCS#12 e chave criptografada ficam fora.
- A assertion fica em `SecretString` desde a leitura até o pedido de token.

### Cache de uma entrada

- Cada `TokenSource` tem um cache de uma entrada (`CachedTokenSource`). Ele renova quando faltam menos de 300 s para `expires_at`.
- Falha não entra no cache: a próxima chamada tenta de novo.

### Espera do primeiro token por cliente

- O rdkafka pede o token só quando o cliente é consultado com `poll`. Por isso `KafkaConnection::create_consumer` espera o primeiro token antes de devolver o cliente: até 30 s, em fatias de `poll(100 ms)`, sem busy-wait.
- A espera é por cliente. Cada cliente tem o seu sinal de "token entregue" e o seu último erro, criados por `OAuthTokenRefresher::for_new_client()`. Um erro antigo de outro cliente não falha a espera deste.
- O principal (claims `upn`, `appid`, `azp`, `oid` ou `sub`, nessa ordem) é compartilhado por conexão. Depois que qualquer cliente recebe um token, `KafkaConnection::refresher().last_principal()` o devolve, e `context test` o mostra como `authenticated_principal`.

### `oauth-oidc` nativo

- O modo `oauth-oidc` usa `sasl.oauthbearer.method=oidc`. O librdkafka busca o token no endpoint e o renova sozinho. O kafcli não vê esse token, por isso `authenticated_principal` é `None` nesse modo.
- Esse modo também não tem falha rápida no primeiro token. `create_token_source` devolve `None` e o librdkafka busca o token por conta própria. Um client secret ruim aparece como erro do librdkafka ou como timeout, e não com a mensagem do Entra ID ou do IdP. Nos modos que têm token source, a espera do primeiro token é fixa em 30 s (`INITIAL_TOKEN_TIMEOUT`) e não depende de `--timeout-seconds`.
- `ca_location` também vai como `https.ca.location`, porque o librdkafka verifica o endpoint de token com essa propriedade, e não com `ssl.ca.location`.

### Scope

- Scope do broker Azure: o campo `scope` quando presente. Senão, `https://<host do primeiro broker>/.default`, se o host terminar em `.servicebus.windows.net`. Senão, erro `scope is required for azure auth when brokers are not *.servicebus.windows.net`.
- Scope do Schema Registry Azure: `https://eventhubs.azure.net/.default`, fixo.

### Cadeia do `azure-default` e regra de indisponível versus erro

- A cadeia tenta, nesta ordem: variáveis de ambiente de service principal (`AZURE_CLIENT_SECRET`, depois `AZURE_CLIENT_CERTIFICATE_PATH`), workload identity (só com `AZURE_FEDERATED_TOKEN_FILE`), managed identity (IMDS) e Azure CLI.
- Antes da primeira escolha, um elo indisponível é pulado e um erro para a cadeia. Depois que um elo entrega token, os erros dele são devolvidos como estão.
- "Indisponível" e "erro" são uma classificação estruturada interna do crate (`LinkFailure`), e não um texto comparado. Um elo sem as variáveis dele fica indisponível. Um elo usável que falha fica em erro.
- A sonda da managed identity tem timeout de 1 s, e esse timeout vale só até o primeiro token. Depois do primeiro token, a sonda usa o timeout normal de 30 s.

### Azure CLI

- `az account get-access-token` roda com `std::process::Command`, sem shell. Escopo e tenant são validados antes de qualquer processo: o escopo aceita só letras, dígitos e `_ - . : /`, sem `-` inicial; o tenant aceita letras, dígitos, `.` e `-`.
- No Windows, o diretório de trabalho do processo é `SYSTEMROOT`, para que `az` não seja resolvido a partir do diretório atual (injeção por metacaracteres do `cmd`).
- A espera não usa `thread::sleep` (proibido pelo clippy e pela regra do ADR 0004). O stdout e o stderr são lidos por threads próprias, e a espera usa `recv_timeout` até o fim do pipe. No timeout, o processo é morto e recolhido.
- Limites documentados: se o filho fecha os pipes mas continua rodando, a espera final é ilimitada; se um neto herda os pipes, o timeout dispara mesmo com o `az` já encerrado; o kill atinge só o processo filho direto. O stdout (o token) nunca aparece em erro. Só o início do stderr entra na mensagem, neutralizado.

### Authority host

- Padrão `https://login.microsoftonline.com`. A ordem de escolha é: `authority_host` do contexto, depois `AZURE_AUTHORITY_HOST` (com espaços removidos), depois o padrão.
- O valor precisa ser `https://` e não pode ter userinfo. Um `@` é recusado.
- `http://` só vale com host `127.0.0.1` ou `localhost`, e só na configuração de um contexto não protegido. Isso existe para os fakes dos testes. A variável de ambiente nunca aceita `http://`.

### `bearer`

- O `bearer` resolve a referência do token uma vez, em `create_token_source`, e guarda o `SecretString`. A razão é que `CompositeSecretResolver` não é `Send + Sync` e é só emprestado. Por isso um arquivo de token rotacionado durante a execução não é relido. Essa é a alternativa escolhida na Task 3, no lugar de reler a referência a cada refresh.

### Configuração e registry

- Cada credencial Azure tem uma variante própria de `AuthConfig` (não `#[serde(untagged)]`), porque o `untagged` degradava o erro do serde para todos os modos. `AuthConfig::azure_credential()` reconstrói o `AzureCredentialConfig` quando o código comum precisa dele.
- O Schema Registry Azure (`--registry-auth-type azure`) herda a credencial azure-* do broker. Uma credencial própria do registry fica só no JSON de configuração.
- Quando o token do registry falha por transporte, o erro vira `CoreError::AuthenticationTransient`, que o cliente mapeia para `SchemaRegistryTransient`. Os outros erros de autenticação continuam definitivos.

## Consequências

- Os modos Azure não dependem de `azure_identity`, `azure_core`, `jsonwebtoken`, `oauth2`, `rsa`, `openssl` nem de tokio. O custo é o código de HTTP e de assinatura próprio, coberto por testes com fakes.
- Os testes automatizados usam fakes locais do Entra ID (`FakeRegistryServer`, feature `test-support`). Eles cobrem a forma das requisições, o formato de erro, a redação, o cache, a espera do primeiro token, a cadeia do `azure-default` e o teste de vazamento de segredos.
- Sem teste automatizado, e por isso com roteiro manual em `docs/spike/0002-azure-auth-runbook.md`:
  - a conversa real com o Entra ID (app registration, papéis e resposta de produção);
  - o uso real do Event Hubs com esses tokens;
  - o workload identity dentro de um pod AKS com o webhook;
  - a managed identity dentro de uma VM ou de um Container App;
  - o `az` real, com a sua saída e o seu tempo de resposta.
- O `azure-cli` tem os limites listados acima: um filho que fecha os pipes mas continua rodando pode prender a espera final sem limite, e um neto que herda os pipes faz o timeout disparar mesmo com o `az` encerrado.
- O `bearer` não acompanha a rotação de um arquivo de token durante uma execução longa. Quem precisa disso reinicia o comando.
- O principal exibido por `context test` é lido das claims do token sem verificar a assinatura. Quem valida o token é o broker ou o Entra ID. Por isso o principal serve de diagnóstico, e não de decisão de segurança.

## Fora do escopo

- `aws-msk-iam` e `kerberos` (spec §7).
- Extensões SASL do OAUTHBEARER.
- Chave privada criptografada no `azure-sp-cert` (só PEM sem senha) e PKCS#12 (spec §3).
- Managed identity de Azure Arc, Service Fabric e Cloud Shell. Só IMDS e o endpoint de App Service e Container Apps (`IDENTITY_ENDPOINT`).
- Teste automatizado contra Entra ID e Event Hubs reais. Esses modos ficam com o roteiro manual.
