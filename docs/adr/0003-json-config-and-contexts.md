# ADR 0003: Configuração em JSON e contextos

- Status: aceito
- Data: 2026-11-16
- Escopo: plano 01b (configuração JSON e contextos), Tasks 1 a 4

## Contexto

O plano 01 entregou a configuração em TOML, com perfis (`profile list/show/add/use/test`). Antes de publicar qualquer versão, decidimos trocar o formato e o vocabulário para evitar uma migração depois. O ADR 0002 registra as decisões do plano 01. Este ADR registra as mudanças do plano 01b e substitui as partes do 0002 que tratam de TOML, `config.toml`, `profile *` e `default_profile`.

A troca também resolve um problema concreto: `profile add` e `profile use` já reescreviam o TOML e perdiam comentários. Com JSON, a edição feita pelo comando e a edição feita à mão ficam no mesmo formato, sem depender de um escritor que preserve comentários.

## Decisão

### Configuração em JSON

- O arquivo é `config.json`, no caminho padrão `dirs::config_dir()/kafcli/config.json`. `--config` e `KAFCLI_CONFIG` continuam substituindo o caminho.
- Só JSON é aceito. Nenhum leitor de TOML permanece: a crate `toml` sai das dependências e a feature `toml` do figment é removida.
- Motivos para JSON: o formato é o que `jq` e editores leem sem plugin, e um JSON Schema pode ser publicado no futuro para validar o arquivo. A publicação do schema fica no plano 06.
- Não há migração automática de `config.toml`. A configuração do plano 01 não foi publicada, e o usuário recria os contextos com `kafcli context add`.
- O arquivo com as chaves `profiles` ou `default_profile` falha ao carregar, com a mensagem indicando a chave a renomear para `contexts` ou `current_context`. Sem essa rejeição explícita, `Configuration` (que aceita chaves desconhecidas) carregaria vazio em silêncio.
- `null` em campo opcional vale como ausente (`"ca_location": null`, `"schema_registry": null`). Arquivo vazio vale como configuração vazia. Vírgula sobrando é erro com linha e coluna.
- O JSON é lido uma única vez com `serde_json` e entregue ao figment via `Serialized`, como no plano 01. O erro do `serde_json` nunca é repassado com `Display`: a mensagem dele repete o valor, e um segredo colado no lugar errado vazaria. Erros de configuração descrevem linha e coluna, ou o caminho da chave, e nunca o valor. A única exceção deliberada são os erros de campo ou valor desconhecido (`unknown field`, `unknown value`): repetem o nome digitado como dica de digitação, e por isso podem ecoar um texto que o operador escreveu no lugar errado.
- `ConnectionContext` mantém `deny_unknown_fields`, pelo mesmo motivo do `Profile` no ADR 0002. `Configuration` continua sem `deny_unknown_fields`.

### Contextos no lugar de perfis

- O vocabulário passa a ser `context`: `kafcli context list|show|add|use|current|test`. `profile` e `--profile` são removidos sem alias.
- O tipo `Profile` vira `ConnectionContext`, `profiles` vira `contexts`, e `default_profile` vira `current_context`.
- Precedência do contexto usado por um comando: argumento posicional do subcomando > `--context` > `KAFCLI_CONTEXT` > `current_context` do arquivo > `KAFCLI_CURRENT_CONTEXT`.
- `KAFCLI_CONTEXT` existe para scripts e sessões de terminal. Um valor esquecido que aponte para um contexto inexistente faz o comando falhar com `context 'x' not found` e exit `1`. A variável nunca é gravada no arquivo: `context add` e `context use` ignoram `KAFCLI_CURRENT_CONTEXT` ao gravar.
- `context current` imprime só o nome do contexto selecionado, para uso em scripts e no prompt do shell. Com `--output json`, o nome sai no envelope padrão.
- `context list` marca o contexto efetivo, pela mesma regra de `context current`: `--context` ou `KAFCLI_CONTEXT` têm prioridade sobre o `current_context` do arquivo. Um nome inexistente nessas fontes não gera erro na listagem; nenhuma linha fica marcada.

### Aviso de produção

- Todo contexto `protected` é tratado como produção. Quando um comando age sobre ele, o aviso é impresso no stderr com o texto exato `WARNING: context '<nome>' is PRODUCTION`. O nome passa por `sanitize_for_terminal`.
- Cor: o aviso sai em `\x1b[1;33m` ... `\x1b[0m` somente quando o stderr é terminal e `NO_COLOR` está ausente ou vazio. Em pipe, em arquivo ou com `NO_COLOR` definido, sai texto puro.
- O aviso vai sempre para o stderr, então `--output json` continua produzindo stdout JSON válido, mesmo em pipe.
- Não existe flag para silenciar o aviso. A proteção de `--protected` só é útil se o aviso aparecer em todo uso, e um flag de silêncio seria o primeiro a ser usado em scripts.
- Comandos que avisam: `context test` (logo após selecionar o contexto e antes de conectar, para que o aviso venha antes de qualquer `error:`), `context use` (depois de gravar o arquivo) e `context add --use` (depois de gravar, e só quando o contexto é `protected`).
- Comandos que não avisam: `context list`, `context show` e `context current`. A listagem já marca os contextos de produção com `PRODUCTION`, e os três não conectam a nada.
- Regra para comandos futuros: todo comando que conecta a um contexto chama `warn_if_production` logo após `select_context`, antes de qualquer conexão. Essa regra vale para os comandos dos planos 02 em diante.

## Consequências

- Usuários do plano 01 precisam recriar os contextos. Como o plano 01 não foi publicado, não há usuários externos afetados.
- JSON não tem comentários, então a perda de comentários causada pela reescrita do TOML deixa de existir. A documentação dos campos fica no `--help` e neste ADR.
- O aviso é impresso em todo uso de contexto `protected`, inclusive em scripts. Quem automatiza precisa ignorar o stderr ou filtrar a linha. Não há flag para desligar, por decisão.
- `context show` e `context list` não avisam. Um script que só lê a configuração não recebe o aviso, e isso é intencional, porque nada é conectado.
- Escritas em JSON herdam a limitação já registrada no ADR 0002: não há lock entre escritores concorrentes, e quem grava por último vence.

## Fora do escopo

- Separar clusters e credenciais como no kubeconfig. O contexto continua agrupando brokers e autenticação.
- Publicar o JSON Schema do `config.json`. Fica no plano 06.
