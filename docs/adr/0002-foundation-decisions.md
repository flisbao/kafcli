# ADR 0002: Decisões de fundação do kafcli

- Status: aceito
- Data: 2026-11-15
- Escopo: plano 01 (fundação), Tasks 1 a 11

## Contexto

O plano 01 entrega a base do `kafcli`: configuração por perfis, resolução de segredos, autenticação para os brokers, guard de operações, auditoria e os primeiros comandos (`profile list/show/add/use/test`). Durante a implementação surgiram decisões que não estavam fechadas no design e que afetam manutenção, segurança e testes. Este ADR registra essas decisões e as razões de cada uma.

A `0001` está reservada ao spike do Event Hubs.

## Decisão

### Estrutura do workspace

- Dois crates em vez de cinco: `kafcli` (binário, parsing de CLI, saída, prompt) e `kafcli-core` (biblioteca com configuração, segredos, auth, conexão, guard e auditoria). Crates separados por camada só se justificam quando houver um segundo consumidor ou uma fronteira de dependência real.
- Os comandos dependem de traits do core para segredos (`&dyn SecretResolver`, injetado por `main.rs`). O único tipo concreto de serviço construído é `CompositeSecretResolver`, e só em `main.rs`. `TerminalPrompter` e `JsonLinesAuditWriter` ainda não têm consumidor no plano 01 (o guard e a auditoria entram nos comandos do plano 02) e serão ligados em `main.rs` nesse momento. Modelos de dados (`Configuration`, `Profile`) são concretos por natureza.

### Configuração

- Precedência: flags de linha de comando > arquivo TOML > variáveis `KAFCLI_*`.
- O arquivo TOML é lido uma única vez com `toml` 1.x e alimentado ao figment via `Serialized`. O parser TOML interno do figment (toml 0.8) ecoava linhas do arquivo nas mensagens de erro, o que podia vazar segredos. Erros de configuração descrevem a posição do problema, nunca repetem o valor.
- `Profile` usa `deny_unknown_fields`: um erro de digitação como `protectd` desabilitaria a proteção em silêncio, então deve falhar ao carregar. `Configuration` não usa `deny_unknown_fields`: variáveis `KAFCLI_*` soltas no ambiente não podem quebrar o carregamento.
- Caminho padrão: `dirs::config_dir()/kafcli/config.toml`, com override por `--config` ou `KAFCLI_CONFIG`.
- Referências de segredo aceitas: `env:VAR`, `file:/caminho` e `keyring:serviço/conta`. O TOML nunca guarda segredo em claro.
- `profile show` exibe as referências de segredo. Referências não são segredos, mas o comando nunca resolve os valores.
- A reescrita do TOML feita por `profile add` e `profile use` não preserva comentários. Esse é um custo aceito para manter o escritor simples e previsível.
- `profile add` e `profile use` leem apenas o arquivo (`load_configuration_file_only`). Variáveis `KAFCLI_*` nunca são gravadas nele.
- Validação de perfil rejeita endereços de broker vazios ou só com espaços (ex.: `a:9092,,b:9092`).

### Guard de operações

- Níveis de operação com exit code `3` para operação abortada pelo guard ou pelo usuário.
- Operação de escrita em perfil `protected`: `--yes` dispensa a pergunta sim/não.
- Operação destrutiva em perfil `protected`: `--yes` não dispensa a confirmação digitada. O guard exige que o usuário digite o alvo da operação. Sem terminal interativo, a operação aborta.
- `use_tls = false` só é aceito fora de perfis `protected`. A validação roda antes de qualquer conexão.

### Segurança de saída

- Segredos nunca aparecem em log, mensagem de erro, saída `--output json`, `profile show` ou audit log.
- Saída em tabela passa todo texto vindo do broker (nomes de tópico, grupos, metadados) por `sanitize_for_terminal`, que troca caracteres de controle por `?`. Isso bloqueia injeção de sequências de escape de terminal a partir de nomes controlados pelo broker.
- Saída JSON não sofre sanitização, para ser lossless. Ela segue o envelope `{"kafcli_output": 1, "data": ...}`.
- Exit codes: `0` sucesso, `1` erro, `2` limite violado (reservado ao plano 04), `3` operação abortada.
- Erros de uso da linha de comando (subcomando desconhecido, valor inválido como `--timeout-seconds 0`) saem com `1`. O `2` padrão do clap não é usado, porque `2` é reservado para limites. `--help` e `--version` saem com `0`.
- Permissões do audit log em unix: diretório `0700` e arquivo `0600`, aplicadas na criação. Um arquivo já existente mantém o modo que tinha. O arquivo recebe `sync_all` após cada registro.
- Mensagens de confirmação do guard são montadas uma única vez, no guard. O terminal imprime o texto como está, sanitizado.

### Convenções de código

- `unsafe_code = "forbid"` no workspace. O `unsafe` interno do rdkafka não conta.
- `unwrap`, `expect`, `panic` e `todo` são `deny` no clippy de produção. Testes podem usá-los (`clippy.toml`).
- `Box::leak` e `std::mem::forget` são proibidos (`disallowed-methods` e `clippy::mem_forget`).
- Funções começam com verbo e expressam a intenção. A função de mapeamento de exit code foi nomeada `resolve_exit_code` (inicialmente planejada como `exit_code_for_error`) para seguir a regra de verbo primeiro. Conversões `from_*` e `to_*` são aceitas como equivalentes a verbo.
- Testes unitários ficam no próprio arquivo, em `#[cfg(test)] mod tests`. Testes de CLI e integração ficam em `crates/kafcli/tests/`.

### Dependências

- `rdkafka` com features `cmake-build`, `ssl-vendored`, `libz-static`, `curl-static` e `zstd`. O librdkafka 2.12 inclui `curl.h` mesmo com `WITH_CURL=0`, então o curl precisa ser vendorizado para o build compilar. Isso exige `cmake` no build. A feature `zstd` compila `zstd-sys` a partir do fonte, sem biblioteca do sistema, e é necessária para ler tópicos comprimidos com zstd no plano 02.
- Segredos do keyring: a feature `keyring` de `kafcli-core` é padrão, mas opcional. Quando o keyring não estiver disponível, o fallback sugerido é `env:` ou `file:`.
- O `cargo audit` não cobre o código C vendorizado (`openssl-src`, `curl-sys`, `rdkafka-sys`, `zstd-sys`, `libz-sys`). A cada release, as versões embutidas são conferidas contra os advisories do OpenSSL, do curl, do librdkafka, do zstd e do zlib. O processo de release do plano 06 inclui essa conferência.
- Revisão de 2026-11-19: o `deny.toml` ignora RUSTSEC-2026-0370, -0371 e -0372 (`zvariant`, `zvariant_derive`, `zbus_names` sem manutenção). Eles entram via `keyring` 4.2 e `zbus` 5, e não existe versão segura para atualizar. A exceção é revista a cada release do `keyring`.

### Testes de integração

- Fixtures com Docker Compose e Redpanda em vez de testcontainers. Portas fixas e advertised listeners previsíveis tornam a configuração de teste determinística e legível.
- O compose tem cenários para plaintext, SASL/SCRAM e mTLS.
- O serviço `redpanda-scram-bootstrap` é o gate: ele cria o usuário, habilita o SASL, verifica `enable_sasl` e só então marca o healthcheck (`test -f /tmp/ready`). `up --wait` só retorna depois disso, evitando testes que conectam antes do broker aceitar autenticação. Ele fica em execução porque `up --wait` falha quando um serviço de uma única execução sai.
- Testes de integração são `#[ignore]` e rodam em job separado do CI.

### Build de release

- Workflow `release-build.yml` com dois alvos: `x86_64-unknown-linux-musl` (ubuntu, com `musl-tools` e `cmake`) e `aarch64-apple-darwin` (macos, com `cmake` via brew).
- Verificações pós-build: `kafcli --version` começa com `kafcli 0.1.0`; no Linux, o binário é `statically linked` ou `static-pie linked`; no macOS, `otool -L` não lista `librdkafka` nem `libssl`.
- Windows e Linux aarch64 ficam no plano 06.

## Consequências

- O build musl depende de um toolchain C para musl. O `ssl-vendored` compila OpenSSL a partir do fonte e precisa de um compilador C para musl. O pacote `musl-tools` no Ubuntu fornece o wrapper `musl-gcc`, e o workflow aponta `CC_x86_64_unknown_linux_musl` para ele.
- O librdkafka também é configurado com CMake, e o projeto habilita a linguagem C++. O `musl-tools` não fornece um compilador C++ para musl, então o workflow aponta `CXX_x86_64_unknown_linux_musl` para o `g++` do host. O wrapper C++ (`rdkafka++`) é compilado mas não é linkado no kafcli, então o `g++` do host só precisa passar pela configuração e compilar objetos não usados.
- **Build musl verificado apenas em CI.** Na máquina de desenvolvimento, `cargo build --release --target x86_64-unknown-linux-musl -p kafcli` falhou na compilação do OpenSSL vendorizado com `/bin/sh: 1: x86_64-linux-musl-gcc: not found` seguido de `Error building OpenSSL`. O `musl-gcc` não está instalado nessa máquina e não foi instalado, por decisão do controlador. O target musl está instalado no rustup, então o problema é só o toolchain C. A verificação de link estático fica por conta do job `release-build.yml` em `ubuntu-latest`, que instala `musl-tools`. Esse job também roda em `pull_request` quando `Cargo.toml`, `Cargo.lock`, `crates/**` ou o próprio workflow mudam, então o build estático é checado antes do merge.
- O guard de perfis `protected` é mais rígido que o esperado por quem usa `--yes` em scripts. Operações destrutivas exigem terminal interativo. Automação que precisa apagar em perfil protegido deve ser tratada em plano futuro, com decisão explícita.
- `profile add` e `profile use` apagam comentários do TOML. Usuários que comentam o arquivo devem saber disso.
- `Profile` com `deny_unknown_fields` faz a configuração falhar em chaves novas ou erradas. Isso é intencional, mas exige que versões futuras do formato sejam migradas com cuidado.
- Saída em tabela não exibe caracteres de controle, então nomes de tópico com esses caracteres aparecem alterados. A saída JSON preserva o valor original.
- Fixtures em Compose exigem Docker no CI e na máquina do desenvolvedor. Os testes de integração não rodam sem ele.
- O escritor de configuração tem duas limitações conhecidas. (1) Se `config.toml` for um link simbólico, ele é substituído por um arquivo regular, e o link se perde. (2) Não há lock entre escritores concorrentes: quem grava por último vence, e uma edição feita em paralelo pode ser perdida.
