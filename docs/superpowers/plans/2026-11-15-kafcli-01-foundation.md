# kafcli — Plano 01: Fundação (esqueleto + auth onda A) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Modelo de execução definido pelo autor:** cada task é implementada por um subagente **Haiku 5.5** (`model: "haiku"`) e validada por um revisor **Sonnet 5.5** (`model: "sonnet"`). A revisão final do branch inteiro também é feita pelo Sonnet 5.5.
>
> **Espaço em disco (exigência do autor):** ao final de cada task — depois do commit e da aprovação do revisor — o controlador roda `cargo clean` na raiz do workspace. Se a task subiu os fixtures, roda também `docker compose -f tests/fixtures/compose.yaml down -v`. Nenhuma task começa com `target/` herdado da anterior.

**Goal:** Entregar um binário `kafcli` que carrega perfis de um TOML, resolve segredos por referência, monta a configuração do librdkafka para os modos de auth da onda A e valida a conexão com `kafcli profile test`, com guard, audit log e CI prontos para os planos seguintes.

**Architecture:** Workspace Cargo com dois crates: `kafcli-core` (lib: config, segredos, auth, conexão, guard, audit — sem I/O de terminal) e `kafcli` (bin: clap, renderização table/json, prompts, exit codes). Cada modo de auth é um arquivo que implementa o trait `AuthProvider`; cada fonte de segredo é um arquivo que implementa `SecretResolver`. Funções do core retornam structs `Serialize`; só o bin renderiza.

**Tech Stack:** Rust 1.99 (edition 2024), rdkafka 0.39 (librdkafka vendorizado), clap 4.6, figment 0.10, secrecy 0.10, keyring 4.2 (opcional), thiserror 2, anyhow 1, tracing 0.1, comfy-table 8, serde/serde_json/toml, time 0.3, dirs 7; testes com tempfile, assert_cmd, predicates, figment `test` (Jail), Docker Compose (Redpanda) para integração.

**Spec:** `docs/superpowers/specs/2026-11-15-kafcli-design.md` (seções 5, 6.1, 7 ondas A, 9, 10). Roadmap: `docs/superpowers/plans/2026-11-15-kafcli-roadmap.md`.

## Global Constraints

- Nome do binário e do crate: `kafcli`; lib: `kafcli-core`. Licença `MIT OR Apache-2.0`.
- Rust edition 2024; `rust-version = "1.99"`.
- `unsafe_code = "forbid"` no workspace (o `unsafe` interno do rdkafka não conta).
- Sem `unwrap()`/`expect()` em código de produção: `clippy::unwrap_used` e `clippy::expect_used` = `deny`; liberados só em testes via `clippy.toml` (`allow-unwrap-in-tests = true`, `allow-expect-in-tests = true`).
- **SOLID no código e nos arquivos:** um arquivo = uma responsabilidade (a lista de arquivos de cada task é obrigatória). Novos modos de auth/fontes de segredo entram como novo arquivo + uma linha na factory, sem editar os providers existentes. O bin depende de traits do core (`SecretResolver`, `ConfirmationPrompter`, `AuditWriter`), não de tipos concretos, exceto no ponto de composição (`main.rs`).
- **Nomes Clean Code:** funções começam com verbo e dizem a intenção (`resolve_secret`, `build_client_config`, `probe_profile_connection`). Proibido: abreviações (`cfg`, `cfg_path`, `res`, `req`, `ctx`), nomes genéricos (`process`, `handle`, `data`, `do_it`, `util`, `helpers.rs`, `misc.rs`).
- **Sem memory leak:** proibido `Box::leak`, `std::mem::forget`, `Rc` em ciclos; `clippy::mem_forget = "deny"` e `Box::leak` em `disallowed-methods` no `clippy.toml`. Arquivos abertos só pelo escopo da chamada. Leituras de arquivo/stdin com limite de tamanho explícito.
- **Sem CPU throttling:** nenhum loop sem espera (`loop {}` com polling sem timeout é proibido). Toda chamada de rede ao broker tem timeout explícito. Nenhum thread/runtime criado por comando sem necessidade.
- Precedência de config: flags de linha de comando > arquivo TOML > variáveis `KAFCLI_*`.
- Config padrão: `dirs::config_dir()/kafcli/config.toml`; override por `--config PATH` ou env `KAFCLI_CONFIG`. Estado: `dirs::state_dir()` (fallback `dirs::data_local_dir()`) + `/kafcli`.
- Referências de segredo aceitas: `env:VAR`, `file:/caminho`, `keyring:serviço/conta`. O TOML nunca guarda segredo em claro.
- Segredos nunca aparecem em log, mensagem de erro, `--output json`, `profile show` ou audit log. Toda mensagem de erro sobre uma entrada inválida descreve **onde** está o erro, nunca repete o valor.
- Exit codes: `0` sucesso, `1` erro, `2` limite violado (reservado, usado no plano 04), `3` operação abortada pelo guard ou pelo usuário.
- Saída JSON sempre no envelope `{"kafcli_output": 1, "data": <valor>}`.
- Todo subcomando tem seção `Examples:` no `--help` (via `after_help`).
- Mensagens ao usuário e `--help` em inglês (projeto open source); docs/ADRs em português.
- Mensagens de commit: Conventional Commits (`feat:`, `test:`, `ci:`, `docs:`, `chore:`).
- **Espaço em disco:** todo task termina com `cargo clean` (rodado pelo controlador após a revisão). Para reduzir o tamanho de `target/` durante a task, o workspace usa `[profile.dev] debug = "line-tables-only"` e `[profile.dev.package."*"] debug = false`. O target musl da Task 11 também é limpo ao final (`cargo clean` remove todos os targets).

## Review Focus

1. **Segredo colado no lugar errado do TOML** (ex.: connection string inteira em `secret_ref`, ou campo `password = "..."`): o carregamento falha com erro que aponta o campo e **não contém o valor**. Teste na Task 3.
2. **Ambiente de CI sem TTY com perfil `protected`**: operação de escrita/destrutiva sem `--yes`/`--execute` aborta com exit 3 em vez de travar esperando stdin. Testes na Task 7 (guard) e Task 9 (prompter não interativo).
3. **Broker inalcançável ou porta errada**: `profile test` termina dentro do timeout (padrão 10 s) com exit 1 e mensagem clara, sem travar nem consumir CPU. Testes nas Tasks 6 e 10.
4. **Arquivo de segredo criado com `echo` (newline no fim) ou variável de ambiente vazia**: newline final é removido; variável vazia é erro explícito. Testes na Task 2.
5. **Primeiro uso sem arquivo de config ou com perfil inexistente**: `profile list` mostra lista vazia; `profile test nope` falha com `profile 'nope' not found` e exit 1, sem panic. Testes nas Tasks 3, 4 e 9.

---

## Estrutura de arquivos

```
Cargo.toml                      # workspace, lints, profile.release
clippy.toml                     # disallowed-methods, allow-unwrap-in-tests
deny.toml                       # cargo-deny: licenças e advisories
rust-toolchain.toml             # channel = "1.99"
LICENSE-MIT, LICENSE-APACHE, README.md
.github/workflows/ci.yml        # fmt, clippy, test, deny, integração
.github/workflows/release-build.yml  # build estático Linux musl + macOS (smoke)
docs/adr/0002-foundation-decisions.md
tests/fixtures/compose.yaml     # Redpanda plaintext, SCRAM, mTLS
tests/fixtures/generate-certificates.sh
crates/kafcli-core/
  Cargo.toml
  src/lib.rs
  src/error.rs                  # CoreError, CoreResult
  src/secret/mod.rs
  src/secret/reference.rs       # SecretReference (parse/display)
  src/secret/resolver.rs        # trait SecretResolver
  src/secret/environment_resolver.rs
  src/secret/file_resolver.rs
  src/secret/keyring_resolver.rs    # #[cfg(feature = "keyring")]
  src/secret/composite_resolver.rs
  src/secret/fixed_resolver.rs      # #[cfg(any(test, feature = "test-support"))]
  src/config/mod.rs
  src/config/model.rs           # Configuration, Profile, AuthConfig e structs de auth
  src/config/loader.rs          # load_configuration
  src/config/error_description.rs   # describe_figment_error (sem valores)
  src/config/paths.rs
  src/config/selection.rs       # select_profile
  src/config/overrides.rs       # ProfileOverrides, apply_profile_overrides
  src/config/validation.rs      # validate_profile
  src/config/writer.rs          # save_configuration
  src/auth/mod.rs
  src/auth/client_properties.rs # ClientProperties, PropertyValue
  src/auth/provider.rs          # trait AuthProvider
  src/auth/tls_settings.rs      # apply_tls_settings
  src/auth/plaintext_provider.rs
  src/auth/tls_provider.rs
  src/auth/mutual_tls_provider.rs
  src/auth/sasl_plain_provider.rs
  src/auth/sasl_scram_provider.rs
  src/auth/azure_connection_string_provider.rs
  src/auth/provider_factory.rs  # create_auth_provider
  src/connection/mod.rs
  src/connection/client_config_builder.rs
  src/connection/connection_probe.rs
  src/guard/mod.rs
  src/guard/operation_level.rs
  src/guard/guard_request.rs
  src/guard/confirmation_prompter.rs   # trait
  src/guard/guard_enforcer.rs
  src/audit/mod.rs
  src/audit/audit_record.rs
  src/audit/audit_writer.rs     # trait + JsonLinesAuditWriter
crates/kafcli/
  Cargo.toml
  src/main.rs                   # composição: parse → run → exit code
  src/exit_code.rs
  src/logging.rs
  src/execution_context.rs
  src/cli/mod.rs
  src/cli/arguments.rs          # CommandLineArguments, GlobalArguments, TopLevelCommand
  src/cli/profile_arguments.rs  # ProfileCommand, AddProfileArguments, AuthTypeArgument
  src/output/mod.rs
  src/output/output_format.rs
  src/output/renderer.rs        # trait TableRenderable, render_output
  src/output/profile_views.rs   # ProfileListView, ProfileDetailView + TableRenderable
  src/output/connection_report_view.rs  # TableRenderable for ConnectionReport
  src/prompt/mod.rs
  src/prompt/terminal_prompter.rs
  src/commands/mod.rs
  src/commands/profile/mod.rs
  src/commands/profile/list_profiles.rs
  src/commands/profile/show_profile.rs
  src/commands/profile/add_profile.rs
  src/commands/profile/use_profile.rs
  src/commands/profile/test_profile.rs
  tests/cli_version.rs
  tests/cli_profile.rs
  tests/integration_connection.rs     # #[ignore], precisa de Docker Compose
```

Testes unitários ficam no próprio arquivo (`#[cfg(test)] mod tests`). Testes de CLI e integração ficam em `crates/kafcli/tests/`.

---

### Task 1: Workspace, lints, licenças e CI básico

**Files:**
- Create: `Cargo.toml`, `clippy.toml`, `deny.toml`, `rust-toolchain.toml`, `.gitignore`, `LICENSE-MIT`, `LICENSE-APACHE`, `README.md`
- Create: `crates/kafcli-core/Cargo.toml`, `crates/kafcli-core/src/lib.rs`
- Create: `crates/kafcli/Cargo.toml`, `crates/kafcli/src/main.rs`
- Create: `.github/workflows/ci.yml`
- Test: `crates/kafcli/tests/cli_version.rs`

**Interfaces:**
- Produces: workspace compilável; `[workspace.dependencies]` com as versões abaixo, que todas as tasks seguintes usam via `dep.workspace = true`.

- [ ] **Step 1: `git init` na raiz do projeto, criar branch `feat/foundation`.**

- [ ] **Step 2: Criar o workspace.**

`Cargo.toml` raiz:

```toml
[workspace]
resolver = "3"
members = ["crates/kafcli", "crates/kafcli-core"]

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.99"
license = "MIT OR Apache-2.0"
repository = "https://github.com/OWNER/kafcli"

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
mem_forget = "deny"
panic = "deny"
todo = "deny"

[workspace.dependencies]
kafcli-core = { path = "crates/kafcli-core" }
rdkafka = { version = "0.39", default-features = false, features = ["cmake-build", "ssl-vendored", "libz-static"] }
clap = { version = "4.6", features = ["derive", "env"] }
figment = { version = "0.10", features = ["toml", "env"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "1"
secrecy = "0.10"
keyring = "4.2"
thiserror = "2"
anyhow = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
comfy-table = "8"
time = { version = "0.3", features = ["formatting"] }
dirs = "7"
tempfile = "3"
assert_cmd = "2"
predicates = "3"

[profile.dev]
debug = "line-tables-only"

[profile.dev.package."*"]
debug = false

[profile.release]
lto = "thin"
codegen-units = 1
strip = true
```

`clippy.toml`:

```toml
allow-unwrap-in-tests = true
allow-expect-in-tests = true
allow-panic-in-tests = true
disallowed-methods = [
  { path = "std::boxed::Box::leak", reason = "memory leak proibido (Global Constraints)" },
  { path = "std::thread::sleep", reason = "use timeouts do cliente; sleep só com justificativa em ADR" },
]
```

`crates/kafcli-core/Cargo.toml`: `[lints] workspace = true`; features `default = ["keyring"]`, `keyring = ["dep:keyring"]`, `test-support = []`; dev-dependency `figment = { workspace = true, features = ["test"] }`, `tempfile`.
`crates/kafcli/Cargo.toml`: `[[bin]] name = "kafcli"`, `[lints] workspace = true`, depende de `kafcli-core` com feature `test-support` só em dev-dependencies.
`rust-toolchain.toml`: `channel = "1.99"`, `components = ["rustfmt", "clippy"]`, `targets = ["x86_64-unknown-linux-musl"]`.
`deny.toml`: licenças permitidas `MIT`, `Apache-2.0`, `Apache-2.0 WITH LLVM-exception`, `BSD-2-Clause`, `BSD-3-Clause`, `ISC`, `Unicode-3.0`, `Zlib`, `OpenSSL`, `MPL-2.0`; `advisories` com `yanked = "deny"`.

- [ ] **Step 3: Escrever o teste que falha** em `crates/kafcli/tests/cli_version.rs`:

```rust
#[test]
fn prints_version_with_binary_name() {
    assert_cmd::Command::cargo_bin("kafcli").unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicates::str::starts_with("kafcli 0.1.0"));
}
```

- [ ] **Step 4: Rodar e ver falhar.** Run: `cargo test -p kafcli --test cli_version` — Expected: FAIL (sem `main` com clap ou compilação falha).

- [ ] **Step 5: Implementar `main.rs` mínimo** com `#[derive(clap::Parser)] #[command(name = "kafcli", version)] struct CommandLineArguments {}` e `fn main() { CommandLineArguments::parse(); }`. (A Task 9 substitui pelo `main` definitivo.)

- [ ] **Step 6: Rodar e ver passar.** Run: `cargo test -p kafcli --test cli_version` — Expected: PASS. Também: `cargo clippy --all-targets -- -D warnings` sem warnings e `cargo fmt --check` limpo.

- [ ] **Step 7: CI.** `.github/workflows/ci.yml` com jobs em `ubuntu-latest`: `fmt` (`cargo fmt --all --check`), `clippy` (`cargo clippy --workspace --all-targets -- -D warnings`), `test` (`cargo test --workspace`), `deny` (`EmbarkStudios/cargo-deny-action@v2`). Instalar `cmake` no runner antes de compilar (librdkafka). Cache com `Swatinem/rust-cache@v2`.

- [ ] **Step 8: README** com uma frase de descrição, status "pre-alpha", licença dupla. LICENSE-MIT e LICENSE-APACHE com o texto padrão.

- [ ] **Step 9: Commit.** `git add -A && git commit -m "chore: bootstrap kafcli workspace, lints and CI"`

---

### Task 2: Erros do core e resolução de segredos

**Files:**
- Create: `crates/kafcli-core/src/error.rs`, `src/secret/{mod,reference,resolver,environment_resolver,file_resolver,keyring_resolver,composite_resolver,fixed_resolver}.rs`
- Modify: `crates/kafcli-core/src/lib.rs` (declarar `pub mod error; pub mod secret;`)

**Interfaces:**
- Produces:
  ```rust
  // error.rs
  #[derive(Debug, thiserror::Error)]
  pub enum CoreError {
      #[error("configuration error: {0}")] Configuration(String),
      #[error("profile '{0}' not found")] ProfileNotFound(String),
      #[error("no profile selected: pass --profile or set default_profile")] NoProfileSelected,
      #[error("invalid secret reference: {0}")] InvalidSecretReference(String),
      #[error("secret could not be resolved: {0}")] SecretResolution(String),
      #[error("profile validation failed: {0}")] ProfileValidation(String),
      #[error("kafka error: {0}")] Kafka(String),
      #[error("operation aborted: {0}")] OperationAborted(String),
      #[error("i/o error: {0}")] Io(#[from] std::io::Error),
  }
  pub type CoreResult<T> = Result<T, CoreError>;

  // secret/reference.rs — serde via String (try_from/into)
  #[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
  #[serde(try_from = "String", into = "String")]
  pub enum SecretReference {
      Environment { variable: String },
      File { path: std::path::PathBuf },
      Keyring { service: String, account: String },
  }
  impl std::str::FromStr for SecretReference { type Err = CoreError; }
  impl std::fmt::Display for SecretReference {}   // "env:VAR" | "file:/p" | "keyring:s/a"

  // secret/resolver.rs
  pub trait SecretResolver {
      fn resolve_secret(&self, reference: &SecretReference) -> CoreResult<secrecy::SecretString>;
  }

  // environment_resolver.rs
  pub struct EnvironmentSecretResolver { /* lookup: Box<dyn Fn(&str) -> Option<String>> */ }
  impl EnvironmentSecretResolver {
      pub fn from_process_environment() -> Self;
      pub fn with_lookup(lookup: impl Fn(&str) -> Option<String> + 'static) -> Self;
  }
  // file_resolver.rs
  pub const MAXIMUM_SECRET_FILE_BYTES: u64 = 64 * 1024;
  pub struct FileSecretResolver;
  // keyring_resolver.rs (#[cfg(feature = "keyring")])
  pub struct KeyringSecretResolver;
  // composite_resolver.rs
  pub struct CompositeSecretResolver { /* environment, file, keyring: Option<Box<dyn SecretResolver>> */ }
  impl CompositeSecretResolver { pub fn with_default_sources() -> Self; }
  // fixed_resolver.rs (#[cfg(any(test, feature = "test-support"))])
  pub struct FixedSecretResolver { /* value */ }
  impl FixedSecretResolver { pub fn returning(value: &str) -> Self; }
  ```

- [ ] **Step 1: Escrever os testes que falham** (cada um no arquivo do tipo testado):

```rust
// reference.rs
#[test] fn parses_environment_reference() {
    assert_eq!("env:KAFKA_PASSWORD".parse::<SecretReference>().unwrap(),
        SecretReference::Environment { variable: "KAFKA_PASSWORD".into() });
}
#[test] fn parses_file_reference() {
    assert_eq!("file:/run/secrets/kafka".parse::<SecretReference>().unwrap(),
        SecretReference::File { path: "/run/secrets/kafka".into() });
}
#[test] fn parses_keyring_reference() {
    assert_eq!("keyring:kafcli/prd-sp".parse::<SecretReference>().unwrap(),
        SecretReference::Keyring { service: "kafcli".into(), account: "prd-sp".into() });
}
#[test] fn displays_reference_in_parseable_form() {
    assert_eq!("keyring:kafcli/prd-sp".parse::<SecretReference>().unwrap().to_string(), "keyring:kafcli/prd-sp");
}
#[test] fn rejects_unknown_scheme_without_echoing_input() {
    let error = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=TOPSECRET"
        .parse::<SecretReference>().unwrap_err().to_string();
    assert!(!error.contains("TOPSECRET"));
    assert!(error.contains("env:") && error.contains("file:") && error.contains("keyring:"));
}
#[test] fn rejects_keyring_reference_without_account() {
    assert!("keyring:kafcli".parse::<SecretReference>().is_err());
}
#[test] fn rejects_empty_variable_name() { assert!("env:".parse::<SecretReference>().is_err()); }

// environment_resolver.rs
#[test] fn resolves_variable_value() {
    let resolver = EnvironmentSecretResolver::with_lookup(|name| (name == "KAFKA_PASSWORD").then(|| "s3cret".to_string()));
    let secret = resolver.resolve_secret(&"env:KAFKA_PASSWORD".parse().unwrap()).unwrap();
    assert_eq!(secrecy::ExposeSecret::expose_secret(&secret), "s3cret");
}
#[test] fn reports_missing_variable_by_name() {
    let resolver = EnvironmentSecretResolver::with_lookup(|_| None);
    let error = resolver.resolve_secret(&"env:KAFKA_PASSWORD".parse().unwrap()).unwrap_err().to_string();
    assert!(error.contains("KAFKA_PASSWORD"));
}
#[test] fn rejects_empty_variable_value() {
    let resolver = EnvironmentSecretResolver::with_lookup(|_| Some(String::new()));
    assert!(resolver.resolve_secret(&"env:KAFKA_PASSWORD".parse().unwrap()).is_err());
}

// file_resolver.rs
#[test] fn trims_trailing_newline() { /* tempfile com "s3cret\n" → "s3cret"; também "s3cret\r\n" */ }
#[test] fn rejects_file_larger_than_limit() { /* arquivo com MAXIMUM_SECRET_FILE_BYTES + 1 bytes → Err */ }
#[test] fn reports_missing_file_by_path() { /* erro contém o caminho */ }

// composite_resolver.rs
#[test] fn dispatches_to_source_matching_reference() { /* env e file resolvem pela fonte certa */ }
#[test] fn suggests_alternatives_when_keyring_is_unavailable() {
    // CompositeSecretResolver com keyring = None; erro contém "env:" e "file:"
}
```

- [ ] **Step 2: Rodar e ver falhar.** Run: `cargo test -p kafcli-core secret` — Expected: FAIL (tipos não definidos).

- [ ] **Step 3: Implementar `error.rs` e os tipos de `secret/`** conforme Interfaces. Regras: `FromStr` divide no primeiro `:`; o erro de esquema desconhecido usa texto fixo (`"expected env:VAR, file:/path or keyring:service/account"`), nunca o input. `FileSecretResolver` checa `metadata().len()` antes de ler e lê com `File::take(MAXIMUM_SECRET_FILE_BYTES + 1)`; remove só `\n`/`\r\n` final. Cada resolver retorna `CoreError::SecretResolution` quando recebe uma variante que não é a sua. `KeyringSecretResolver` usa a API de entrada por `(service, account)` do crate `keyring` 4.x (consultar docs.rs da versão travada no `Cargo.lock`); qualquer erro do keyring vira `SecretResolution("keyring unavailable for <service>/<account>; use env: or file: instead")`. `with_default_sources()` inclui keyring só com a feature ligada.

- [ ] **Step 4: Rodar e ver passar.** Run: `cargo test -p kafcli-core secret` — Expected: PASS. Também: `cargo test -p kafcli-core --no-default-features secret` (keyring desligado) PASS.

- [ ] **Step 5: Commit.** `git commit -am "feat(core): add secret references and resolvers"`

---

### Task 3: Modelo de configuração e carregamento

**Files:**
- Create: `crates/kafcli-core/src/config/{mod,model,loader,error_description,paths}.rs`
- Modify: `crates/kafcli-core/src/lib.rs` (`pub mod config;`)

**Interfaces:**
- Consumes: `SecretReference`, `CoreError`, `CoreResult` (Task 2).
- Produces:
  ```rust
  // model.rs — todos derivam Clone, Debug, PartialEq, Deserialize, Serialize
  pub struct Configuration {
      pub default_profile: Option<String>,
      #[serde(default)] pub profiles: std::collections::BTreeMap<String, Profile>,
  }
  pub struct Profile {
      pub brokers: Vec<String>,
      pub auth: AuthConfig,
      #[serde(default)] pub protected: bool,
      #[serde(default, skip_serializing_if = "Option::is_none")]
      pub schema_registry: Option<toml::Table>, // tipado no plano 02
  }
  #[serde(tag = "type")]
  pub enum AuthConfig {
      #[serde(rename = "none")] Plaintext,
      #[serde(rename = "tls")] Tls(TlsAuthConfig),
      #[serde(rename = "mtls")] MutualTls(MutualTlsAuthConfig),
      #[serde(rename = "sasl-plain")] SaslPlain(SaslCredentialsConfig),
      #[serde(rename = "sasl-scram-256")] SaslScram256(SaslCredentialsConfig),
      #[serde(rename = "sasl-scram-512")] SaslScram512(SaslCredentialsConfig),
      #[serde(rename = "azure-connection-string")] AzureConnectionString(AzureConnectionStringConfig),
  }
  impl AuthConfig { pub fn type_name(&self) -> &'static str; } // "none", "tls", ...
  #[serde(deny_unknown_fields)] pub struct TlsAuthConfig { pub ca_location: Option<PathBuf> }
  #[serde(deny_unknown_fields)] pub struct MutualTlsAuthConfig {
      pub ca_location: Option<PathBuf>, pub certificate_location: PathBuf,
      pub key_location: PathBuf, pub key_password_ref: Option<SecretReference>,
  }
  #[serde(deny_unknown_fields)] pub struct SaslCredentialsConfig {
      pub username: String, pub secret_ref: SecretReference,
      pub ca_location: Option<PathBuf>,
      #[serde(default = "enabled")] pub use_tls: bool, // false = SASL_PLAINTEXT (dev/local)
  }
  #[serde(deny_unknown_fields)] pub struct AzureConnectionStringConfig { pub secret_ref: SecretReference }

  // loader.rs
  pub const ENVIRONMENT_PREFIX: &str = "KAFCLI_";
  pub fn load_configuration(config_path: &Path) -> CoreResult<Configuration>;
  // error_description.rs
  pub fn describe_figment_error(error: &figment::Error) -> String;
  // paths.rs
  pub fn default_config_path() -> CoreResult<PathBuf>;
  pub fn default_state_directory() -> CoreResult<PathBuf>;
  ```

- [ ] **Step 1: Escrever os testes que falham.** Helper de teste: `write_config(contents) -> tempfile::TempDir` com `config.toml`.

```rust
// loader.rs
#[test] fn parses_profiles_for_every_wave_a_auth_type() {
    // TOML com perfis: local (none), tls, mtls, plain, scram256, scram512 (use_tls = false), eh (azure-connection-string)
    // e um bloco [profiles.eh.schema_registry] type = "azure"
    // assert: configuration.profiles.len() == 7
    // assert: profiles["scram512"].auth == AuthConfig::SaslScram512(SaslCredentialsConfig { username: "app".into(),
    //     secret_ref: "env:KAFKA_PASSWORD".parse().unwrap(), ca_location: None, use_tls: false })
    // assert: profiles["eh"].schema_registry.is_some()
}
#[test] fn missing_file_yields_empty_configuration() {
    assert_eq!(load_configuration(Path::new("/nonexistent/kafcli.toml")).unwrap(), Configuration::default());
}
#[test] fn rejects_plaintext_password_field_without_echoing_it() {
    // auth = { type = "sasl-plain", username = "u", secret_ref = "env:X", password = "hunter2" }
    let error = /* load */ .unwrap_err().to_string();
    assert!(error.contains("password"));
    assert!(!error.contains("hunter2"));
}
#[test] fn invalid_secret_reference_error_hides_value() {
    // secret_ref = "Endpoint=sb://ns/;SharedAccessKey=TOPSECRET"
    let error = /* load */ .unwrap_err().to_string();
    assert!(!error.contains("TOPSECRET"));
    assert!(error.contains("secret_ref") || error.contains("secret reference"));
}
#[test] fn file_overrides_environment() {
    figment::Jail::expect_with(|jail| {
        jail.set_env("KAFCLI_DEFAULT_PROFILE", "from-env");
        jail.create_file("config.toml", r#"default_profile = "from-file""#)?;
        assert_eq!(load_configuration(Path::new("config.toml")).unwrap().default_profile.as_deref(), Some("from-file"));
        Ok(())
    });
}
#[test] fn environment_fills_values_absent_from_file() {
    // KAFCLI_DEFAULT_PROFILE=from-env, arquivo vazio → Some("from-env")
}
#[test] fn rejects_unknown_auth_type_naming_the_type_field() {
    // auth = { type = "kerberos" } → erro contém "type"
}
```

- [ ] **Step 2: Rodar e ver falhar.** Run: `cargo test -p kafcli-core config::loader` — Expected: FAIL.

- [ ] **Step 3: Implementar.** `load_configuration`: se o arquivo não existe, `Configuration::default()` (o env ainda entra); senão `Figment::new().merge(Env::prefixed(ENVIRONMENT_PREFIX).split("__")).merge(Toml::file(path)).extract()`. Excluir `KAFCLI_CONFIG` do env do figment (`Env::...ignore(&["CONFIG"])`). Toda falha do figment vira `CoreError::Configuration(describe_figment_error(&error))`. `describe_figment_error` monta `"<caminho.da.chave>: <descrição>"` usando `error.path` e um texto por `error.kind`: `Message(m)` → `m` (as mensagens vêm do nosso `TryFrom`, já seguras), `MissingField(f)` → `missing field '{f}'`, `UnknownField(f, _)` → `unknown field '{f}'`, `UnknownVariant(v, _)` → `unknown value '{v}' for this field` (nome de variante/`type`, não segredo), `InvalidType(..)`/`InvalidValue(..)`/demais → `invalid value` **sem o valor atual**. Itera `error.clone().into_iter()` para juntar múltiplos erros com `; `. Se `deny_unknown_fields` não funcionar com newtype variants de enum tagged, troque para struct variants com o mesmo nome de campos — os testes definem o comportamento.

- [ ] **Step 4: Rodar e ver passar.** Run: `cargo test -p kafcli-core config` — Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -am "feat(core): add configuration model and loader"`

---

### Task 4: Seleção de perfil, overrides, validação e escrita da config

**Files:**
- Create: `crates/kafcli-core/src/config/{selection,overrides,validation,writer}.rs`

**Interfaces:**
- Consumes: `Configuration`, `Profile`, `AuthConfig` (Task 3).
- Produces:
  ```rust
  // selection.rs
  pub struct SelectedProfile { pub name: String, pub profile: Profile }
  pub fn select_profile(configuration: &Configuration, requested_name: Option<&str>) -> CoreResult<SelectedProfile>;
  // overrides.rs
  #[derive(Default)] pub struct ProfileOverrides { pub brokers: Option<Vec<String>> }
  pub fn apply_profile_overrides(profile: Profile, overrides: &ProfileOverrides) -> Profile;
  // validation.rs
  pub fn validate_profile(profile: &Profile, insecure_requested: bool) -> CoreResult<()>;
  // writer.rs
  pub fn save_configuration(config_path: &Path, configuration: &Configuration) -> CoreResult<()>;
  ```

- [ ] **Step 1: Escrever os testes que falham.**

```rust
// selection.rs
#[test] fn selects_requested_profile() {}            // requested "prd" → name == "prd"
#[test] fn falls_back_to_default_profile() {}        // requested None, default_profile = "dev" → "dev"
#[test] fn reports_missing_requested_profile() {
    assert!(matches!(select_profile(&configuration, Some("nope")), Err(CoreError::ProfileNotFound(name)) if name == "nope"));
}
#[test] fn reports_missing_default_profile_by_name() {} // default_profile = "ghost" inexistente → ProfileNotFound("ghost")
#[test] fn reports_when_nothing_is_selected() {}       // sem requested e sem default → NoProfileSelected

// overrides.rs
#[test] fn brokers_override_replaces_profile_brokers() {}
#[test] fn empty_overrides_keep_profile_unchanged() {}

// validation.rs
#[test] fn rejects_plaintext_auth_on_protected_profile() {}
#[test] fn rejects_insecure_on_protected_profile() {}
#[test] fn rejects_disabled_tls_on_protected_profile() {}  // SaslScram512 com use_tls = false + protected
#[test] fn rejects_profile_without_brokers() {}
#[test] fn accepts_tls_profile_marked_protected() {}

// writer.rs
#[test] fn saved_configuration_loads_back_identically() {} // save → load_configuration → assert_eq!
#[test] fn creates_missing_parent_directories() {}
#[test] fn leaves_no_temporary_file_behind() {}             // só config.toml no diretório após salvar
```

- [ ] **Step 2: Rodar e ver falhar.** Run: `cargo test -p kafcli-core config` — Expected: FAIL nos novos testes.

- [ ] **Step 3: Implementar.** `validate_profile` retorna `ProfileValidation` com texto fixo por regra (`"auth type 'none' is not allowed on a protected profile"`, `"--insecure is not allowed on a protected profile"`, `"use_tls = false is not allowed on a protected profile"`, `"profile has no brokers"`). `save_configuration`: `create_dir_all(parent)`, serializa com `toml::to_string_pretty`, grava em `tempfile::NamedTempFile::new_in(parent)` e `persist(config_path)` (rename atômico). `tempfile` passa a ser dependência normal do core.

- [ ] **Step 4: Rodar e ver passar.** Run: `cargo test -p kafcli-core config` — Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -am "feat(core): add profile selection, overrides, validation and writer"`

---

### Task 5: Propriedades de cliente e providers de auth da onda A

**Files:**
- Create: `crates/kafcli-core/src/auth/{mod,client_properties,provider,tls_settings,plaintext_provider,tls_provider,mutual_tls_provider,sasl_plain_provider,sasl_scram_provider,azure_connection_string_provider,provider_factory}.rs`
- Modify: `crates/kafcli-core/src/lib.rs` (`pub mod auth;`), `crates/kafcli-core/Cargo.toml` (`rdkafka`)

**Interfaces:**
- Consumes: `SecretResolver`, `FixedSecretResolver` (Task 2); structs de auth (Task 3).
- Produces:
  ```rust
  // client_properties.rs
  pub enum PropertyValue { Plain(String), Secret(secrecy::SecretString) }
  #[derive(Default)] pub struct ClientProperties { /* BTreeMap<String, PropertyValue> */ }
  impl ClientProperties {
      pub fn set_plain(&mut self, key: &str, value: impl Into<String>);
      pub fn set_secret(&mut self, key: &str, value: secrecy::SecretString);
      pub fn plain_value(&self, key: &str) -> Option<&str>;
      pub fn is_secret(&self, key: &str) -> bool;
      pub fn apply_to(&self, client_config: &mut rdkafka::ClientConfig);
  }
  impl std::fmt::Debug for ClientProperties {} // segredos como "***"

  // provider.rs
  pub trait AuthProvider {
      fn build_client_properties(&self, resolver: &dyn SecretResolver) -> CoreResult<ClientProperties>;
      /// Nome exibível do principal configurado (nunca o segredo).
      fn describe_principal(&self) -> Option<String> { None }
  }
  // tls_settings.rs
  pub fn apply_tls_settings(properties: &mut ClientProperties, security_protocol: &str, ca_location: Option<&Path>);
  // provider_factory.rs
  pub fn create_auth_provider(auth: &AuthConfig) -> Box<dyn AuthProvider>;
  // Um struct por arquivo: PlaintextAuthProvider, TlsAuthProvider, MutualTlsAuthProvider,
  // SaslPlainAuthProvider, SaslScramAuthProvider { mechanism: ScramMechanism }, AzureConnectionStringAuthProvider.
  pub enum ScramMechanism { Sha256, Sha512 } // "SCRAM-SHA-256" / "SCRAM-SHA-512"
  ```

Valores fixados pela spec / librdkafka:

| Provider | `security.protocol` | Outras propriedades |
|---|---|---|
| none | `PLAINTEXT` | — |
| tls | `SSL` | `ssl.ca.location` se informado; `ssl.endpoint.identification.algorithm=https` |
| mtls | `SSL` | + `ssl.certificate.location`, `ssl.key.location`, `ssl.key.password` (secret, se `key_password_ref`) |
| sasl-plain | `SASL_SSL` (ou `SASL_PLAINTEXT` se `use_tls=false`) | `sasl.mechanism=PLAIN`, `sasl.username`, `sasl.password` (secret) |
| sasl-scram-256/512 | idem | `sasl.mechanism=SCRAM-SHA-256`/`SCRAM-SHA-512`, `sasl.username`, `sasl.password` (secret) |
| azure-connection-string | `SASL_SSL` | `sasl.mechanism=PLAIN`, `sasl.username=$ConnectionString`, `sasl.password` (secret) |

`ssl.endpoint.identification.algorithm=https` vale para todo protocolo `SSL`/`SASL_SSL` (aplicado por `apply_tls_settings`). `describe_principal`: SASL → `Some(username)`; azure-connection-string → `Some("$ConnectionString")`; demais → `None`.

- [ ] **Step 1: Escrever os testes que falham** (um módulo de teste por provider, usando `FixedSecretResolver::returning("s3cret")`):

```rust
// sasl_scram_provider.rs
#[test] fn configures_scram_sha_512_over_tls() {
    let provider = SaslScramAuthProvider::new(ScramMechanism::Sha512, scram_config(/*use_tls*/ true));
    let properties = provider.build_client_properties(&FixedSecretResolver::returning("s3cret")).unwrap();
    assert_eq!(properties.plain_value("security.protocol"), Some("SASL_SSL"));
    assert_eq!(properties.plain_value("sasl.mechanism"), Some("SCRAM-SHA-512"));
    assert_eq!(properties.plain_value("sasl.username"), Some("app"));
    assert!(properties.is_secret("sasl.password"));
    assert_eq!(properties.plain_value("ssl.endpoint.identification.algorithm"), Some("https"));
}
#[test] fn uses_sasl_plaintext_when_tls_disabled() {}      // security.protocol == "SASL_PLAINTEXT", sem ssl.*
// client_properties.rs
#[test] fn debug_output_masks_secret_values() {
    let mut properties = ClientProperties::default();
    properties.set_secret("sasl.password", "s3cret".to_string().into());
    let rendered = format!("{properties:?}");
    assert!(!rendered.contains("s3cret") && rendered.contains("***"));
}
#[test] fn applies_secret_values_to_client_config() {}   // apply_to → client_config.get("sasl.password") == Some("s3cret")
// plaintext_provider.rs
#[test] fn configures_plaintext_protocol() {}
// tls_provider.rs
#[test] fn sets_ca_location_and_hostname_verification() {}
#[test] fn omits_ca_location_when_not_configured() {}
// mutual_tls_provider.rs
#[test] fn configures_client_certificate_and_key() {}
#[test] fn resolves_key_password_as_secret() {}
// sasl_plain_provider.rs
#[test] fn configures_plain_mechanism_with_username() {}
// azure_connection_string_provider.rs
#[test] fn uses_connection_string_literal_as_username() {
    // sasl.username == "$ConnectionString", security.protocol == "SASL_SSL", sasl.mechanism == "PLAIN", sasl.password secreto
}
// provider_factory.rs
#[test] fn creates_provider_matching_each_auth_type() {} // para cada AuthConfig, security.protocol esperado
#[test] fn propagates_secret_resolution_failure() {}     // resolver que falha → Err, mensagem sem valor
```

- [ ] **Step 2: Rodar e ver falhar.** Run: `cargo test -p kafcli-core auth` — Expected: FAIL.

- [ ] **Step 3: Implementar** conforme Interfaces e tabela. `ClientProperties::apply_to` é o **único** ponto que chama `expose_secret()` no crate. A factory é um `match` exaustivo sobre `AuthConfig`; nenhum provider conhece outro.

- [ ] **Step 4: Rodar e ver passar.** Run: `cargo test -p kafcli-core auth` — Expected: PASS. `cargo clippy --workspace --all-targets -- -D warnings` limpo. `grep -rn "expose_secret" crates/kafcli-core/src | grep -v "#\[cfg(test)\]"` mostra só `client_properties.rs` (fora dos módulos de teste).

- [ ] **Step 5: Commit.** `git commit -am "feat(core): add wave A auth providers"`

---

### Task 6: Montagem do ClientConfig e probe de conexão

**Files:**
- Create: `crates/kafcli-core/src/connection/{mod,client_config_builder,connection_probe}.rs`
- Modify: `crates/kafcli-core/src/lib.rs` (`pub mod connection;`)

**Interfaces:**
- Consumes: `Profile` (Task 3), `validate_profile` (Task 4), `create_auth_provider`, `ClientProperties` (Task 5), `SecretResolver` (Task 2).
- Produces:
  ```rust
  // client_config_builder.rs
  pub const CLIENT_IDENTIFIER: &str = "kafcli";
  pub struct ConnectionSettings { pub insecure: bool }
  pub fn build_client_config(profile: &Profile, resolver: &dyn SecretResolver, settings: &ConnectionSettings)
      -> CoreResult<rdkafka::ClientConfig>;
  // connection_probe.rs
  #[derive(Debug, serde::Serialize)]
  pub struct ConnectionReport {
      pub profile_name: String,
      pub auth_type: String,
      pub principal: Option<String>,
      pub broker_count: usize,
      pub visible_topics: Vec<String>, // ordenados
  }
  pub fn probe_profile_connection(profile_name: &str, profile: &Profile, resolver: &dyn SecretResolver,
      settings: &ConnectionSettings, timeout: std::time::Duration) -> CoreResult<ConnectionReport>;
  ```

- [ ] **Step 1: Escrever os testes que falham.**

```rust
// client_config_builder.rs
#[test] fn joins_brokers_and_sets_client_identifier() {
    // brokers ["a:9092", "b:9092"] → bootstrap.servers == "a:9092,b:9092", client.id == "kafcli"
}
#[test] fn insecure_disables_certificate_and_hostname_verification() {
    // tls profile não protegido + insecure → enable.ssl.certificate.verification == "false",
    // ssl.endpoint.identification.algorithm == "none"
}
#[test] fn refuses_insecure_on_protected_profile() {}   // Err(ProfileValidation)
// connection_probe.rs
#[test] fn unreachable_broker_fails_within_timeout() {
    let started = std::time::Instant::now();
    let result = probe_profile_connection("local", &plaintext_profile("127.0.0.1:1"),
        &FixedSecretResolver::returning("unused"), &ConnectionSettings { insecure: false },
        std::time::Duration::from_secs(2));
    assert!(matches!(result, Err(CoreError::Kafka(_))));
    assert!(started.elapsed() < std::time::Duration::from_secs(6));
}
```

- [ ] **Step 2: Rodar e ver falhar.** Run: `cargo test -p kafcli-core connection` — Expected: FAIL.

- [ ] **Step 3: Implementar.** `build_client_config` chama `validate_profile(profile, settings.insecure)` primeiro, depois aplica `bootstrap.servers`, `client.id`, as propriedades do provider, e `socket.connection.setup.timeout.ms` igual ao timeout padrão de 10000. Com `insecure`, sobrescreve as duas propriedades do teste e emite `tracing::warn!("TLS verification disabled by --insecure")`. `probe_profile_connection` cria um `rdkafka::consumer::BaseConsumer` (sem `group.id`, sem subscribe), chama `fetch_metadata(None, timeout)` uma única vez e descarta o consumer ao sair da função (sem threads extras, sem retry em loop). Erros do rdkafka viram `CoreError::Kafka(error.to_string())` — mensagens do librdkafka não contêm senha.

- [ ] **Step 4: Rodar e ver passar.** Run: `cargo test -p kafcli-core connection` — Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -am "feat(core): build client config and probe connection"`

---

### Task 7: Guard de operações

**Files:**
- Create: `crates/kafcli-core/src/guard/{mod,operation_level,guard_request,confirmation_prompter,guard_enforcer}.rs`
- Modify: `crates/kafcli-core/src/lib.rs` (`pub mod guard;`)

**Interfaces:**
- Produces:
  ```rust
  pub enum OperationLevel { Read, Write, Destructive }
  pub struct GuardRequest {
      pub level: OperationLevel,
      pub description: String,          // ex.: "reset offsets of group pedidos-service"
      pub confirmation_target: String,  // valor a digitar em Destructive+protected (ex.: nome do grupo)
      pub assume_yes: bool,             // --yes
      pub execute: bool,                // --execute
  }
  pub trait ConfirmationPrompter {
      fn is_interactive(&self) -> bool;
      fn confirm_yes_no(&self, message: &str) -> CoreResult<bool>;
      fn confirm_typed_value(&self, message: &str, expected_value: &str) -> CoreResult<bool>;
  }
  #[derive(Debug, PartialEq, Eq)] pub enum GuardOutcome { Proceed, DryRun }
  pub fn enforce_guard(request: &GuardRequest, profile_protected: bool, prompter: &dyn ConfirmationPrompter)
      -> CoreResult<GuardOutcome>;
  ```

Tabela de decisão (spec §9, com as decisões conservadoras do design):

| Nível | protected | Condição | Resultado |
|---|---|---|---|
| Read | qualquer | — | `Proceed` |
| Write | false | — | `Proceed` |
| Write | true | `assume_yes` | `Proceed` |
| Write | true | não interativo | `Err(OperationAborted("confirmation required on protected profile: pass --yes"))` |
| Write | true | prompt y/N → não | `Err(OperationAborted("cancelled by user"))` |
| Destructive | qualquer | `!execute` | `DryRun` |
| Destructive | false | `execute` | `Proceed` |
| Destructive | true | `execute`, não interativo | `Err(OperationAborted("typed confirmation required on protected profile; run interactively"))` — `--yes` **não** dispensa |
| Destructive | true | `execute`, digitado ≠ alvo | `Err(OperationAborted("typed value did not match"))` |

- [ ] **Step 1: Escrever os testes que falham** com um `ScriptedPrompter { interactive: bool, yes_no_answer: bool, typed_answer: bool, calls: Cell<u32> }`: um teste por linha da tabela, nomeados `read_always_proceeds`, `write_on_unprotected_proceeds_without_prompt`, `write_on_protected_proceeds_with_assume_yes`, `write_on_protected_aborts_without_terminal`, `write_on_protected_aborts_when_user_declines`, `destructive_without_execute_is_dry_run`, `destructive_on_unprotected_proceeds_with_execute`, `destructive_on_protected_ignores_assume_yes_without_terminal`, `destructive_on_protected_aborts_on_mismatch`, `destructive_on_protected_proceeds_on_match`. Nos casos `Proceed` sem prompt, assert `calls == 0`.

- [ ] **Step 2: Rodar e ver falhar.** Run: `cargo test -p kafcli-core guard` — Expected: FAIL.

- [ ] **Step 3: Implementar `enforce_guard`** seguindo a tabela. A mensagem do prompt digitado é `"Type '<confirmation_target>' to confirm: <description>"`.

- [ ] **Step 4: Rodar e ver passar.** Run: `cargo test -p kafcli-core guard` — Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -am "feat(core): add operation guard"`

---

### Task 8: Audit log

**Files:**
- Create: `crates/kafcli-core/src/audit/{mod,audit_record,audit_writer}.rs`
- Modify: `crates/kafcli-core/src/lib.rs` (`pub mod audit;`)

**Interfaces:**
- Produces:
  ```rust
  #[derive(Debug, Clone, Copy, serde::Serialize)] #[serde(rename_all = "kebab-case")]
  pub enum AuditOutcome { Executed, DryRun, Aborted, Failed }
  #[derive(Debug, serde::Serialize)]
  pub struct AuditRecord {
      pub timestamp: String,   // RFC 3339 UTC
      pub operator: String,    // env USER, senão USERNAME, senão "unknown"
      pub profile: String,
      pub command: String,     // ex.: "offsets set"
      pub target: String,      // ex.: "group=pedidos-service topic=pedidos"
      pub before: Option<serde_json::Value>,
      pub after: Option<serde_json::Value>,
      pub outcome: AuditOutcome,
  }
  impl AuditRecord { pub fn new(profile: &str, command: &str, target: &str, outcome: AuditOutcome) -> Self; }
  pub trait AuditWriter { fn append_record(&self, record: &AuditRecord) -> CoreResult<()>; }
  pub const AUDIT_LOG_FILE_NAME: &str = "audit.log";
  pub struct JsonLinesAuditWriter { /* log_path: PathBuf */ }
  impl JsonLinesAuditWriter { pub fn in_directory(state_directory: &Path) -> Self; }
  ```

- [ ] **Step 1: Escrever os testes que falham.**

```rust
#[test] fn appends_one_json_line_per_record() {
    // dois append_record → arquivo com 2 linhas; cada linha parseia como JSON com "outcome" e "timestamp"
}
#[test] fn creates_state_directory_when_missing() {}
#[test] fn timestamp_is_rfc3339_utc() {}   // termina com "Z" e parseia com time::format_description::well_known::Rfc3339
#[test] fn serializes_outcome_in_kebab_case() {} // AuditOutcome::DryRun → "dry-run"
```

- [ ] **Step 2: Rodar e ver falhar.** Run: `cargo test -p kafcli-core audit` — Expected: FAIL.

- [ ] **Step 3: Implementar.** `append_record` cria o diretório, abre com `OpenOptions::new().create(true).append(true)`, grava `serde_json::to_string(record)? + "\n"` numa única chamada `write_all` e fecha ao sair do escopo (nenhum handle guardado no struct).

- [ ] **Step 4: Rodar e ver passar.** Run: `cargo test -p kafcli-core audit` — Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -am "feat(core): add JSON lines audit log"`

---

### Task 9: CLI — argumentos, saída, prompter, exit codes e `profile list|show|add|use`

**Files:**
- Create: `crates/kafcli/src/{exit_code,logging,execution_context}.rs`, `src/cli/{mod,arguments,profile_arguments}.rs`, `src/output/{mod,output_format,renderer,profile_views}.rs`, `src/prompt/{mod,terminal_prompter}.rs`, `src/commands/mod.rs`, `src/commands/profile/{mod,list_profiles,show_profile,add_profile,use_profile}.rs`
- Modify: `crates/kafcli/src/main.rs` (substitui o mínimo da Task 1), `crates/kafcli/Cargo.toml`
- Test: `crates/kafcli/tests/cli_profile.rs`

**Interfaces:**
- Consumes: `load_configuration`, `save_configuration`, `select_profile`, `default_config_path`, `Configuration`, `Profile`, `AuthConfig` (Tasks 3–4); `ConfirmationPrompter` (Task 7); `CoreError` (Task 2).
- Produces:
  ```rust
  // cli/arguments.rs
  #[derive(clap::Parser)] #[command(name = "kafcli", version, about = "Produce, consume and operate Kafka topics")]
  pub struct CommandLineArguments {
      #[command(flatten)] pub global: GlobalArguments,
      #[command(subcommand)] pub command: TopLevelCommand,
  }
  #[derive(clap::Args)]
  pub struct GlobalArguments {
      #[arg(long, global = true)] pub profile: Option<String>,
      #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Table)] pub output: OutputFormat,
      #[arg(short = 'v', long = "verbose", global = true, action = clap::ArgAction::Count)] pub verbosity: u8,
      #[arg(long, global = true, env = "KAFCLI_CONFIG")] pub config: Option<PathBuf>,
      #[arg(long, global = true, value_delimiter = ',')] pub brokers: Option<Vec<String>>,
      #[arg(long, global = true)] pub insecure: bool,
  }
  #[derive(clap::Subcommand)] pub enum TopLevelCommand {
      /// Manage connection profiles
      Profile { #[command(subcommand)] command: ProfileCommand },
  }
  // cli/profile_arguments.rs
  #[derive(clap::Subcommand)] pub enum ProfileCommand {
      List,
      Show { name: Option<String> },
      Add(AddProfileArguments),
      Use { name: String },
      Test { name: Option<String>, #[arg(long, default_value_t = 10)] timeout_seconds: u64 }, // handler na Task 10
  }
  #[derive(clap::Args)] pub struct AddProfileArguments {
      pub name: String,
      #[arg(long, value_delimiter = ',', required = true)] pub brokers: Vec<String>,
      #[arg(long, value_enum)] pub auth_type: AuthTypeArgument,
      #[arg(long)] pub username: Option<String>,
      #[arg(long)] pub secret_ref: Option<String>,
      #[arg(long)] pub ca_location: Option<PathBuf>,
      #[arg(long)] pub certificate_location: Option<PathBuf>,
      #[arg(long)] pub key_location: Option<PathBuf>,
      #[arg(long)] pub key_password_ref: Option<String>,
      #[arg(long)] pub no_tls: bool,
      #[arg(long)] pub protected: bool,
      #[arg(long)] pub set_default: bool,
      #[arg(long)] pub force: bool,
  }
  #[derive(clap::ValueEnum, Clone, Copy)] pub enum AuthTypeArgument {
      None, Tls, Mtls, SaslPlain,
      #[value(name = "sasl-scram-256")] SaslScram256,
      #[value(name = "sasl-scram-512")] SaslScram512,
      AzureConnectionString,
  }
  // output/output_format.rs
  #[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)] pub enum OutputFormat { Table, Json }
  // output/renderer.rs
  pub const OUTPUT_SCHEMA_VERSION: u32 = 1;
  pub trait TableRenderable { fn render_table(&self) -> String; }
  pub fn render_output<T: serde::Serialize + TableRenderable>(value: &T, format: OutputFormat) -> anyhow::Result<String>;
  // output/profile_views.rs
  #[derive(serde::Serialize)] pub struct ProfileSummary { pub name: String, pub brokers: Vec<String>, pub auth_type: String, pub protected: bool, pub is_default: bool }
  #[derive(serde::Serialize)] pub struct ProfileListView { pub profiles: Vec<ProfileSummary> }
  #[derive(serde::Serialize)] pub struct ProfileDetailView { pub name: String, pub brokers: Vec<String>, pub auth_type: String,
      pub principal: Option<String>, pub secret_references: Vec<String>, pub protected: bool, pub has_schema_registry: bool }
  // execution_context.rs
  pub struct ExecutionContext { pub config_path: PathBuf, pub global: GlobalArguments }
  impl ExecutionContext { pub fn from_global_arguments(global: GlobalArguments) -> anyhow::Result<Self>; }
  // exit_code.rs
  pub enum KafcliExitCode { Success = 0, Failure = 1, LimitExceeded = 2, Aborted = 3 }
  pub fn exit_code_for_error(error: &anyhow::Error) -> std::process::ExitCode; // CoreError::OperationAborted → 3, resto → 1
  // logging.rs
  pub fn initialize_logging(verbosity: u8); // 0=warn, 1=info, 2+=debug; sempre em stderr
  // prompt/terminal_prompter.rs
  pub const MAXIMUM_CONFIRMATION_INPUT_BYTES: u64 = 1024;
  pub struct TerminalPrompter;
  impl ConfirmationPrompter for TerminalPrompter {} // is_interactive = stdin e stderr são TTY
  // commands/profile/*.rs — uma função pública por arquivo
  pub fn list_profiles(context: &ExecutionContext) -> anyhow::Result<ProfileListView>;
  pub fn show_profile(context: &ExecutionContext, name: Option<&str>) -> anyhow::Result<ProfileDetailView>;
  pub fn add_profile(context: &ExecutionContext, arguments: &AddProfileArguments) -> anyhow::Result<ProfileSummary>;
  pub fn use_profile(context: &ExecutionContext, name: &str) -> anyhow::Result<ProfileSummary>;
  ```

- [ ] **Step 1: Escrever os testes que falham** em `crates/kafcli/tests/cli_profile.rs`. Helper `kafcli(config_dir) -> assert_cmd::Command` que passa `--config <dir>/config.toml` e limpa `KAFCLI_*` do ambiente.

```rust
#[test] fn list_on_first_use_shows_no_profiles() {
    // sem arquivo; `--output json profile list` → data.profiles == []
}
#[test] fn add_then_list_shows_profile() {
    // profile add dev --brokers localhost:9092 --auth-type none --set-default
    // profile list --output json → data.profiles[0] == {name:"dev", auth_type:"none", is_default:true, ...}
}
#[test] fn json_output_carries_schema_version() {
    // parse stdout; value["kafcli_output"] == 1
}
#[test] fn add_refuses_to_overwrite_without_force() {
    // segundo add com o mesmo nome → failure, stderr contém "already exists", exit code 1
}
#[test] fn add_requires_credentials_for_sasl() {
    // --auth-type sasl-scram-512 sem --username → failure, stderr contém "--username"
}
#[test] fn add_rejects_plaintext_on_protected_profile() {
    // --auth-type none --protected → failure, stderr contém "not allowed on a protected profile"
}
#[test] fn show_never_prints_resolved_secret() {
    // env KAFKA_PASSWORD=s3cret-value; profile com secret_ref env:KAFKA_PASSWORD
    // `profile show prd -vv` e `--output json profile show prd`: stdout+stderr não contêm "s3cret-value"; stdout contém "env:KAFKA_PASSWORD"
}
#[test] fn use_sets_default_profile() {}
#[test] fn unknown_profile_exits_with_failure() {
    // profile show nope → exit 1, stderr contém "profile 'nope' not found"
}
#[test] fn every_profile_subcommand_help_has_examples() {
    for subcommand in ["list", "show", "add", "use", "test"] {
        // `profile <subcommand> --help` → stdout contém "Examples:"
    }
}
```

E no `terminal_prompter.rs`: `#[test] fn reports_non_interactive_when_stdin_is_not_terminal()` (no `cargo test`, stdin não é TTY → `is_interactive() == false`).

- [ ] **Step 2: Rodar e ver falhar.** Run: `cargo test -p kafcli --test cli_profile` — Expected: FAIL.

- [ ] **Step 3: Implementar.** Pontos não determinados pelas assinaturas:
  - `main.rs` é o ponto de composição: `parse` → `initialize_logging` → `ExecutionContext::from_global_arguments` → `match` do comando → `render_output` → `println!`. Em erro: `eprintln!("error: {error:#}")` e `exit_code_for_error`. Nenhuma lógica de negócio no `main`.
  - `render_output` em JSON: `serde_json::to_string_pretty(&json!({"kafcli_output": OUTPUT_SCHEMA_VERSION, "data": value}))`. Tabela via `comfy_table::Table` com preset `UTF8_FULL_CONDENSED`.
  - `show_profile` **não resolve segredos**: lista as referências (`SecretReference::to_string()`) em `secret_references` e o principal de `create_auth_provider(...).describe_principal()`.
  - `add_profile` monta o `AuthConfig` a partir de `AddProfileArguments` (erro `"--username is required for auth type sasl-scram-512"` etc.), roda `validate_profile(&profile, false)` antes de salvar, e usa `save_configuration`. `--no-tls` vira `use_tls = false`.
  - `TerminalPrompter` lê uma linha de stdin com `take(MAXIMUM_CONFIRMATION_INPUT_BYTES)`; y/N aceita `y`/`yes` (case-insensitive); prompts vão para stderr.
  - Cada subcomando tem `#[command(after_help = "Examples:\n  kafcli profile add dev --brokers localhost:9092 --auth-type none --set-default\n  ...")]` com pelo menos um exemplo real; `test` já recebe o texto de ajuda aqui, mesmo com o handler vindo na Task 10 (até lá, retorna `anyhow::bail!("not implemented")` — removido na Task 10).

- [ ] **Step 4: Rodar e ver passar.** Run: `cargo test -p kafcli` — Expected: PASS. `cargo clippy --workspace --all-targets -- -D warnings` limpo.

- [ ] **Step 5: Commit.** `git commit -am "feat(cli): add profile list, show, add and use commands"`

---

### Task 10: `profile test` e fixtures de integração (plaintext, SCRAM, mTLS)

**Files:**
- Create: `crates/kafcli/src/commands/profile/test_profile.rs`, `crates/kafcli/src/output/connection_report_view.rs`
- Create: `tests/fixtures/compose.yaml`, `tests/fixtures/generate-certificates.sh`, `tests/fixtures/README.md`
- Modify: `crates/kafcli/src/main.rs` (ligar o handler), `.github/workflows/ci.yml` (job `integration`)
- Test: `crates/kafcli/tests/integration_connection.rs`, caso extra em `crates/kafcli/tests/cli_profile.rs`

**Interfaces:**
- Consumes: `probe_profile_connection`, `ConnectionReport`, `ConnectionSettings` (Task 6); `apply_profile_overrides`, `ProfileOverrides` (Task 4); `CompositeSecretResolver::with_default_sources` (Task 2); `ExecutionContext`, `render_output`, `TableRenderable` (Task 9).
- Produces:
  ```rust
  pub fn test_profile(context: &ExecutionContext, name: Option<&str>, timeout: std::time::Duration)
      -> anyhow::Result<ConnectionReport>;
  impl TableRenderable for ConnectionReport {}
  ```
- Fixtures (portas fixas no host, usadas pelos testes):

| Serviço | Porta host | Auth | Credenciais |
|---|---|---|---|
| `redpanda-plaintext` | 19092 | none | — |
| `redpanda-scram` | 29092 | SASL_PLAINTEXT SCRAM-SHA-256 | usuário `kafcli`, senha `kafcli-it-scram-secret` |
| `redpanda-mtls` | 39092 | mTLS (PEM) | certificados em `tests/fixtures/certificates/` (gerados, no `.gitignore`) |

- [ ] **Step 1: Escrever os testes que falham.**

```rust
// cli_profile.rs (roda sempre, sem Docker)
#[test] fn test_against_unreachable_broker_fails_fast() {
    // profile add down --brokers 127.0.0.1:1 --auth-type none --set-default
    // `profile test --timeout-seconds 2` → exit 1 em menos de 10 s; stderr começa com "error: kafka error"
}

// integration_connection.rs — todos #[ignore = "requires docker compose fixtures"]
#[test] #[ignore = "requires docker compose fixtures"] fn connects_with_plaintext() {
    // perfil none em localhost:19092 → `--output json profile test` sucesso, data.broker_count >= 1
}
#[test] #[ignore = "requires docker compose fixtures"] fn connects_with_scram_and_reports_principal() {
    // perfil sasl-scram-256, username kafcli, --secret-ref env:KAFCLI_IT_SCRAM_PASSWORD, --no-tls
    // → sucesso, data.principal == "kafcli"
}
#[test] #[ignore = "requires docker compose fixtures"] fn rejects_wrong_scram_password() {
    // KAFCLI_IT_SCRAM_PASSWORD=wrong → exit 1 dentro do timeout
}
#[test] #[ignore = "requires docker compose fixtures"] fn connects_with_mutual_tls() {
    // perfil mtls com ca/cert/key dos fixtures → sucesso
}
#[test] #[ignore = "requires docker compose fixtures"] fn never_leaks_secret_in_any_output() {
    // `-vv --output json profile test` no perfil SCRAM; stdout + stderr + audit log (se existir)
    // não contêm "kafcli-it-scram-secret"
}
```

- [ ] **Step 2: Rodar e ver falhar.** Run: `cargo test -p kafcli --test cli_profile test_against_unreachable_broker_fails_fast` — Expected: FAIL ("not implemented").

- [ ] **Step 3: Implementar `test_profile`**: `select_profile` → `apply_profile_overrides` (com `--brokers`) → `probe_profile_connection` com `ConnectionSettings { insecure: context.global.insecure }`. Tabela: perfil, auth type, principal, brokers e contagem de tópicos + até 50 nomes (indicar "... and N more").

- [ ] **Step 4: Rodar e ver passar.** Run: `cargo test -p kafcli --test cli_profile` — Expected: PASS.

- [ ] **Step 5: Criar os fixtures.** `generate-certificates.sh` (bash, `set -euo pipefail`, `openssl`): CA própria, certificado de servidor com SAN `DNS:localhost,IP:127.0.0.1`, certificado de cliente; tudo PEM em `tests/fixtures/certificates/`. `compose.yaml` com três serviços Redpanda (imagem `docker.redpanda.com/redpandadata/redpanda`, tag estável mais recente fixada no arquivo), modo `dev-container`, `--smp 1`, cada um com `--kafka-addr` interno + externo e `--advertise-kafka-addr` externo `localhost:<porta host>`. SCRAM: habilitar SASL (`--set redpanda.enable_sasl=true`, superuser `kafcli`) e um serviço one-shot que roda `rpk security user create kafcli -p kafcli-it-scram-secret --mechanism SCRAM-SHA-256` contra a admin API, com `depends_on: condition: service_healthy`. mTLS: listener externo com `kafka_api_tls` (`cert_file`, `key_file`, `truststore_file`, `require_client_auth: true`) montando os certificados. Healthcheck em cada broker (`rpk cluster health`). `tests/fixtures/README.md` explica como subir e rodar.

- [ ] **Step 6: Rodar a integração localmente.**
Run: `bash tests/fixtures/generate-certificates.sh && docker compose -f tests/fixtures/compose.yaml up -d --wait && KAFCLI_IT_SCRAM_PASSWORD=kafcli-it-scram-secret cargo test -p kafcli --test integration_connection -- --ignored`
Expected: 5 testes PASS. Depois `docker compose -f tests/fixtures/compose.yaml down -v`.

- [ ] **Step 7: CI.** Job `integration` em `ci.yml` com os mesmos comandos do Step 6 e `down -v` em `if: always()`.

- [ ] **Step 8: Commit.** `git commit -am "feat(cli): add profile test with integration fixtures"`

---

### Task 11: Build de release estático (smoke) e ADR de fundação

**Files:**
- Create: `.github/workflows/release-build.yml`, `docs/adr/0002-foundation-decisions.md`

**Interfaces:**
- Consumes: binário `kafcli` completo das Tasks 1–10.

- [ ] **Step 1: Workflow** disparado em `push` para `main` e `workflow_dispatch`, matriz:
  - `ubuntu-latest`, target `x86_64-unknown-linux-musl` (instalar `musl-tools`, `cmake`; `cargo build --release --target x86_64-unknown-linux-musl -p kafcli`).
  - `macos-latest`, target `aarch64-apple-darwin` (instalar `cmake` via brew).
  Passos de verificação após o build: `./kafcli --version` começa com `kafcli 0.1.0`; no Linux, `file target/.../kafcli` contém `statically linked` (ou `static-pie linked`); no macOS, `otool -L` não lista `librdkafka` nem `libssl`. Upload como artefato. Windows e aarch64 Linux ficam no plano 06.

- [ ] **Step 2: Rodar o build musl localmente.** Run: `cargo build --release --target x86_64-unknown-linux-musl -p kafcli && file target/x86_64-unknown-linux-musl/release/kafcli` — Expected: `statically linked`. Se falhar, registrar a causa e a solução no ADR (é exatamente o risco que essa task antecipa).

- [ ] **Step 3: Escrever `docs/adr/0002-foundation-decisions.md`** (formato: Contexto, Decisão, Consequências) cobrindo: dois crates em vez de cinco; precedência flags > arquivo > env; `profile show` exibe referências de segredo (não são segredos) e nunca resolve valores; `--yes` não dispensa confirmação digitada em operação destrutiva de perfil `protected`; `use_tls = false` só fora de `protected`; reescrita do TOML por `profile add/use` perde comentários; fixtures com Docker Compose + Redpanda em vez de testcontainers (portas fixas e advertised listeners previsíveis); keyring opcional com fallback sugerido. ADR `0001` fica reservado ao spike do Event Hubs.

- [ ] **Step 4: Verificação final do plano.** Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace` — Expected: tudo verde. `grep -rnE "Box::leak|mem::forget|\.unwrap\(\)|\.expect\(" crates/*/src | grep -v "mod tests"` → nenhuma ocorrência fora de testes.

- [ ] **Step 5: Commit.** `git commit -am "ci: add static release build smoke test and foundation ADR"`
