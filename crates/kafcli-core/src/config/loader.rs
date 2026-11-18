use crate::config::error_description::{describe_figment_error, describe_json_syntax_error};
use crate::config::model::Configuration;
use crate::error::{CoreError, CoreResult};
use figment::Figment;
use figment::providers::{Env, Serialized};
use serde_json::{Map, Value};
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::Path;

/// Prefix of the environment variables that override configuration values.
pub const ENVIRONMENT_PREFIX: &str = "KAFCLI_";

const MAXIMUM_CONFIGURATION_BYTES: u64 = 1024 * 1024;
const UTF8_BYTE_ORDER_MARK: &str = "\u{feff}";

/// Loads the configuration file at `config_path`, merged over `KAFCLI_*` environment variables.
///
/// A missing file yields an empty configuration (environment variables still apply).
/// Values in the file take precedence over environment variables.
pub fn load_configuration(config_path: &Path) -> CoreResult<Configuration> {
    let file_document = read_configuration_document(config_path)?;
    Figment::new()
        .merge(
            Env::prefixed(ENVIRONMENT_PREFIX)
                .split("__")
                .ignore(&["CONFIG", "CONTEXT"]),
        )
        .merge(Serialized::defaults(file_document))
        .extract()
        .map_err(|error| CoreError::Configuration(describe_figment_error(&error)))
}

/// Loads only the values written in the file, ignoring `KAFCLI_*` environment variables.
///
/// Read-modify-write commands must use this: merging the environment would persist values the
/// operator never wrote into the file.
pub fn load_configuration_file_only(config_path: &Path) -> CoreResult<Configuration> {
    let file_document = read_configuration_document(config_path)?;
    Figment::new()
        .merge(Serialized::defaults(file_document))
        .extract()
        .map_err(|error| CoreError::Configuration(describe_figment_error(&error)))
}

/// Parses the file once with `serde_json`, so no other JSON parser (and no parser message
/// echoing source lines) is involved.
///
/// A file with only whitespace reads as an empty document. A leading UTF-8 byte order mark, which
/// some editors write, is ignored.
fn read_configuration_document(config_path: &Path) -> CoreResult<Map<String, Value>> {
    let file_source = read_configuration_source(config_path)?;
    let json_source = file_source
        .strip_prefix(UTF8_BYTE_ORDER_MARK)
        .unwrap_or(&file_source);
    if json_source.trim().is_empty() {
        return Ok(Map::new());
    }
    let document = serde_json::from_str::<Value>(json_source)
        .map_err(|error| CoreError::Configuration(describe_json_syntax_error(&error)))?;
    let Value::Object(entries) = document else {
        return Err(CoreError::Configuration(
            "configuration file must contain a JSON object".to_string(),
        ));
    };
    reject_legacy_key(&entries, "profiles", "contexts")?;
    reject_legacy_key(&entries, "default_profile", "current_context")?;
    Ok(entries)
}

/// Rejects a key renamed by a previous release, naming both the old and the new key.
fn reject_legacy_key(entries: &Map<String, Value>, old_key: &str, new_key: &str) -> CoreResult<()> {
    if entries.contains_key(old_key) {
        return Err(CoreError::Configuration(format!(
            "configuration uses the old key '{old_key}'; rename it to '{new_key}'"
        )));
    }
    Ok(())
}

/// Reads the file with an explicit size limit; a missing file reads as empty text.
fn read_configuration_source(config_path: &Path) -> CoreResult<String> {
    let file = match File::open(config_path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(String::new()),
        Err(error) => return Err(error.into()),
    };
    let mut source = String::new();
    file.take(MAXIMUM_CONFIGURATION_BYTES + 1)
        .read_to_string(&mut source)
        .map_err(|error| match error.kind() {
            ErrorKind::InvalidData => {
                CoreError::Configuration("configuration file is not valid UTF-8".to_string())
            }
            _ => error.into(),
        })?;
    if source.len() as u64 > MAXIMUM_CONFIGURATION_BYTES {
        return Err(CoreError::Configuration(format!(
            "configuration file is larger than {MAXIMUM_CONFIGURATION_BYTES} bytes"
        )));
    }
    Ok(source)
}

#[cfg(test)]
#[allow(
    clippy::result_large_err,
    reason = "figment::Jail closures must return figment::Error"
)]
mod tests {
    use super::*;
    use crate::config::model::{AuthConfig, SaslCredentialsConfig, TlsAuthConfig};

    /// Loads `contents` as the configuration file inside a Jail, so environment reads are isolated.
    fn load_text(contents: &str) -> CoreResult<Configuration> {
        let mut result = None;
        figment::Jail::expect_with(|jail| {
            jail.create_file("config.json", contents)?;
            result = Some(load_configuration(Path::new("config.json")));
            Ok(())
        });
        result.unwrap()
    }

    #[test]
    fn groups_audit_rejects_unknown_field() {
        let result = load_text(
            r#"{"contexts": {}, "groups_audit": {"service_pattern": "^(x)$", "surprise": 1}}"#,
        );

        assert!(
            matches!(result, Err(CoreError::Configuration(_))),
            "{result:?}"
        );
    }

    #[test]
    fn groups_audit_loads_from_file() {
        let configuration =
            load_text(r#"{"groups_audit": {"group_name_pattern": "^[a-z-]+$"}}"#).unwrap();

        let audit = configuration.groups_audit.unwrap();
        assert_eq!(audit.group_name_pattern.as_deref(), Some("^[a-z-]+$"));
        assert_eq!(audit.service_pattern, None);
    }

    const ALL_AUTH_TYPES: &str = r#"{
  "current_context": "local",
  "contexts": {
    "local": {
      "brokers": ["localhost:9092"],
      "auth": { "type": "none" }
    },
    "tls": {
      "brokers": ["tls:9093"],
      "auth": { "type": "tls", "ca_location": "/etc/ca.pem" }
    },
    "mtls": {
      "brokers": ["mtls:9094"],
      "auth": {
        "type": "mtls",
        "certificate_location": "/c.pem",
        "key_location": "/k.pem",
        "key_password_ref": "file:/run/kp"
      }
    },
    "plain": {
      "brokers": ["plain:9095"],
      "auth": { "type": "sasl-plain", "username": "app", "secret_ref": "env:KAFKA_PASSWORD" }
    },
    "scram256": {
      "brokers": ["scram256:9096"],
      "auth": { "type": "sasl-scram-256", "username": "app", "secret_ref": "env:KAFKA_PASSWORD" }
    },
    "scram512": {
      "brokers": ["scram512:9097"],
      "protected": true,
      "auth": {
        "type": "sasl-scram-512",
        "username": "app",
        "secret_ref": "env:KAFKA_PASSWORD",
        "use_tls": false
      }
    },
    "eh": {
      "brokers": ["ns.servicebus.windows.net:9093"],
      "auth": { "type": "azure-connection-string", "secret_ref": "keyring:kafcli/eh" },
      "schema_registry": {
        "type": "azure",
        "url": "https://ns.servicebus.windows.net",
        "group": "pedidos",
        "auth": { "type": "bearer", "token_ref": "env:SR_TOKEN" }
      }
    }
  }
}"#;

    #[test]
    fn parses_contexts_for_every_wave_a_auth_type() {
        let configuration = load_text(ALL_AUTH_TYPES).unwrap();

        assert_eq!(configuration.contexts.len(), 7);
        assert_eq!(
            configuration.contexts["scram512"].auth,
            AuthConfig::SaslScram512(SaslCredentialsConfig {
                username: "app".into(),
                secret_ref: "env:KAFKA_PASSWORD".parse().unwrap(),
                ca_location: None,
                use_tls: false,
            })
        );
        assert!(configuration.contexts["scram512"].protected);
        assert!(configuration.contexts["eh"].schema_registry.is_some());
    }

    #[test]
    fn missing_file_yields_empty_configuration() {
        figment::Jail::expect_with(|_jail| {
            assert_eq!(
                load_configuration(Path::new("/nonexistent/kafcli.json")).unwrap(),
                Configuration::default()
            );
            Ok(())
        });
    }

    #[test]
    fn empty_or_blank_file_yields_empty_configuration() {
        assert_eq!(load_text("").unwrap(), Configuration::default());
        assert_eq!(load_text("  \n").unwrap(), Configuration::default());
    }

    #[test]
    fn rejects_plaintext_password_field_without_echoing_it() {
        let error = load_text(
            r#"{"contexts": {"plain": {"brokers": ["b:9092"], "auth": {"type": "sasl-plain", "username": "u", "secret_ref": "env:X", "password": "hunter2"}}}}"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("password"));
        assert!(!error.contains("hunter2"));
    }

    #[test]
    fn invalid_secret_reference_error_hides_value() {
        let error = load_text(
            r#"{"contexts": {"eh": {"brokers": ["b:9093"], "auth": {"type": "azure-connection-string", "secret_ref": "Endpoint=sb://ns/;SharedAccessKey=TOPSECRET"}}}}"#,
        )
        .unwrap_err()
        .to_string();

        assert!(!error.contains("TOPSECRET"));
        assert!(error.contains("secret_ref") || error.contains("secret reference"));
    }

    #[test]
    fn wrong_type_error_hides_value() {
        let error = load_text(
            r#"{"contexts": {"prd": {"brokers": ["b:9092"], "protected": "TOPSECRET", "auth": {"type": "none"}}}}"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("protected"));
        assert!(!error.contains("TOPSECRET"));
    }

    #[test]
    fn unterminated_string_reports_line_without_echoing_it() {
        let error = load_text(
            r#"{
  "contexts": {
    "eh": {
      "brokers": ["b:9093"],
      "auth": {"type": "azure-connection-string", "secret_ref": "Endpoint=TOPSECRET
    }
  }
}"#,
        )
        .unwrap_err()
        .to_string();

        assert!(
            error.contains("JSON syntax error at line"),
            "error: {error}"
        );
        assert!(!error.contains("TOPSECRET"), "error: {error}");
    }

    #[test]
    fn trailing_comma_reports_line() {
        let error = load_text(
            r#"{
  "contexts": {
    "eh": {
      "brokers": ["b:9093"],}
  }
}"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("line 4"), "error: {error}");
    }

    #[test]
    fn rejects_non_object_document() {
        let error = load_text(r#"["a"]"#).unwrap_err().to_string();

        assert!(error.contains("JSON object"), "error: {error}");
    }

    #[test]
    fn rejects_legacy_profiles_key() {
        let error = load_text(r#"{"profiles": {}}"#).unwrap_err().to_string();

        assert!(error.contains("'profiles'"), "error: {error}");
        assert!(error.contains("'contexts'"), "error: {error}");
    }

    #[test]
    fn rejects_legacy_default_profile_key() {
        let error = load_text(r#"{"default_profile": "x"}"#)
            .unwrap_err()
            .to_string();

        assert!(error.contains("'default_profile'"), "error: {error}");
        assert!(error.contains("'current_context'"), "error: {error}");
    }

    #[test]
    fn null_optional_fields_count_as_absent() {
        let configuration = load_text(
            r#"{"contexts": {"tls": {"brokers": ["tls:9093"], "auth": {"type": "tls", "ca_location": null}, "schema_registry": null}}}"#,
        )
        .unwrap();

        let context = &configuration.contexts["tls"];
        assert_eq!(
            context.auth,
            AuthConfig::Tls(TlsAuthConfig { ca_location: None })
        );
        assert_eq!(context.schema_registry, None);
    }

    #[test]
    fn file_overrides_environment() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("KAFCLI_CURRENT_CONTEXT", "from-env");
            jail.create_file("config.json", r#"{"current_context": "from-file"}"#)?;
            assert_eq!(
                load_configuration(Path::new("config.json"))
                    .unwrap()
                    .current_context
                    .as_deref(),
                Some("from-file")
            );
            Ok(())
        });
    }

    #[test]
    fn file_only_load_ignores_environment() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("KAFCLI_CURRENT_CONTEXT", "from-env");
            jail.create_file("config.json", "")?;
            assert_eq!(
                load_configuration_file_only(Path::new("config.json"))
                    .unwrap()
                    .current_context,
                None
            );
            Ok(())
        });
    }

    #[test]
    fn environment_fills_values_absent_from_file() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("KAFCLI_CURRENT_CONTEXT", "from-env");
            jail.create_file("config.json", "")?;
            assert_eq!(
                load_configuration(Path::new("config.json"))
                    .unwrap()
                    .current_context
                    .as_deref(),
                Some("from-env")
            );
            Ok(())
        });
    }

    #[test]
    fn context_and_config_variables_do_not_break_loading() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("KAFCLI_CONFIG", "/x");
            jail.set_env("KAFCLI_CONTEXT", "dev");
            jail.create_file("config.json", "{}")?;
            assert_eq!(
                load_configuration(Path::new("config.json")).unwrap(),
                Configuration::default()
            );
            Ok(())
        });
    }

    #[test]
    fn rejects_unknown_context_field() {
        let error = load_text(
            r#"{"contexts": {"prod": {"brokers": ["b:9092"], "protectd": true, "auth": {"type": "none"}}}}"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("protectd"));
    }

    #[test]
    fn rejects_unknown_auth_type_naming_the_type_field() {
        let error = load_text(
            r#"{"contexts": {"k": {"brokers": ["b:9092"], "auth": {"type": "kerberos"}}}}"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("type"));
    }

    #[test]
    fn leading_byte_order_mark_is_ignored() {
        let configuration = load_text("\u{feff}{\"current_context\": \"a\"}").unwrap();

        assert_eq!(configuration.current_context.as_deref(), Some("a"));
    }

    #[test]
    fn invalid_utf8_file_reports_configuration_error() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.json");
        std::fs::write(&config_path, [0x7b, 0xff, 0x7d]).unwrap();

        let error = load_configuration(&config_path).unwrap_err().to_string();

        assert!(error.contains("not valid UTF-8"), "error: {error}");
        assert_eq!(
            error.matches("not valid UTF-8").count(),
            1,
            "error: {error}"
        );
    }
}
