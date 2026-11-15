use crate::config::error_description::{describe_figment_error, describe_toml_syntax_error};
use crate::config::model::Configuration;
use crate::error::{CoreError, CoreResult};
use figment::Figment;
use figment::providers::{Env, Serialized};
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::Path;

/// Prefix of the environment variables that override configuration values.
pub const ENVIRONMENT_PREFIX: &str = "KAFCLI_";

const MAXIMUM_CONFIGURATION_BYTES: u64 = 1024 * 1024;

/// Loads the configuration file at `config_path`, merged over `KAFCLI_*` environment variables.
///
/// A missing file yields an empty configuration (environment variables still apply).
/// Values in the file take precedence over environment variables.
pub fn load_configuration(config_path: &Path) -> CoreResult<Configuration> {
    let file_table = read_configuration_table(config_path)?;
    Figment::new()
        .merge(
            Env::prefixed(ENVIRONMENT_PREFIX)
                .split("__")
                .ignore(&["CONFIG"]),
        )
        .merge(Serialized::defaults(file_table))
        .extract()
        .map_err(|error| CoreError::Configuration(describe_figment_error(&error)))
}

/// Loads only the values written in the file, ignoring `KAFCLI_*` environment variables.
///
/// Read-modify-write commands must use this: merging the environment would persist values the
/// operator never wrote into the file.
pub fn load_configuration_file_only(config_path: &Path) -> CoreResult<Configuration> {
    let file_table = read_configuration_table(config_path)?;
    Figment::new()
        .merge(Serialized::defaults(file_table))
        .extract()
        .map_err(|error| CoreError::Configuration(describe_figment_error(&error)))
}

/// Parses the file once with the `toml` crate, so no other TOML parser (and no parser message
/// echoing source lines) is involved.
fn read_configuration_table(config_path: &Path) -> CoreResult<toml::Table> {
    let file_source = read_configuration_source(config_path)?;
    toml::from_str(&file_source)
        .map_err(|error| CoreError::Configuration(describe_toml_syntax_error(&error, &file_source)))
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
        .read_to_string(&mut source)?;
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
    use crate::config::model::{AuthConfig, SaslCredentialsConfig};

    /// Loads `contents` as the configuration file inside a Jail, so environment reads are isolated.
    fn load_text(contents: &str) -> CoreResult<Configuration> {
        let mut result = None;
        figment::Jail::expect_with(|jail| {
            jail.create_file("config.toml", contents)?;
            result = Some(load_configuration(Path::new("config.toml")));
            Ok(())
        });
        result.unwrap()
    }

    const ALL_AUTH_TYPES: &str = r#"
[profiles.local]
brokers = ["localhost:9092"]
auth = { type = "none" }

[profiles.tls]
brokers = ["tls:9093"]
auth = { type = "tls", ca_location = "/etc/ca.pem" }

[profiles.mtls]
brokers = ["mtls:9094"]
auth = { type = "mtls", certificate_location = "/c.pem", key_location = "/k.pem", key_password_ref = "file:/run/kp" }

[profiles.plain]
brokers = ["plain:9095"]
auth = { type = "sasl-plain", username = "app", secret_ref = "env:KAFKA_PASSWORD" }

[profiles.scram256]
brokers = ["scram256:9096"]
auth = { type = "sasl-scram-256", username = "app", secret_ref = "env:KAFKA_PASSWORD" }

[profiles.scram512]
brokers = ["scram512:9097"]
protected = true
auth = { type = "sasl-scram-512", username = "app", secret_ref = "env:KAFKA_PASSWORD", use_tls = false }

[profiles.eh]
brokers = ["ns.servicebus.windows.net:9093"]
auth = { type = "azure-connection-string", secret_ref = "keyring:kafcli/eh" }

[profiles.eh.schema_registry]
type = "azure"
"#;

    #[test]
    fn parses_profiles_for_every_wave_a_auth_type() {
        let configuration = load_text(ALL_AUTH_TYPES).unwrap();

        assert_eq!(configuration.profiles.len(), 7);
        assert_eq!(
            configuration.profiles["scram512"].auth,
            AuthConfig::SaslScram512(SaslCredentialsConfig {
                username: "app".into(),
                secret_ref: "env:KAFKA_PASSWORD".parse().unwrap(),
                ca_location: None,
                use_tls: false,
            })
        );
        assert!(configuration.profiles["scram512"].protected);
        assert!(configuration.profiles["eh"].schema_registry.is_some());
    }

    #[test]
    fn missing_file_yields_empty_configuration() {
        figment::Jail::expect_with(|_jail| {
            assert_eq!(
                load_configuration(Path::new("/nonexistent/kafcli.toml")).unwrap(),
                Configuration::default()
            );
            Ok(())
        });
    }

    #[test]
    fn rejects_plaintext_password_field_without_echoing_it() {
        let error = load_text(
            r#"
[profiles.plain]
brokers = ["b:9092"]
auth = { type = "sasl-plain", username = "u", secret_ref = "env:X", password = "hunter2" }
"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("password"));
        assert!(!error.contains("hunter2"));
    }

    #[test]
    fn invalid_secret_reference_error_hides_value() {
        let error = load_text(
            r#"
[profiles.eh]
brokers = ["b:9093"]
auth = { type = "azure-connection-string", secret_ref = "Endpoint=sb://ns/;SharedAccessKey=TOPSECRET" }
"#,
        )
        .unwrap_err()
        .to_string();

        assert!(!error.contains("TOPSECRET"));
        assert!(error.contains("secret_ref") || error.contains("secret reference"));
    }

    #[test]
    fn file_overrides_environment() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("KAFCLI_DEFAULT_PROFILE", "from-env");
            jail.create_file("config.toml", r#"default_profile = "from-file""#)?;
            assert_eq!(
                load_configuration(Path::new("config.toml"))
                    .unwrap()
                    .default_profile
                    .as_deref(),
                Some("from-file")
            );
            Ok(())
        });
    }

    #[test]
    fn file_only_load_ignores_environment() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("KAFCLI_DEFAULT_PROFILE", "from-env");
            jail.create_file("config.toml", "")?;
            assert_eq!(
                load_configuration_file_only(Path::new("config.toml"))
                    .unwrap()
                    .default_profile,
                None
            );
            Ok(())
        });
    }

    #[test]
    fn environment_fills_values_absent_from_file() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("KAFCLI_DEFAULT_PROFILE", "from-env");
            jail.create_file("config.toml", "")?;
            assert_eq!(
                load_configuration(Path::new("config.toml"))
                    .unwrap()
                    .default_profile
                    .as_deref(),
                Some("from-env")
            );
            Ok(())
        });
    }

    #[test]
    fn syntax_error_reports_position_without_echoing_line() {
        let error = load_text(
            r#"
[profiles.eh]
brokers = ["b:9093"]
auth = { type = "azure-connection-string", secret_ref = "Endpoint=sb://ns/;SharedAccessKey=TOPSECRET }
"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("line 4"));
        assert!(!error.contains("TOPSECRET"));
    }

    #[test]
    fn multi_line_inline_table_error_hides_secret() {
        let error = load_text(
            r#"
[profiles.eh]
brokers = ["b:9093"]
auth = { type = "azure-connection-string", secret_ref = "Endpoint=TOPSECRET",
}
"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("secret reference"));
        assert!(!error.contains("TOPSECRET"));
    }

    #[test]
    fn escape_sequence_error_hides_secret() {
        let error = load_text(
            r#"
[profiles.eh]
brokers = ["b:9093"]
auth = { type = "azure-connection-string", secret_ref = "x\eTOPSECRET" }
"#,
        )
        .unwrap_err()
        .to_string();

        assert!(!error.contains("TOPSECRET"));
    }

    #[test]
    fn bare_time_value_error_hides_secret() {
        let error = load_text(
            r#"
[profiles.plain]
brokers = ["b:9092"]
auth = { type = "sasl-plain", username = 07:32, secret_ref = "env:TOPSECRET" }
"#,
        )
        .unwrap_err()
        .to_string();

        assert!(!error.contains("TOPSECRET"));
    }

    #[test]
    fn keeps_datetime_values_in_schema_registry() {
        let configuration = load_text(
            r#"
[profiles.eh]
brokers = ["b:9093"]
auth = { type = "none" }

[profiles.eh.schema_registry]
issued = 2026-11-15T07:32:00Z
"#,
        )
        .unwrap();

        let registry = configuration.profiles["eh"]
            .schema_registry
            .clone()
            .unwrap();
        assert_eq!(
            registry["issued"].as_datetime().unwrap().to_string(),
            "2026-11-15T07:32:00Z"
        );
    }

    #[test]
    fn rejects_unknown_profile_field() {
        let error = load_text(
            r#"
[profiles.prod]
brokers = ["b:9092"]
protectd = true
auth = { type = "none" }
"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("protectd"));
    }

    #[test]
    fn rejects_unknown_auth_type_naming_the_type_field() {
        let error = load_text(
            r#"
[profiles.k]
brokers = ["b:9092"]
auth = { type = "kerberos" }
"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("type"));
    }
}
