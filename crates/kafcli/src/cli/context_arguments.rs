use std::path::PathBuf;

#[derive(clap::Subcommand)]
pub enum ContextCommand {
    /// List configured contexts
    #[command(after_help = "Examples:\n  kafcli context list\n  kafcli --output json context list")]
    List,
    /// Show one context without resolving its secrets
    #[command(
        after_help = "Examples:\n  kafcli context show prd\n  kafcli context show\n  kafcli --output json context show prd"
    )]
    Show {
        /// Context name; defaults to the selected context
        name: Option<String>,
    },
    /// Add a context to the configuration
    #[command(
        after_help = "Examples:\n  kafcli context add dev --brokers localhost:9092 --auth-type none --use\n  kafcli context add prd --brokers prd:9093 --auth-type sasl-scram-512 --username app --secret-ref env:KAFKA_PASSWORD --protected\n  kafcli context add dev-oauth --brokers kafka:9093 --auth-type bearer --token-ref file:/run/secrets/kafka-token\n  kafcli context add ci-oidc --brokers kafka:9093 --auth-type oauth-oidc --token-endpoint-url https://login.example.com/oauth2/token --client-id kafcli --secret-ref env:KAFCLI_CLIENT_SECRET\n  kafcli context add prd-eh --brokers prd-ns.servicebus.windows.net:9093 --auth-type azure-sp-secret --tenant-id <tenant> --client-id <app> --secret-ref env:PRD_SP_SECRET --protected\n  kafcli context add dev --brokers localhost:9092 --auth-type none --registry-type confluent --registry-url https://sr.example:8081"
    )]
    Add(Box<AddContextArguments>),
    /// Make a context the current one
    #[command(after_help = "Examples:\n  kafcli context use prd")]
    Use {
        /// Context name to make the current one
        name: String,
    },
    /// Print the name of the current context
    #[command(
        after_help = "Examples:\n  kafcli context current\n  kafcli --output json context current"
    )]
    Current,
    /// Test the connection of a context
    #[command(
        after_help = "Examples:\n  kafcli context test dev\n  kafcli context test prd --timeout-seconds 5\n  kafcli --context prd context test"
    )]
    Test {
        /// Context name; defaults to the selected context
        name: Option<String>,
        /// Seconds to wait for the broker before giving up (1 to 3600)
        #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=3600))]
        timeout_seconds: u64,
    },
}

#[derive(clap::Args)]
pub struct AddContextArguments {
    /// Name of the new context
    pub name: String,
    /// Broker addresses, comma separated (host:port)
    #[arg(long, value_delimiter = ',', required = true)]
    pub brokers: Vec<String>,
    /// Authentication mode of the context
    #[arg(long, value_enum)]
    pub auth_type: AuthTypeArgument,
    /// SASL username (sasl-* auth types)
    #[arg(long)]
    pub username: Option<String>,
    /// Secret reference such as env:VAR, file:/path or keyring:service/account; the client secret for oauth-oidc and azure-sp-secret
    #[arg(long)]
    pub secret_ref: Option<String>,
    /// Secret reference of the bearer token (bearer auth type)
    #[arg(long)]
    pub token_ref: Option<String>,
    /// Token endpoint of the OpenID Connect provider, https:// (oauth-oidc auth type)
    #[arg(long)]
    pub token_endpoint_url: Option<String>,
    /// OAuth client id (oauth-oidc and azure-sp-secret auth types)
    #[arg(long)]
    pub client_id: Option<String>,
    /// OAuth scope requested with the token (oauth-oidc and azure-sp-secret auth types)
    #[arg(long)]
    pub scope: Option<String>,
    /// Directory (tenant) id of the Entra ID application (azure-sp-secret auth type)
    #[arg(long)]
    pub tenant_id: Option<String>,
    /// Entra ID authority, https:// (azure-sp-secret auth type); defaults to AZURE_AUTHORITY_HOST or the public cloud
    #[arg(long)]
    pub authority_host: Option<String>,
    /// CA certificate file used to verify the brokers and the oauth-oidc token endpoint
    #[arg(long)]
    pub ca_location: Option<PathBuf>,
    /// Client certificate file (mtls auth type)
    #[arg(long)]
    pub certificate_location: Option<PathBuf>,
    /// Client private key file (mtls auth type)
    #[arg(long)]
    pub key_location: Option<PathBuf>,
    /// Secret reference of the private key password (mtls auth type)
    #[arg(long)]
    pub key_password_ref: Option<String>,
    /// Disables TLS for SASL and bearer auth types (local development only)
    #[arg(long)]
    pub no_tls: bool,
    /// Marks the context as production: warns on every use and refuses plaintext, disabled TLS and --insecure
    #[arg(long)]
    pub protected: bool,
    /// Makes the new context the current one
    #[arg(long = "use")]
    pub use_after_add: bool,
    /// Replaces an existing context with the same name
    #[arg(long)]
    pub force: bool,
    /// Schema Registry type; every other --registry-* flag requires it
    #[arg(long, value_enum)]
    pub registry_type: Option<RegistryTypeArgument>,
    /// Schema Registry base URL (required with --registry-type)
    #[arg(long, requires = "registry_type")]
    pub registry_url: Option<String>,
    /// Group of the Azure Schema Registry (azure registry type)
    #[arg(long, requires = "registry_type")]
    pub registry_group: Option<String>,
    /// Authentication of the Schema Registry
    #[arg(long, value_enum, default_value = "none", requires = "registry_type")]
    pub registry_auth_type: RegistryAuthTypeArgument,
    /// Username of the Schema Registry (basic auth type)
    #[arg(long, requires = "registry_type")]
    pub registry_username: Option<String>,
    /// Secret reference of the Schema Registry password (basic) or token (bearer)
    #[arg(long, requires = "registry_type")]
    pub registry_secret_ref: Option<String>,
    /// CA certificate file used to verify the Confluent Schema Registry
    #[arg(long, requires = "registry_type")]
    pub registry_ca_location: Option<PathBuf>,
    /// Client certificate file of the Schema Registry (mtls auth type)
    #[arg(long, requires = "registry_type")]
    pub registry_certificate_location: Option<PathBuf>,
    /// Client private key file of the Schema Registry (mtls auth type)
    #[arg(long, requires = "registry_type")]
    pub registry_key_location: Option<PathBuf>,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegistryTypeArgument {
    Confluent,
    Azure,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegistryAuthTypeArgument {
    None,
    Basic,
    Bearer,
    Mtls,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthTypeArgument {
    None,
    Tls,
    Mtls,
    SaslPlain,
    #[value(name = "sasl-scram-256")]
    SaslScram256,
    #[value(name = "sasl-scram-512")]
    SaslScram512,
    AzureConnectionString,
    Bearer,
    OauthOidc,
    #[value(name = "azure-sp-secret")]
    AzureSpSecret,
}

impl AuthTypeArgument {
    /// The `--auth-type` spelling, used in error messages.
    pub fn to_flag_spelling(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Tls => "tls",
            Self::Mtls => "mtls",
            Self::SaslPlain => "sasl-plain",
            Self::SaslScram256 => "sasl-scram-256",
            Self::SaslScram512 => "sasl-scram-512",
            Self::AzureConnectionString => "azure-connection-string",
            Self::Bearer => "bearer",
            Self::OauthOidc => "oauth-oidc",
            Self::AzureSpSecret => "azure-sp-secret",
        }
    }
}
