use std::path::PathBuf;

#[derive(clap::Subcommand)]
pub enum ProfileCommand {
    /// List configured profiles
    #[command(after_help = "Examples:\n  kafcli profile list\n  kafcli --output json profile list")]
    List,
    /// Show one profile without resolving its secrets
    #[command(
        after_help = "Examples:\n  kafcli profile show prd\n  kafcli profile show\n  kafcli --output json profile show prd"
    )]
    Show {
        /// Profile name; defaults to the selected profile
        name: Option<String>,
    },
    /// Add a profile to the configuration
    #[command(
        after_help = "Examples:\n  kafcli profile add dev --brokers localhost:9092 --auth-type none --set-default\n  kafcli profile add prd --brokers prd:9093 --auth-type sasl-scram-512 --username app --secret-ref env:KAFKA_PASSWORD --protected"
    )]
    Add(AddProfileArguments),
    /// Make a profile the default
    #[command(after_help = "Examples:\n  kafcli profile use prd")]
    Use {
        /// Profile name to make the default
        name: String,
    },
    /// Test the connection of a profile
    #[command(
        after_help = "Examples:\n  kafcli profile test dev\n  kafcli profile test prd --timeout-seconds 5\n  kafcli --profile prd profile test"
    )]
    Test {
        /// Profile name; defaults to the selected profile
        name: Option<String>,
        /// Seconds to wait for the broker before giving up
        #[arg(long, default_value_t = 10)]
        timeout_seconds: u64,
    },
}

#[derive(clap::Args)]
pub struct AddProfileArguments {
    /// Name of the new profile
    pub name: String,
    /// Broker addresses, comma separated (host:port)
    #[arg(long, value_delimiter = ',', required = true)]
    pub brokers: Vec<String>,
    /// Authentication mode of the profile
    #[arg(long, value_enum)]
    pub auth_type: AuthTypeArgument,
    /// SASL username (sasl-* auth types)
    #[arg(long)]
    pub username: Option<String>,
    /// Secret reference such as env:VAR, file:/path or keyring:service/account
    #[arg(long)]
    pub secret_ref: Option<String>,
    /// CA certificate file used to verify the brokers
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
    /// Disables TLS for SASL auth types (local development only)
    #[arg(long)]
    pub no_tls: bool,
    /// Marks the profile as protected: no plaintext, no disabled TLS, no --insecure
    #[arg(long)]
    pub protected: bool,
    /// Makes the new profile the default
    #[arg(long)]
    pub set_default: bool,
    /// Replaces an existing profile with the same name
    #[arg(long)]
    pub force: bool,
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
        }
    }
}
