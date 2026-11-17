use crate::cli::schema_source_arguments::parse_schema_version;
use kafcli_core::registry::registered_schema::{SchemaKind, SchemaVersionSelector};
use std::path::PathBuf;

/// Subcommands of `kafcli schema`.
#[derive(clap::Subcommand)]
pub enum SchemaCommand {
    /// Register a schema under a subject (the only command that writes to the registry)
    #[command(after_help = r#"Examples:
  kafcli schema register --subject pedidos-value --schema-file pedido.avsc
  kafcli schema register --subject pedidos-value --type json --schema-file pedido.schema.json --yes
  kafcli --context prd schema register --subject pedidos-value --schema-file pedido.avsc --yes --timeout-seconds 60

Notes:
  The schema file is read (up to 1 MiB) and checked locally before anything is sent.
  Protected contexts ask for confirmation; pass --yes to skip it in scripts.
  The audit log keeps the subject, schema id and version, never the schema text.
  Azure registries accept Avro only, and the JSON type is refused."#)]
    Register(RegisterSchemaArguments),
    /// Show a schema by subject or by id
    #[command(after_help = r#"Examples:
  kafcli schema get --subject pedidos-value
  kafcli schema get --subject pedidos-value --version 2
  kafcli schema get --id 7 --output json

Notes:
  Give either --subject or --id. --version applies only to --subject and defaults to latest.
  The table view shows the schema indented and with control characters replaced by '?'."#)]
    Get(GetSchemaArguments),
    /// Check a schema against a version of a subject without registering it
    #[command(after_help = r#"Examples:
  kafcli schema check-compat --subject pedidos-value --schema-file pedido-v2.avsc
  kafcli schema check-compat --subject pedidos-value --schema-file pedido-v2.avsc --version 1
  kafcli schema check-compat --subject pedidos-value --type json --schema-file pedido.schema.json --output json

Notes:
  Exit code 1 means the schema is not compatible; the report is still printed.
  Azure registries do not support this check."#)]
    CheckCompat(CheckCompatibilityArguments),
}

/// Schema format given with `--type`.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaTypeArgument {
    Avro,
    Json,
}

impl SchemaTypeArgument {
    pub fn to_schema_kind(self) -> SchemaKind {
        match self {
            Self::Avro => SchemaKind::Avro,
            Self::Json => SchemaKind::Json,
        }
    }
}

#[derive(clap::Args)]
pub struct RegisterSchemaArguments {
    /// Subject that receives the schema
    #[arg(long, value_name = "SUBJECT")]
    pub subject: String,
    /// Schema file, read and checked locally before it is sent (up to 1 MiB)
    #[arg(long, value_name = "PATH")]
    pub schema_file: PathBuf,
    /// Schema format of the file
    #[arg(long = "type", value_enum, default_value_t = SchemaTypeArgument::Avro)]
    pub schema_type: SchemaTypeArgument,
    /// Skips the confirmation on a protected context
    #[arg(long)]
    pub yes: bool,
    /// Seconds to wait for the registry before giving up (1 to 3600)
    #[arg(long, value_name = "N", default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}

#[derive(clap::Args)]
#[command(group(
    clap::ArgGroup::new("schema_lookup")
        .required(true)
        .args(["subject", "id"])
))]
pub struct GetSchemaArguments {
    /// Subject whose schema is read (with --version)
    #[arg(long, value_name = "SUBJECT")]
    pub subject: Option<String>,
    /// Version of the subject: latest (the default) or a number from 1 (needs --subject)
    #[arg(
        long,
        value_name = "latest|N",
        requires = "subject",
        value_parser = parse_schema_version
    )]
    pub version: Option<SchemaVersionSelector>,
    /// Schema id in the context's Schema Registry
    #[arg(long, value_name = "ID")]
    pub id: Option<String>,
    /// Seconds to wait for the registry before giving up (1 to 3600)
    #[arg(long, value_name = "N", default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}

#[derive(clap::Args)]
pub struct CheckCompatibilityArguments {
    /// Subject whose version the schema is checked against
    #[arg(long, value_name = "SUBJECT")]
    pub subject: String,
    /// Schema file, read and checked locally before it is sent (up to 1 MiB)
    #[arg(long, value_name = "PATH")]
    pub schema_file: PathBuf,
    /// Schema format of the file
    #[arg(long = "type", value_enum, default_value_t = SchemaTypeArgument::Avro)]
    pub schema_type: SchemaTypeArgument,
    /// Version of the subject to check against: latest (the default) or a number from 1
    #[arg(long, value_name = "latest|N", value_parser = parse_schema_version)]
    pub version: Option<SchemaVersionSelector>,
    /// Seconds to wait for the registry before giving up (1 to 3600)
    #[arg(long, value_name = "N", default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}

impl CheckCompatibilityArguments {
    pub fn selected_version(&self) -> SchemaVersionSelector {
        self.version.unwrap_or(SchemaVersionSelector::Latest)
    }
}
