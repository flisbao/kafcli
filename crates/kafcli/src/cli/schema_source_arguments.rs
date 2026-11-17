use kafcli_core::codec::codec_selection::SchemaSource;
use kafcli_core::registry::registered_schema::SchemaVersionSelector;
use std::path::PathBuf;
use std::str::FromStr;

/// Where the schema of `--format avro` or `--format json` comes from. At most one source is given.
#[derive(clap::Args)]
#[command(group(
    clap::ArgGroup::new("schema_source")
        .multiple(false)
        .args(["schema_file", "schema_subject", "schema_id"])
))]
pub struct SchemaSourceArguments {
    /// Schema file (Avro or JSON Schema, by --format); not usable with --wire confluent or azure
    #[arg(long, value_name = "PATH")]
    pub schema_file: Option<PathBuf>,
    /// Subject of the schema in the context's Schema Registry
    #[arg(long, value_name = "SUBJECT")]
    pub schema_subject: Option<String>,
    /// Version of the subject: latest (the default) or a number from 1 (needs --schema-subject)
    #[arg(
        long,
        value_name = "latest|N",
        requires = "schema_subject",
        value_parser = parse_schema_version
    )]
    pub schema_version: Option<SchemaVersionSelector>,
    /// Schema id in the context's Schema Registry
    #[arg(long, value_name = "ID")]
    pub schema_id: Option<String>,
}

impl SchemaSourceArguments {
    /// The schema source the flags select; `Absent` when no flag is given.
    pub fn to_schema_source(&self) -> SchemaSource {
        if let Some(path) = &self.schema_file {
            SchemaSource::File(path.clone())
        } else if let Some(subject) = &self.schema_subject {
            SchemaSource::Subject {
                subject: subject.clone(),
                version: self.schema_version.unwrap_or(SchemaVersionSelector::Latest),
            }
        } else if let Some(id) = &self.schema_id {
            SchemaSource::Id(id.clone())
        } else {
            SchemaSource::Absent
        }
    }
}

fn parse_schema_version(text: &str) -> Result<SchemaVersionSelector, String> {
    SchemaVersionSelector::from_str(text).map_err(|error| error.to_string())
}
