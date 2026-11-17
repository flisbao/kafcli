//! Azure wire format (ADR 0001): the schema id travels in the Kafka `content-type` header as
//! `avro/binary+<schema id>`, and the body is pure Avro binary.

use crate::message::record_header::RecordHeader;

/// Kafka header name that carries the Azure schema id.
pub const AZURE_CONTENT_TYPE_HEADER: &str = "content-type";
const AZURE_AVRO_PREFIX: &str = "avro/binary";
const MISSING_HEADER: &str = "value has no content-type header with avro/binary+<schema id>; the legacy 36-byte prefix format is not supported";
const INVALID_HEADER: &str = "content-type header is not avro/binary+<schema id>";

/// Builds the `content-type` value for an Azure schema id.
pub fn build_azure_content_type(schema_id: &str) -> String {
    format!("{AZURE_AVRO_PREFIX}+{schema_id}")
}

/// Reads the Azure schema id from the record headers.
///
/// The header name matches without regard to case and the last matching header wins.
/// The schema id is untrusted message data; callers must neutralize it before printing.
pub fn read_azure_schema_id(headers: &[RecordHeader]) -> Result<String, String> {
    let Some(header) = headers
        .iter()
        .rev()
        .find(|header| header.name.eq_ignore_ascii_case(AZURE_CONTENT_TYPE_HEADER))
    else {
        return Err(MISSING_HEADER.to_string());
    };
    let Some(value) = header.value.as_deref() else {
        return Err(INVALID_HEADER.to_string());
    };
    let text = std::str::from_utf8(value).map_err(|_| INVALID_HEADER.to_string())?;
    let Some((prefix, schema_id)) = text.split_once('+') else {
        return Err(INVALID_HEADER.to_string());
    };
    if prefix != AZURE_AVRO_PREFIX || schema_id.is_empty() {
        return Err(INVALID_HEADER.to_string());
    }
    Ok(schema_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content_type(value: &[u8]) -> Vec<RecordHeader> {
        vec![RecordHeader {
            name: AZURE_CONTENT_TYPE_HEADER.to_string(),
            value: Some(value.to_vec()),
        }]
    }

    #[test]
    fn content_type_parses_first_plus() {
        let headers = content_type(b"avro/binary+a+b");

        assert_eq!(read_azure_schema_id(&headers).expect("id"), "a+b");
        assert_eq!(build_azure_content_type("a+b"), "avro/binary+a+b");
    }

    #[test]
    fn content_type_prefix_is_case_sensitive() {
        for text in [
            "Avro/binary+1",
            "avro/Binary+1",
            "avro/binary",
            "avro/binary+",
        ] {
            let error = read_azure_schema_id(&content_type(text.as_bytes()))
                .expect_err("wrong prefix or empty id must fail");

            assert_eq!(
                error, "content-type header is not avro/binary+<schema id>",
                "input {text}"
            );
        }
    }

    #[test]
    fn content_type_name_is_case_insensitive_and_last_wins() {
        let headers = vec![
            RecordHeader {
                name: "Content-Type".to_string(),
                value: Some(b"avro/binary+1".to_vec()),
            },
            RecordHeader {
                name: "CONTENT-TYPE".to_string(),
                value: Some(b"avro/binary+2".to_vec()),
            },
        ];

        assert_eq!(read_azure_schema_id(&headers).expect("id"), "2");
    }

    #[test]
    fn non_utf8_content_type_is_invalid() {
        let error = read_azure_schema_id(&content_type(&[0xff, 0xfe]))
            .expect_err("non-UTF-8 value must fail");

        assert_eq!(error, "content-type header is not avro/binary+<schema id>");
    }

    #[test]
    fn missing_content_type_reports_legacy_note() {
        let error = read_azure_schema_id(&[]).expect_err("no header must fail");

        assert_eq!(
            error,
            "value has no content-type header with avro/binary+<schema id>; the legacy 36-byte prefix format is not supported"
        );
    }
}
