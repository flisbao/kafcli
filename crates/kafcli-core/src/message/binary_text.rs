use crate::error::{CoreError, CoreResult};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;

/// Encodes bytes with the standard base64 alphabet, with padding.
pub fn encode_base64(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

/// Decodes standard base64 text. Any malformed input yields `InvalidInput("invalid base64")`.
pub fn decode_base64(text: &str) -> CoreResult<Vec<u8>> {
    STANDARD
        .decode(text)
        .map_err(|_| CoreError::InvalidInput("invalid base64".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_with_standard_alphabet_and_padding() {
        assert_eq!(encode_base64(&[0xff, 0x00]), "/wA=");
    }

    #[test]
    fn decodes_back_to_original_bytes() -> CoreResult<()> {
        assert_eq!(decode_base64("/wA=")?, vec![0xff, 0x00]);
        Ok(())
    }

    #[test]
    fn rejects_malformed_base64_without_echoing_it() {
        let error = decode_base64("not base64!!").err();

        assert!(
            matches!(error, Some(CoreError::InvalidInput(ref message)) if message == "invalid base64")
        );
    }
}
