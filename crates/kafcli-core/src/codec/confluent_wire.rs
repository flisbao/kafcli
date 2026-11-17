//! Confluent wire framing: magic byte `0`, a 4-byte big-endian schema id, then the body.

const CONFLUENT_MAGIC_BYTE: u8 = 0;
const CONFLUENT_PREFIX_LEN: usize = 5;

/// Prepends the Confluent prefix to a serialized body.
pub fn frame_confluent_payload(schema_id: u32, body: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(CONFLUENT_PREFIX_LEN + body.len());
    framed.push(CONFLUENT_MAGIC_BYTE);
    framed.extend_from_slice(&schema_id.to_be_bytes());
    framed.extend_from_slice(body);
    framed
}

/// Splits a Confluent frame into its schema id and body.
///
/// The errors name the problem only; they never include the bytes.
pub fn split_confluent_frame(bytes: &[u8]) -> Result<(u32, &[u8]), String> {
    const SHORT: &str = "value is shorter than the 5-byte Confluent prefix";
    let Some((&magic, after_magic)) = bytes.split_first() else {
        return Err(SHORT.to_string());
    };
    let Some((id_bytes, body)) = after_magic.split_first_chunk::<4>() else {
        return Err(SHORT.to_string());
    };
    if magic != CONFLUENT_MAGIC_BYTE {
        return Err("value does not start with the Confluent magic byte 0".to_string());
    }
    Ok((u32::from_be_bytes(*id_bytes), body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_with_magic_byte_and_big_endian_id() {
        let framed = frame_confluent_payload(7, &[0xaa, 0xbb]);

        assert_eq!(framed, vec![0, 0, 0, 0, 7, 0xaa, 0xbb]);
    }

    #[test]
    fn split_rejects_short_and_wrong_magic() {
        assert_eq!(
            split_confluent_frame(&[0, 0, 0, 1]).expect_err("short frame must fail"),
            "value is shorter than the 5-byte Confluent prefix"
        );
        assert_eq!(
            split_confluent_frame(&[]).expect_err("empty frame must fail"),
            "value is shorter than the 5-byte Confluent prefix"
        );
        assert_eq!(
            split_confluent_frame(&[1, 0, 0, 0, 7, 0xaa]).expect_err("wrong magic must fail"),
            "value does not start with the Confluent magic byte 0"
        );
    }

    #[test]
    fn split_returns_id_and_body() {
        let (schema_id, body) = split_confluent_frame(&[0, 0, 0, 1, 2, 0xaa]).expect("valid frame");

        assert_eq!(schema_id, 258);
        assert_eq!(body, &[0xaa]);
    }
}
