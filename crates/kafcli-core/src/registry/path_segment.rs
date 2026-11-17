/// Percent-encodes text for use as one path segment. Only `A-Z a-z 0-9 - . _ ~` pass unchanged.
pub fn encode_path_segment(text: &str) -> String {
    let mut encoded = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_path_segments() {
        assert_eq!(encode_path_segment("pedidos/v1 é"), "pedidos%2Fv1%20%C3%A9");
        assert_eq!(encode_path_segment("a-b_c.d~E9"), "a-b_c.d~E9");
    }
}
