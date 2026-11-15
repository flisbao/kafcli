/// Replaces control characters (C0, DEL and C1) so that text from the broker or the configuration
/// cannot move the cursor, change colors or rewrite earlier lines of a table.
pub fn sanitize_for_terminal(text: &str) -> String {
    text.chars()
        .map(|character| {
            if is_terminal_control(character) {
                '?'
            } else {
                character
            }
        })
        .collect()
}

fn is_terminal_control(character: char) -> bool {
    let code_point = character as u32;
    code_point < 0x20 || code_point == 0x7f || (0x80..=0x9f).contains(&code_point)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_ansi_escape_and_c1_control_characters() {
        let sanitized = sanitize_for_terminal("\x1b[31mred\u{9b}31m\ntab\there\u{7f}");

        assert!(!sanitized.chars().any(is_terminal_control));
        assert_eq!(sanitized, "?[31mred?31m?tab?here?");
    }

    #[test]
    fn keeps_printable_text_and_unicode_unchanged() {
        assert_eq!(sanitize_for_terminal("pedidos-ção ✓"), "pedidos-ção ✓");
    }
}
