/// Replaces control characters (C0, DEL, C1 and bidirectional controls) so that text from the broker or the configuration
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
    code_point < 0x20
        || code_point == 0x7f
        || (0x80..=0x9f).contains(&code_point)
        || is_bidirectional_control(character)
}

/// Marks, embeddings, overrides and isolates that reorder how a line is shown ("Trojan Source").
fn is_bidirectional_control(character: char) -> bool {
    matches!(
        character,
        '\u{061C}' | '\u{200E}' | '\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}'
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_bidirectional_controls() {
        assert_eq!(
            sanitize_for_terminal("a\u{202E}b\u{2066}c\u{200F}d\u{061C}"),
            "a?b?c?d?"
        );
    }

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
