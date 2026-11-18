const MAX_REGISTRY_TEXT_CHARS: usize = 200;

/// Makes text that came from a registry safe to print: control characters become `?`
/// and at most 200 characters are kept. Secret redaction happens before this step.
pub fn neutralize_registry_text(text: &str) -> String {
    text.chars()
        .map(|character| {
            if is_control_or_bidirectional(character) {
                '?'
            } else {
                character
            }
        })
        .take(MAX_REGISTRY_TEXT_CHARS)
        .collect()
}

/// Control characters plus the bidirectional embedding, override and isolate characters, which
/// reorder the text around them when it is printed (`U+202E` makes `exe.fdp` look like `pdf.exe`).
fn is_control_or_bidirectional(character: char) -> bool {
    character.is_control()
        || matches!(
            character,
            '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}'
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neutralizes_control_characters_and_truncates() {
        let text = format!("bad\u{1b}[2J{}", "x".repeat(300));

        let neutralized = neutralize_registry_text(&text);

        assert!(neutralized.starts_with("bad?[2J"));
        assert_eq!(neutralized.chars().count(), 200);
    }

    #[test]
    fn neutralizes_bidirectional_override_and_isolate_characters() {
        let text = "invoice\u{202E}fdp.exe\u{2066}x\u{2069}\u{202A}\u{202C}";

        assert_eq!(neutralize_registry_text(text), "invoice?fdp.exe?x???");
    }
}
