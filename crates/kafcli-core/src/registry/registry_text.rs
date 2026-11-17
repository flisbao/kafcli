const MAX_REGISTRY_TEXT_CHARS: usize = 200;

/// Makes text that came from a registry safe to print: control characters become `?`
/// and at most 200 characters are kept. Secret redaction happens before this step.
pub fn neutralize_registry_text(text: &str) -> String {
    text.chars()
        .map(|character| {
            if character.is_control() {
                '?'
            } else {
                character
            }
        })
        .take(MAX_REGISTRY_TEXT_CHARS)
        .collect()
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
}
