use crate::error::{CoreError, CoreResult};

/// A piece of template text: literal text, or the content between `{{` and `}}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateSegment<'a> {
    Literal(&'a str),
    Placeholder(&'a str),
}

/// Splits text into literal runs and `{{...}}` placeholders, without regular expressions.
///
/// A `{{` with no matching `}}` is an error that reports the byte offset of the `{{`.
/// A lone `}}` outside a placeholder is literal text.
pub fn split_template_segments(text: &str) -> CoreResult<Vec<TemplateSegment<'_>>> {
    let mut segments = Vec::new();
    let mut literal_start = 0;
    let mut cursor = 0;

    while let Some(relative_open) = text[cursor..].find("{{") {
        let open = cursor + relative_open;
        let content_start = open + "{{".len();
        let Some(relative_close) = text[content_start..].find("}}") else {
            return Err(CoreError::Template(format!(
                "unclosed template starting at byte {open}"
            )));
        };
        let close = content_start + relative_close;

        if literal_start < open {
            segments.push(TemplateSegment::Literal(&text[literal_start..open]));
        }
        segments.push(TemplateSegment::Placeholder(&text[content_start..close]));
        cursor = close + "}}".len();
        literal_start = cursor;
    }

    if literal_start < text.len() {
        segments.push(TemplateSegment::Literal(&text[literal_start..]));
    }
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_literals_and_placeholders() -> CoreResult<()> {
        let segments = split_template_segments("a{{seq}}b")?;

        assert_eq!(
            segments,
            vec![
                TemplateSegment::Literal("a"),
                TemplateSegment::Placeholder("seq"),
                TemplateSegment::Literal("b"),
            ]
        );
        Ok(())
    }

    #[test]
    fn reports_byte_offset_of_unclosed_open_marker() {
        let error = split_template_segments("abc {{seq").err();

        assert!(
            matches!(error, Some(CoreError::Template(ref message)) if message == "unclosed template starting at byte 4")
        );
    }
}
