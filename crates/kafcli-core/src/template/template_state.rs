/// Per-run state for templates: the sequence number of the message being produced.
///
/// The first message uses sequence 1. The caller advances the state once per produced message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateState {
    sequence: u64,
}

impl TemplateState {
    pub fn new() -> Self {
        Self { sequence: 1 }
    }

    /// Current sequence number, used by `{{seq}}`.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Moves to the next sequence number. Call once per produced message.
    pub fn advance(&mut self) {
        self.sequence = self.sequence.saturating_add(1);
    }
}

impl Default for TemplateState {
    fn default() -> Self {
        Self::new()
    }
}
