//! Payload and key templates: `{{uuid}}`, `{{seq}}`, `{{now}}`, `{{field:<path>}}` and `{{env:<VAR>}}`.

pub mod environment_lookup;
pub mod template_parser;
pub mod template_renderer;
pub mod template_state;

pub use environment_lookup::{EnvironmentLookup, ProcessEnvironment};
pub use template_renderer::{render_text_template, render_value_templates};
pub use template_state::TemplateState;
