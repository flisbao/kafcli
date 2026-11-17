/// Reads environment variables for `{{env:<VAR>}}`.
pub trait EnvironmentLookup {
    /// Returns the variable's value, or `None` when it is not set.
    fn read_variable(&self, name: &str) -> Option<String>;
}

/// Reads variables from the current process environment.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessEnvironment;

impl EnvironmentLookup for ProcessEnvironment {
    fn read_variable(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}
