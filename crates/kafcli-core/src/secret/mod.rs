pub mod composite_resolver;
pub mod environment_resolver;
pub mod file_resolver;
#[cfg(any(test, feature = "test-support"))]
pub mod fixed_resolver;
#[cfg(feature = "keyring")]
pub mod keyring_resolver;
pub mod reference;
pub mod resolver;
