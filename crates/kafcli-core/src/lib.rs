//! Core library for kafcli.
//!
//! Modules: configuration, secrets, auth, connection, capability, codec, error, guard, audit, message, registry and template.

pub mod audit;
pub mod auth;
pub mod capability;
pub mod codec;
pub mod config;
pub mod connection;
pub mod error;
pub mod guard;
pub mod io_limits;
pub mod message;
pub mod ops;
pub mod registry;
pub mod secret;
pub mod template;
