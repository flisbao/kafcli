//! Core library for kafcli.
//!
//! Modules (configuration, secrets, auth, guards, audit) are added by later tasks.

pub mod auth;
pub mod config;
pub mod connection;
pub mod error;
pub mod guard;
pub mod secret;
