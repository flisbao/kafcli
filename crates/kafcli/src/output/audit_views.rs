use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::ops::groups::audit_rules::{AuditFinding, AuditSeverity};
use serde::Serialize;

/// The `kafcli groups audit` result.
#[derive(Debug, Serialize)]
pub struct AuditView {
    pub groups_checked: usize,
    pub findings: Vec<AuditFinding>,
    pub warnings: Vec<String>,
}

impl TableRenderable for AuditView {
    fn render_table(&self) -> String {
        if self.findings.is_empty() {
            return format!("no findings in {} group(s)", self.groups_checked);
        }
        let mut table = Table::new();
        *table.style_mut() = UTF8_FULL_CONDENSED;
        table.set_header(vec![
            Cell::new("GROUP"),
            Cell::new("RULE"),
            Cell::new("SEVERITY"),
            Cell::new("MESSAGE"),
        ]);
        for finding in &self.findings {
            table.add_row(vec![
                Cell::new(sanitize_for_terminal(&finding.group)),
                Cell::new(finding.rule),
                Cell::new(describe_severity(finding.severity)),
                Cell::new(sanitize_for_terminal(&finding.message)),
            ]);
        }
        table.to_string()
    }
}

fn describe_severity(severity: AuditSeverity) -> &'static str {
    match severity {
        AuditSeverity::Error => "ERROR",
        AuditSeverity::Warning => "WARNING",
        AuditSeverity::Info => "INFO",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_lists_findings() {
        let view = AuditView {
            groups_checked: 2,
            findings: vec![AuditFinding {
                group: "pedidos-service".to_owned(),
                rule: "R1",
                severity: AuditSeverity::Error,
                message: "members belong to more than one service: a, b".to_owned(),
            }],
            warnings: Vec::new(),
        };

        let table = view.render_table();

        for column in [
            "GROUP",
            "RULE",
            "SEVERITY",
            "MESSAGE",
            "pedidos-service",
            "R1",
            "ERROR",
        ] {
            assert!(table.contains(column), "missing {column}: {table}");
        }
    }

    #[test]
    fn table_without_findings_says_so() {
        let view = AuditView {
            groups_checked: 3,
            findings: Vec::new(),
            warnings: Vec::new(),
        };

        assert_eq!(view.render_table(), "no findings in 3 group(s)");
    }

    #[test]
    fn broker_text_is_sanitized() {
        let view = AuditView {
            groups_checked: 1,
            findings: vec![AuditFinding {
                group: "g\x1b[2J".to_owned(),
                rule: "R1",
                severity: AuditSeverity::Error,
                message: "members belong to more than one service: a\x07, b".to_owned(),
            }],
            warnings: Vec::new(),
        };

        let table = view.render_table();

        assert!(
            !table.contains('\x1b') && !table.contains('\x07'),
            "{table:?}"
        );
    }

    #[test]
    fn json_has_rule_and_uppercase_severity() {
        let view = AuditView {
            groups_checked: 1,
            findings: vec![AuditFinding {
                group: "g".to_owned(),
                rule: "R3",
                severity: AuditSeverity::Warning,
                message: "no active members and total lag 4".to_owned(),
            }],
            warnings: vec!["w".to_owned()],
        };

        let json = serde_json::to_value(&view).unwrap();

        assert_eq!(json["groups_checked"], 1);
        assert_eq!(json["findings"][0]["rule"], "R3");
        assert_eq!(json["findings"][0]["severity"], "WARNING");
        assert_eq!(json["warnings"][0], "w");
    }
}
