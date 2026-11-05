//! The values behind the settings a plugin declared (Kuestenlogik/Bowire#693),
//! as the host sent them with one call.
//!
//! The host reads its settings store on the plugin's behalf and puts a
//! `settings` object on the params of `discover`, `invoke`, `invokeStream` and
//! `openChannel`: fresh per call, so a changed setting takes effect without a
//! restart. Only keys somebody actually set travel, and every value is a
//! string whatever the declared type; an absent key, or one that does not
//! parse as its type, reads as the declared default.

use std::collections::HashMap;

use serde_json::Value;

use crate::models::PluginSetting;

/// The setting values of one call, with the declared defaults behind them.
#[derive(Debug, Clone, Default)]
pub struct Settings {
    values: HashMap<String, String>,
    defaults: HashMap<String, Value>,
}

impl Settings {
    /// The values of `raw` (the call's `settings` object, absent when nobody
    /// set anything), backed by the defaults of `declared`.
    pub fn from_wire(raw: Option<&Value>, declared: &[PluginSetting]) -> Self {
        let values = match raw {
            Some(Value::Object(map)) => map
                .iter()
                .map(|(k, v)| {
                    // Strings on the wire; anything else still gets through as its JSON text.
                    let text = match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (k.clone(), text)
                })
                .collect(),
            _ => HashMap::new(),
        };
        let defaults = declared
            .iter()
            .filter_map(|s| s.default_value.clone().map(|d| (s.key.clone(), d)))
            .collect();
        Self { values, defaults }
    }

    /// Whether the host sent a value for `key`.
    pub fn has(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    /// The text the host sent for `key`, if any — no default applied.
    pub fn raw(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    /// `key` as text: the value sent, else the declared default.
    pub fn string(&self, key: &str) -> Option<String> {
        if let Some(v) = self.values.get(key) {
            return Some(v.clone());
        }
        match self.defaults.get(key)? {
            Value::String(s) => Some(s.clone()),
            Value::Null => None,
            other => Some(other.to_string()),
        }
    }

    /// `key` as a bool (`true` / `false` in any case); a value that does not
    /// parse reads as the declared default.
    pub fn bool(&self, key: &str) -> Option<bool> {
        if let Some(v) = self.values.get(key) {
            match v.trim().to_ascii_lowercase().as_str() {
                "true" => return Some(true),
                "false" => return Some(false),
                _ => {}
            }
        }
        match self.defaults.get(key)? {
            Value::Bool(b) => Some(*b),
            Value::String(s) => s.trim().parse::<bool>().ok(),
            _ => None,
        }
    }

    /// `key` as a number; a value that does not parse as a finite number
    /// reads as the declared default.
    pub fn number(&self, key: &str) -> Option<f64> {
        if let Some(n) = self.values.get(key).and_then(|v| parse_finite(v)) {
            return Some(n);
        }
        match self.defaults.get(key)? {
            Value::Number(n) => n.as_f64(),
            Value::String(s) => parse_finite(s),
            _ => None,
        }
    }
}

fn parse_finite(text: &str) -> Option<f64> {
    text.trim().parse::<f64>().ok().filter(|n| n.is_finite())
}

/// What is wrong with the declared settings, or nothing: a setting without
/// a key, a key declared twice, a `select` without options.
pub(crate) fn settings_problems(declared: &[PluginSetting]) -> Vec<String> {
    let mut problems = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for s in declared {
        if s.key.is_empty() {
            problems.push("a setting has no key".to_string());
        } else if !seen.insert(s.key.as_str()) {
            problems.push(format!("setting '{}' is declared twice", s.key));
        }
        if s.type_name == "select" && s.options.is_empty() {
            problems.push(format!("select setting '{}' declares no options", s.key));
        }
    }
    problems
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::models::SettingOption;

    fn declared() -> Vec<PluginSetting> {
        vec![
            PluginSetting::new("prefix", "Prefix", "string").with_default("echo"),
            PluginSetting::new("verbose", "Verbose", "bool").with_default(false),
            PluginSetting::new("ticks", "Ticks", "number").with_default(3),
            PluginSetting::new("mode", "Mode", "select")
                .with_default("fast")
                .with_options([
                    SettingOption::new("fast"),
                    SettingOption::labelled("safe", "Safe"),
                ]),
            PluginSetting::new("token", "Token", "string"),
        ]
    }

    #[test]
    fn values_sent_win_and_parse_as_their_type() {
        let raw = json!({"prefix": "hi", "verbose": "TRUE", "ticks": "7", "mode": "safe"});
        let s = Settings::from_wire(Some(&raw), &declared());
        assert_eq!(s.string("prefix").as_deref(), Some("hi"));
        assert_eq!(s.bool("verbose"), Some(true));
        assert_eq!(s.number("ticks"), Some(7.0));
        assert_eq!(s.string("mode").as_deref(), Some("safe"));
        assert!(s.has("prefix"));
        assert_eq!(s.raw("ticks"), Some("7"));
    }

    #[test]
    fn an_absent_key_reads_as_the_declared_default() {
        let s = Settings::from_wire(None, &declared());
        assert_eq!(s.string("prefix").as_deref(), Some("echo"));
        assert_eq!(s.bool("verbose"), Some(false));
        assert_eq!(s.number("ticks"), Some(3.0));
        assert!(!s.has("prefix"));
        assert_eq!(s.string("token"), None);
    }

    #[test]
    fn a_value_that_does_not_parse_falls_back_to_the_default() {
        let raw = json!({"verbose": "maybe", "ticks": "lots"});
        let s = Settings::from_wire(Some(&raw), &declared());
        assert_eq!(s.bool("verbose"), Some(false));
        assert_eq!(s.number("ticks"), Some(3.0));
    }

    #[test]
    fn a_select_without_options_and_a_repeated_key_are_problems() {
        let bad = vec![
            PluginSetting::new("mode", "Mode", "select"),
            PluginSetting::new("mode", "Mode again", "string"),
        ];
        assert_eq!(
            settings_problems(&bad),
            vec![
                "select setting 'mode' declares no options".to_string(),
                "setting 'mode' is declared twice".to_string(),
            ]
        );
        assert!(settings_problems(&declared()).is_empty());
    }
}
