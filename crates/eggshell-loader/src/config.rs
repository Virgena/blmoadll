//! `eggshell.toml` -> the plugin set the kernel runs.
//!
//! Parsing, `${VAR}` expansion and path resolution live here and nowhere else:
//! the kernel receives resolved commands and working directories, so it never
//! has to know where the config file lived.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::Value;

use crate::spec::{Limits, PluginSpec, Timeouts};

#[derive(Debug, Clone)]
pub struct ConfigError {
    pub message: String,
    pub field: Option<String>,
}

impl ConfigError {
    pub fn new(message: impl Into<String>) -> Self {
        ConfigError { message: message.into(), field: None }
    }

    pub fn at(field: &str, message: impl Into<String>) -> Self {
        ConfigError { message: message.into(), field: Some(field.to_string()) }
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.field {
            Some(field) => write!(f, "{} ({})", self.message, field),
            None => write!(f, "{}", self.message),
        }
    }
}

/// One `[plugins.<id>]` table, as written. Strings are still raw here.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPlugin {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    clear_env: bool,
    #[serde(default)]
    config: Option<toml::Value>,
    #[serde(default)]
    initialize_timeout_ms: Option<u64>,
    #[serde(default)]
    start_timeout_ms: Option<u64>,
    #[serde(default)]
    shutdown_grace_ms: Option<u64>,
    #[serde(default)]
    request_timeout_ms: Option<u64>,
    #[serde(default)]
    stream_idle_timeout_ms: Option<u64>,
    #[serde(default)]
    max_inflight: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    kernel: Option<Limits>,
    #[serde(default)]
    plugins: BTreeMap<String, RawPlugin>,
    #[serde(default)]
    capability: BTreeMap<String, String>,
}

/// A config file, fully resolved. This is what the kernel boots.
#[derive(Debug, Clone)]
pub struct Config {
    pub path: PathBuf,
    /// The config file's directory: where every relative path resolves to.
    pub dir: PathBuf,
    pub limits: Limits,
    /// Plugin id -> how to run it.
    pub plugins: BTreeMap<String, PluginSpec>,
    /// Capability id -> plugin id. Slots nobody requires are allowed.
    pub capability: BTreeMap<String, String>,
}

impl Config {
    pub fn load(path: &Path, env: &dyn Fn(&str) -> Option<String>) -> Result<Config, ConfigError> {
        let bytes = std::fs::read(path)
            .map_err(|e| ConfigError::new(format!("cannot read {}: {e}", path.display())))?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| ConfigError::new(format!("{} is not UTF-8: {e}", path.display())))?;
        Config::parse(text, path, env)
    }

    pub fn parse(
        text: &str,
        path: &Path,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Config, ConfigError> {
        let raw: RawConfig =
            toml::from_str(text).map_err(|e| ConfigError::new(format!("invalid TOML: {e}")))?;
        let limits = raw.kernel.unwrap_or_default();
        let dir = path
            .parent()
            .map(Path::to_path_buf)
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| PathBuf::from("."));

        let mut plugins = BTreeMap::new();
        for (id, plugin) in &raw.plugins {
            plugins.insert(id.clone(), plugin.resolve(id, &dir, env)?);
        }

        if plugins.len() > limits.max_plugins {
            return Err(ConfigError::new(format!(
                "{} plugins configured, but max_plugins is {}",
                plugins.len(),
                limits.max_plugins
            )));
        }
        if plugins.is_empty() {
            return Err(ConfigError::new("no [plugins.<id>] entries configured"));
        }



        if plugins.contains_key(eggshell_protocol::HOST) {
            return Err(ConfigError::at(
                &format!("plugins.{}", eggshell_protocol::HOST),
                "this id is reserved for the kernel embedder",
            ));
        }

        Ok(Config {
            path: path.to_path_buf(),
            dir,
            limits,
            plugins,
            capability: raw.capability.clone(),
        })
    }

    /// Stable across runs, so a reloader can tell "the bytes I already tried"
    /// from "new bytes" without keeping the file contents around.
    pub fn content_hash(text: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        text.hash(&mut hasher);
        hasher.finish()
    }
}

impl RawPlugin {
    /// Expands every string, then resolves `command` and `cwd` against the
    /// config file's directory. `args` are passed through untouched: a plugin's
    /// own arguments are its business.
    fn resolve(
        &self,
        id: &str,
        dir: &Path,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<PluginSpec, ConfigError> {
        let base = format!("plugins.{id}");
        if self.command.trim().is_empty() {
            return Err(ConfigError::at(&format!("{base}.command"), "command must not be empty"));
        }
        let command = expand(&self.command, &format!("{base}.command"), env)?;
        let mut args = Vec::with_capacity(self.args.len());
        for (index, arg) in self.args.iter().enumerate() {
            args.push(expand(arg, &format!("{base}.args[{index}]"), env)?);
        }
        let cwd = match &self.cwd {
            Some(cwd) => dir.join(expand(cwd, &format!("{base}.cwd"), env)?),
            None => dir.to_path_buf(),
        };
        let mut expanded = BTreeMap::new();
        for (key, value) in &self.env {
            expanded.insert(key.clone(), expand(value, &format!("{base}.env.{key}"), env)?);
        }
        Ok(PluginSpec {
            id: id.to_string(),
            command: resolve_command(dir, &command),
            args,
            env: expanded,
            cwd,
            clear_env: self.clear_env,
            config: match &self.config {
                Some(value) => serde_json::to_value(value).unwrap_or(Value::Null),
                None => Value::Object(serde_json::Map::new()),
            },
            timeouts: Timeouts {
                initialize_timeout_ms: self.initialize_timeout_ms,
                start_timeout_ms: self.start_timeout_ms,
                shutdown_grace_ms: self.shutdown_grace_ms,
                request_timeout_ms: self.request_timeout_ms,
                stream_idle_timeout_ms: self.stream_idle_timeout_ms,
                max_inflight: self.max_inflight,
            },
        })
    }
}

/// A `command` containing a path separator is relative to the config file; a
/// bare name is left alone for the OS to find on `PATH`.
fn resolve_command(dir: &Path, command: &str) -> PathBuf {
    let path = Path::new(command);
    if path.is_absolute() || !(command.contains('/') || command.contains('\\')) {
        path.to_path_buf()
    } else {
        dir.join(path)
    }
}

/// Expands `${VAR}` and `$$`. Undefined variables are a hard error: a config
/// that silently becomes empty is worse than one that refuses to start.
/// Expansion is not recursive: a value that looks like `${OTHER}` stays as
/// written. `clear_env` only controls inheritance; it never blocks an explicit
/// `env` entry from being injected.
pub fn expand(
    input: &str,
    field: &str,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<String, ConfigError> {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' {
            out.push(c);
            continue;
        }
        match chars.peek().copied() {
            Some('$') => {
                chars.next();
                out.push('$');
            }
            Some('{') => {
                chars.next();
                let mut name = String::new();
                let mut closed = false;
                for c in chars.by_ref() {
                    if c == '}' {
                        closed = true;
                        break;
                    }
                    name.push(c);
                }
                if !closed {
                    return Err(ConfigError::at(field, "unterminated ${...}"));
                }
                match env(&name) {
                    Some(value) => out.push_str(&value),
                    None => {
                        return Err(ConfigError::at(
                            field,
                            format!("environment variable {name} is not set"),
                        ))
                    }
                }
            }
            _ => out.push('$'),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::MIB;

    fn env_of<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name: &str| {
            pairs
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| (*v).to_string())
        }
    }

    const SAMPLE: &str = r#"
[kernel]
request_timeout_ms = 1234

[plugins.provider]
command = "../target/debug/eggshell-plugin-example"
args = ["--prefix", "${PREFIX}"]
env = { API_KEY = "${TOKEN}" }
cwd = "sub"
clear_env = true
initialize_timeout_ms = 77
max_inflight = 3
[plugins.provider.config]
prefix = "echo: "

[capability]
"demo.text" = "provider"
"#;

    fn sample(env: &dyn Fn(&str) -> Option<String>) -> Config {
        Config::parse(SAMPLE, Path::new("/tmp/proj/eggshell.toml"), env).unwrap()
    }

    #[test]
    fn parses_and_expands_a_config() {
        let env = env_of(&[("PREFIX", "hi"), ("TOKEN", "t0k")]);
        let config = sample(&env);
        assert_eq!(config.limits.request_timeout_ms, 1234);
        assert_eq!(config.limits.max_frame_bytes, 64 * MIB);
        let plugin = &config.plugins["provider"];
        assert_eq!(plugin.args, vec!["--prefix".to_string(), "hi".to_string()]);
        assert_eq!(plugin.env["API_KEY"], "t0k");
        assert!(plugin.clear_env);
        assert_eq!(plugin.timeouts.initialize(&config.limits), 77);
        assert_eq!(plugin.timeouts.start(&config.limits), 10_000);
        assert_eq!(plugin.timeouts.max_inflight(&config.limits), 3);
        assert_eq!(plugin.config["prefix"], Value::from("echo: "));
        assert_eq!(config.capability["demo.text"], "provider");
    }

    #[test]
    fn resolves_paths_relative_to_the_config_file() {
        let env = env_of(&[("PREFIX", "hi"), ("TOKEN", "t0k")]);
        let config = sample(&env);
        let plugin = &config.plugins["provider"];
        assert_eq!(plugin.cwd, config.dir.join("sub"));
        assert_eq!(plugin.command, config.dir.join("../target/debug/eggshell-plugin-example"));

        let bare = Config::parse(
            "[plugins.p]\ncommand = \"my-plugin\"\n[capability]\n",
            Path::new("/tmp/proj/eggshell.toml"),
            &env,
        )
        .unwrap();
        assert_eq!(bare.plugins["p"].command, Path::new("my-plugin"));
        assert_eq!(bare.plugins["p"].cwd, bare.dir);
    }

    #[test]
    fn rejects_typos_undefined_vars_and_empty_command() {
        let env = env_of(&[]);
        let err = Config::parse(
            "[plugins.p]\ncomand = \"x\"\ncommand = \"y\"\n[capability]\n",
            Path::new("c.toml"),
            &env,
        )
        .unwrap_err();
        assert!(err.message.contains("invalid TOML"), "{}", err.message);

        let err = Config::parse(
            "[plugins.p]\ncommand = \"${MISSING}\"\n[capability]\n",
            Path::new("c.toml"),
            &env,
        )
        .unwrap_err();
        assert_eq!(err.field.unwrap(), "plugins.p.command");

        let err =
            Config::parse("[plugins.p]\n[capability]\n", Path::new("c.toml"), &env).unwrap_err();
        assert!(err.message.contains("invalid TOML"), "{}", err.message);

        let err = Config::parse("", Path::new("c.toml"), &env).unwrap_err();
        assert!(err.message.contains("no [plugins"), "{}", err.message);
    }

    #[test]
    fn hashes_content_stably() {
        assert_eq!(Config::content_hash("a"), Config::content_hash("a"));
        assert_ne!(Config::content_hash("a"), Config::content_hash("b"));
    }
}
