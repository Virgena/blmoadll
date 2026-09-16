//! Config files -> the plugin set the kernel runs.
//!
//! Parsing, layer merging, `${VAR}` expansion and path resolution live here and
//! nowhere else: the kernel receives resolved commands and working directories,
//! so it never has to know which files the config came from.
//!
//! A config file may name the files it builds on:
//!
//! ```toml
//! extends = ["eggshell.base.toml", "team.toml"]   # loaded first, in order
//! ```
//!
//! Every file named must exist and is read from disk. Later layers win: the
//! file that names them wins on every key it writes. Tables merge key by key,
//! so `[plugins.api.config]` overrides just the keys it mentions and the rest
//! survive from below; arrays and scalars are replaced whole. A layer can
//! therefore be a single key:
//!
//! ```toml
//! extends = ["eggshell.toml"]
//!
//! [plugins.api.config]
//! model = "deepseek-reasoner"
//! ```
//!
//! A file reached twice - a diamond, or a cycle - contributes once, at its
//! first position, so a layer stack is always finite and its order is stable.

use std::collections::{BTreeMap, BTreeSet};
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

/// The one key that is file machinery rather than plugin schema: the files this
/// one is layered on top of.
const EXTENDS: &str = "extends";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    kernel: Option<Limits>,
    #[serde(default)]
    plugins: BTreeMap<String, RawPlugin>,
    /// Ties only: capability id -> the plugin id that must serve it.
    #[serde(default)]
    capability: BTreeMap<String, String>,
}

/// A config file, fully resolved. This is what the kernel boots.
#[derive(Debug, Clone)]
pub struct Config {
    /// The file the caller named: the last layer, so the winning one.
    pub path: PathBuf,
    /// The config file's directory: where every relative path resolves to.
    pub dir: PathBuf,
    /// Every file that fed this config, base first and `path` last.
    pub sources: Vec<PathBuf>,
    pub limits: Limits,
    /// Plugin id -> how to run it.
    pub plugins: BTreeMap<String, PluginSpec>,
    /// Capability id -> the plugin id that must serve it.
    ///
    /// Routing is derived from each plugin's own `provides`, so this is only
    /// needed to break a tie when two configured plugins offer the same
    /// capability. Slots nobody requires are allowed.
    pub capability: BTreeMap<String, String>,
}

impl Config {
    /// Read `path` and every file it `extends`, merge them, and resolve.
    pub fn load(path: &Path, env: &dyn Fn(&str) -> Option<String>) -> Result<Config, ConfigError> {
        let layers = read_layers(path)?;
        let mut sources = Vec::with_capacity(layers.len());
        let mut merged = toml::Table::new();
        for layer in layers {
            merge(&mut merged, &layer.table);
            sources.push(layer.path);
        }
        // `read_layers` honoured every `extends` it found; the key itself is
        // file machinery, never part of the plugin schema.
        merged.remove(EXTENDS);
        Config::from_table(merged, path, sources, env)
    }

    /// One layer from text, with no file behind it. `extends` needs a directory
    /// to resolve against, so it is refused here instead of silently ignored.
    pub fn parse(
        text: &str,
        path: &Path,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Config, ConfigError> {
        let table = parse_table(text, path)?;
        if table.contains_key(EXTENDS) {
            return Err(ConfigError::at(
                EXTENDS,
                "extends needs a file to resolve against; load this config with Config::load",
            ));
        }
        Config::from_table(table, path, vec![path.to_path_buf()], env)
    }

    fn from_table(
        table: toml::Table,
        path: &Path,
        sources: Vec<PathBuf>,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Config, ConfigError> {
        let raw: RawConfig = toml::Value::Table(table)
            .try_into()
            .map_err(|e| ConfigError::new(format!("{}: {e}", path.display())))?;
        let limits = raw.kernel.unwrap_or_default();
        let dir = parent_dir(path);

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
            sources,
            limits,
            plugins,
            capability: raw.capability.clone(),
        })
    }

    /// A stable hash over every layer this config is made of, so a reloader can
    /// tell "the bytes I already tried" from "new bytes" without keeping the
    /// file contents around. `0` means nothing could be read, which never
    /// equals a real change.
    pub fn fingerprint(path: &Path) -> u64 {
        let Ok(layers) = read_layers(path) else { return 0 };
        let mut joined = String::new();
        for layer in layers {
            joined.push_str(&layer.path.to_string_lossy());
            joined.push('\u{0}');
            joined.push_str(&layer.text);
            joined.push('\u{0}');
        }
        Config::content_hash(&joined)
    }

    /// Stable across runs, so a reloader can tell "the bytes I already tried"
    /// from "new bytes" without keeping the file contents around.
    pub fn content_hash(text: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        text.hash(&mut hasher);
        hasher.finish()
    }
}

/// What one config file contributed, parsed once for both merging and hashing.
struct Layer {
    path: PathBuf,
    text: String,
    table: toml::Table,
}

/// Lay `over` on top of `base`: tables merge key by key, everything else -
/// arrays included - is replaced whole by the later layer.
fn merge(base: &mut toml::Table, over: &toml::Table) {
    for (key, value) in over {
        if let toml::Value::Table(incoming) = value {
            if let Some(toml::Value::Table(slot)) = base.get_mut(key) {
                merge(slot, incoming);
                continue;
            }
        }
        base.insert(key.clone(), value.clone());
    }
}

/// Every file behind `path`, base first and `path` last, each read once.
fn read_layers(path: &Path) -> Result<Vec<Layer>, ConfigError> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    walk(path, &mut out, &mut seen)?;
    Ok(out)
}

/// Depth-first through `extends`: a file's own layers are read before the file
/// itself, and a file already read anywhere in the stack is skipped, so a
/// diamond loads its base once and a cycle terminates.
fn walk(
    path: &Path,
    out: &mut Vec<Layer>,
    seen: &mut BTreeSet<PathBuf>,
) -> Result<(), ConfigError> {
    let key = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    if !seen.insert(key) {
        return Ok(());
    }
    let bytes = std::fs::read(path)
        .map_err(|e| ConfigError::new(format!("cannot read {}: {e}", path.display())))?;
    let text = String::from_utf8(bytes)
        .map_err(|e| ConfigError::new(format!("{} is not UTF-8: {e}", path.display())))?;
    let table = parse_table(&text, path)?;
    if let Some(value) = table.get(EXTENDS) {
        let list = value.as_array().ok_or_else(|| {
            ConfigError::at(
                EXTENDS,
                format!("{}: extends must be an array of paths", path.display()),
            )
        })?;
        let dir = parent_dir(path);
        for item in list {
            let target = item.as_str().ok_or_else(|| {
                ConfigError::at(
                    EXTENDS,
                    format!("{}: extends entries must be strings", path.display()),
                )
            })?;
            walk(&resolve_from(&dir, target), out, seen)?;
        }
    }
    out.push(Layer { path: path.to_path_buf(), text, table });
    Ok(())
}

/// One file's bytes as a TOML table, with the file named in the error.
fn parse_table(text: &str, path: &Path) -> Result<toml::Table, ConfigError> {
    toml::from_str(text)
        .map_err(|e| ConfigError::new(format!("{}: invalid TOML: {e}", path.display())))
}

/// The directory a config file's relative paths resolve against.
fn parent_dir(path: &Path) -> PathBuf {
    path.parent()
        .map(Path::to_path_buf)
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| PathBuf::from("."))
}

/// An `extends` target: absolute stays as written, a relative one resolves
/// against the file that named it.
fn resolve_from(dir: &Path, target: &str) -> PathBuf {
    let path = Path::new(target);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        dir.join(path)
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
        // The file, the bad key and the row it sits in, all in one message.
        assert!(err.message.contains("c.toml"), "{}", err.message);
        assert!(err.message.contains("comand"), "{}", err.message);
        assert!(err.message.contains("plugins.p"), "{}", err.message);

        let err = Config::parse(
            "[plugins.p]\ncommand = \"${MISSING}\"\n[capability]\n",
            Path::new("c.toml"),
            &env,
        )
        .unwrap_err();
        assert_eq!(err.field.unwrap(), "plugins.p.command");

        let err =
            Config::parse("[plugins.p]\n[capability]\n", Path::new("c.toml"), &env).unwrap_err();
        assert!(err.message.contains("missing field `command`"), "{}", err.message);

        let err = Config::parse("", Path::new("c.toml"), &env).unwrap_err();
        assert!(err.message.contains("no [plugins"), "{}", err.message);
    }

    #[test]
    fn hashes_content_stably() {
        assert_eq!(Config::content_hash("a"), Config::content_hash("a"));
        assert_ne!(Config::content_hash("a"), Config::content_hash("b"));
    }

    /// Somewhere to put real files: layering is a filesystem feature, so it is
    /// tested through files rather than through `parse`.
    fn scratch(name: &str) -> PathBuf {
        let path = std::env::temp_dir()
            .join(format!("eggshell-config-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn write(path: &Path, body: &str) -> PathBuf {
        std::fs::write(path, body).unwrap();
        path.to_path_buf()
    }

    #[test]
    fn a_layer_overrides_only_the_keys_it_writes() {
        let dir = scratch("layers");
        let base = write(
            &dir.join("base.toml"),
            r#"
[kernel]
request_timeout_ms = 1234

[plugins.api]
command = "node"
args = ["api.ts"]
[plugins.api.config]
model = "cheap"
base_url = "https://example.invalid"
"#,
        );
        let local = write(
            &dir.join("local.toml"),
            r#"
extends = ["base.toml"]

[plugins.api.config]
model = "good"
"#,
        );

        let config = Config::load(&local, &env_of(&[])).unwrap();
        let api = &config.plugins["api"];
        assert_eq!(api.config["model"], Value::from("good"));
        // The keys the layer did not mention survive from below, on both
        // sides of the merge: a nested table and the row that owns it.
        assert_eq!(api.config["base_url"], Value::from("https://example.invalid"));
        assert_eq!(api.args, vec!["api.ts".to_string()]);
        assert_eq!(config.limits.request_timeout_ms, 1234);
        // Base first, entry last: the order the report prints.
        assert_eq!(config.sources, vec![base, local]);
    }

    #[test]
    fn arrays_are_replaced_whole_rather_than_joined() {
        let dir = scratch("arrays");
        write(&dir.join("base.toml"), "[plugins.p]\ncommand = \"node\"\nargs = [\"a\", \"b\"]\n");
        let local = write(
            &dir.join("local.toml"),
            "extends = [\"base.toml\"]\n[plugins.p]\nargs = [\"c\"]\n",
        );

        let config = Config::load(&local, &env_of(&[])).unwrap();
        assert_eq!(config.plugins["p"].args, vec!["c".to_string()]);
    }

    #[test]
    fn a_layer_may_add_a_whole_plugin() {
        let dir = scratch("adds");
        write(&dir.join("base.toml"), "[plugins.p]\ncommand = \"node\"\n");
        let local = write(
            &dir.join("local.toml"),
            "extends = [\"base.toml\"]\n[plugins.extra]\ncommand = \"node\"\n",
        );

        let config = Config::load(&local, &env_of(&[])).unwrap();
        assert_eq!(config.plugins.keys().collect::<Vec<_>>(), vec!["extra", "p"]);
        // A layer is relative to the file that names it, not to the entry.
        assert_eq!(config.dir, dir);
    }

    #[test]
    fn a_diamond_or_a_cycle_reads_each_file_once() {
        let dir = scratch("diamond");
        // a extends b and c, b extends c, and c extends a: both a diamond and
        // a cycle, which must terminate with each file in first-read order.
        let row = |id: &str| format!("[plugins.{id}]\ncommand = \"node\"\n");
        let a = write(&dir.join("a.toml"), &format!("extends = [\"b.toml\", \"c.toml\"]\n{}", row("a")));
        let b = write(&dir.join("b.toml"), &format!("extends = [\"c.toml\"]\n{}", row("b")));
        let c = write(&dir.join("c.toml"), &format!("extends = [\"a.toml\"]\n{}", row("c")));

        let config = Config::load(&a, &env_of(&[])).unwrap();
        assert_eq!(config.sources, vec![c, b, a]);
        assert_eq!(config.plugins.keys().collect::<Vec<_>>(), vec!["a", "b", "c"]);
    }

    #[test]
    fn a_missing_layer_is_an_error_that_names_it() {
        let dir = scratch("missing");
        let local = write(&dir.join("local.toml"), "extends = [\"nope.toml\"]\n");

        let err = Config::load(&local, &env_of(&[])).unwrap_err();
        assert!(err.message.contains("nope.toml"), "{}", err.message);
        assert!(err.message.contains("cannot read"), "{}", err.message);
    }

    #[test]
    fn extends_is_refused_without_a_file_to_resolve_against() {
        let err = Config::parse(
            "extends = [\"base.toml\"]\n[plugins.p]\ncommand = \"node\"\n",
            Path::new("c.toml"),
            &env_of(&[]),
        )
        .unwrap_err();
        assert_eq!(err.field.unwrap(), "extends");
    }

    /// The reloader compares this hash, so an edit to any layer has to move it.
    #[test]
    fn the_fingerprint_follows_every_layer() {
        let dir = scratch("fingerprint");
        let base = dir.join("base.toml");
        write(&base, "[plugins.p]\ncommand = \"node\"\n");
        let local = write(
            &dir.join("local.toml"),
            "extends = [\"base.toml\"]\n[plugins.p]\ncommand = \"node\"\n",
        );

        let before = Config::fingerprint(&local);
        assert_ne!(before, 0);
        assert_eq!(before, Config::fingerprint(&local));
        // Editing the layer underneath the entry still counts as a change.
        write(&base, "[plugins.p]\ncommand = \"node\"\nargs = [\"x\"]\n");
        assert_ne!(before, Config::fingerprint(&local));
    }
}
