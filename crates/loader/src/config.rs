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
        ConfigError {
            message: message.into(),
            field: None,
        }
    }

    pub fn at(field: &str, message: impl Into<String>) -> Self {
        ConfigError {
            message: message.into(),
            field: Some(field.to_string()),
        }
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
    #[serde(default)]
    disabled: bool,
    /// The package this row runs, resolved out of the node_modules chain
    /// that starts at the config file's own directory. Exclusive with
    /// `command`, which is the raw-program form of the same row.
    #[serde(default)]
    name: Option<String>,
    /// The program to run. A row that names a package gets `node` by
    /// default; a row that does not must say which program to run.
    #[serde(default)]
    command: Option<String>,
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
    /// Plugin ids whose merged row says `disabled = true`. They are not in
    /// `plugins`, but a `[capability]` pin or a report can still name them.
    pub disabled: BTreeSet<String>,
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
        let mut disabled = BTreeSet::new();
        for (id, plugin) in &raw.plugins {
            if plugin.disabled {
                disabled.insert(id.clone());
                continue;
            }
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
            return Err(ConfigError::new(
                "no enabled [plugins.<id>] entries configured",
            ));
        }

        if plugins.contains_key(protocol::HOST) {
            return Err(ConfigError::at(
                &format!("plugins.{}", protocol::HOST),
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
            disabled,
        })
    }

    /// A stable hash over every layer this config is made of, so a reloader can
    /// tell "the bytes I already tried" from "new bytes" without keeping the
    /// file contents around. `0` means nothing could be read, which never
    /// equals a real change.
    pub fn fingerprint(path: &Path) -> u64 {
        let Ok(layers) = read_layers(path) else {
            return 0;
        };
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
    out.push(Layer {
        path: path.to_path_buf(),
        text,
        table,
    });
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
    /// Expands every string, then resolves the row into a program to run and
    /// the arguments to run it with. A row names either a package (resolved
    /// out of the node_modules chain above the config file's directory, then
    /// run with `node`) or a `command`; `cwd` resolves against the config
    /// file's directory.
    fn resolve(
        &self,
        id: &str,
        dir: &Path,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<PluginSpec, ConfigError> {
        let base = format!("plugins.{id}");
        let (command, args) = match (&self.name, &self.command) {
            (Some(_), Some(_)) => {
                return Err(ConfigError::at(
                    &format!("{base}.name"),
                    "name and command are alternatives: a row that names a package runs through node",
                ));
            }
            (Some(name), None) => {
                let name = expand(name, &format!("{base}.name"), env)?;
                if name.trim().is_empty() {
                    return Err(ConfigError::at(
                        &format!("{base}.name"),
                        "name must not be empty",
                    ));
                }
                let entry = resolve_package(&name, dir)
                    .map_err(|message| ConfigError::at(&format!("{base}.name"), message))?;
                let mut args = Vec::with_capacity(self.args.len() + 1);
                args.push(entry.to_string_lossy().into_owned());
                for (index, arg) in self.args.iter().enumerate() {
                    args.push(expand(arg, &format!("{base}.args[{index}]"), env)?);
                }
                (PathBuf::from("node"), args)
            }
            (None, Some(command)) => {
                if command.trim().is_empty() {
                    return Err(ConfigError::at(
                        &format!("{base}.command"),
                        "command must not be empty",
                    ));
                }
                let command = expand(command, &format!("{base}.command"), env)?;
                let mut args = Vec::with_capacity(self.args.len());
                for (index, arg) in self.args.iter().enumerate() {
                    args.push(expand(arg, &format!("{base}.args[{index}]"), env)?);
                }
                (resolve_command(dir, &command), args)
            }
            (None, None) => {
                return Err(ConfigError::at(
                    &base,
                    "a row names either a package or a command",
                ));
            }
        };
        let cwd = match &self.cwd {
            Some(cwd) => dir.join(expand(cwd, &format!("{base}.cwd"), env)?),
            None => dir.to_path_buf(),
        };
        let mut expanded = BTreeMap::new();
        for (key, value) in &self.env {
            expanded.insert(
                key.clone(),
                expand(value, &format!("{base}.env.{key}"), env)?,
            );
        }
        Ok(PluginSpec {
            id: id.to_string(),
            command,
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

/// The entry file of the package `name`, found the way Node finds it: the
/// nearest `node_modules/<name>/package.json` at or above `dir`, whose entry
/// comes from its own manifest. The result is the real file on disk, so a row
/// that reaches the package through the config directory's link still spawns
/// the repository copy.
fn resolve_package(name: &str, dir: &Path) -> Result<PathBuf, String> {
    if name.starts_with('.') || Path::new(name).is_absolute() {
        return Err(format!("{name} is not a package name"));
    }
    let mut package = None;
    for ancestor in dir.ancestors() {
        let candidate = ancestor
            .join("node_modules")
            .join(name.replace('/', std::path::MAIN_SEPARATOR_STR));
        if candidate.join("package.json").is_file() {
            package = Some(candidate);
            break;
        }
    }
    let package =
        package.ok_or_else(|| format!("{name} is not installed at or above {}", dir.display()))?;
    let manifest = std::fs::read_to_string(package.join("package.json"))
        .map_err(|e| format!("cannot read {name}'s package.json: {e}"))?;
    let parsed: Value = serde_json::from_str(&manifest)
        .map_err(|e| format!("{name}'s package.json is not JSON: {e}"))?;
    let entry = package.join(entry_of(&parsed));
    if !entry.is_file() {
        return Err(format!("{name}'s entry {} is missing", entry.display()));
    }
    Ok(simplify(entry.canonicalize().unwrap_or(entry)))
}

/// A package manifest's entry file, in Node's own order: the root export's
/// `import`, its `default`, the export itself when it is a string, then `main`.
fn entry_of(manifest: &Value) -> PathBuf {
    let exports = manifest.get("exports").and_then(|value| value.get("."));
    let from_exports = exports.and_then(|value| match value {
        Value::String(path) => Some(path.as_str()),
        Value::Object(fields) => ["import", "default"]
            .iter()
            .find_map(|key| fields.get(*key).and_then(|value| value.as_str())),
        _ => None,
    });
    let path = from_exports
        .or_else(|| manifest.get("main").and_then(|value| value.as_str()))
        .unwrap_or("index.js");
    PathBuf::from(path.trim_start_matches("./"))
}

/// `canonicalize` answers with a verbatim path on Windows; a drive path with
/// the `\\?\` prefix taken back off is what every tool here expects.
fn simplify(path: PathBuf) -> PathBuf {
    let text = path.to_string_lossy();
    match text.strip_prefix(r"\\?\") {
        Some(rest) if rest.as_bytes().get(1) == Some(&b':') => PathBuf::from(rest),
        _ => path,
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
                        ));
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
        assert_eq!(
            plugin.command,
            config.dir.join("../target/debug/eggshell-plugin-example")
        );

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
        // A row with neither key is not a typo hunt: it names nothing to run.
        assert!(
            err.message.contains("either a package or a command"),
            "{}",
            err.message
        );

        let err = Config::parse("", Path::new("c.toml"), &env).unwrap_err();
        assert!(
            err.message.contains("no enabled [plugins"),
            "{}",
            err.message
        );
    }

    #[test]
    fn hashes_content_stably() {
        assert_eq!(Config::content_hash("a"), Config::content_hash("a"));
        assert_ne!(Config::content_hash("a"), Config::content_hash("b"));
    }

    /// Somewhere to put real files: layering is a filesystem feature, so it is
    /// tested through files rather than through `parse`.
    fn scratch(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("eggshell-config-{name}-{}", std::process::id()));
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
        assert_eq!(
            api.config["base_url"],
            Value::from("https://example.invalid")
        );
        assert_eq!(api.args, vec!["api.ts".to_string()]);
        assert_eq!(config.limits.request_timeout_ms, 1234);
        // Base first, entry last: the order the report prints.
        assert_eq!(config.sources, vec![base, local]);
    }

    #[test]
    fn a_disabled_row_is_configured_but_never_resolved() {
        let dir = scratch("disabled");
        let base = write(
            &dir.join("base.toml"),
            r#"
[plugins.on]
command = "node"

[plugins.off]
disabled = true
command = "node"
"#,
        );

        let config = Config::load(&base, &env_of(&[])).unwrap();
        assert_eq!(config.plugins.keys().collect::<Vec<_>>(), vec!["on"]);
        assert_eq!(config.disabled.iter().collect::<Vec<_>>(), vec!["off"]);

        let local = write(
            &dir.join("local.toml"),
            "extends = [\"base.toml\"]\n[plugins.off]\ndisabled = false\n",
        );
        let config = Config::load(&local, &env_of(&[])).unwrap();
        assert_eq!(config.plugins.keys().collect::<Vec<_>>(), vec!["off", "on"]);
        assert!(config.disabled.is_empty());
    }
    #[test]
    fn arrays_are_replaced_whole_rather_than_joined() {
        let dir = scratch("arrays");
        write(
            &dir.join("base.toml"),
            "[plugins.p]\ncommand = \"node\"\nargs = [\"a\", \"b\"]\n",
        );
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
        assert_eq!(
            config.plugins.keys().collect::<Vec<_>>(),
            vec!["extra", "p"]
        );
        // A layer is relative to the file that names it, not to the entry.
        assert_eq!(config.dir, dir);
    }

    #[test]
    fn a_diamond_or_a_cycle_reads_each_file_once() {
        let dir = scratch("diamond");
        // a extends b and c, b extends c, and c extends a: both a diamond and
        // a cycle, which must terminate with each file in first-read order.
        let row = |id: &str| format!("[plugins.{id}]\ncommand = \"node\"\n");
        let a = write(
            &dir.join("a.toml"),
            &format!("extends = [\"b.toml\", \"c.toml\"]\n{}", row("a")),
        );
        let b = write(
            &dir.join("b.toml"),
            &format!("extends = [\"c.toml\"]\n{}", row("b")),
        );
        let c = write(
            &dir.join("c.toml"),
            &format!("extends = [\"a.toml\"]\n{}", row("c")),
        );

        let config = Config::load(&a, &env_of(&[])).unwrap();
        assert_eq!(config.sources, vec![c, b, a]);
        assert_eq!(
            config.plugins.keys().collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
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

    /// Put a package where Node would look for it: `dir/node_modules/<name>`.
    fn install(dir: &Path, package: &str, manifest: &str, files: &[(&str, &str)]) -> PathBuf {
        let root = dir
            .join("node_modules")
            .join(package.replace('/', std::path::MAIN_SEPARATOR_STR));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("package.json"), manifest).unwrap();
        for (rel, body) in files {
            let path = root.join(rel);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(path, body).unwrap();
        }
        root
    }

    /// A package installed beside the config is found by walking up the
    /// node_modules chain, and the row runs that package's own entry.
    #[test]
    fn a_row_can_name_a_package_instead_of_a_command() {
        let dir = scratch("package-row");
        let installed = install(
            &dir,
            "@scope/thing",
            r#"{ "name": "@scope/thing", "exports": { ".": { "types": "./lib/index.d.ts", "default": "./lib/index.js" } } }"#,
            &[("lib/index.js", "// entry\n")],
        );
        let path = write(
            &dir.join("eggshell.toml"),
            "[plugins.thing]\nname = \"@scope/thing\"\nargs = [\"--flag\"]\n",
        );

        let config = Config::load(&path, &env_of(&[])).unwrap();
        let row = &config.plugins["thing"];
        assert_eq!(row.command, PathBuf::from("node"));
        assert_eq!(
            row.args[0],
            installed.join("lib").join("index.js").to_string_lossy()
        );
        assert_eq!(row.args[1], "--flag");
        assert_eq!(row.cwd, dir);
    }

    #[test]
    fn a_package_is_found_from_an_ancestor_of_the_config() {
        let dir = scratch("package-ancestor");
        let installed = install(
            &dir,
            "plain",
            r#"{ "name": "plain", "main": "lib/main.js" }"#,
            &[("lib/main.js", "// entry\n")],
        );
        let nested = dir.join("profiles").join("default");
        std::fs::create_dir_all(&nested).unwrap();
        let path = write(
            &nested.join("eggshell.toml"),
            "[plugins.p]\nname = \"plain\"\n",
        );

        let config = Config::load(&path, &env_of(&[])).unwrap();
        assert_eq!(
            config.plugins["p"].args[0],
            installed.join("lib").join("main.js").to_string_lossy()
        );
        // cwd stays the config file's own directory, not the package's.
        assert_eq!(config.plugins["p"].cwd, nested);
    }

    #[test]
    fn a_package_row_reports_a_missing_package_and_a_missing_entry() {
        let dir = scratch("package-missing");
        install(&dir, "hollow", r#"{ "name": "hollow" }"#, &[]);
        let absent = write(
            &dir.join("absent.toml"),
            "[plugins.gone]\nname = \"ghost\"\n",
        );
        let err = Config::load(&absent, &env_of(&[])).unwrap_err();
        assert_eq!(err.field.unwrap(), "plugins.gone.name");
        assert!(err.message.contains("ghost"), "{}", err.message);

        let empty = write(
            &dir.join("empty.toml"),
            "[plugins.hollow]\nname = \"hollow\"\n",
        );
        let err = Config::load(&empty, &env_of(&[])).unwrap_err();
        assert_eq!(err.field.unwrap(), "plugins.hollow.name");
        assert!(err.message.contains("index.js"), "{}", err.message);
    }

    #[test]
    fn a_row_needs_exactly_one_of_name_and_command() {
        let dir = scratch("package-exclusive");
        install(
            &dir,
            "both",
            r#"{ "name": "both", "main": "index.js" }"#,
            &[("index.js", "//\n")],
        );

        let mixed = write(
            &dir.join("mixed.toml"),
            "[plugins.p]\nname = \"both\"\ncommand = \"node\"\n",
        );
        let err = Config::load(&mixed, &env_of(&[])).unwrap_err();
        assert_eq!(err.field.unwrap(), "plugins.p.name");
        assert!(err.message.contains("alternatives"), "{}", err.message);

        let neither = write(&dir.join("neither.toml"), "[plugins.p]\nargs = []\n");
        let err = Config::load(&neither, &env_of(&[])).unwrap_err();
        assert_eq!(err.field.unwrap(), "plugins.p");
        assert!(
            err.message.contains("either a package or a command"),
            "{}",
            err.message
        );
    }

    #[test]
    fn a_command_row_still_runs_its_own_program() {
        let dir = scratch("package-command");
        let path = write(
            &dir.join("eggshell.toml"),
            "[plugins.p]\ncommand = \"node\"\nargs = [\"x.js\"]\n",
        );

        let config = Config::load(&path, &env_of(&[])).unwrap();
        assert_eq!(config.plugins["p"].command, PathBuf::from("node"));
        assert_eq!(config.plugins["p"].args, vec!["x.js".to_string()]);
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
