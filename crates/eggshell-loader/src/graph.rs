//! Dependency graph.
//!
//! Two sources of truth must agree: the config maps capability slots to plugin
//! ids, and each plugin declares what it provides and requires. Everything here
//! is pure so it can be unit tested without spawning anything.

use std::collections::{BTreeMap, BTreeSet};

use semver::{Version, VersionReq};
use serde_json::Value;

use eggshell_protocol::codes;

pub use crate::route::{Route, RoutingTable};
#[derive(Debug, Clone, PartialEq)]
pub struct Req {
    pub capability: String,
    pub version: String,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Decl {
    pub id: String,
    pub provides: Vec<(String, String)>,
    pub requires: Vec<Req>,
}

#[derive(Debug, Clone)]
pub struct Issue {
    pub code: i64,
    pub message: String,
    pub field: Option<String>,
}

impl Issue {
    pub fn error(code: i64, message: impl Into<String>) -> Self {
        Issue { code, message: message.into(), field: None }
    }

    pub fn warning(message: impl Into<String>) -> Self {
        Issue { code: codes::INVALID_CONFIG, message: message.into(), field: None }
    }

    pub fn at(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

#[derive(Debug, Default)]
pub struct Report {
    pub table: RoutingTable,
    pub errors: Vec<Issue>,
    pub warnings: Vec<Issue>,
}

/// Reads `provides` / `requires` out of an `initialize` reply.
pub fn decl_from_initialize(id: &str, params: &Value) -> Result<Decl, Issue> {
    let provides_raw = params
        .get("provides")
        .and_then(Value::as_array)
        .ok_or_else(|| Issue::error(codes::INVALID_PARAMS, "initialize reply has no `provides` array"))?;
    let requires_raw = params
        .get("requires")
        .and_then(Value::as_array)
        .ok_or_else(|| Issue::error(codes::INVALID_PARAMS, "initialize reply has no `requires` array"))?;

    let mut provides = Vec::new();
    for item in provides_raw {
        let capability = item.get("capability").and_then(Value::as_str).ok_or_else(|| {
            Issue::error(codes::INVALID_PARAMS, "provides[] entry without `capability`")
        })?;
        let version = item.get("version").and_then(Value::as_str).ok_or_else(|| {
            Issue::error(codes::INVALID_PARAMS, "provides[] entry without `version`")
        })?;
        if let Err(e) = Version::parse(version) {
            return Err(Issue::error(
                codes::INVALID_PARAMS,
                format!("provided version {version:?} of {capability} is not semver: {e}"),
            ));
        }
        provides.push((capability.to_string(), version.to_string()));
    }

    let mut requires = Vec::new();
    for item in requires_raw {
        let capability = item.get("capability").and_then(Value::as_str).ok_or_else(|| {
            Issue::error(codes::INVALID_PARAMS, "requires[] entry without `capability`")
        })?;
        let version = item.get("version").and_then(Value::as_str).ok_or_else(|| {
            Issue::error(codes::INVALID_PARAMS, "requires[] entry without `version`")
        })?;
        if let Err(e) = VersionReq::parse(version) {
            return Err(Issue::error(
                codes::INVALID_PARAMS,
                format!("required range {version:?} of {capability} is not supported: {e}"),
            ));
        }
        requires.push(Req {
            capability: capability.to_string(),
            version: version.to_string(),
            optional: item.get("optional").and_then(Value::as_bool).unwrap_or(false),
        });
    }

    Ok(Decl { id: id.to_string(), provides, requires })
}

pub fn satisfies(range: &str, version: &str) -> Result<bool, String> {
    let req = VersionReq::parse(range).map_err(|e| format!("bad range {range:?}: {e}"))?;
    let ver = Version::parse(version).map_err(|e| format!("bad version {version:?}: {e}"))?;
    Ok(req.matches(&ver))
}

/// Cross-checks config against plugin declarations.
pub fn validate(decls: &[Decl], capability: &BTreeMap<String, String>) -> Report {
    let mut report = Report::default();
    let by_id: BTreeMap<&str, &Decl> = decls.iter().map(|d| (d.id.as_str(), d)).collect();

    for (slot, plugin) in capability {
        let Some(decl) = by_id.get(plugin.as_str()) else {
            report.errors.push(
                Issue::error(
                    codes::INVALID_CONFIG,
                    format!("capability `{slot}` is mapped to `{plugin}`, which is not configured"),
                )
                .at(format!("capability.{slot}")),
            );
            continue;
        };
        match decl.provides.iter().find(|(cap, _)| cap == slot) {
            Some((_, version)) => {
                report.table.insert(
                    slot.clone(),
                    Route { plugin: plugin.clone(), version: version.clone() },
                );
            }
            None => {
                let offered = if decl.provides.is_empty() {
                    "nothing".to_string()
                } else {
                    decl.provides
                        .iter()
                        .map(|(c, v)| format!("{c} {v}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                let hint = closest(slot, decl.provides.iter().map(|(c, _)| c.as_str()));
                report.errors.push(
                    Issue::error(
                        codes::INVALID_CONFIG,
                        format!(
                            "`{plugin}` does not provide `{slot}`; it provides {offered}{}",
                            hint
                        ),
                    )
                    .at(format!("capability.{slot}")),
                );
            }
        }
    }

    for decl in decls {
        for (cap, version) in &decl.provides {
            if let Err(e) = Version::parse(version) {
                report.errors.push(Issue::error(
                    codes::INVALID_CONFIG,
                    format!("`{}` declares `{cap}` version {version:?}, which is not semver: {e}", decl.id),
                ));
            }
        }
    }

    let mut used: BTreeSet<String> = BTreeSet::new();
    for decl in decls {
        for req in &decl.requires {
            match report.table.get(&req.capability) {
                Some(route) => match satisfies(&req.version, &route.version) {
                    Ok(true) => {
                        used.insert(req.capability.clone());
                    }
                    Ok(false) => {
                        let message = format!(
                            "`{}` requires `{}` {} but `{}` provides {}",
                            decl.id, req.capability, req.version, route.plugin, route.version
                        );
                        if req.optional {
                            report.warnings.push(Issue::warning(format!(
                                "optional dependency skipped: {message}"
                            )));
                        } else {
                            report.errors.push(Issue::error(codes::INVALID_CONFIG, message));
                        }
                    }
                    Err(e) => {
                        report.errors.push(Issue::error(
                            codes::INVALID_CONFIG,
                            format!("`{}`: {e}", decl.id),
                        ));
                    }
                },
                None => {
                    let hint = closest(&req.capability, report.table.keys().map(String::as_str));
                    if req.optional {
                        report.warnings.push(Issue::warning(format!(
                            "optional dependency skipped: `{}` requires `{}` but no provider is configured{}",
                            decl.id, req.capability, hint
                        )));
                    } else {
                        report.errors.push(Issue::error(
                            codes::INVALID_CONFIG,
                            format!(
                                "`{}` requires `{}` but no provider is configured{}",
                                decl.id, req.capability, hint
                            ),
                        ));
                    }
                }
            }
        }
    }

    for slot in report.table.keys() {
        if !used.contains(slot) {
            report.warnings.push(Issue::warning(format!(
                "capability slot `{slot}` is configured but no plugin requires it"
            )));
        }
    }

    report
}

/// Topological start order; ties are broken by plugin id so the order is stable.
pub fn start_order(decls: &[Decl], table: &RoutingTable) -> Result<Vec<String>, Issue> {
    let ids: BTreeSet<String> = decls.iter().map(|d| d.id.clone()).collect();
    let mut indegree: BTreeMap<String, usize> = ids.iter().map(|id| (id.clone(), 0)).collect();
    let mut dependents: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for decl in decls {
        for req in &decl.requires {
            let Some(route) = table.get(&req.capability) else { continue };
            if route.plugin == decl.id {
                continue;
            }
            if dependents
                .entry(route.plugin.clone())
                .or_default()
                .insert(decl.id.clone())
            {
                *indegree.get_mut(&decl.id).unwrap() += 1;
            }
        }
    }

    let mut ready: BTreeSet<String> = indegree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(id, _)| id.clone())
        .collect();
    let mut order = Vec::with_capacity(ids.len());

    while let Some(id) = ready.iter().next().cloned() {
        ready.remove(&id);
        order.push(id.clone());
        if let Some(followers) = dependents.get(&id) {
            for follower in followers {
                let degree = indegree.get_mut(follower).unwrap();
                *degree -= 1;
                if *degree == 0 {
                    ready.insert(follower.clone());
                }
            }
        }
    }

    if order.len() == ids.len() {
        return Ok(order);
    }

    let remaining: BTreeSet<String> = ids.difference(&order.iter().cloned().collect()).cloned().collect();
    let mut path = Vec::new();
    let mut seen = BTreeSet::new();
    let mut cursor = remaining.iter().next().cloned().unwrap();
    loop {
        if !seen.insert(cursor.clone()) {
            break;
        }
        path.push(cursor.clone());
        let next = dependents
            .get(&cursor)
            .and_then(|set| set.iter().find(|id| remaining.contains(*id)).cloned());
        match next {
            Some(next) => cursor = next,
            None => break,
        }
    }
    if let Some(rest) = path.iter().position(|id| *id == cursor) {
        path = path[rest..].to_vec();
    }
    path.push(cursor);
    let mut cycle = path;
    cycle.dedup();
    Err(Issue::error(
        codes::INVALID_CONFIG,
        format!("dependency cycle: {}", cycle.join(" -> ")),
    ))
}

/// Best-effort typo hint: ` (did you mean `demo.store`?)`.
fn closest<'a>(name: &str, candidates: impl Iterator<Item = &'a str>) -> String {
    let mut best: Option<(usize, &str)> = None;
    for candidate in candidates {
        let distance = edit_distance(name, candidate);
        if distance <= 2 && best.map(|(d, _)| distance < d).unwrap_or(true) {
            best = Some((distance, candidate));
        }
    }
    match best {
        Some((_, candidate)) => format!(" (did you mean `{candidate}`?)"),
        None => String::new(),
    }
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0usize; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        current[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            current[j + 1] = (previous[j] + cost).min(previous[j + 1] + 1).min(current[j] + 1);
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn decl(id: &str, provides: &[(&str, &str)], requires: &[(&str, &str, bool)]) -> Decl {
        Decl {
            id: id.to_string(),
            provides: provides.iter().map(|(c, v)| (c.to_string(), v.to_string())).collect(),
            requires: requires
                .iter()
                .map(|(c, v, o)| Req {
                    capability: c.to_string(),
                    version: v.to_string(),
                    optional: *o,
                })
                .collect(),
        }
    }

    fn slots(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(c, p)| (c.to_string(), p.to_string())).collect()
    }

    #[test]
    fn reads_declarations_from_an_initialize_reply() {
        let params = json!({
            "protocol": 1,
            "provides": [{"capability": "demo.text", "version": "1.0.0"}],
            "requires": [
                {"capability": "demo.store", "version": ">=1.0.0, <2.0.0"},
                {"capability": "demo.tools", "version": "^1", "optional": true},
            ],
        });
        let parsed = decl_from_initialize("consumer", &params).unwrap();
        assert_eq!(parsed.provides, vec![("demo.text".to_string(), "1.0.0".to_string())]);
        assert_eq!(parsed.requires[0].optional, false);
        assert_eq!(parsed.requires[1].optional, true);

        assert!(decl_from_initialize("x", &json!({"provides": []})).is_err());
        assert!(decl_from_initialize("x", &json!({"provides": [{"capability": "a", "version": "nope"}], "requires": []})).is_err());
        // `||` is not part of the supported subset.
        assert!(decl_from_initialize(
            "x",
            &json!({"provides": [], "requires": [{"capability": "a", "version": ">=1.0.0 || <2.0.0"}]})
        )
        .is_err());
    }

    #[test]
    fn builds_the_table_and_reports_an_unprovided_slot() {
        let decls = vec![decl("provider", &[("demo.text", "1.0.0")], &[])];
        let report = validate(&decls, &slots(&[("demo.text", "provider"), ("demo.web", "provider")]));
        assert_eq!(report.table["demo.text"].version, "1.0.0");
        assert_eq!(report.errors.len(), 1, "{report:?}");
        assert!(report.errors[0].message.contains("does not provide `demo.web`"));
        assert_eq!(report.errors[0].field.as_deref(), Some("capability.demo.web"));
    }

    #[test]
    fn missing_required_dependency_fails_but_optional_is_skipped() {
        let decls = vec![decl(
            "consumer",
            &[("demo.loop", "1.0.0")],
            &[("demo.text", "^1", false), ("demo.tools", "^1", true)],
        )];
        let report = validate(&decls, &slots(&[("demo.loop", "consumer")]));
        assert_eq!(report.errors.len(), 1, "{report:?}");
        assert!(report.errors[0].message.contains("requires `demo.text`"));
        // Two warnings: the skipped optional dependency, plus the unused `demo.loop` slot.
        assert_eq!(report.warnings.len(), 2, "{report:?}");
        assert!(
            report.warnings.iter().any(|w| w.message.contains("optional dependency skipped")),
            "{report:?}"
        );
    }

    #[test]
    fn version_mismatch_is_an_error_when_required_and_a_warning_when_optional() {
        let decls = vec![
            decl("provider", &[("demo.text", "1.0.0")], &[]),
            decl("consumer", &[("demo.loop", "1.0.0")], &[("demo.text", "^2", false)]),
        ];
        let report = validate(
            &decls,
            &slots(&[("demo.loop", "consumer"), ("demo.text", "provider")]),
        );
        assert_eq!(report.errors.len(), 1, "{report:?}");
        assert!(report.errors[0].message.contains("provides 1.0.0"), "{report:?}");

        let optional = vec![
            decl("provider", &[("demo.text", "1.0.0")], &[]),
            decl("consumer", &[("demo.loop", "1.0.0")], &[("demo.text", "^2", true)]),
        ];
        let report = validate(
            &optional,
            &slots(&[("demo.loop", "consumer"), ("demo.text", "provider")]),
        );
        assert!(report.errors.is_empty(), "{report:?}");
        assert!(
            report.warnings.iter().any(|w| w.message.contains("optional dependency skipped")),
            "{report:?}"
        );
    }

    #[test]
    fn suggests_a_close_capability_name() {
        let decls = vec![decl("store-provider", &[("demo.store", "1.0.0")], &[])];
        let report = validate(&decls, &slots(&[("demo.store", "store-provider")]));
        assert!(report.errors.is_empty(), "{report:?}");

        let decls = vec![
            decl("store-provider", &[("demo.store", "1.0.0")], &[]),
            decl("consumer", &[("demo.loop", "1.0.0")], &[("demoo.store", "^1", false)]),
        ];
        let report = validate(
            &decls,
            &slots(&[("demo.store", "store-provider"), ("demo.loop", "consumer")]),
        );
        assert_eq!(report.errors.len(), 1, "{report:?}");
        assert!(report.errors[0].message.contains("did you mean `demo.store`?"), "{report:?}");
    }

    #[test]
    fn orders_providers_before_consumers_and_breaks_ties_by_id() {
        let decls = vec![
            decl("consumer", &[("demo.loop", "1.0.0")], &[("demo.text", "^1", false)]),
            decl("provider", &[("demo.text", "1.0.0")], &[]),
            decl("subscriber", &[], &[("demo.loop", "^1", false)]),
        ];
        let report = validate(
            &decls,
            &slots(&[
                ("demo.loop", "consumer"),
                ("demo.text", "provider"),
            ]),
        );
        assert!(report.errors.is_empty(), "{report:?}");
        let order = start_order(&decls, &report.table).unwrap();
        assert_eq!(order, vec!["provider", "consumer", "subscriber"]);
    }

    #[test]
    fn detects_a_cycle() {
        let decls = vec![
            decl("a", &[("cap.a", "1.0.0")], &[("cap.b", "^1", false)]),
            decl("b", &[("cap.b", "1.0.0")], &[("cap.a", "^1", false)]),
        ];
        let report = validate(&decls, &slots(&[("cap.a", "a"), ("cap.b", "b")]));
        assert!(report.errors.is_empty(), "{report:?}");
        let err = start_order(&decls, &report.table).unwrap_err();
        assert!(err.message.contains("dependency cycle"), "{}", err.message);
        assert!(err.message.contains("a") && err.message.contains("b"));
    }

    #[test]
    fn a_plugin_may_require_what_it_provides() {
        let decls = vec![decl("solo", &[("cap.x", "1.0.0")], &[("cap.x", "^1", false)])];
        let report = validate(&decls, &slots(&[("cap.x", "solo")]));
        assert!(report.errors.is_empty(), "{report:?}");
        assert_eq!(start_order(&decls, &report.table).unwrap(), vec!["solo"]);
    }
}

