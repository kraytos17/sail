use std::collections::HashMap;
use std::fmt;

/// Error for a dependency cycle between rules.
///
/// `members` holds the rule names that participate in (or depend on) the
/// cycle, in input order. Self-references count as cycles of one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleError {
    pub members: Vec<String>,
}

impl fmt::Display for CycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "dependency cycle involving rules: {}",
            self.members.join(", ")
        )
    }
}

impl std::error::Error for CycleError {}

/// Deterministically orders rule names so each rule follows the rules it uses.
///
/// `rules` maps each rule name to the names it references; `inputs` lists
/// known input-variable names. References to inputs — and to unknown names —
/// are ignored, never errors. The output is stable for a given input
/// (input order breaks ties); only edge-respect is contractual.
///
/// ```rust
/// # use sail_rhai_udf::sort_rules;
/// let rules = vec![
///     ("c".to_string(), vec!["b".to_string()]),
///     ("b".to_string(), vec!["a".to_string()]),
///     ("a".to_string(), vec![]),
/// ];
/// assert_eq!(
///     sort_rules(&rules, &[]).unwrap(),
///     vec!["a".to_string(), "b".to_string(), "c".to_string()]
/// );
/// ```
pub fn sort_rules(
    rules: &[(String, Vec<String>)],
    inputs: &[String],
) -> Result<Vec<String>, CycleError> {
    let index_of: HashMap<&str, usize> = rules
        .iter()
        .enumerate()
        .map(|(i, (name, _))| (name.as_str(), i))
        .collect();
    // Edge dep -> rule for rule-to-rule references only.
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); rules.len()];
    let mut indegree = vec![0_usize; rules.len()];
    for (i, (_, deps)) in rules.iter().enumerate() {
        let mut seen = Vec::new();
        for dep in deps {
            if inputs.iter().any(|input| input == dep) {
                continue;
            }
            let Some(&d) = index_of.get(dep.as_str()) else {
                // Unknown names are ignored, never errors.
                continue;
            };
            if seen.contains(&d) {
                continue;
            }

            seen.push(d);
            indegree[i] += 1;
            if d != i {
                dependents[d].push(i);
            }
            // A self-reference only raises the indegree, so the rule can
            // never become ready and is reported as a cycle of one.
        }
    }
    // Kahn's algorithm with input-ordered selection for determinism.
    let mut ready: Vec<usize> = indegree
        .iter()
        .enumerate()
        .filter(|&(_, &deg)| deg == 0)
        .map(|(i, _)| i)
        .collect();
    let mut sorted = Vec::with_capacity(rules.len());
    while let Some(next) = ready.iter().min().copied() {
        ready.retain(|&i| i != next);
        sorted.push(next);
        for &dependent in &dependents[next] {
            indegree[dependent] -= 1;
            if indegree[dependent] == 0 {
                ready.push(dependent);
            }
        }
    }
    if sorted.len() == rules.len() {
        Ok(sorted.into_iter().map(|i| rules[i].0.clone()).collect())
    } else {
        let mut in_sorted = vec![false; rules.len()];
        for i in sorted {
            in_sorted[i] = true;
        }
        Err(CycleError {
            members: rules
                .iter()
                .enumerate()
                .filter(|(i, _)| !in_sorted[*i])
                .map(|(_, (name, _))| name.clone())
                .collect(),
        })
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn ruleset(pairs: &[(&str, &[&str])]) -> Vec<(String, Vec<String>)> {
        pairs
            .iter()
            .map(|(name, deps)| {
                (
                    name.to_string(),
                    deps.iter().map(ToString::to_string).collect(),
                )
            })
            .collect()
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn chain_and_diamond() {
        let rules = ruleset(&[("c", &["b"]), ("b", &["a"]), ("a", &[])]);
        assert_eq!(sort_rules(&rules, &[]).unwrap(), names(&["a", "b", "c"]));
        let rules = ruleset(&[
            ("top", &["left", "right"]),
            ("left", &["base"]),
            ("right", &["base"]),
            ("base", &[]),
        ]);
        let sorted = sort_rules(&rules, &[]).unwrap();
        let pos = |n: &str| sorted.iter().position(|x| x == n).unwrap();
        assert!(pos("base") < pos("left"));
        assert!(pos("base") < pos("right"));
        assert!(pos("left") < pos("top"));
        assert!(pos("right") < pos("top"));
    }

    #[test]
    fn cycles_name_their_members() {
        let rules = ruleset(&[("a", &["b"]), ("b", &["a"]), ("c", &[])]);
        let err = sort_rules(&rules, &[]).unwrap_err();
        assert_eq!(err.members, names(&["a", "b"]));
        let rules = ruleset(&[("a", &["a"])]);
        let err = sort_rules(&rules, &[]).unwrap_err();
        assert_eq!(err.members, names(&["a"]));
    }

    #[test]
    fn unknown_and_input_names_are_ignored() {
        // Both rules have no rule-to-rule edges, so input order breaks the tie.
        let rules = ruleset(&[("b", &["missing", "IN1"]), ("a", &[])]);
        assert_eq!(
            sort_rules(&rules, &[String::from("IN1")]).unwrap(),
            names(&["b", "a"])
        );
        // Input names that shadow nothing still sort fine when empty.
        let empty: Vec<(String, Vec<String>)> = Vec::new();
        assert!(sort_rules(&empty, &[]).unwrap().is_empty());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        #[test]
        fn random_dags_respect_edges(n in 1usize..8, seed in 0u64..1_000_000) {
            // Edges only point from higher to lower indices, so the graph is acyclic.
            let mut rand_state = seed;
            let mut next_bit = || {
                rand_state = rand_state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (rand_state >> 33) & 1 == 1
            };
            let mut rules: Vec<(String, Vec<String>)> = Vec::new();
            for i in 0..n {
                let mut deps = Vec::new();
                for j in 0..i {
                    if next_bit() {
                        deps.push(format!("r{j}"));
                    }
                }
                rules.push((format!("r{i}"), deps));
            }
            let sorted = sort_rules(&rules, &[]).unwrap();
            prop_assert_eq!(sorted.len(), n);
            for (name, deps) in &rules {
                let pos = sorted.iter().position(|x| x == name).unwrap();
                for dep in deps {
                    let dep_pos = sorted.iter().position(|x| x == dep).unwrap();
                    prop_assert!(dep_pos < pos, "{dep} must precede {name}");
                }
            }
        }
    }
}
