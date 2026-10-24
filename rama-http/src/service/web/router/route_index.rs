use crate::matcher::UriParams;
use rama_net::uri::{PathPattern, PathPatternSegmentKind, PathPatternSegmentSpecificity, PathRef};
use std::cmp::Reverse;

/// Shared full-path ordering for both endpoint and concrete-input routing.
#[derive(Clone)]
pub(super) struct RouteIndex<T> {
    entries: Vec<Entry<T>>,
}

#[derive(Clone)]
struct Entry<T> {
    pattern: PathPattern,
    specificity: Vec<SegmentRank>,
    value: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SegmentRank {
    kind: u8,
    literal_bytes: usize,
    fewer_dynamic_parts: Reverse<usize>,
    fewer_optional_parts: Reverse<usize>,
}

fn rank(spec: PathPatternSegmentSpecificity) -> SegmentRank {
    SegmentRank {
        kind: match spec.kind {
            PathPatternSegmentKind::Literal => 2,
            PathPatternSegmentKind::Dynamic => 1,
            PathPatternSegmentKind::CatchAll => 0,
        },
        literal_bytes: spec.literal_bytes,
        fewer_dynamic_parts: Reverse(spec.dynamic_parts),
        fewer_optional_parts: Reverse(spec.optional_parts),
    }
}

impl<T> Default for RouteIndex<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<T: Default> RouteIndex<T> {
    pub(super) fn with_entry(&mut self, pattern: PathPattern, update: impl FnOnce(&mut T)) {
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.pattern == pattern)
        {
            update(&mut entry.value);
            return;
        }
        let specificity = specificity_of(&pattern);
        let position = self
            .entries
            .partition_point(|entry| entry.specificity >= specificity);
        let mut value = T::default();
        update(&mut value);
        self.entries.insert(
            position,
            Entry {
                pattern,
                specificity,
                value,
            },
        );
    }
}

impl<T> RouteIndex<T> {
    pub(super) fn find(&self, path: PathRef<'_>) -> Option<(&T, UriParams)> {
        self.entries.iter().find_map(|entry| {
            entry
                .pattern
                .captures(path)
                .map(|captures| (&entry.value, UriParams::from_captures(&captures)))
        })
    }

    pub(super) fn into_entries(self) -> impl Iterator<Item = (PathPattern, T)> {
        self.entries
            .into_iter()
            .map(|entry| (entry.pattern, entry.value))
    }
}

fn specificity_of(pattern: &PathPattern) -> Vec<SegmentRank> {
    pattern.segment_specificity().map(rank).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matcher::{PathMatchPolicy, path::compile_pattern_with_policy};

    #[test]
    fn specificity_reflects_segment_kinds()
    -> Result<(), Vec<(PathPattern, Vec<u8>, Vec<SegmentRank>)>> {
        let observations: Vec<_> = [
            ("/a/b/c", vec![2, 2, 2]),
            ("/users/{id}", vec![2, 1]),
            ("/files/{}.json", vec![2, 1]),
            ("/assets/{*path}", vec![2, 0]),
            ("/api/{*bad name}", vec![2, 2]),
        ]
        .into_iter()
        .map(|(path, expected)| {
            let pattern = compile_pattern_with_policy(path, PathMatchPolicy::default());
            let received = specificity_of(&pattern);
            (pattern, expected, received)
        })
        .collect();
        observations
            .iter()
            .all(|(_, expected, received)| {
                received
                    .iter()
                    .map(|rank| rank.kind)
                    .eq(expected.iter().copied())
            })
            .then_some(())
            .ok_or(observations)
    }

    #[test]
    fn specificity_breaks_dynamic_ties_with_literal_weight() -> Result<(), [Vec<SegmentRank>; 3]> {
        let compile = |path| {
            specificity_of(&compile_pattern_with_policy(
                path,
                PathMatchPolicy::default(),
            ))
        };
        let [plain, json, wildcard_json] = [
            compile("/files/{name}"),
            compile("/files/{name}.json"),
            compile("/files/{}.json"),
        ];
        (json > plain && wildcard_json > plain)
            .then_some(())
            .ok_or([plain, json, wildcard_json])
    }
}
