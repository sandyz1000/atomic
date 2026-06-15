//! Stateful windowed aggregation: the mergeable aggregate set, the keyed state
//! store, and the per-batch partial-aggregate SQL.
//!
//! Only **mergeable** aggregates are supported — those whose cross-batch combine
//! is associative (count, sum, min, max, and avg via sum+count). Anything else
//! (median, exact distinct, arbitrary UDAF) is rejected when the query starts.

use std::collections::HashMap;

use crate::errors::{StructuredError, StructuredResult};

/// A mergeable aggregate function.
#[derive(Clone, Copy, Debug, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub enum AggKind {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// One aggregate in a windowed query: `kind(input_col) AS output_col`.
#[derive(Clone, Debug)]
pub struct Agg {
    pub kind: AggKind,
    /// `None` only for `Count` (which is `COUNT(*)`).
    pub input_col: Option<String>,
    pub output_col: String,
}

impl Agg {
    pub fn count(output: &str) -> Self {
        Agg {
            kind: AggKind::Count,
            input_col: None,
            output_col: output.into(),
        }
    }
    pub fn sum(col: &str, output: &str) -> Self {
        Agg::numeric(AggKind::Sum, col, output)
    }
    pub fn min(col: &str, output: &str) -> Self {
        Agg::numeric(AggKind::Min, col, output)
    }
    pub fn max(col: &str, output: &str) -> Self {
        Agg::numeric(AggKind::Max, col, output)
    }
    pub fn avg(col: &str, output: &str) -> Self {
        Agg::numeric(AggKind::Avg, col, output)
    }

    fn numeric(kind: AggKind, col: &str, output: &str) -> Self {
        Agg {
            kind,
            input_col: Some(col.into()),
            output_col: output.into(),
        }
    }

    /// The `SELECT` expressions this aggregate contributes to the per-batch
    /// partial query, aliased so the engine can read them back positionally.
    /// `Avg` contributes two partials (sum + count); the rest contribute one.
    pub(crate) fn select_exprs(&self, idx: usize) -> Vec<String> {
        let col = self.input_col.as_deref().unwrap_or("*");
        match self.kind {
            AggKind::Count => vec![format!("COUNT(*) AS _agg{idx}")],
            AggKind::Sum => vec![format!("SUM({col}) AS _agg{idx}")],
            AggKind::Min => vec![format!("MIN({col}) AS _agg{idx}")],
            AggKind::Max => vec![format!("MAX({col}) AS _agg{idx}")],
            AggKind::Avg => vec![
                format!("SUM({col}) AS _agg{idx}_s"),
                format!("COUNT({col}) AS _agg{idx}_c"),
            ],
        }
    }

    /// Number of result columns this aggregate occupies in the partial query.
    pub(crate) fn partial_width(&self) -> usize {
        match self.kind {
            AggKind::Avg => 2,
            _ => 1,
        }
    }
}

/// Running value of one aggregate within a (window, group) cell.
#[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
pub enum AggState {
    Count(i64),
    Sum(f64),
    Min(f64),
    Max(f64),
    Avg { sum: f64, count: i64 },
}

impl AggState {
    /// Combine a same-kind partial into this running value (associative).
    pub fn merge(&mut self, other: &AggState) {
        match (self, other) {
            (AggState::Count(a), AggState::Count(b)) => *a += b,
            (AggState::Sum(a), AggState::Sum(b)) => *a += b,
            (AggState::Min(a), AggState::Min(b)) => *a = a.min(*b),
            (AggState::Max(a), AggState::Max(b)) => *a = a.max(*b),
            (AggState::Avg { sum: s1, count: c1 }, AggState::Avg { sum: s2, count: c2 }) => {
                *s1 += s2;
                *c1 += c2;
            }
            _ => {} // mismatched kinds never occur for a fixed query
        }
    }

    /// Emitted numeric value (avg divides; count returned as f64).
    pub fn output_value(&self) -> f64 {
        match self {
            AggState::Count(c) => *c as f64,
            AggState::Sum(v) | AggState::Min(v) | AggState::Max(v) => *v,
            AggState::Avg { sum, count } => {
                if *count > 0 {
                    sum / *count as f64
                } else {
                    0.0
                }
            }
        }
    }

    pub fn is_count(&self) -> bool {
        matches!(self, AggState::Count(_))
    }
}

/// A value of a grouping key column (the subset of Arrow types we group on).
#[derive(Clone, Debug, PartialEq, Eq, Hash, bincode::Encode, bincode::Decode)]
pub enum GroupVal {
    Str(String),
    Int(i64),
    Null,
}

/// Identity of one aggregation cell: its window plus grouping-key values.
#[derive(Clone, Debug, PartialEq, Eq, Hash, bincode::Encode, bincode::Decode)]
pub struct StateKey {
    pub window_start_ms: u64,
    pub group: Vec<GroupVal>,
}

/// The keyed aggregation state, persisted to/restored from a checkpoint.
#[derive(Default, bincode::Encode, bincode::Decode)]
pub struct StateStore {
    map: HashMap<StateKey, Vec<AggState>>,
}

impl StateStore {
    pub fn new() -> Self {
        StateStore::default()
    }

    /// Merge a partial cell into the store; returns `true` if the cell changed.
    pub fn merge(&mut self, key: StateKey, partial: Vec<AggState>) {
        match self.map.get_mut(&key) {
            Some(existing) => {
                for (e, p) in existing.iter_mut().zip(partial.iter()) {
                    e.merge(p);
                }
            }
            None => {
                self.map.insert(key, partial);
            }
        }
    }

    /// Current value of a cell, if present.
    pub fn get(&self, key: &StateKey) -> Option<&Vec<AggState>> {
        self.map.get(key)
    }

    /// All cells, in arbitrary order (Complete mode).
    pub fn iter(&self) -> impl Iterator<Item = (&StateKey, &Vec<AggState>)> {
        self.map.iter()
    }

    /// Remove and return all cells whose window has closed
    /// (`window_start + window_size_ms <= watermark`). Used by Append mode.
    pub fn evict_final(
        &mut self,
        watermark_ms: u64,
        window_size_ms: u64,
    ) -> Vec<(StateKey, Vec<AggState>)> {
        let final_keys: Vec<StateKey> = self
            .map
            .keys()
            .filter(|k| k.window_start_ms + window_size_ms <= watermark_ms)
            .cloned()
            .collect();
        final_keys
            .into_iter()
            .map(|k| {
                let v = self.map.remove(&k).unwrap();
                (k, v)
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Serialize for checkpointing.
    pub fn encode(&self) -> StructuredResult<Vec<u8>> {
        bincode::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| StructuredError::Checkpoint(e.to_string()))
    }

    /// Restore from a checkpoint blob.
    pub fn decode(bytes: &[u8]) -> StructuredResult<Self> {
        bincode::decode_from_slice(bytes, bincode::config::standard())
            .map(|(s, _)| s)
            .map_err(|e| StructuredError::Checkpoint(e.to_string()))
    }
}

/// Validate that every aggregate is mergeable (all currently are, but this is the
/// guard point where a future non-mergeable agg would be rejected at start).
pub fn ensure_mergeable(aggs: &[Agg]) -> StructuredResult<()> {
    for agg in aggs {
        match agg.kind {
            AggKind::Count | AggKind::Sum | AggKind::Min | AggKind::Max | AggKind::Avg => {}
            #[allow(unreachable_patterns)]
            other => {
                return Err(StructuredError::Unsupported(format!(
                    "aggregate {other:?} is not incrementally mergeable"
                )));
            }
        }
    }
    Ok(())
}
