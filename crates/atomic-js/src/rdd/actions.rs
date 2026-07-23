use std::collections::HashMap;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde_json::Value as JsonValue;

use super::{JsRdd, SimpleLcg};

#[napi]
impl JsRdd {
    /// Return all elements as a JavaScript array.
    #[napi]
    pub fn collect(&mut self) -> Result<Vec<JsonValue>> {
        if self.context.is_distributed() && self.staged.is_some() {
            return self.dispatch_and_collect();
        }
        Ok(self.elements.clone())
    }

    /// Return the number of elements.
    #[napi]
    pub fn count(&mut self) -> Result<u32> {
        if self.context.is_distributed() && self.staged.is_some() {
            let items = self.dispatch_and_collect()?;
            return Ok(items.len() as u32);
        }
        Ok(self.elements.len() as u32)
    }

    /// Return the first element. Throws if the RDD is empty.
    #[napi]
    pub fn first(&self) -> Result<JsonValue> {
        self.elements
            .first()
            .cloned()
            .ok_or_else(|| Error::from_reason("first: RDD is empty"))
    }

    /// Return the first `n` elements.
    #[napi]
    pub fn take(&self, n: u32) -> Vec<JsonValue> {
        self.elements.iter().take(n as usize).cloned().collect()
    }

    /// Aggregate all elements using `f(acc, element) => acc`. Throws if empty.
    #[napi]
    pub fn reduce(
        &mut self,
        f: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsonValue> {
        if self.context.is_distributed() {
            let fn_src = Self::fn_to_source(&f)?;
            let partition_fn = format!(
                "(partition) => partition.length === 0 ? [] : [partition.reduce(({}))]",
                fn_src
            );
            self.stage_js_task(
                partition_fn,
                atomic_data::distributed::TaskAction::Reduce,
                None,
            )?;
            let staged = self
                .staged
                .as_ref()
                .ok_or_else(|| Error::from_reason("no staged pipeline"))?;
            let result_bytes = self
                .context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.steps.clone())
                .map_err(|e| Error::from_reason(format!("dispatch_pipeline: {e}")))?;

            let mut acc: Option<JsonValue> = None;
            for bytes in result_bytes {
                let items: Vec<JsonValue> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::from_reason(format!("reduce decode: {e}")))?;
                for item in items {
                    acc = Some(match acc {
                        None => item,
                        Some(a) => f.call(FnArgs::from((a, item)))?,
                    });
                }
            }
            return acc.ok_or_else(|| Error::from_reason("reduce on empty RDD"));
        }
        let mut iter = self.elements.iter();
        let first = iter
            .next()
            .ok_or_else(|| Error::from_reason("reduce on empty RDD"))?;
        let mut acc = first.clone();
        for val in iter {
            acc = f.call(FnArgs::from((acc, val.clone())))?;
        }
        Ok(acc)
    }

    /// Aggregate with an initial value using `f(acc, element) => acc`.
    #[napi]
    pub fn fold(
        &mut self,
        zero: JsonValue,
        f: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsonValue> {
        if self.context.is_distributed() {
            let zero_json = zero.to_string();
            let fn_src = Self::fn_to_source(&f)?;
            let partition_fn = format!(
                "(partition) => [partition.reduce(({})  , {})]",
                fn_src, zero_json
            );
            self.stage_js_task(
                partition_fn,
                atomic_data::distributed::TaskAction::Fold,
                None,
            )?;
            let staged = self
                .staged
                .as_ref()
                .ok_or_else(|| Error::from_reason("no staged pipeline"))?;
            let result_bytes = self
                .context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.steps.clone())
                .map_err(|e| Error::from_reason(format!("dispatch_pipeline: {e}")))?;

            let mut acc = zero;
            for bytes in result_bytes {
                let items: Vec<JsonValue> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::from_reason(format!("fold decode: {e}")))?;
                for item in items {
                    acc = f.call(FnArgs::from((acc, item)))?;
                }
            }
            return Ok(acc);
        }
        let mut acc = zero;
        for val in &self.elements {
            acc = f.call(FnArgs::from((acc, val.clone())))?;
        }
        Ok(acc)
    }

    /// Apply `f` to each element for side effects.
    #[napi]
    pub fn for_each(&self, f: Function<JsonValue, Unknown>) -> Result<()> {
        for elem in &self.elements {
            f.call(elem.clone())?;
        }
        Ok(())
    }

    /// Apply `f` to each logical partition for side effects.
    #[napi]
    pub fn for_each_partition(&self, f: Function<Vec<JsonValue>, Unknown>) -> Result<()> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        for (start, end) in super::slice_positions(total, np) {
            f.call(self.elements[start..end].to_vec())?;
        }
        Ok(())
    }

    /// Count occurrences of each distinct element. Returns `{element: count}`.
    #[napi]
    pub fn count_by_value(&self) -> JsonValue {
        let mut counts: HashMap<String, u64> = HashMap::new();
        for elem in &self.elements {
            *counts.entry(elem.to_string()).or_insert(0) += 1;
        }
        serde_json::to_value(counts).unwrap_or(JsonValue::Object(Default::default()))
    }

    /// Count elements per key in a pair RDD. Returns `{key: count}`.
    #[napi]
    pub fn count_by_key(&self) -> Result<JsonValue> {
        let mut counts: HashMap<String, u64> = HashMap::new();
        for elem in &self.elements {
            let pair = elem
                .as_array()
                .ok_or_else(|| Error::from_reason("count_by_key requires [key, value] arrays"))?;
            if pair.is_empty() {
                return Err(Error::from_reason("count_by_key requires non-empty arrays"));
            }
            let key = Self::key_to_string(&pair[0])?;
            *counts.entry(key).or_insert(0) += 1;
        }
        Ok(serde_json::to_value(counts).unwrap_or(JsonValue::Object(Default::default())))
    }

    /// Return true if the RDD has no elements.
    #[napi]
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Return the maximum element. Optional comparator `f(a, b) => number` (positive = a > b).
    #[napi]
    pub fn max(
        &self,
        comparator: Option<Function<FnArgs<(JsonValue, JsonValue)>, f64>>,
    ) -> Result<JsonValue> {
        let elements = self.collect_rows()?;
        if elements.is_empty() {
            return Err(Error::from_reason("max on empty RDD"));
        }
        let mut result = elements[0].clone();
        for elem in &elements[1..] {
            let is_greater = match &comparator {
                Some(cmp) => cmp.call(FnArgs::from((elem.clone(), result.clone())))? > 0.0,
                None => Self::json_compare(elem, &result) == std::cmp::Ordering::Greater,
            };
            if is_greater {
                result = elem.clone();
            }
        }
        Ok(result)
    }

    /// Return the minimum element. Optional comparator `f(a, b) => number` (negative = a < b).
    #[napi]
    pub fn min(
        &self,
        comparator: Option<Function<FnArgs<(JsonValue, JsonValue)>, f64>>,
    ) -> Result<JsonValue> {
        let elements = self.collect_rows()?;
        if elements.is_empty() {
            return Err(Error::from_reason("min on empty RDD"));
        }
        let mut result = elements[0].clone();
        for elem in &elements[1..] {
            let is_less = match &comparator {
                Some(cmp) => cmp.call(FnArgs::from((elem.clone(), result.clone())))? < 0.0,
                None => Self::json_compare(elem, &result) == std::cmp::Ordering::Less,
            };
            if is_less {
                result = elem.clone();
            }
        }
        Ok(result)
    }

    /// Write each element as a line to `path`.
    ///
    /// Accepts a local file path or an S3 URI (`s3://bucket/prefix`).
    #[napi]
    pub fn save_as_text_file(&self, path: String) -> Result<()> {
        if path.starts_with("s3://") {
            return self.write_s3(&path);
        }
        use std::io::Write;
        let mut file = std::fs::File::create(&path)
            .map_err(|e| Error::from_reason(format!("save_as_text_file: {e}")))?;
        for elem in &self.elements {
            let line = match elem {
                JsonValue::String(s) => s.clone(),
                other => other.to_string(),
            };
            writeln!(file, "{}", line)
                .map_err(|e| Error::from_reason(format!("save_as_text_file write: {e}")))?;
        }
        Ok(())
    }

    /// Two-phase aggregation: `seqFn(acc, elem) => acc` within partitions,
    /// `combFn(acc, acc) => acc` across partition results.
    #[napi]
    pub fn aggregate(
        &self,
        zero: JsonValue,
        seq_fn: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
        comb_fn: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsonValue> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();

        let mut partition_results = Vec::new();
        for (start, end) in super::slice_positions(total, np) {
            let mut acc = zero.clone();
            for elem in &self.elements[start..end] {
                acc = seq_fn.call(FnArgs::from((acc, elem.clone())))?;
            }
            partition_results.push(acc);
        }

        let mut result = zero;
        for part_acc in partition_results {
            result = comb_fn.call(FnArgs::from((result, part_acc)))?;
        }
        Ok(result)
    }

    /// Return elements divided into `num_partitions` logical partition arrays.
    #[napi]
    pub fn glom(&self) -> Vec<Vec<JsonValue>> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        super::slice_positions(total, np)
            .map(|(start, end)| self.elements[start..end].to_vec())
            .collect()
    }

    /// Mark this RDD to be cached in memory on first action (no-op in local mode).
    #[napi]
    pub fn cache(&self) -> JsRdd {
        JsRdd::from_data(
            self.elements.clone(),
            self.num_partitions,
            Arc::clone(&self.context),
        )
    }

    /// Mark this RDD to be persisted (alias for `cache` in local mode).
    #[napi]
    pub fn persist(&self) -> JsRdd {
        self.cache()
    }

    /// Remove this RDD from the cache (no-op in local mode).
    #[napi]
    pub fn unpersist(&self) -> JsRdd {
        self.cache()
    }

    /// Write all elements to `{path}/checkpoint.json` and return a new RDD
    /// backed by the same elements.
    ///
    /// The write is atomic: data is first written to `{path}/checkpoint.json.tmp`
    /// and then renamed to `{path}/checkpoint.json`.
    #[napi]
    pub fn checkpoint(&self, path: String) -> Result<JsRdd> {
        std::fs::create_dir_all(&path)
            .map_err(|e| Error::from_reason(format!("checkpoint: create_dir_all failed: {e}")))?;

        let tmp_path = format!("{path}/checkpoint.json.tmp");
        let final_path = format!("{path}/checkpoint.json");

        let json = serde_json::to_vec(&self.elements)
            .map_err(|e| Error::from_reason(format!("checkpoint: serialize failed: {e}")))?;

        std::fs::write(&tmp_path, &json)
            .map_err(|e| Error::from_reason(format!("checkpoint: write failed: {e}")))?;

        std::fs::rename(&tmp_path, &final_path)
            .map_err(|e| Error::from_reason(format!("checkpoint: rename failed: {e}")))?;

        Ok(JsRdd::from_data(
            self.elements.clone(),
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Return the logical number of partitions.
    #[napi(getter)]
    pub fn num_partitions(&self) -> u32 {
        self.num_partitions as u32
    }

    #[napi]
    pub fn length(&self) -> u32 {
        self.elements.len() as u32
    }

    /// Balanced tree-reduce. `f(a, b)` merges two partial results.
    /// `depth` (default 2) controls the number of tree merge levels.
    #[napi]
    pub fn tree_reduce(
        &self,
        f: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
        depth: Option<u32>,
    ) -> Result<JsonValue> {
        let mut partials = self.collect_rows()?;
        if partials.is_empty() {
            return Err(Error::from_reason("treeReduce on empty RDD"));
        }
        let levels = depth.unwrap_or(2).max(1) as usize;
        for _ in 0..levels {
            if partials.len() <= 1 {
                break;
            }
            let mut next = Vec::with_capacity(partials.len() / 2 + 1);
            let mut iter = partials.into_iter();
            while let Some(a) = iter.next() {
                match iter.next() {
                    Some(b) => next.push(f.call(FnArgs::from((a, b)))?),
                    None => next.push(a),
                }
            }
            partials = next;
        }
        partials
            .into_iter()
            .next()
            .ok_or_else(|| Error::from_reason("treeReduce produced no value"))
    }

    /// Balanced tree-aggregate. `seqFn(acc, elem)` within partitions,
    /// `combFn(acc, acc)` in a balanced tree across partitions.
    #[napi]
    pub fn tree_aggregate(
        &self,
        zero: JsonValue,
        seq_fn: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
        comb_fn: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
        depth: Option<u32>,
    ) -> Result<JsonValue> {
        let elements = self.collect_rows()?;
        let np = self.num_partitions.max(1);
        let total = elements.len();
        let levels = depth.unwrap_or(2).max(1) as usize;
        let mut partials: Vec<JsonValue> = Vec::with_capacity(np);
        for (start, end) in super::slice_positions(total, np) {
            let mut acc = zero.clone();
            for elem in &elements[start..end] {
                acc = seq_fn.call(FnArgs::from((acc, elem.clone())))?;
            }
            partials.push(acc);
        }
        for _ in 0..levels {
            if partials.len() <= 1 {
                break;
            }
            let mut next = Vec::with_capacity(partials.len() / 2 + 1);
            let mut iter = partials.into_iter();
            while let Some(a) = iter.next() {
                match iter.next() {
                    Some(b) => next.push(comb_fn.call(FnArgs::from((a, b)))?),
                    None => next.push(a),
                }
            }
            partials = next;
        }
        Ok(partials.into_iter().next().unwrap_or(zero))
    }

    /// Single-pass summary statistics over numeric elements.
    /// Returns `{count, mean, sum, min, max, variance, stdev}`.
    /// All values are `NaN` for an empty RDD.
    #[napi]
    pub fn stats(&self) -> Result<serde_json::Value> {
        let elements = self.collect_rows()?;
        if elements.is_empty() {
            return Ok(serde_json::json!({
                "count": 0,
                "mean": f64::NAN,
                "sum": f64::NAN,
                "min": f64::NAN,
                "max": f64::NAN,
                "variance": f64::NAN,
                "stdev": f64::NAN,
            }));
        }
        let nums: Vec<f64> = elements
            .iter()
            .map(|e| {
                e.as_f64().ok_or_else(|| {
                    Error::from_reason(format!("stats: element is not numeric: {e}"))
                })
            })
            .collect::<Result<_>>()?;
        let count = nums.len() as u64;
        let sum: f64 = nums.iter().sum();
        let mean = sum / count as f64;
        let min = nums.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let m2: f64 = nums.iter().map(|x| (x - mean) * (x - mean)).sum();
        let variance = m2 / count as f64;
        let stdev = variance.sqrt();
        Ok(serde_json::json!({
            "count": count,
            "mean": mean,
            "sum": sum,
            "min": min,
            "max": max,
            "variance": variance,
            "stdev": stdev,
        }))
    }

    /// Arithmetic mean. Returns `NaN` if empty.
    #[napi]
    pub fn mean(&self) -> Result<f64> {
        let elements = self.collect_rows()?;
        if elements.is_empty() {
            return Ok(f64::NAN);
        }
        let sum: f64 = elements
            .iter()
            .map(|e| {
                e.as_f64()
                    .ok_or_else(|| Error::from_reason(format!("mean: element is not numeric: {e}")))
            })
            .collect::<Result<Vec<_>>>()?
            .iter()
            .sum();
        Ok(sum / elements.len() as f64)
    }

    /// Population variance. Returns `NaN` if empty.
    #[napi]
    pub fn variance(&self) -> Result<f64> {
        let elements = self.collect_rows()?;
        if elements.is_empty() {
            return Ok(f64::NAN);
        }
        let nums: Vec<f64> = elements
            .iter()
            .map(|e| {
                e.as_f64().ok_or_else(|| {
                    Error::from_reason(format!("variance: element is not numeric: {e}"))
                })
            })
            .collect::<Result<_>>()?;
        let m = nums.iter().sum::<f64>() / nums.len() as f64;
        let m2: f64 = nums.iter().map(|v| (v - m) * (v - m)).sum();
        Ok(m2 / nums.len() as f64)
    }

    /// Population standard deviation — `sqrt(variance())`.
    #[napi]
    pub fn stdev(&self) -> Result<f64> {
        Ok(self.variance()?.sqrt())
    }

    /// Bucketed counts over ascending `bounds`. `bounds` has `n + 1` edges defining
    /// `n` buckets. Returns `Array<number>` of length `n` counting elements in each
    /// half-open interval `[bounds[i], bounds[i+1])`, with the final bucket
    /// right-inclusive.
    #[napi]
    pub fn histogram(&self, bounds: Vec<f64>) -> Result<Vec<u32>> {
        if bounds.len() < 2 {
            return Ok(vec![]);
        }
        let n = bounds.len() - 1;
        let lo = bounds[0];
        let hi = bounds[n];
        let elements = self.collect_rows()?;
        let mut counts = vec![0u32; n];
        for elem in &elements {
            let v = elem.as_f64().unwrap_or(f64::NAN);
            if v < lo || v > hi {
                continue;
            }
            let idx = bounds
                .partition_point(|&b| b <= v)
                .saturating_sub(1)
                .min(n - 1);
            counts[idx] += 1;
        }
        Ok(counts)
    }

    /// Return a sampled subset of this RDD (lazy transform).
    ///
    /// `withReplacement = true` — Poisson sampling (elements may repeat).
    /// `withReplacement = false` — Bernoulli sampling (each element at most once).
    /// Optional `seed` makes the sample reproducible.
    #[napi]
    pub fn sample(
        &self,
        with_replacement: bool,
        fraction: f64,
        seed: Option<u32>,
    ) -> Result<JsRdd> {
        let source = self.collect_rows()?;
        let s = seed.unwrap_or(0) as u64;
        if with_replacement {
            let elements: Vec<JsonValue> = source
                .iter()
                .enumerate()
                .flat_map(|(i, elem)| {
                    let mut rng = SimpleLcg::new(s ^ (i as u64));
                    let mut n = 0u32;
                    let mut remaining = fraction;
                    loop {
                        let roll = rng.next_f64();
                        if roll < remaining {
                            n += 1;
                            remaining -= roll;
                        } else {
                            break;
                        }
                    }
                    std::iter::repeat_n(elem.clone(), n as usize)
                })
                .collect();
            Ok(JsRdd::from_data(
                elements,
                self.num_partitions,
                Arc::clone(&self.context),
            ))
        } else {
            let elements: Vec<JsonValue> = source
                .iter()
                .enumerate()
                .filter(|(i, _elem)| {
                    let mut rng = SimpleLcg::new(s ^ (*i as u64));
                    rng.next_f64() < fraction
                })
                .map(|(_, e)| e.clone())
                .collect();
            Ok(JsRdd::from_data(
                elements,
                self.num_partitions,
                Arc::clone(&self.context),
            ))
        }
    }

    /// Return a fixed-size random sample (action).
    ///
    /// `withReplacement = true` draws with replacement; `false` draws distinct
    /// elements. `seed` makes the sample reproducible.
    #[napi]
    pub fn take_sample(
        &self,
        with_replacement: bool,
        num: u32,
        seed: Option<u32>,
    ) -> Result<Vec<JsonValue>> {
        let source = self.collect_rows()?;
        if source.is_empty() || num == 0 {
            return Ok(vec![]);
        }
        let s = seed.unwrap_or(0) as u64;
        let mut rng = SimpleLcg::new(s);
        let n = num as usize;
        if with_replacement {
            let result: Vec<JsonValue> = (0..n)
                .map(|_| {
                    let idx = (rng.next_f64() * source.len() as f64) as usize;
                    source[idx.min(source.len() - 1)].clone()
                })
                .collect();
            Ok(result)
        } else {
            let take = n.min(source.len());
            let mut keyed: Vec<(f64, &JsonValue)> =
                source.iter().map(|e| (rng.next_f64(), e)).collect();
            keyed.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            Ok(keyed
                .into_iter()
                .take(take)
                .map(|(_, e)| e.clone())
                .collect())
        }
    }

    /// Approximate distinct count via hash-set cardinality (driver-side).
    #[napi]
    pub fn count_approx_distinct(&self) -> Result<u32> {
        let elements = self.collect_rows()?;
        let mut seen = HashMap::new();
        for elem in &elements {
            seen.insert(elem.to_string(), ());
        }
        Ok(seen.len() as u32)
    }
}

// A plain (non-`#[napi]`) impl block — napi's proc-macro rejects any impl item it
// doesn't recognize as a napi export, so private helpers live in their own block.
impl JsRdd {
    fn write_s3(&self, path: &str) -> Result<()> {
        use atomic_compute::io::s3::{S3Uri, write_text};
        let s3uri = S3Uri::parse(path).ok_or_else(|| {
            Error::from_reason(format!("save_as_text_file: invalid S3 URI: {path}"))
        })?;
        let content: String = self
            .elements
            .iter()
            .map(|elem| {
                let line = match elem {
                    JsonValue::String(s) => s.clone(),
                    other => other.to_string(),
                };
                format!("{line}\n")
            })
            .collect();
        let key = format!("{}/part-0", s3uri.key.trim_end_matches('/'));
        write_text(&s3uri.bucket, &key, content)
            .map_err(|e| Error::from_reason(format!("save_as_text_file S3: {e}")))?;
        Ok(())
    }
}
