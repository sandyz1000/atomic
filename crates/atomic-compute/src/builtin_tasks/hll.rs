use std::hash::{DefaultHasher, Hash, Hasher};

use crate::register_aggregate_task;
use crate::task_traits::AggregateTask;

/// Default HyperLogLog precision: `2^P` registers. `P = 12` → 4096 registers, ~1.6% standard
/// error. The precision is not fixed — it travels in the accumulator vector's length, so the
/// driver can pick any `P` per call (see [`hll_zero_p`] and [`precision_for`]).
pub const HLL_P: u32 = 12;
/// Number of registers at the default precision.
pub const HLL_M: usize = 1 << HLL_P;

/// The identity accumulator at the default precision (all registers zero).
pub fn hll_zero() -> Vec<u8> {
    hll_zero_p(HLL_P)
}

/// The identity accumulator at precision `p`, sized `2^p`. `p` is clamped to `[4, 18]`.
pub fn hll_zero_p(p: u32) -> Vec<u8> {
    vec![0u8; 1 << p.clamp(4, 18)]
}

/// Precision `P` giving approximately `relative_sd` standard error, since HLL error is
/// `1.04 / sqrt(2^P)`. Clamped to `[4, 18]`.
pub fn precision_for(relative_sd: f64) -> u32 {
    let sd = relative_sd.clamp(1e-4, 0.5);
    let m = (1.04 / sd).powi(2);
    (m.log2().ceil() as u32).clamp(4, 18)
}

/// Deterministic 64-bit hash of an element, stable across driver and workers (fixed-seed
/// `DefaultHasher`, unlike the randomized `RandomState`).
fn stable_hash<T: Hash>(x: &T) -> u64 {
    let mut h = DefaultHasher::new();
    x.hash(&mut h);
    h.finish()
}

/// Fold one element's hash into the register vector. The precision `p` is derived from the
/// vector length (`m = regs.len()`, `p = log2(m)`): the low `p` bits pick the register, the
/// rank of the remaining bits (position of the first set bit + 1) is stored as a running max.
fn hll_add(regs: &mut [u8], hash: u64) {
    let m = regs.len() as u64;
    let p = m.trailing_zeros();
    let idx = (hash & (m - 1)) as usize;
    let rest = hash >> p;
    // Sentinel bit caps the rank at (64 - p) + 1 and avoids trailing_zeros(0) == 64.
    let rank = ((rest | (1u64 << (64 - p))).trailing_zeros() + 1) as u8;
    if rank > regs[idx] {
        regs[idx] = rank;
    }
}

/// Cardinality estimate from the register vector, with linear-counting correction for the
/// small-cardinality range. Precision is taken from the vector length.
pub fn hll_estimate(regs: &[u8]) -> u64 {
    let m = regs.len() as f64;
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    let sum: f64 = regs.iter().map(|&r| 2f64.powi(-(r as i32))).sum();
    let raw = alpha * m * m / sum;

    if raw <= 2.5 * m {
        let zeros = regs.iter().filter(|&&r| r == 0).count();
        if zeros > 0 {
            return (m * (m / zeros as f64).ln()).round() as u64;
        }
    }
    raw.round() as u64
}

/// Built-in: HyperLogLog register vector for approximate distinct counts.
///
/// The [`AggregateTask`] shape `A = Vec<u8>` (the registers): each worker folds its partition
/// into a register vector, the driver merges them register-wise (max) and estimates. Registered
/// for the hashable primitives via [`register_aggregate_task!`], keyed
/// `atomic::builtin::hll::<ty>`.
#[derive(Clone, Copy, Default)]
pub struct HllTask<T>(std::marker::PhantomData<T>);

macro_rules! impl_hll_task {
    ($ty:ty) => {
        impl AggregateTask<Vec<u8>, $ty> for HllTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::hll::", stringify!($ty));

            fn seq(&self, mut regs: Vec<u8>, x: $ty) -> Vec<u8> {
                // The precision travels in the accumulator length; only a broken (non-power-of-
                // two) accumulator falls back to the default precision.
                if !regs.len().is_power_of_two() {
                    regs = hll_zero();
                }
                hll_add(&mut regs, stable_hash(&x));
                regs
            }

            fn comb(&self, mut a: Vec<u8>, b: Vec<u8>) -> Vec<u8> {
                if !a.len().is_power_of_two() {
                    a = hll_zero();
                }
                for (ra, rb) in a.iter_mut().zip(b.iter()) {
                    if *rb > *ra {
                        *ra = *rb;
                    }
                }
                a
            }
        }

        register_aggregate_task!(HllTask<$ty>, Vec<u8>, $ty);
    };
}

impl_hll_task!(i32);
impl_hll_task!(i64);
impl_hll_task!(u32);
impl_hll_task!(u64);
impl_hll_task!(String);

#[cfg(test)]
mod tests {
    use super::*;

    /// Fold `0..n` into a register vector at precision `p` and estimate.
    fn estimate(n: u64, p: u32) -> u64 {
        let mut regs = hll_zero_p(p);
        for x in 0..n {
            hll_add(&mut regs, stable_hash(&x));
        }
        hll_estimate(&regs)
    }

    #[test]
    fn test_precision_scales() {
        // Higher relative_sd → fewer registers.
        assert!(precision_for(0.2) < precision_for(0.01));
        assert_eq!(hll_zero_p(14).len(), 1 << 14);
    }

    #[test]
    fn test_estimate_accuracy() {
        // 10k distinct at P=14 should land within ~5% of truth.
        let est = estimate(10_000, 14);
        let err = (est as f64 - 10_000.0).abs() / 10_000.0;
        assert!(err < 0.05, "estimate {est} off by {err}");
    }
}
