use std::hash::{DefaultHasher, Hash, Hasher};

use crate::register_aggregate_task;
use crate::task_traits::AggregateTask;

/// HyperLogLog precision: `2^P` registers. `P = 12` → 4096 registers, ~1.6% standard error.
pub const HLL_P: u32 = 12;
/// Number of HLL registers.
pub const HLL_M: usize = 1 << HLL_P;

/// The identity accumulator — all registers zero.
pub fn hll_zero() -> Vec<u8> {
    vec![0u8; HLL_M]
}

/// Deterministic 64-bit hash of an element, stable across driver and workers (fixed-seed
/// `DefaultHasher`, unlike the randomized `RandomState`).
fn stable_hash<T: Hash>(x: &T) -> u64 {
    let mut h = DefaultHasher::new();
    x.hash(&mut h);
    h.finish()
}

/// Fold one element's hash into the register vector: low `P` bits pick the register, the
/// rank of the remaining bits (position of the first set bit + 1) is stored as a running max.
fn hll_add(regs: &mut [u8], hash: u64) {
    let idx = (hash & (HLL_M as u64 - 1)) as usize;
    let rest = hash >> HLL_P;
    // Sentinel bit caps the rank at (64 - P) + 1 and avoids trailing_zeros(0) == 64.
    let rank = ((rest | (1u64 << (64 - HLL_P))).trailing_zeros() + 1) as u8;
    if rank > regs[idx] {
        regs[idx] = rank;
    }
}

/// Cardinality estimate from the register vector, with linear-counting correction for the
/// small-cardinality range.
pub fn hll_estimate(regs: &[u8]) -> u64 {
    let m = HLL_M as f64;
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
                if regs.len() != HLL_M {
                    regs = hll_zero();
                }
                hll_add(&mut regs, stable_hash(&x));
                regs
            }

            fn comb(&self, mut a: Vec<u8>, b: Vec<u8>) -> Vec<u8> {
                if a.len() != HLL_M {
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
