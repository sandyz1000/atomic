/// Compute a stable FNV-1a 64-bit hash of a token stream's normalized text.
///
/// Used to generate a content-stable `task_name` for `#[task]`/`task_fn!`. The
/// result is stable across line-number changes and reformatting because it
/// hashes the logical token stream, not the source position.
pub(crate) fn fnv1a_hash(s: &str) -> u64 {
    const OFFSET: u64 = 14695981039346656037;
    const PRIME: u64 = 1099511628211;
    let mut h = OFFSET;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h
}
