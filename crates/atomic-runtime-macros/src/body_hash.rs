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

/// Hash a body's token text into the two forms `#[task]`/`task_fn!` each need for
/// `task_name` generation: an 8-hex-digit short form, and a `u64`-suffixed token literal
/// for embedding in generated code.
pub(crate) fn body_hash_parts(body_token_str: &str) -> (String, proc_macro2::Literal) {
    let hash = fnv1a_hash(body_token_str);
    let short = format!("{:08x}", hash as u32);
    let lit = proc_macro2::Literal::u64_suffixed(hash);
    (short, lit)
}
