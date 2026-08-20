use syn::Type;

/// Whether `ty` is exactly `bool` — `#[task]`/`task_fn!` generate `Filter` dispatch when
/// a unary task's return type is bool.
pub(crate) fn is_bool_type(ty: &Type) -> bool {
    matches!(ty, Type::Path(tp) if tp.path.is_ident("bool"))
}

/// Whether `ty`'s outer type is `Vec<_>` — `#[task]`/`task_fn!` generate `FlatMap`
/// dispatch when a unary task's return type is a `Vec`.
pub(crate) fn is_vec_type(ty: &Type) -> bool {
    matches!(ty, Type::Path(tp) if tp.path.segments.last().is_some_and(|s| s.ident == "Vec"))
}
