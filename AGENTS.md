# Project Instructions

## Code-comment policy

- Make the smallest correct change.
- Do not use `unwrap()` or `expect()` in production paths unless the invariant is
  documented and genuinely unrecoverable.
- Avoid unnecessary `clone()`, allocation, `Arc`, `Mutex`, `Box`, trait objects,
  async boundaries, and generic abstraction.
- Remove the use of map_err and define explicit error derieved from thiserror with variant return the right message if possible
- Prefer static dispatch and concrete types unless runtime extension is required.
- Add comments only for non-obvious invariants, external constraints, safety,
  performance reasoning, or intentional trade-offs. Explain why, not what.
- Do not add dependencies without explaining why the standard library or existing
  dependencies cannot solve the problem.
- Do not add comments that restate the code.
- Do not add tutorial comments, section banners, or control-flow narration.
- Preserve existing comments unless the task requires changing them.
- Add comments only for non-obvious invariants, external-system constraints,
  security/correctness/performance rationale, or intentional trade-offs.
- When adding a comment, explain why, not what.
- Prefer clear names, types, small functions, and tests over explanatory comments.
- Do not reformat, refactor, rename, or edit unrelated code.
- Do not add comments unless they preserve non-obvious reasoning or constraints.
- Prefer minimal, focused diffs with no unrelated cleanup.
- Ensure the test doesn't stall for long, it should complete within 5 minutes