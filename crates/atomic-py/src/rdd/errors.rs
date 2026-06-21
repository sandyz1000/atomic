use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum PyTaskStageError {
    #[error(
        "task function serialized but failed to load back (workers would fail): {0}. \
         Avoid capturing open files, locks, or C-extension handles."
    )]
    Unpicklable(String),
}
