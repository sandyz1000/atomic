use std::fmt::Debug;
use std::sync::Arc;

use crate::partial::error::PartialJobError;
use parking_lot::Mutex;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct PartialResult<R>
where
    R: Clone + Debug,
{
    final_value: Arc<Mutex<Option<R>>>,
    failure: Arc<Mutex<Option<Error>>>,
    /// The partial result initial value, which may be or not the final value.
    pub initial_value: R,
    /// Whether this is the final value or not.
    pub is_final: bool,
}

impl<R> PartialResult<R>
where
    R: Clone + Debug + Send + Sync + 'static,
{
    pub fn new(initial_value: R, is_final: bool) -> PartialResult<R> {
        let final_value = if is_final {
            Arc::new(Mutex::new(Some(initial_value.clone())))
        } else {
            Arc::new(Mutex::new(None))
        };

        PartialResult {
            final_value,
            failure: Arc::new(Mutex::new(None)),
            initial_value,
            is_final,
        }
    }

    /// Blocking method to wait for and return the final value.
    pub fn get_final_value(self) -> Result<R> {
        while self.final_value.lock().is_none() && self.failure.lock().is_none() {
            // TODO: improve this with channels for notification
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let final_value = &mut *self.final_value.lock();
        let failure = &mut *self.failure.lock();
        if final_value.is_some() {
            Ok(final_value.take().ok_or(PartialJobError::None)?)
        } else {
            Err(failure.take().ok_or(PartialJobError::None)?)
        }
    }

    pub fn set_final_value(&mut self, value: R) -> Result<()> {
        let final_value = &mut *self.final_value.lock();
        if final_value.is_some() {
            return Err(PartialJobError::SetFinalValTwice.into());
        }
        *final_value = Some(value);
        Ok(())
    }

    pub fn set_failure(&mut self, err: Error) -> Result<()> {
        let mut failure = self.failure.lock();
        if failure.is_some() {
            return Err(PartialJobError::SetFailureValTwice.into());
        }
        failure.replace(err);
        Ok(())
    }
}

impl<R> Debug for PartialResult<R>
where
    R: Clone + Debug,
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self.final_value.lock() {
            Some(ref value) => write!(fmt, "PartialResult {{ final: {:?} }})", value),
            None => write!(
                fmt,
                "PartialResult {{ partial: {:?} }})",
                self.initial_value
            ),
        }
    }
}
