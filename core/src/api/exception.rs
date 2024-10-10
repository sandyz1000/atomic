//

trait Error: std::fmt::Display {
    fn description(&self) -> &str {
        "An error occurred"
    }

    fn cause(&self) -> Option<&dyn Error> {
        None
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }

    fn full_message(&self) -> String {
        format!("{}: {}", self.description(), self)
    }
}

#[derive(Debug)]
pub struct AtomicConfigException {
    message: String,    
}

impl AtomicConfigException {
    pub fn new(message: String) -> Self {
        todo!()
    }
}

impl Error for AtomicConfigException {
    
}