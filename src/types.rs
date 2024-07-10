
#[derive(Debug)]
pub struct CrabDBError {
    message: String,
}

impl CrabDBError {
    pub fn new(message: String) -> Self {
        CrabDBError { message }
    }

    pub fn message(&self) -> &String {
        &self.message
    }
}

pub type CrabDbResult<T> = Result<T, CrabDBError>;