use std::fmt::Display;


#[derive(Debug)]
pub struct CrabDBError {
    message: String,
}

impl Display for CrabDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
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