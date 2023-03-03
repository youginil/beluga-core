pub enum LaputaError {
    NotFound,
    InvalidDictionary,
    InvalidName
}

pub type LaputaResult<T> = std::result::Result<T, LaputaError>;