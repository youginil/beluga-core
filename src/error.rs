pub enum LaputaError {
    InvalidDictionary,
    InvalidName
}

pub type LaputaResult<T> = std::result::Result<T, LaputaError>;