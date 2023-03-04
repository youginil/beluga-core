pub enum LaputaError {
    InvalidDictName,
    InvalidDictFile,
    InvalidJS,
    InvalidCSS
}

pub type LaputaResult<T> = std::result::Result<T, LaputaError>;