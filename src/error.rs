pub enum LaputaError {
    InvalidDictName = 1,
    InvalidDictFile = 2,
    InvalidJS = 3,
    InvalidCSS = 4
}

pub type LaputaResult<T> = std::result::Result<T, LaputaError>;