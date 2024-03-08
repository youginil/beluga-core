use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("file error")]
    FileError(#[from] io::Error),
    #[error("{0}")]
    Msg(String),
}

pub type Result<T> = std::result::Result<T, Error>;
