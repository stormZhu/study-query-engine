use std::io;

use thiserror::Error;

// pub type Result<T> = std::result::Result<T, DBError>;

pub type Result<T> = anyhow::Result<T>;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("io error: {0}")]
    IoError(#[from] io::Error),
}
