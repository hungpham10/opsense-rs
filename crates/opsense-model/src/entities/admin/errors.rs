use std::error::Error as StdError;
use std::fmt;
use std::io::Error as IoError;

/// Lỗi nội bộ của admin — đóng gói cả sqlx lẫn DbErr-shaped messages để
/// upstream HTTP layer chỉ cần một kiểu duy nhất.
#[derive(Debug)]
pub enum AdminError {
    Sqlx(sqlx::Error),
    Io(IoError),
    Other(String),
}

impl fmt::Display for AdminError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdminError::Sqlx(e) => write!(f, "sqlx error: {e}"),
            AdminError::Io(e) => write!(f, "io error: {e}"),
            AdminError::Other(msg) => f.write_str(msg),
        }
    }
}

impl StdError for AdminError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            AdminError::Sqlx(e) => Some(e),
            AdminError::Io(e) => Some(e),
            AdminError::Other(_) => None,
        }
    }
}

impl From<sqlx::Error> for AdminError {
    fn from(e: sqlx::Error) -> Self {
        AdminError::Sqlx(e)
    }
}

impl From<IoError> for AdminError {
    fn from(e: IoError) -> Self {
        AdminError::Io(e)
    }
}