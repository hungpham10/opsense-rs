use std::io::Error as IoError;

/// Lỗi nội bộ của admin — đóng gói cả sqlx lẫn DbErr-shaped messages để
/// upstream HTTP layer chỉ cần một kiểu duy nhất.
#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("io error: {0}")]
    Io(#[from] IoError),
    #[error("{0}")]
    Other(String),
}
