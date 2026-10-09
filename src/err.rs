use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Work queue is full")]
    QueueFull,
    #[error("Work is cancelled")]
    Cancelled,
    #[error("Work panicked")]
    Panicked,
    #[error("Tokio error: {0}")]
    TokioError(#[from] tokio::sync::watch::error::RecvError)
}