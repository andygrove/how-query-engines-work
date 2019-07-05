use std::result;

use datafusion::error::ExecutionError;

pub type Result<T> = result::Result<T, BallistaError>;

/// Ballista error
#[derive(Debug)]
pub enum BallistaError {
    NotImplemented,
    General(String),
    DataFusionError(ExecutionError),
}

impl From<ExecutionError> for BallistaError {
    fn from(e: ExecutionError) -> Self {
        BallistaError::DataFusionError(e)
    }
}
