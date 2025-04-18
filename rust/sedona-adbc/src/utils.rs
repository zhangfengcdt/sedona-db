use adbc_core::error::{Error, Status};
use datafusion::error::DataFusionError;

pub(crate) fn from_datafusion_error(value: DataFusionError) -> Error {
    let status = match &value {
        DataFusionError::IoError(_) => Status::IO,
        DataFusionError::NotImplemented(_) => Status::NotImplemented,
        _ => Status::Internal,
    };

    Error::with_message_and_status(value.message(), status)
}

#[macro_export]
macro_rules! err_not_implemented {
    () => {
        Err(Error::with_message_and_status(
            "not implemented",
            Status::NotImplemented,
        ))
    };
    ($msg:expr) => {
        Err(Error::with_message_and_status($msg, Status::NotImplemented))
    };
}

#[macro_export]
macro_rules! err_unrecognized_option {
    ($key:expr) => {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {:?}", $key),
            Status::NotFound,
        ))
    };
}
