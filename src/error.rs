use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct InvalidArgument {
    pub key: String,
    pub value: String,
}

impl fmt::Display for InvalidArgument {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Invalid argument. key:{}, value:{}",
            self.key, self.value
        )
    }
}

impl Error for InvalidArgument {
    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

#[derive(Debug)]
pub enum TreasureDataError {
    JsonDecodeError(::rustc_serialize::json::DecoderError),
    JsonParseError(::rustc_serialize::json::ParserError),
    MsgpackDecodeError(::rmpv::decode::Error),
    MsgpackUnexpectedValueError(::rmpv::Value),
    TimeStampParseError(::chrono::ParseError),
    HttpError(::reqwest::Error),
    ApiError(::reqwest::StatusCode, String),
    InvalidArgumentError(InvalidArgument),
    IoError(::std::io::Error),
}

impl From<::rustc_serialize::json::DecoderError> for TreasureDataError {
    fn from(err: ::rustc_serialize::json::DecoderError) -> TreasureDataError {
        TreasureDataError::JsonDecodeError(err)
    }
}

impl From<::rustc_serialize::json::ParserError> for TreasureDataError {
    fn from(err: ::rustc_serialize::json::ParserError) -> TreasureDataError {
        TreasureDataError::JsonParseError(err)
    }
}

impl From<::rmpv::decode::Error> for TreasureDataError {
    fn from(err: ::rmpv::decode::Error) -> TreasureDataError {
        TreasureDataError::MsgpackDecodeError(err)
    }
}

impl From<::reqwest::Error> for TreasureDataError {
    fn from(err: ::reqwest::Error) -> TreasureDataError {
        TreasureDataError::HttpError(err)
    }
}

impl From<InvalidArgument> for TreasureDataError {
    fn from(err: InvalidArgument) -> TreasureDataError {
        TreasureDataError::InvalidArgumentError(err)
    }
}

impl From<::std::io::Error> for TreasureDataError {
    fn from(err: ::std::io::Error) -> TreasureDataError {
        TreasureDataError::IoError(err)
    }
}

impl From<::chrono::ParseError> for TreasureDataError {
    fn from(err: ::chrono::ParseError) -> TreasureDataError {
        TreasureDataError::TimeStampParseError(err)
    }
}

impl fmt::Display for TreasureDataError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error for TreasureDataError {
    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            TreasureDataError::JsonDecodeError(ref x) => Some(x),
            TreasureDataError::JsonParseError(ref x) => Some(x),
            TreasureDataError::MsgpackDecodeError(ref x) => Some(x),
            TreasureDataError::MsgpackUnexpectedValueError(..) => None,
            TreasureDataError::TimeStampParseError(ref x) => Some(x),
            TreasureDataError::HttpError(ref x) => Some(x),
            TreasureDataError::ApiError(..) => None,
            TreasureDataError::InvalidArgumentError(ref x) => Some(x),
            TreasureDataError::IoError(ref x) => Some(x),
        }
    }
}
