use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct InvalidArgument {
    pub key: String,
    pub value: String
}

impl fmt::Display for InvalidArgument {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}. key:{}, value:{}", self.description(), self.key, self.value)
    }
}

impl Error for InvalidArgument {
    fn description(&self) -> &str {
        "invalid argument"
    }

    fn cause(&self) -> Option<&Error> {
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
    HttpError(::hyper::error::Error),
    ApiError(::hyper::status::StatusCode, String),
    InvalidArgumentError(InvalidArgument),
    IoError(::std::io::Error)
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

impl From<::hyper::error::Error> for TreasureDataError {
    fn from(err: ::hyper::error::Error) -> TreasureDataError {
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
        write!(f, "{}", self.description())
    }
}

impl Error for TreasureDataError {
    fn description(&self) -> &str {
        match *self {
            TreasureDataError::JsonDecodeError(ref x) => x.description(),
            TreasureDataError::JsonParseError(ref x) => x.description(),
            TreasureDataError::MsgpackDecodeError(ref x) => x.description(),
            TreasureDataError::MsgpackUnexpectedValueError(..) =>
                "recieved unexpected MessagePack value",
            TreasureDataError::TimeStampParseError(ref x) => x.description(),
            TreasureDataError::HttpError(ref x) => x.description(),
            TreasureDataError::ApiError(..) =>
                "recieved unexpected status code",
            TreasureDataError::InvalidArgumentError(ref x) => x.description(),
            TreasureDataError::IoError(ref x) => x.description()
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            TreasureDataError::JsonDecodeError(ref x) => Some(x),
            TreasureDataError::JsonParseError(ref x) => Some(x),
            TreasureDataError::MsgpackDecodeError(ref x) => Some(x),
            TreasureDataError::MsgpackUnexpectedValueError(..) => None,
            TreasureDataError::TimeStampParseError(ref x) => Some(x),
            TreasureDataError::HttpError(ref x) => Some(x),
            TreasureDataError::ApiError(..) => None,
            TreasureDataError::InvalidArgumentError(ref x) => Some(x),
            TreasureDataError::IoError(ref x) => Some(x)
        }
    }
}
