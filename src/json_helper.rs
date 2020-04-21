macro_rules! expected_err {
    ($json:expr, $item_name:expr, $type_name:expr) => (
        DecoderError::ExpectedError(
            format!("{} : {}", $item_name, $type_name).to_string(), format!("{:?}", $json))
    )
}

macro_rules! pick_item_base {
    ($json:expr, $item_name:expr, $conv_func:ident, $type_name:expr) => (
        $json.
             find($item_name).
             ok_or(DecoderError::MissingFieldError($item_name.to_string())).
             and_then(|x| x.$conv_func().
                      ok_or(DecoderError::ExpectedError(
                              format!("{} : {}", $item_name, $type_name).to_string(),
                              format!("{:?}", x))))
    )
}

macro_rules! pick_item {
    ($json:expr, $item_name:expr, $conv_func:ident, $type_name:expr) => (
        pick_item_base!($json, $item_name, $conv_func, $type_name)?
    )
}

macro_rules! pick_string_item {
    ($json:expr, $item_name:expr) => (
        pick_item!($json, $item_name, as_string, "String").to_string();
    )
}

macro_rules! pick_timestamp_item {
    ($json:expr, $item_name:expr) => (
        TimeStamp::from_str(pick_item!($json, $item_name, as_string, "String"))?
    )
}

macro_rules! pick_u64_item {
    ($json:expr, $item_name:expr) => (
        pick_item!($json, $item_name, as_u64, "u64");
    )
}

#[allow(unused_macros)]
macro_rules! pick_array_string_item {
    ($json:expr, $item_name:expr) => ({
        let items: &Vec<json::Json> = pick_item!($json, $item_name, as_array, "Array");
        items.iter().
            map(|j| {
                let result: Result<String, DecoderError> =
                    j.as_string().
                        ok_or(DecoderError::ExpectedError(
                              "String".to_string(), format!("{:?}", j))).
                        and_then(|x| Ok(x.to_string()));
                result
            }).collect::<Result<Vec<String>, _>>()
        ?
    })
}

macro_rules! pick_opt_item {
    ($json:expr, $item_name:expr, $conv_func:ident, $type_name:expr) => (
        pick_item_base!($json, $item_name, $conv_func, $type_name).ok()
    )
}

macro_rules! pick_opt_string_item {
    ($json:expr, $item_name:expr) => (
        pick_opt_item!($json, $item_name, as_string, "String").map(|x| x.to_string())
    )
}

macro_rules! pick_opt_timestamp_item {
    ($json:expr, $item_name:expr) => (
        match pick_opt_item!($json, $item_name, as_string, "String") {
            Some("") => None,
            Some(x) => Some(TimeStamp::from_str(x)?),
            None => None
        }
    )
}

macro_rules! pick_opt_u64_item {
    ($json:expr, $item_name:expr) => (
        pick_opt_item!($json, $item_name, as_u64, "u64");
    )
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use rustc_serialize::json::{DecoderError, Json};
    use model::*;
    use error::*;

    #[test]
    fn pick_string_item() {
        let j = Json::from_str(r#"{"x": "foo"}"#).unwrap();
        let r: Result<String, TreasureDataError> = (|| Ok(pick_string_item!(j, "x")))();
        assert_eq!(r.unwrap(), "foo".to_string());
    }

    #[test]
    fn pick_opt_string_item() {
        let j = Json::from_str(r#"{"x": "foo"}"#).unwrap();
        let r: Result<Option<String>, TreasureDataError> = (|| Ok(pick_opt_string_item!(j, "x")))();
        assert_eq!(r.unwrap(), Some("foo".to_string()));

        let j = Json::from_str(r#"{"y": "foo"}"#).unwrap();
        let r: Result<Option<String>, TreasureDataError> = (|| Ok(pick_opt_string_item!(j, "x")))();
        assert_eq!(r.unwrap(), None);
    }

    #[test]
    fn pick_timestamp_item() {
        let j = Json::from_str(r#"{"x": "2016-07-29 16:00:00 UTC"}"#).unwrap();
        let r: Result<TimeStamp, TreasureDataError> = (|| Ok(pick_timestamp_item!(j, "x")))();
        assert_eq!(r.unwrap(), TimeStamp::from_str("2016-07-29 16:00:00 UTC").unwrap());
    }

    #[test]
    fn pick_opt_timestamp_item() {
        let j = Json::from_str(r#"{"x": "2016-07-29 16:00:00 UTC"}"#).unwrap();
        let r: Result<Option<TimeStamp>, TreasureDataError> =
            (|| Ok(pick_opt_timestamp_item!(j, "x")))();
        assert_eq!(r.unwrap(), Some(TimeStamp::from_str("2016-07-29 16:00:00 UTC").unwrap()));

        let j = Json::from_str(r#"{"y": "2016-07-29 16:00:00 UTC"}"#).unwrap();
        let r: Result<Option<TimeStamp>, TreasureDataError> =
            (|| Ok(pick_opt_timestamp_item!(j, "x")))();
        assert_eq!(r.unwrap(), None);

        let j = Json::from_str(r#"{"x": ""}"#).unwrap();
        let r: Result<Option<TimeStamp>, TreasureDataError> =
            (|| Ok(pick_opt_timestamp_item!(j, "x")))();
        assert_eq!(r.unwrap(), None);
    }
}
