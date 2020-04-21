#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Integer {
    U64(u64),
    I64(i64),
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Float {
    F32(f32),
    F64(f64),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Nil,
    Boolean(bool),
    Integer(Integer),
    Float(Float),
    String(String),
    Binary(Vec<u8>),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Ext(i8, Vec<u8>),
}

impl From<::rmpv::Value> for Value {
    fn from(src: ::rmpv::Value) -> Value {
        match src {
            ::rmpv::Value::Nil => Value::Nil,
            ::rmpv::Value::Boolean(x) => Value::Boolean(x),
            ::rmpv::Value::Integer(x) => {
                if x.is_i64() {
                    Value::Integer(Integer::I64(x.as_i64().unwrap()))
                } else {
                    Value::Integer(Integer::U64(x.as_u64().unwrap()))
                }
            }
            ::rmpv::Value::F32(x) => Value::Float(Float::F32(x)),
            ::rmpv::Value::F64(x) => Value::Float(Float::F64(x)),
            ::rmpv::Value::String(x) => Value::String(x.into_str().unwrap()),
            ::rmpv::Value::Binary(x) => Value::Binary(x),
            ::rmpv::Value::Array(xs) => {
                Value::Array(xs.into_iter().map(|x| Value::from(x)).collect())
            }
            ::rmpv::Value::Map(xs) => Value::Map(
                xs.into_iter()
                    .map(|(k, v)| (Value::from(k), Value::from(v)))
                    .collect(),
            ),
            ::rmpv::Value::Ext(i, x) => Value::Ext(i, x),
        }
    }
}
