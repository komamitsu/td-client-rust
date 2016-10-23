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

impl From<::msgpack::value::Value> for Value {
    fn from(src: ::msgpack::value::Value) -> Value {
        match src {
            ::msgpack::value::Value::Nil => Value::Nil,
            ::msgpack::value::Value::Boolean(x) => Value::Boolean(x),
            ::msgpack::value::Value::Integer(x) => {
                match x {
                    ::msgpack::value::Integer::U64(i) =>
                        Value::Integer(Integer::U64(i)),
                    ::msgpack::value::Integer::I64(i) =>
                        Value::Integer(Integer::I64(i))
                }
            },
            ::msgpack::value::Value::Float(x)=> {
                match x {
                    ::msgpack::value::Float::F32(f) =>
                        Value::Float(Float::F32(f)),
                    ::msgpack::value::Float::F64(f) =>
                        Value::Float(Float::F64(f))
                }
            },
            ::msgpack::value::Value::String(x) => Value::String(x),
            ::msgpack::value::Value::Binary(x) => Value::Binary(x),
            ::msgpack::value::Value::Array(xs) =>
                Value::Array(xs.into_iter().map(|x| Value::from(x)).collect()),
            ::msgpack::value::Value::Map(xs) =>
                Value::Map(xs.into_iter().map(|(k, v)| {
                    (Value::from(k), Value::from(v))
                }).collect()),
            ::msgpack::value::Value::Ext(i, x) => Value::Ext(i, x)
        }
    }
}
