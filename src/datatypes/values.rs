use std::fmt::Display;

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Null,
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Uint8(Option<u8>),
    Uint16(Option<u16>),
    Uint32(Option<u32>),
    Uint64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    String(Option<String>),
}

macro_rules! format_option {
    ($f:expr, $expr:expr) => {
        match $expr {
            Some(v) => write!($f, "{}", v),
            None => write!($f, "NULL"),
        }
    };
}

impl Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "NULL"),
            ScalarValue::Boolean(v) => format_option!(f, v),
            ScalarValue::Int8(v) => format_option!(f, v),
            ScalarValue::Int16(v) => format_option!(f, v),
            ScalarValue::Int32(v) => format_option!(f, v),
            ScalarValue::Int64(v) => format_option!(f, v),
            ScalarValue::Uint8(v) => format_option!(f, v),
            ScalarValue::Uint16(v) => format_option!(f, v),
            ScalarValue::Uint32(v) => format_option!(f, v),
            ScalarValue::Uint64(v) => format_option!(f, v),
            ScalarValue::Float32(v) => format_option!(f, v),
            ScalarValue::Float64(v) => format_option!(f, v),
            ScalarValue::String(v) => format_option!(f, v),
        }
    }
}
