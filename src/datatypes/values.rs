use crate::error::Result;
use arrow::array::{
    Array, ArrayData, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, Scalar, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    make_array,
};
use arrow_schema::DataType;
use std::{fmt::Display, iter, sync::Arc};

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

/// Macro to build an array from an optional scalar value.
macro_rules! build_array_from_option {
    ($data_type:ident, $array_type:ident, $expr:expr, $size:expr) => {
        match $expr {
            Some(v) => Arc::new($array_type::from_value(*v, $size)),
            None => make_array(ArrayData::new_null(&DataType::$data_type, $size)),
        }
    };
}

impl ScalarValue {
    pub fn to_array(&self, num_rows: usize) -> ArrayRef {
        match self {
            ScalarValue::Null => make_array(ArrayData::new_null(&DataType::Null, num_rows)),
            ScalarValue::Boolean(v) => Arc::new(BooleanArray::from(vec![*v; num_rows])) as ArrayRef,
            ScalarValue::Int8(v) => build_array_from_option!(Int8, Int8Array, v, num_rows),
            ScalarValue::Int16(v) => build_array_from_option!(Int16, Int16Array, v, num_rows),
            ScalarValue::Int32(v) => build_array_from_option!(Int32, Int32Array, v, num_rows),
            ScalarValue::Int64(v) => build_array_from_option!(Int64, Int64Array, v, num_rows),
            ScalarValue::Uint8(v) => build_array_from_option!(UInt8, UInt8Array, v, num_rows),
            ScalarValue::Uint16(v) => build_array_from_option!(UInt16, UInt16Array, v, num_rows),
            ScalarValue::Uint32(v) => build_array_from_option!(UInt32, UInt32Array, v, num_rows),
            ScalarValue::Uint64(v) => build_array_from_option!(UInt64, UInt64Array, v, num_rows),
            ScalarValue::Float32(v) => build_array_from_option!(Float32, Float32Array, v, num_rows),
            ScalarValue::Float64(v) => build_array_from_option!(Float64, Float64Array, v, num_rows),
            ScalarValue::String(v) => match v {
                Some(v) => Arc::new(StringArray::from_iter_values(
                    iter::repeat(v).take(num_rows),
                )),
                None => make_array(ArrayData::new_null(&DataType::Utf8, num_rows)),
            },
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Uint8(_) => DataType::UInt8,
            ScalarValue::Uint16(_) => DataType::UInt16,
            ScalarValue::Uint32(_) => DataType::UInt32,
            ScalarValue::Uint64(_) => DataType::UInt64,
            ScalarValue::String(_) => DataType::Utf8,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
        }
    }

    pub fn to_scalar(&self) -> Result<Scalar<ArrayRef>> {
        Ok(Scalar::new(self.to_array(1)))
    }

    // pub fn try_from_array(array: &dyn Array, index: usize) -> Result<Self> {
    //     Ok(match array.data_type() {
    //         DataType::Null => ScalarValue::Null,
    //         DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean)?,
    //         DataType::Int8 => typed_cast!(array, index, Int8Array, Int8)?,
    //         DataType::Int16 => typed_cast!(array, index, Int16Array, Int16)?,
    //         DataType::Int32 => typed_cast!(array, index, Int32Array, Int32)?,
    //         DataType::Int64 => typed_cast!(array, index, Int64Array, Int64)?,
    //         DataType::UInt8 => typed_cast!(array, index, UInt8Array, UInt8)?,
    //         DataType::UInt16 => typed_cast!(array, index, UInt16Array, UInt16)?,
    //         DataType::UInt32 => typed_cast!(array, index, UInt32Array, UInt32)?,
    //         DataType::UInt64 => typed_cast!(array, index, UInt64Array, UInt64)?,
    //         DataType::Utf8 => typed_cast!(array, index, StringArray, Utf8)?,
    //         DataType::Float32 => typed_cast!(array, index, Float32Array, Float32)?,
    //         DataType::Float64 => typed_cast!(array, index, Float64Array, Float64)?,
    //         other => unimplemented!(),
    //     })
    // }
}
