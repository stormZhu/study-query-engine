use arrow::array::RecordBatch;

use crate::{datatypes::values::ScalarValue, error::Result};

use super::ColumnarValue;

#[derive(Debug)]
pub struct LiteralExpr {
    value: ScalarValue,
}

impl LiteralExpr {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }

    pub fn evalate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(self.value.clone()))
    }
}
