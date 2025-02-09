use std::sync::Arc;

use crate::error::Result;
use arrow::array::RecordBatch;

use super::ColumnarValue;

#[derive(Debug)]
pub struct ColumnExpr {
    /// The column name.
    name: String,
    /// The column index.
    index: usize,
}

impl ColumnExpr {
    pub fn new(name: impl Into<String>, index: usize) -> Self {
        Self {
            name: name.into(),
            index,
        }
    }

    pub fn evalate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = Arc::new(batch.column(self.index).clone());

        Ok(ColumnarValue::Array(array))
    }
}
