pub mod binary;
pub mod column;
pub mod literal;

use crate::{datatypes::values::ScalarValue, error::Result};
use arrow::array::{ArrayRef, RecordBatch};

#[derive(Debug)]
pub enum ColumnarValue {
    /// An arrow array.
    Array(ArrayRef),
    /// A `ScalarValue`.
    Scalar(ScalarValue),
}

impl ColumnarValue {
    /// Convert the variant into an [`arrow::array::ArrayRef`].
    pub fn into_array(self, num_rows: usize) -> Result<ArrayRef> {
        use ColumnarValue::*;

        Ok(match self {
            Array(e) => e,
            Scalar(e) => e.to_array(num_rows),
        })
    }
}

#[derive(Debug)]
pub enum PhysicalExpression {
    Binary(binary::BinaryExpr),
    Column(column::ColumnExpr),
    Literal(literal::LiteralExpr),
}

impl PhysicalExpression {
    pub fn evalate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self {
            PhysicalExpression::Binary(expr) => expr.evalate(batch),
            PhysicalExpression::Column(expr) => expr.evalate(batch),
            PhysicalExpression::Literal(expr) => expr.evalate(batch),
        }
    }
}
