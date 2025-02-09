use std::sync::Arc;

use crate::{datatypes::operator::Operator, error::Result};

use arrow::array::RecordBatch;

use super::{ColumnarValue, PhysicalExpression};

#[derive(Debug)]
pub struct BinaryExpr {
    lhs: Arc<PhysicalExpression>,
    op: Operator,
    rhs: Arc<PhysicalExpression>,
}

impl BinaryExpr {
    pub fn new(lhs: Arc<PhysicalExpression>, op: Operator, rhs: Arc<PhysicalExpression>) -> Self {
        Self { lhs, op, rhs }
    }

    pub fn evalate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        todo!()
    }
}
