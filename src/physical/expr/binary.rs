use std::sync::Arc;

use crate::{
    datatypes::{operator::Operator, values::ScalarValue},
    error::Result,
};

use arrow::{
    array::{ArrayRef, Datum, RecordBatch},
    compute::kernels::numeric::{add_wrapping, div, mul_wrapping, sub_wrapping},
};
use arrow_schema::ArrowError;

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
        let lhs = self.lhs.evalate(batch)?;
        let rhs = self.rhs.evalate(batch)?;

        match self.op.clone() {
            // Eq => Self::apply_cmp(&lhs, &rhs, eq),
            // NotEq => Self::apply_cmp(&lhs, &rhs, neq),
            // Lt => Self::apply_cmp(&lhs, &rhs, lt),
            // LtEq => Self::apply_cmp(&lhs, &rhs, lt_eq),
            // Gt => Self::apply_cmp(&lhs, &rhs, gt),
            // GtEq => Self::apply_cmp(&lhs, &rhs, gt_eq),
            Operator::Plus => Self::apply(&lhs, &rhs, add_wrapping),
            // Minus => Self::apply(&lhs, &rhs, sub_wrapping),
            // Multiply => Self::apply(&lhs, &rhs, mul_wrapping),
            // Divide => Self::apply(&lhs, &rhs, div),
            _ => unimplemented!(),
        }
    }

    fn apply(
        lhs: &ColumnarValue,
        rhs: &ColumnarValue,
        f: impl Fn(&dyn Datum, &dyn Datum) -> std::result::Result<ArrayRef, ArrowError>,
    ) -> Result<ColumnarValue> {
        use ColumnarValue::*;

        match (&lhs, &rhs) {
            (Array(l), Array(r)) => Ok(Array(f(&l.as_ref(), &r.as_ref())?)),
            (Scalar(l), Array(r)) => Ok(Array(f(&l.to_scalar()?, &r.as_ref())?)),
            (Array(l), Scalar(r)) => Ok(Array(f(&l.as_ref(), &r.to_scalar()?)?)),
            // (Scalar(l), Scalar(r)) => {
            //     let arr = f(&l.to_scalar()?, &r.to_scalar()?)?;
            //     let value = ScalarValue::try_from_array(arr.as_ref(), 0)?;
            //     Ok(Scalar(value))
            // }
            (Scalar(l), Scalar(r)) => unimplemented!(),
        }
    }
}
