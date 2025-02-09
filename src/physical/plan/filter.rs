use std::sync::Arc;

use arrow::array::RecordBatch;

use super::PhysicalPlan;
use crate::error::Result;
use crate::physical::expr::PhysicalExpression;

pub struct FilterExec {
    pub input: Arc<PhysicalPlan>,
    pub predicate: PhysicalExpression,
}

impl FilterExec {
    pub fn new(input: Arc<PhysicalPlan>, predicate: PhysicalExpression) -> Self {
        Self { input, predicate }
    }

    pub fn execute(&self) -> Result<RecordBatch> {
        self.input.execute() // 暂时不对结果做过滤
    }
}
