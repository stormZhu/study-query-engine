use std::sync::Arc;

use crate::error::Result;
use crate::physical::expr::PhysicalExpression;
use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow_schema::SchemaRef;

use super::PhysicalPlan;

pub struct ProjectionExec {
    pub input: Arc<PhysicalPlan>,
    pub schema: SchemaRef,
    pub exprs: Vec<PhysicalExpression>,
}

impl ProjectionExec {
    pub fn new(
        input: Arc<PhysicalPlan>,
        schema: SchemaRef,
        exprs: Vec<PhysicalExpression>,
    ) -> Self {
        Self {
            input,
            schema,
            exprs,
        }
    }

    pub fn execute(&self) -> Result<RecordBatch> {
        // self.input.execute()
        let mut batch = self.input.execute()?;
        let columns = self
            .exprs
            .iter()
            .map(|expr| {
                expr.evalate(&batch)
                    .and_then(|res| res.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;

        if columns.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            return Ok(RecordBatch::try_new_with_options(
                self.schema.clone(),
                columns,
                &options,
            )?);
        }

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}
