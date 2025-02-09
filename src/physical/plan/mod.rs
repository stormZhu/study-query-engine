mod filter;
pub mod projection;
mod scan;

use crate::error::Result;
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
pub use filter::FilterExec;
pub use projection::ProjectionExec;
pub use scan::ScanExec;

pub enum PhysicalPlan {
    Projection(ProjectionExec),
    Filter(FilterExec),
    Scan(ScanExec),
}

impl PhysicalPlan {
    pub fn execute(&self) -> Result<RecordBatch> {
        match self {
            PhysicalPlan::Projection(exec) => exec.execute(),
            PhysicalPlan::Filter(exec) => exec.execute(),
            PhysicalPlan::Scan(exec) => exec.execute(),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            PhysicalPlan::Projection(exec) => exec.schema.clone(),
            PhysicalPlan::Filter(exec) => exec.input.schema(),
            PhysicalPlan::Scan(exec) => exec.schema(),
        }
    }
}
