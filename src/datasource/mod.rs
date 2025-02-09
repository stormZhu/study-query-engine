mod csv_source;
mod mem_source;

use std::fmt::Debug;

use crate::error::Result;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
pub use csv_source::*;
pub use mem_source::*;

pub trait DataSource: Debug {
    fn schema(&self) -> SchemaRef;
    /// Creates an [`ExecutionPlan`] to scan the [`DataSource`].
    fn scan(&self, projection: Option<Vec<String>>) -> Result<RecordBatch>;
}
