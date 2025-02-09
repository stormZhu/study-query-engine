use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::{datasource::DataSource, error::Result};

pub struct ScanExec {
    path: String,
    ds: Arc<dyn DataSource>,
    projection: Option<Vec<String>>,
}

impl ScanExec {
    pub fn new(path: String, ds: Arc<dyn DataSource>, projection: Option<Vec<String>>) -> Self {
        Self {
            path,
            ds,
            projection,
        }
    }

    pub fn execute(&self) -> Result<RecordBatch> {
        self.ds.scan(self.projection.clone())
    }

    pub fn schema(&self) -> SchemaRef {
        self.ds.schema()
    }
}
