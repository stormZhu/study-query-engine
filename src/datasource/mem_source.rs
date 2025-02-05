use std::sync::Arc;

use arrow::array::{RecordBatch, record_batch};

use super::DataSource;

#[derive(Debug, Clone)]
pub struct MemDataSource {}

impl MemDataSource {
    pub fn new() -> Self {
        Self {}
    }

    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

impl DataSource for MemDataSource {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("c1", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("c2", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("c3", arrow::datatypes::DataType::Int32, false),
        ]))
    }

    fn scan(&self, projection: Option<&Vec<String>>) -> crate::error::Result<RecordBatch> {
        let batch = record_batch!(
            ("c1", Utf8, ["alpha", "beta", "gamma"]),
            ("c2", Int32, [1, 2, 3]),
            ("c3", Int32, [Some(10), Some(20), Some(30)])
        )?;
        // batch.project(indices)
        Ok(batch)
    }
}
