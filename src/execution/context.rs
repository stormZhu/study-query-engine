use std::sync::Arc;

use arrow::array::RecordBatch;

use crate::{
    datasource::{CsvDataSource, CsvReadOptions},
    logical::plan::{LogicalPlan, Scan},
};

use super::DataFrame;

#[derive(Debug, Default)]
pub struct SessionContext;
use crate::error::Result;

impl SessionContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn csv(&self, path: impl Into<String>, options: CsvReadOptions) -> Result<DataFrame> {
        let path = path.into();
        let source = CsvDataSource::try_new(&path, options)?;
        let plan = LogicalPlan::Scan(Scan::new(&path, Arc::new(source), None));
        Ok(DataFrame::new(plan))
    }
}

#[cfg(test)]
mod tests {
    use arrow::util::pretty;

    use crate::{
        datasource::CsvReadOptionsBuilder,
        execution::context::SessionContext,
        logical::expression::expr_fn::{col, lit},
    };
    #[test]
    fn test_session_context() -> anyhow::Result<()> {
        let ctx = SessionContext::new();
        let opts = CsvReadOptionsBuilder::default().build()?;
        let df = ctx
            .csv("testdata/csv/simple.csv", opts)?
            .filter(col("c1").eq(lit(1)))
            .project(vec![col("c1"), col("c2")]);

        assert_eq!(
            df.plan().to_string(),
            vec![
                "Projection: c1, c2\n",
                "\tFilter: c1 = 1\n",
                "\t\tScan: testdata/csv/simple.csv; projection=None\n"
            ]
            .join("")
        );

        Ok(())
    }

    #[test]
    fn test_session_execute() -> anyhow::Result<()> {
        let ctx = SessionContext::new();
        let opts = CsvReadOptionsBuilder::default()
            .has_header(true)
            .delimiter(b',')
            .quote(b'"')
            .build()?;
        let df = ctx
            .csv("testdata/csv/simple.csv", opts)?
            // .filter(col("c1").eq(lit(1)))
            .project(vec![col("c3"), col("c2")]);

        let ret = df.collect()?;
        let _ = pretty::print_batches(&[ret]);
        Ok(())
    }

    #[test]
    fn test_session_execute2() -> anyhow::Result<()> {
        let ctx = SessionContext::new();
        let opts = CsvReadOptionsBuilder::default()
            .has_header(true)
            .delimiter(b',')
            .quote(b'"')
            .build()?;
        let df = ctx
            .csv("testdata/csv/simple.csv", opts)?
            // .filter(col("c1").eq(lit(1)))
            .project(vec![col("c3"), lit(1)]);
        // .project(vec![col("c3"), col("c3").add(lit(1))]);

        let ret = df.collect()?;
        let _ = pretty::print_batches(&[ret]);
        Ok(())
    }
}
