mod aggregate;
mod filter;
mod plan;
mod projection;
mod scan;

pub use filter::*;
pub use plan::*;
pub use projection::*;
pub use scan::*;

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::{
        datasource::MemDataSource,
        logical::{
            expression::expr_fn::{col, lit},
            plan::{filter::Filter, plan::LogicalPlan, projection::Projection, scan::Scan},
        },
    };

    #[test]
    fn test_build_plan_manually() {
        let scan = LogicalPlan::Scan(Scan::new(
            "testdata/csv/simple.csv",
            MemDataSource::new_arc(),
            None,
        ));

        let filter = LogicalPlan::Filter(Filter::new(Arc::new(scan), col("c1").eq(lit(1))));

        let projection = LogicalPlan::Projection(Projection::new(Arc::new(filter), vec![
            col("c1"),
            col("c2"),
        ]));

        assert_eq!(
            projection.to_string().as_str(),
            "Projection: c1, c2\n".to_string()
                + "\tFilter: c1 = 1\n"
                + "\t\tScan: testdata/csv/simple.csv; projection=None\n"
        )
    }

    #[test]
    fn test_build_plan_manually2() {
        let scan = LogicalPlan::Scan(Scan::new(
            "testdata/csv/simple.csv",
            MemDataSource::new_arc(),
            Some(vec!["c1".to_string(), "c2".to_string()]),
        ));

        let filter = LogicalPlan::Filter(Filter::new(Arc::new(scan), col("c1").eq(lit(1))));

        let projection = LogicalPlan::Projection(Projection::new(Arc::new(filter), vec![
            col("c1"),
            col("c2"),
        ]));

        assert_eq!(
            projection.to_string().as_str(),
            "Projection: c1, c2\n".to_string()
                + "\tFilter: c1 = 1\n"
                + "\t\tScan: testdata/csv/simple.csv; projection=[\"c1\", \"c2\"]\n"
        )
    }
}
