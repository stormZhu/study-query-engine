use std::sync::Arc;

use crate::error::Result;
use crate::logical::{
    expression::expr::LogicalExpr,
    plan::{Filter, LogicalPlan, Projection},
};
use arrow::array::RecordBatch;

use super::planner::Planner;

pub struct DataFrame {
    plan: LogicalPlan,
}

impl DataFrame {
    pub fn new(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    pub fn project(self, columns: Vec<LogicalExpr>) -> Self {
        let plan = LogicalPlan::Projection(Projection::new(Arc::new(self.plan), columns));
        Self { plan }
    }

    pub fn filter(self, expr: LogicalExpr) -> Self {
        let plan = LogicalPlan::Filter(Filter::new(Arc::new(self.plan), expr));
        Self { plan }
    }

    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }

    pub fn collect(&self) -> Result<RecordBatch> {
        let optimized = &self.plan;
        let physical_plan = Planner::create_physical_plan(optimized)?;
        physical_plan.execute()
    }
}
