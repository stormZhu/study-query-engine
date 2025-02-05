use std::sync::Arc;

use crate::logical::{
    expression::expr::LogicalExpr,
    plan::{Filter, LogicalPlan, Projection},
};

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

    pub fn plan(self) -> LogicalPlan {
        self.plan
    }
}
