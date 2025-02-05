use std::{fmt::Display, sync::Arc};

use crate::logical::expression::expr::LogicalExpr;

use super::plan::LogicalPlan;

#[derive(Debug, Clone)]
pub struct Filter {
    pub input: Arc<LogicalPlan>,
    pub predicate: LogicalExpr,
}

impl Filter {
    pub fn new(input: Arc<LogicalPlan>, predicate: LogicalExpr) -> Self {
        Self { input, predicate }
    }
}
impl Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter: {}", self.predicate)
    }
}
