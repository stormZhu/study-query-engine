use std::{fmt::Display, sync::Arc};

use arrow::datatypes::SchemaRef;

use crate::logical::expression::expr::LogicalExpr;

use super::plan::LogicalPlan;

#[derive(Debug, Clone)]

pub struct Projection {
    pub input: Arc<LogicalPlan>,
    pub exprs: Vec<LogicalExpr>,
    // pub schema: SchemaRef,
}

impl Projection {
    pub fn new(input: Arc<LogicalPlan>, exprs: Vec<LogicalExpr>) -> Self {
        Self {
            input,
            exprs,
            // schema: input.schema().clone(),
            // schema: SchemaRef::defa
        }
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self
            .exprs
            .iter()
            .map(|e| format!("{}", e))
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "Projection: {}", s)
    }
}
