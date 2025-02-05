use std::{fmt::Display, sync::Arc};

use crate::datatypes::operator::Operator;

use super::expr::LogicalExpr;

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    pub lhs: Arc<LogicalExpr>,
    pub op: Operator,
    pub rhs: Arc<LogicalExpr>,
}

impl BinaryExpr {
    pub fn new(lhs: Arc<LogicalExpr>, op: Operator, rhs: Arc<LogicalExpr>) -> Self {
        Self { lhs, op, rhs }
    }
}

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.lhs, self.op, self.rhs)
    }
}
