use std::fmt::Display;

use super::{aggregate::AggregateExpr, binary::BinaryExpr, column::Column};
use crate::datatypes::values::ScalarValue;

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Column(Column),
    Literal(ScalarValue),
    Binary(BinaryExpr),
    Aggregate(AggregateExpr),
}

impl Display for LogicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalExpr::Column(column) => write!(f, "{}", column),
            LogicalExpr::Literal(literal) => write!(f, "{}", literal),
            LogicalExpr::Binary(binary) => write!(f, "{}", binary),
            // LogicExpr::Aggregate(aggregate) => write!(f, "{}", aggregate),
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datatypes::operator::Operator;

    use super::*;
    #[test]
    fn test_expr_display() {
        let expr = LogicalExpr::Column(Column::new("a".to_string()));
        assert_eq!(expr.to_string(), "a");

        let expr = LogicalExpr::Literal(ScalarValue::Int32(Some(1)));
        assert_eq!(expr.to_string(), "1");

        let expr = LogicalExpr::Literal(ScalarValue::Null);
        assert_eq!(expr.to_string(), "NULL");

        let expr = LogicalExpr::Binary(BinaryExpr::new(
            Arc::new(LogicalExpr::Column(Column::new("a".to_string()))),
            Operator::Eq,
            Arc::new(LogicalExpr::Literal(ScalarValue::Int32(Some(1)))),
        ));
        assert_eq!(expr.to_string(), "a = 1");
    }
}
