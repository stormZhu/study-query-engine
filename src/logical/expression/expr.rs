use std::fmt::Display;

use anyhow::Ok;
use arrow_schema::{DataType, Field, Schema};

use super::{aggregate::AggregateExpr, binary::BinaryExpr, column::Column};
use crate::datatypes::values::ScalarValue;
use crate::error::Result;
use crate::logical::plan::LogicalPlan;

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

impl LogicalExpr {
    pub fn to_field(&self, plan: &LogicalPlan) -> Result<Field> {
        match self {
            LogicalExpr::Column(e) => e.to_field_from_plan(plan),
            LogicalExpr::Literal(e) => {
                let data_type = e.data_type();
                Ok(Field::new(self.to_string(), data_type, true))
            }
            _ => unimplemented!(),
        }
    }

    /// Returns the [`DataType`] of the expression.
    pub fn data_type(&self, schema: &Schema) -> Result<DataType> {
        match self {
            LogicalExpr::Column(e) => Ok(e.to_field(schema)?.data_type().clone()),
            _ => unimplemented!(),
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
