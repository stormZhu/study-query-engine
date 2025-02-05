use std::sync::Arc;

use super::{binary::BinaryExpr, column::Column, expr::LogicalExpr};
use crate::datatypes::operator::Operator;
use crate::datatypes::values::ScalarValue;

pub fn col(name: impl Into<String>) -> LogicalExpr {
    LogicalExpr::Column(Column::new(name))
}

pub fn lit<T: LiteralExt>(value: T) -> LogicalExpr {
    value.lit()
}

//  为什么入参不加Arc呢
pub fn binary_expr(lhs: LogicalExpr, op: Operator, rhs: LogicalExpr) -> LogicalExpr {
    LogicalExpr::Binary(BinaryExpr::new(Arc::new(lhs), op, Arc::new(rhs)))
}

impl LogicalExpr {
    pub fn eq(self, other: LogicalExpr) -> LogicalExpr {
        binary_expr(self, Operator::Eq, other)
    }
}

pub trait LiteralExt {
    fn lit(&self) -> LogicalExpr;
}

macro_rules! make_lit {
    ($ty:ident, $scalar:ident) => {
        impl LiteralExt for $ty {
            fn lit(&self) -> LogicalExpr {
                LogicalExpr::Literal(ScalarValue::$scalar(Some(*self)))
            }
        }
    };
}
make_lit!(i8, Int8);
make_lit!(i16, Int16);
make_lit!(i32, Int32);
make_lit!(i64, Int64);
make_lit!(u8, Uint8);
make_lit!(u16, Uint16);
make_lit!(u32, Uint32);
make_lit!(u64, Uint64);
