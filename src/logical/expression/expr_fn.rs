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

pub fn binary_expr(lhs: LogicalExpr, op: Operator, rhs: LogicalExpr) -> LogicalExpr {
    LogicalExpr::Binary(BinaryExpr::new(Arc::new(lhs), op, Arc::new(rhs)))
}

macro_rules! make_expr_fn {
    ($fn:ident, $op:ident) => {
        impl LogicalExpr {
            pub fn $fn(self, other: LogicalExpr) -> LogicalExpr {
                binary_expr(self, Operator::$op, other)
            }
        }
    };
}

make_expr_fn!(eq, Eq);
make_expr_fn!(neq, NotEq);
make_expr_fn!(lt, Lt);
make_expr_fn!(lt_eq, LtEq);
make_expr_fn!(gt, Gt);
make_expr_fn!(gt_eq, GtEq);
make_expr_fn!(and, And);
make_expr_fn!(or, Or);
make_expr_fn!(add, Plus);
make_expr_fn!(minus, Minus);
