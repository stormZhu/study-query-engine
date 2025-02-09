use std::sync::Arc;

use arrow_schema::Schema;

use crate::error::Result;
use crate::logical::expression::expr::LogicalExpr;
use crate::physical::expr::PhysicalExpression;
use crate::physical::expr::binary::BinaryExpr;
use crate::physical::expr::column::ColumnExpr;
use crate::physical::expr::literal::LiteralExpr;
use crate::physical::plan::{FilterExec, ProjectionExec, ScanExec};
use crate::{logical::plan::LogicalPlan, physical::plan::PhysicalPlan};

pub struct Planner;

impl Planner {
    pub fn create_physical_plan(plan: &LogicalPlan) -> Result<Arc<PhysicalPlan>> {
        let phy_plan = match plan {
            LogicalPlan::Scan(scan) => PhysicalPlan::Scan(ScanExec::new(
                scan.path.clone(),
                scan.source.clone(),
                scan.projection.clone(),
            )),
            LogicalPlan::Projection(projection) => {
                let input = Self::create_physical_plan(&projection.input)?;

                let mut fields = Vec::with_capacity(projection.exprs.len());
                let mut project_expr = Vec::with_capacity(projection.exprs.len());
                for curr_expr in projection.exprs.iter() {
                    let field = curr_expr.to_field(plan)?;
                    let expr = Self::create_physical_expr(&projection.input.schema(), curr_expr);
                    fields.push(field);
                    project_expr.push(expr);
                }
                let schema = Arc::new(Schema::new(fields));

                // TODO: 计算新的 schema
                PhysicalPlan::Projection(ProjectionExec::new(input.clone(), schema, project_expr))
            }
            LogicalPlan::Filter(filter) => {
                let input = Self::create_physical_plan(&filter.input)?;
                let predicate =
                    Self::create_physical_expr(&filter.input.schema(), &filter.predicate);
                PhysicalPlan::Filter(FilterExec::new(input, predicate))
            }
            _ => unimplemented!(),
        };
        Ok(Arc::new(phy_plan))
    }

    pub fn create_physical_expr(schema: &Schema, expr: &LogicalExpr) -> PhysicalExpression {
        match expr {
            LogicalExpr::Column(v) => {
                let (index, _) = schema.column_with_name(&v.name).unwrap();
                PhysicalExpression::Column(ColumnExpr::new(v.name.clone(), index))
            }
            LogicalExpr::Literal(v) => PhysicalExpression::Literal(LiteralExpr::new(v.clone())),
            LogicalExpr::Binary(v) => {
                let left = Self::create_physical_expr(schema, &v.lhs);
                let right = Self::create_physical_expr(schema, &v.rhs);
                PhysicalExpression::Binary(BinaryExpr::new(
                    Arc::new(left),
                    v.op.clone(),
                    Arc::new(right),
                ))
            }
            _ => {
                println!("expr: {:?}", expr);
                todo!()
            }
        }
    }
}
