use std::fmt::Display;

use super::{aggregate::Aggregate, filter::Filter, projection::Projection, scan::Scan};

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan(Scan),
    Filter(Filter),
    Projection(Projection),
    Aggregate(Aggregate),
}

impl LogicalPlan {
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan(_) => vec![],
            LogicalPlan::Filter(selection) => vec![&selection.input],
            LogicalPlan::Projection(projection) => vec![&projection.input],
            // LogicalPlan::Aggregate(aggregate) => vec![&aggregate.input],
            _ => todo!(),
        }
    }
}

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_plan(self, 0))
    }
}
fn format_plan(plan: &LogicalPlan, indent: usize) -> String {
    let mut s = String::new();
    for _ in 0..indent {
        s.push_str("\t");
    }
    let cur_plan = match plan {
        LogicalPlan::Scan(scan) => scan.to_string(),
        LogicalPlan::Filter(selection) => selection.to_string(),
        LogicalPlan::Projection(projection) => projection.to_string(),
        // LogicalPlan::Aggregate(aggregate) => s.push_str(aggregate.to_string().as_str()),
        _ => todo!(),
    };

    s.push_str(cur_plan.as_str());
    s.push_str("\n");

    for child in plan.children() {
        s.push_str(format_plan(child, indent + 1).as_str());
    }
    s
}
