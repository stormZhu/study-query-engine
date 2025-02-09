use std::fmt::Display;

use crate::{error::Result, logical::plan::LogicalPlan};
use arrow_schema::{Field, Schema};
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
}

impl Column {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Resolves this column to its [`Field`] definition from a schema.
    pub fn to_field(&self, schema: &Schema) -> Result<Field> {
        let (_, field) = schema.column_with_name(&self.name).unwrap();
        Ok(field.clone())
    }

    pub fn to_field_from_plan(&self, plan: &LogicalPlan) -> Result<Field> {
        self.to_field(&plan.schema())
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}
