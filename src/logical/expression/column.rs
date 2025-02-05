use std::fmt::Display;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
}

impl Column {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}
