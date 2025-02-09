use std::{fmt::Display, sync::Arc};

use crate::datasource::DataSource;

#[derive(Debug, Clone)]

pub struct Scan {
    pub path: String,
    pub source: Arc<dyn DataSource>,
    pub projection: Option<Vec<String>>, // 为什么不用expr
}

impl Scan {
    pub fn new(
        path: impl Into<String>,
        source: Arc<dyn DataSource>,
        projection: Option<Vec<String>>,
    ) -> Self {
        Self {
            path: path.into(),
            source,
            projection,
        }
    }
}

impl Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.projection {
            None => write!(f, "Scan: {}; projection=None", self.path),
            Some(ref projection) => {
                write!(f, "Scan: {}; projection={:?}", self.path, projection)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_scan_display_none() {
        // let scan = Scan::new("scan", , None);
        // println!("{}", scan);

        // assert_eq!(format!("{}", scan), "Scan: scan; projection=None");
        // let scan = Scan::new(
        //     "scan",
        //     "source".to_string(),
        //     Some(vec!["a".to_string(), "b".to_string()]),
        // );
        // assert_eq!(format!("{}", scan), r#"Scan: scan; projection=["a", "b"]"#);
    }
}
