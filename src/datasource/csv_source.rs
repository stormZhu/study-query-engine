use std::{fs::File, sync::Arc};

use crate::error::Result;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use derive_builder::Builder;

use super::DataSource;

#[derive(Debug, Default, Builder, Clone)]
#[builder(default)]
pub struct CsvReadOptions {
    schema: Option<SchemaRef>,
    /// Whether the first row should be treated as a header.
    has_header: bool,
    /// The character used as a field delimiter.
    delimiter: u8,
    /// The character used for quoting fields.
    quote: u8,
}

#[derive(Debug, Clone)]
pub struct CsvDataSource {
    path: String,
    schema: SchemaRef,
    options: CsvReadOptions,
}

impl CsvDataSource {
    pub fn try_new(path: impl Into<String>, options: CsvReadOptions) -> Result<Self> {
        let path = path.into();
        let schema = match &options.schema {
            Some(schema) => schema.clone(),
            None => Self::infer_schema(&path, &options)?,
        };
        Ok(Self {
            path: path.to_string(),
            schema,
            options,
        })
    }

    pub fn infer_schema(path: &str, options: &CsvReadOptions) -> Result<SchemaRef> {
        let file = File::open(path)?;
        let format = arrow::csv::reader::Format::default()
            .with_header(options.has_header)
            .with_delimiter(options.delimiter)
            .with_quote(options.quote);
        let (schema, _) = format.infer_schema(file, Some(10))?;
        Ok(Arc::new(schema))
    }
}

impl DataSource for CsvDataSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<Vec<String>>) -> Result<RecordBatch> {
        let file: File = File::open(self.path.as_str())?;
        let mut csv = arrow::csv::ReaderBuilder::new(self.schema.clone())
            .with_batch_size(1024)
            .with_header(self.options.has_header)
            .with_delimiter(self.options.delimiter)
            .with_quote(self.options.quote)
            .build(file)?;
        let batch = csv.next().unwrap()?;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use arrow::util::pretty;

    use super::*;
    #[test]
    fn test_scan() -> anyhow::Result<()> {
        let path = "testdata/csv/simple.csv";
        let opts = CsvReadOptionsBuilder::default()
            .has_header(true)
            .delimiter(b',')
            .quote(b'"')
            .build()?;

        let csv_source = CsvDataSource::try_new(path, opts);
        let ret = csv_source.unwrap().scan(None)?;
        pretty::print_batches(&[ret]);
        Ok(())
    }
}
