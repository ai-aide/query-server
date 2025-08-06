use crate::CustomError;
use crate::DataSet;
use anyhow::Result;
use polars::prelude::*;
use std::io::Cursor;

pub trait Load {
    type Error;
    fn load(self) -> Result<DataSet, Self::Error>;
}

#[derive(Debug)]
pub enum FormatType {
    Csv,
    Json,
}

impl TryFrom<&str> for FormatType {
    type Error = CustomError;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "csv" => Ok(FormatType::Csv),
            "json" => Ok(FormatType::Json),
            v => Err(CustomError::LoadTypeError(v.to_string())),
        }
    }
}

#[derive(Debug)]
pub enum Loader {
    Csv(CsvLoader),
    Json(JsonLoader),
}

#[derive(Default, Debug)]
pub struct CsvLoader(pub(crate) String);

#[derive(Default, Debug)]
pub struct JsonLoader(pub(crate) String);

impl Loader {
    pub fn load(self) -> Result<DataSet> {
        match self {
            Loader::Csv(csv) => csv.load(),
            Loader::Json(json) => json.load(),
        }
    }
}

pub fn detect_content(format_type: FormatType, data: String) -> Loader {
    // ToDo Content Detection
    match format_type {
        FormatType::Csv => Loader::Csv(CsvLoader(data)),
        FormatType::Json => Loader::Json(JsonLoader(data)),
    }
}

impl Load for CsvLoader {
    type Error = anyhow::Error;

    fn load(self) -> Result<DataSet, Self::Error> {
        let df = CsvReader::new(Cursor::new(self.0)).finish()?;
        Ok(DataSet(df))
    }
}

impl Load for JsonLoader {
    type Error = anyhow::Error;

    fn load(self) -> Result<DataSet, Self::Error> {
        let df = JsonReader::new(Cursor::new(self.0)).finish()?;
        Ok(DataSet(df))
    }
}
