use crate::DataSet;
use anyhow::Result;
use polars::prelude::*;
use std::io::Cursor;

pub trait Load {
    type Error;
    fn load(self) -> Result<DataSet, Self::Error>;
}

#[derive(Debug)]
pub enum LoadType {
    Csv,
    Json,
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

pub fn detect_content(load_type: LoadType, data: String) -> Loader {
    // ToDo Content Detection
    match load_type {
        LoadType::Csv => Loader::Csv(CsvLoader(data)),
        LoadType::Json => Loader::Json(JsonLoader(data)),
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
