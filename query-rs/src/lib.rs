pub mod convert;
pub mod dialect;
pub mod fetcher;
pub mod loader;

use crate::loader::FormatType;
use anyhow::{Result, anyhow};
use convert::{OrderType, Sql};
pub use dialect::TyrDialect;
pub use dialect::example_sql;
use fetcher::retrieve_data;
use loader::detect_content;
use polars::prelude::*;
use sqlparser::parser::Parser;
use std::convert::TryInto;
use std::ops::{Deref, DerefMut};
use thiserror::Error;
type FetchResult<T> = Result<T, CustomError>;

#[derive(Debug, Error)]
pub enum CustomError {
    #[error("sql expression {0} is not supported")]
    SqlExpressionError(String),
    #[error("sql operator {0} is not supported")]
    SqlOperatorError(String),
    #[error("sql table {0} is not supported")]
    SqlTableError(String),
    #[error("sql select item {0} is not supported")]
    SqlSelectItemError(String),
    #[error("sql order by {0} is not supported")]
    SqlOrderError(String),
    #[error("sql value {0} is not supported")]
    SqlValueError(String),
    #[error("sql statement {0} is not supported")]
    SqlStatementError(String),
    #[error("load type {0} is not supported")]
    LoadTypeError(String),
    #[error("fetch resource {url} error is {error}")]
    FetchError { url: String, error: String },
    #[error("fetch resource type {0} is not support")]
    FetchResourceError(String),
}

#[derive(Debug)]
pub struct DataSet(DataFrame);

impl Deref for DataSet {
    type Target = DataFrame;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DataSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl DataSet {
    /// Convert DataSet To Csv
    pub fn to_csv(&mut self) -> Result<String> {
        let mut buf = Vec::new();
        let mut writer = CsvWriter::new(&mut buf);
        writer.finish(self)?;
        Ok(String::from_utf8(buf)?)
    }

    /// Convert DataSet To Json
    pub fn to_json(&mut self) -> Result<String> {
        println!("------------- come in to_json scope");
        let mut buf = Vec::new();
        let mut writer = JsonWriter::new(&mut buf);
        writer.finish(self)?;
        Ok(String::from_utf8(buf)?)
    }
}

/// 从 from 中获取数据，从 where 中过滤，最后选取需要返回的列
pub async fn query<T: AsRef<str>>(sql: T, load_type: FormatType) -> Result<DataSet> {
    let ast = Parser::parse_sql(&TyrDialect::default(), sql.as_ref())?;

    if ast.len() != 1 {
        return Err(anyhow!("We only support one statement at a time"));
    }

    let sql = &ast[0];

    let Sql {
        source,
        condition,
        selection,
        offset,
        limit,
        order_by,
    } = sql.try_into()?;

    let ds = detect_content(load_type, retrieve_data(source).await?).load()?;
    let mut filtered = match condition {
        Some(expr) => ds.0.lazy().filter(expr),
        None => ds.0.lazy(),
    };

    let order_list = order_by
        .into_iter()
        .map(|(col, order_type)| (col, order_type == OrderType::Desc))
        .collect::<Vec<(String, bool)>>();
    let (cols, orders): (Vec<String>, Vec<bool>) = order_list.into_iter().unzip();

    filtered = filtered.sort(
        cols,
        SortMultipleOptions::default().with_order_descending_multi(orders),
    );

    if offset.is_some() || limit.is_some() {
        filtered = filtered.slice(offset.unwrap_or(0), limit.unwrap_or(20) as IdxSize);
    }

    Ok(DataSet(
        filtered
            .select(selection)
            .with_new_streaming(true)
            .collect()?,
    ))
}
