pub mod convert;
pub mod dialect;
pub mod fetcher;
pub mod loader;

use crate::loader::FormatType;
use anyhow::Result;
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
type QueryResult<T> = Result<T, CustomError>;

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
    #[error("sql expr function item {0} is not supported")]
    SqlExprFuncItem(String),
    #[error("sql expr function args item {0} is not supported")]
    SqlExprFuncArgsItem(String),
    #[error("sql order by {0} is not supported")]
    SqlOrderError(String),
    #[error("sql value {0} is not supported")]
    SqlValueError(String),
    #[error("sql statement {0} is not supported")]
    SqlStatementError(String),
    #[error("sql convert {0} is not supported")]
    SqlConvertError(String),
    #[error("load type {0} is not supported")]
    LoadTypeError(String),
    #[error("fetch resource {url} error is {error}")]
    FetchError { url: String, error: String },
    #[error("fetch resource type {0} is not support")]
    FetchResourceError(String),
    #[error("polars error is {error}")]
    PolarsError { error: String },
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

#[derive(PartialEq, Eq, Debug)]
pub struct ColumnType(DataType);

impl Deref for ColumnType {
    type Target = DataType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ColumnType {
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
        let mut buf = Vec::new();
        let mut writer = JsonWriter::new(&mut buf);
        writer.finish(self)?;
        Ok(String::from_utf8(buf)?)
    }
}

pub async fn show_columns<T: AsRef<str>>(
    sql: T,
    format_type: FormatType,
) -> QueryResult<Vec<(String, ColumnType)>> {
    let ast = Parser::parse_sql(&TyrDialect::default(), sql.as_ref())
        .map_err(|e| CustomError::SqlConvertError(e.to_string()))?;

    if ast.len() != 1 {
        return Err(CustomError::SqlConvertError(format!("{:?}", ast)));
    }

    let Sql { source, .. } = (&ast[0]).try_into()?;

    let ds = detect_content(format_type, retrieve_data(source).await?)
        .load()
        .map_err(|e| CustomError::FetchError {
            url: "".to_string(),
            error: e.to_string(),
        })?;

    let list = ds
        .fields()
        .into_iter()
        .map(|inner| (inner.name.to_string(), ColumnType(inner.dtype)))
        .collect::<Vec<(String, ColumnType)>>();

    Ok(list)
}

pub async fn query<T: AsRef<str>>(sql: T, format_type: FormatType) -> QueryResult<DataSet> {
    let ast = Parser::parse_sql(&TyrDialect::default(), sql.as_ref())
        .map_err(|e| CustomError::SqlConvertError(e.to_string()))?;

    if ast.len() != 1 {
        return Err(CustomError::SqlConvertError(format!("{:?}", ast)));
    }

    let Sql {
        source,
        condition,
        selection,
        aggregation,
        offset,
        limit,
        order_by,
        group_by,
    } = (&ast[0]).try_into()?;

    let ds = detect_content(format_type, retrieve_data(source).await?)
        .load()
        .map_err(|e| CustomError::FetchError {
            url: "".to_string(),
            error: e.to_string(),
        })?;
    let mut filtered = match condition {
        Some(expr) => ds.0.lazy().filter(expr),
        None => ds.0.lazy(),
    };

    let dataset = if group_by.len() > 0 {
        // group by select
        let filtered =
            filtered.group_by(group_by.iter().map(|item| col(item)).collect::<Vec<Expr>>());
        DataSet(
            filtered
                .agg(aggregation)
                .with_new_streaming(true)
                .select(selection)
                .collect()
                .map_err(|e| CustomError::PolarsError {
                    error: e.to_string(),
                })?,
        )
    } else {
        // general select
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

        DataSet(
            filtered
                .select(selection)
                .with_new_streaming(true)
                .collect()
                .map_err(|e| CustomError::PolarsError {
                    error: e.to_string(),
                })?,
        )
    };

    Ok(dataset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loader::FormatType;
    use tokio;

    #[tokio::test]
    async fn csv_show_columns_work() {
        let show_columns_sql = "SHOW COLUMNS FROM https://raw.githubusercontent.com/ai-aide/query-server/refs/heads/master/resource/owid-covid-latest.csv";
        let columns = show_columns(show_columns_sql, FormatType::Csv).await;
        assert_eq!(columns.is_ok(), true);
        if let Ok(column_list) = columns {
            assert_eq!(column_list.len(), 67);
            assert_eq!(column_list[0].0, "iso_code");
            assert_eq!(column_list[1].1, ColumnType(DataType::String));
        }
    }

    #[tokio::test]
    async fn csv_query_work() {
        let url = "https://raw.githubusercontent.com/ai-aide/query-server/refs/heads/master/resource/owid-covid-latest.csv";
        let sql = format!(
            "SELECT total_deaths, new_deaths  FROM {} where new_deaths >= 5 and total_deaths>29.0  ORDER BY total_deaths, new_deaths DESC LIMIT 10 OFFSET 0",
            url
        );
        let res = query(sql, FormatType::Csv).await;
        assert_eq!(res.is_ok(), true);
        if let Ok(dataset) = res {
            assert_eq!(dataset.height(), 10);
            assert_eq!(dataset.width(), 2);
        }
    }

    #[tokio::test]
    async fn csv_group_by_query_work() {
        let url = "https://raw.githubusercontent.com/ai-aide/query-server/refs/heads/master/resource/owid-covid-latest.csv";
        let sql = format!(
            "SELECT max(iso_code) as bac
            , iso_code FROM {} group by iso_code",
            url
        );
        let res = query(sql, FormatType::Csv).await;
        // assert_eq!(res.is_ok(), true);
        // if let Ok(dataset) = res {
        //     // println!("----: {:?}", dataset);
        //     assert_eq!(dataset.height(), 10);
        //     assert_eq!(dataset.width(), 2);
        // }
    }

    #[tokio::test]
    async fn json_show_columns_work() {
        let show_columns_sql = "SHOW COLUMNS FROM https://raw.githubusercontent.com/ai-aide/query-server/refs/heads/master/resource/iris.json";
        let columns = show_columns(show_columns_sql, FormatType::Json).await;
        assert_eq!(columns.is_ok(), true);
        if let Ok(column_list) = columns {
            assert_eq!(column_list.len(), 5);
            assert_eq!(column_list[0].0, "sepalLength");
            assert_eq!(column_list[1].1, ColumnType(DataType::Float64));
        }
    }

    #[tokio::test]
    async fn json_query_work() {
        let url = "https://raw.githubusercontent.com/ai-aide/query-server/refs/heads/master/resource/iris.json";
        let sql = format!(
            "SELECT sepalLength, sepalWidth FROM {} WHERE sepalLength > 5.0 LIMIT 10 offset 1",
            url
        );
        let res = query(sql, FormatType::Json).await;
        assert_eq!(res.is_ok(), true);
        if let Ok(dataset) = res {
            assert_eq!(dataset.height(), 10);
            assert_eq!(dataset.width(), 2);
        }
    }

    #[tokio::test]
    async fn json_group_by_query_work() {
        let url = "https://raw.githubusercontent.com/ai-aide/query-server/refs/heads/master/resource/iris.json";
        let sql = format!(
            "SELECT count(*) as count_num, sepalLength FROM {} WHERE sepalLength > 5.0 group by sepalLength",
            url
        );
        let res = query(sql, FormatType::Json).await;

        assert_eq!(res.is_ok(), true);
        if let Ok(dataset) = res {
            let count_num = dataset
                .0
                .lazy()
                .filter(col("sepalLength").eq(lit(6.7)))
                .collect()
                .unwrap()
                .slice(0, 1)
                .column("count_num")
                .unwrap()
                .get(0)
                .unwrap()
                .try_extract::<i32>()
                .unwrap();
            assert_eq!(count_num, 8);
        }
    }

    #[tokio::test]
    async fn json_query_wildcard_work() {
        let url = "https://raw.githubusercontent.com/ai-aide/query-server/refs/heads/master/resource/iris.json";
        let sql = format!(
            "SELECT * FROM {} WHERE sepalLength > 5.0 LIMIT 10 offset 1",
            url
        );
        let res = query(sql, FormatType::Json).await;
        assert_eq!(res.is_ok(), true);
        if let Ok(dataset) = res {
            assert_eq!(dataset.height(), 10);
            assert_eq!(dataset.width(), 5);
        }
    }
}
