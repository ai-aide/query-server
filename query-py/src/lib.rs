use pyo3::{exceptions, prelude::*};
use query_rs::loader::FormatType;

#[pyfunction]
pub fn example_sql() -> PyResult<String> {
    Ok(query_rs::example_sql())
}

#[pyfunction]
pub fn query(sql: &str, output: Option<&str>) -> PyResult<String> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut data = rt.block_on(async { query_rs::query(sql, FormatType::Csv).await.unwrap() });
    match output {
        Some("csv") | None => Ok(data.to_csv().unwrap()),
        Some(v) => Err(exceptions::PyTypeError::new_err(format!(
            "Output type {} not supported",
            v
        ))),
    }
}

#[pymodule]
pub fn query_py<'py>(m: &Bound<'py, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(example_sql, m)?)?;
    Ok(())
}
