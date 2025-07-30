use anyhow::Result;
use query_rs::query;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv";
    let sql = format!(
        "SELECT location, total_cases, new_cases, total_deaths, new_deaths  FROM {} where new_deaths >= 5 ORDER BY new_cases DESC LIMIT 6 OFFSET 5",
        url
    );

    let df = query(sql).await?;
    println!("{:?}", df);

    Ok(())
}
