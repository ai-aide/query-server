use sqlparser::dialect::Dialect;

#[derive(Debug, Default)]
pub struct TyrDialect;

impl Dialect for TyrDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch) || ('A'..='Z').contains(&ch) || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ('0'..='9').contains(&ch)
            || [':', '/', '?', '&', '=', '-', '_', '.'].contains(&ch)
    }
}

pub fn example_sql() -> String {
    let url = "https://raw.githubusercontent.com/ai-aide/query-server/refs/heads/master/resource/owid-covid-latest.csv";
    let sql = format!(
        "SELECT total_deaths, new_deaths as new_deaths_1  FROM {} where new_deaths >= 5 and total_deaths>29.0  ORDER BY total_deaths, new_deaths DESC LIMIT 10 OFFSET 0",
        url
    );
    sql
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::parser::Parser;

    #[test]
    fn it_works() {
        assert!(Parser::parse_sql(&TyrDialect::default(), &example_sql()).is_ok())
    }
}
