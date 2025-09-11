'use strict';
const q = require('.');

const main = async function() {
    const show_columns_sql = "SHOW COLUMNS FROM https://raw.githubusercontent.com/ai-aide/query-server/refs/heads/master/resource/owid-covid-latest.csv";
    const columns = q.show_columns(show_columns_sql, 'csv');
    console.log(columns);

    const sql = q.example_sql();
    const res = q.query(sql, 'csv');
    console.log(res);
}

main();
