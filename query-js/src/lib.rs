use neon::prelude::*;

pub fn example_sql(mut cx: FunctionContext) -> JsResult<JsString> {
    Ok(cx.string(query_rs::example_sql()))
}

fn query(mut cx: FunctionContext) -> JsResult<JsString> {
    let sql = cx.argument::<JsString>(0)?.value(&mut cx);
    let output = match cx.argument_opt(1) {
        Some(v) => v.to_string(&mut cx)?.value(&mut cx),
        None => "csv".to_string(),
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut data = rt.block_on(async { query_rs::query(sql).await.unwrap() });

    match output.as_str() {
        "csv" => Ok(cx.string(data.to_csv().unwrap_or("csv type error".to_owned()))),
        "json" => Ok(cx.string(data.to_json().unwrap_or("json type error".to_owned()))),
        v => cx.throw_type_error(format!("Output type {} not supported", v)),
    }
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("example_sql", example_sql)?;
    cx.export_function("query", query)?;
    Ok(())
}
