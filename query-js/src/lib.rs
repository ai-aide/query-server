use neon::prelude::*;
use query_rs::loader::FormatType;

pub fn example_sql(mut cx: FunctionContext) -> JsResult<JsString> {
    Ok(cx.string(query_rs::example_sql()))
}

fn show_columns(mut cx: FunctionContext) -> JsResult<JsArray> {
    let sql = cx.argument::<JsString>(0)?.value(&mut cx);
    let arg_prams = match cx.argument_opt(1) {
        Some(v) => v.to_string(&mut cx)?.value(&mut cx),
        None => "csv".to_string(),
    };
    let load_type = match arg_prams.as_str().try_into() {
        Ok(inner) => inner,
        Err(e) => {
            println!("custom error for {:?} is {:?}", arg_prams, e);
            FormatType::Csv
        }
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let column_list = rt.block_on(async { query_rs::show_columns(sql, load_type).await.unwrap() });

    let array = cx.empty_array();
    for (index, item) in column_list.into_iter().enumerate() {
        let key = cx.string(item.0);
        let value = cx.string(item.1.to_string());
        let object = cx
            .empty_object()
            .prop(&mut cx, key)
            .set(value)
            .unwrap()
            .this();
        array.set(&mut cx, index as u32, object)?;
    }

    Ok(array)
}

fn query(mut cx: FunctionContext) -> JsResult<JsString> {
    let sql = cx.argument::<JsString>(0)?.value(&mut cx);
    let arg_prams = match cx.argument_opt(1) {
        Some(v) => v.to_string(&mut cx)?.value(&mut cx),
        None => "csv".to_string(),
    };
    let load_type: FormatType = match arg_prams.as_str().try_into() {
        Ok(inner) => inner,
        Err(e) => {
            println!("custom error for {:?} is {:?}", arg_prams, e);
            FormatType::Csv
        }
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut data = rt.block_on(async { query_rs::query(sql, load_type).await.unwrap() });

    let output_format = match cx.argument_opt(2) {
        Some(v) => v.to_string(&mut cx)?.value(&mut cx),
        None => "csv".to_string(),
    };
    match output_format.as_str() {
        "csv" => Ok(cx.string(data.to_csv().unwrap_or("csv type error".to_owned()))),
        "json" => Ok(cx.string(data.to_json().unwrap_or("json type error".to_owned()))),
        v => cx.throw_type_error(format!("Output type {} not supported", v)),
    }
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("example_sql", example_sql)?;
    cx.export_function("query", query)?;
    cx.export_function("show_columns", show_columns)?;
    Ok(())
}
