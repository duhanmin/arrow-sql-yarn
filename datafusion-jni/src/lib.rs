use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::arrow::util::pretty::pretty_format_batches_with_options;
use datafusion::prelude::SessionContext;
use jni::objects::*;
use jni::sys::jstring;
use jni::JNIEnv;
use serde_json::{json, Value};
use std::collections::LinkedList;
use tokio::runtime::Runtime;

#[no_mangle]
pub unsafe extern "C" fn Java_com_on_yarn_datafusion_DatafusionJni_sql(
    mut env: JNIEnv,
    _class: JObject,
    map: JObject,
) -> jstring {
    // 使用 env.get_map 方法将 JObject 转换为 JMap
    let list: JList = env.get_list(map.as_ref()).expect("Failed to convert JObject to JMap");
    // 创建一个空的 BTreeMap
    let mut rust_list: LinkedList<String> = LinkedList::new();
    // 获取迭代器并解包
    let mut iter = list.iter(&mut env).expect("Failed to iterate over map");
    // 遍历 JMapIter 并将键值对插入 BTreeMap
    while let Some(key) = iter.next(&mut env).unwrap() {
        let key_str = env.get_string((&key).into())
            .expect("Failed to convert key to string")
            .into();
        rust_list.push_back(key_str);
    }
    let runtime = Runtime::new().expect("Failed to create Tokio runtime");
    let result = runtime.block_on(sql(rust_list));

    match result {
        Ok(log) => env.new_string(log).unwrap().into_raw(),
        Err(err) => {
            env.throw_new("Ljava/lang/Exception;", format!("{err:?}"))
                .expect("throw");
            std::ptr::null_mut()
        }
    }
}

async fn sql(list: LinkedList<String>) -> Result<String, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let mut json_data: Value = json!({});
    let mut num = 0;
    for key in &list {
        let df = ctx.sql(key).await?;
        let results = df.collect().await?;
        let options = FormatOptions::default().with_display_error(true);
        let log = pretty_format_batches_with_options(&results, &options).unwrap();
        let mut json = record_batch_to_json(&results, &options)?;
        if json.is_empty(){
            json = String::new();
        }
        json_data[num.to_string()] = json!(json);
        println!("results:\n{}", log);
        num = num + 1;
    }
    Ok(json_data.to_string())
}


fn record_batch_to_json(results: &[RecordBatch], options: &FormatOptions) -> Result<String, serde_json::Error> {
    let mut json_data = Vec::new();
    for batch in results {
        let schema = batch.schema();
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), options))
            .collect::<Result<Vec<_>, ArrowError>>().unwrap();

        for row in 0..batch.num_rows() {
            let mut row_map = serde_json::Map::new();
            for (i, formatter) in formatters.iter().enumerate() {
                let field_name = schema.field(i).name();
                let value = formatter.value(row).to_string();
                row_map.insert(field_name.clone(), Value::String(value));
            }
            json_data.push(Value::Object(row_map));
        }
    }
    serde_json::to_string(&json_data)

}


