use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::prelude::SessionContext;
use serde_json::Value;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let str ="CREATE EXTERNAL TABLE example STORED AS PARQUET LOCATION '/oss/ads2/test1/'";
    let ctx = SessionContext::new();
    let df = ctx.sql(str).await.unwrap();
    let results = df.collect().await.unwrap();
    let options = FormatOptions::default().with_display_error(true);
    // let log = pretty_format_batches_with_options(&results, &options).unwrap();
    let json = record_batch_to_json(&results, &options).unwrap();
    println!("results:\n{}", json)
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
