
mod highlighter;
mod interactive;
mod prompt;

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;
use polars::prelude::{
    CsvWriter, DslPlan, IdxSize, IpcWriter, JsonWriter, ParquetWriter, PlHashMap, PlIndexMap,
    SerWriter,
};
use serde::{Deserialize, Serialize};
use polars::prelude::DataFrame;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;

use std::io::{self, BufRead, Cursor};
use std::str::FromStr;

use clap::{Parser, ValueEnum};
use interactive::run_tty;
use polars::sql::SQLContext;

use jni::objects::*;
use jni::sys::jstring;
use jni::JNIEnv;
use std::collections::{BTreeMap, LinkedList};
use serde_json::{json, Value};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about)]
struct Args {
    /// Execute "COMMAND" and exit
    #[arg(short = 'c')]
    command: Option<String>,
    /// Optional query to operate on. Equivalent of `polars -c "QUERY"`
    query: Option<String>,
    #[arg(short = 'o')]
    /// Optional output mode. Defaults to 'table'
    output_mode: Option<OutputMode>,
}

#[derive(ValueEnum, Debug, Default, Clone)]
pub enum OutputMode {
    Csv,
    Json,
    Parquet,
    Arrow,
    #[default]
    Table,
    #[value(alias("md"))]
    Markdown,
}

impl OutputMode {
    fn execute_query(&self, query: &str, ctx: &mut SQLContext) -> DataFrame{
        ctx
            .execute(query)
            .and_then(|lf| {
                if matches!(self, OutputMode::Table | OutputMode::Markdown) {
                    let max_rows = std::env::var("POLARS_FMT_MAX_ROWS")
                        .unwrap_or("20".into())
                        .parse::<IdxSize>()
                        .unwrap_or(20);
                    lf.limit(max_rows).collect()
                } else {
                    lf.collect()
                }
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e)).unwrap()
    }
}

impl FromStr for OutputMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "csv" => Ok(OutputMode::Csv),
            "json" => Ok(OutputMode::Json),
            "parquet" => Ok(OutputMode::Parquet),
            "arrow" => Ok(OutputMode::Arrow),
            "table" => Ok(OutputMode::Table),
            _ => Err(format!("Invalid output mode: {}", s)),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SerializableContext {
    table_map: PlIndexMap<String, DslPlan>,
    tables: Vec<String>,
}

impl From<&'_ mut SQLContext> for SerializableContext {
    fn from(ctx: &mut SQLContext) -> Self {
        let table_map = ctx
            .clone()
            .get_table_map()
            .into_iter()
            .map(|(k, v)| (k, v.logical_plan))
            .collect::<PlIndexMap<_, _>>();
        let tables = ctx.get_tables();

        Self { table_map, tables }
    }
}

impl From<SerializableContext> for SQLContext {
    fn from(ctx: SerializableContext) -> Self {
        SQLContext::new_from_table_map(
            ctx.table_map
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<PlHashMap<_, _>>(),
        )
    }
}

fn run_noninteractive(output_mode: OutputMode) -> io::Result<()> {
    let mut context = SQLContext::new();
    let mut input: Vec<u8> = Vec::with_capacity(1024);
    let stdin = std::io::stdin();

    loop {
        input.clear();
        stdin.lock().read_until(b';', &mut input)?;

        let query = std::str::from_utf8(&input).unwrap_or("").trim();
        if query.is_empty() {
            break;
        }

        output_mode.execute_query(query, &mut context);
    }

    Ok(())
}


#[no_mangle]
pub unsafe extern "C" fn Java_com_on_yarn_polars_PolarsJni_sql(
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

    let json = execute(rust_list);
    env.new_string(json).unwrap().into_raw()
}

pub fn execute(rust_map: LinkedList<String>) -> String {
    let output_mode = OutputMode::Table;
    let mut sqlContext = SQLContext::new();
    let mut json_data: Value = json!({});
    let mut num = 0;
    for key in &rust_map {
        let mut df = output_mode.execute_query(key, &mut sqlContext);
        println!("{}", df);
        let mut buffer = Cursor::new(Vec::new());
        let mut writer = JsonWriter::new(&mut buffer);
        let _ = writer.finish(&mut df.clone());
        let json_str = String::from_utf8(buffer.into_inner()).expect("TODO: panic message");
        let mut json = format!("[{}]", json_str.trim().replace('\n', ","));
        if json.is_empty(){
            json = String::new();
        }
        json_data[num.to_string()] = json!(json);
        num = num + 1;
    }
    json_data.to_string()
}

fn main() {
    let mut rust_map: LinkedList<String> = LinkedList::new();
    rust_map.push_back("SELECT * FROM read_parquet('/Users/duhanmin/Desktop/2/*.parquet') limit 10".into());
    let json = execute(rust_map);
    println!("{}", json)
}