
use chrono::prelude::*;
use atomic::io::*;
use atomic::*;
use serde_closure::{traits::Fn, Fn};

fn main() -> Result<()> {
    
    let context = Context::new()?;

    let deserializer = Fn!(|file: Vec<u8>| {
        String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
    });

    let lines = context.read_source(LocalFsReaderConfig::new("./csv_folder"), deserializer);
    // Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    let line = lines.flat_map(Fn!(|lines: Vec<String>| {
        let iter = lines.into_iter().map(|line| {
            let line = line.split(' ').collect::<Vec<_>>();
            let mut time: i64 = line[8].parse::<i64>().unwrap();
            time /= 1000;
            let time = Utc.timestamp(time, 0).hour();
            (
                (line[0].to_string(), line[1].to_string(), time),
                (line[7].parse::<i64>().unwrap(), 1.0),
            )
        });
        Box::new(iter) as Box<dyn Iterator<Item = _>>
    }));
    
    let sum = line.reduce_by_key(Fn!(|((vl, cl), (vr, cr))| (vl + vr, cl + cr)), 1);
    let avg = sum.map(Fn!(|(k, (v, c))| (k, v as f64 / c)));
    let res = avg.collect().unwrap();
    println!("result: {:?}", &res[0]);
    Ok(())
}
