use std::time::Instant;
use atomic::io::*;
use atomic::*;
use std::{env, fs, os::unix::prelude::FileExt, io::Write};

fn main() -> Result<()> {
    //std::env::set_current_dir("/home/lml");
    std::env::set_var("ATOMIC_HOME", "/home/lml/.jdk/jdk1.8.0_371");
    std::env::set_var("HADOOP_HOME", "/home/lml/hadoop-3.3.5");
    let start = Instant::now();
    let mut file = std::fs::File::create("/tmp/env1.txt").expect("create failed");
    for (key, value) in env::vars() {
        let msg = format!("{}: {}\n", key, value);
        file.write(msg.as_bytes()).expect("write failed");
    }

    let context = Context::new()?;
    let deserializer = Fn!(|file: Vec<u8>| {
        String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    });
    let lines = context.read_source(HdfsReaderConfig::new("/csv"), deserializer);
    // let H = HdfsIO::new("192.168.179.129".to_string());
    // let mut H = H.unwrap();
    // let lines = H.read_dir_to_rdd("/csv_folder", &context, 32, deserializer).unwrap();
    println!("successfully read source");
    let line = lines.flat_map(Fn!(|lines: Vec<String>| {
        println!("flat map");
        let iter = lines.into_iter().map(|line| {
            let line = line.split(',').collect::<Vec<_>>();
            (
                (line[0].to_string()),
                (line[7].parse::<f64>().unwrap(), 1.0),
            )
        });
        Box::new(iter) as Box<dyn Iterator<Item = _>>
    }));
    println!("successfully flat map");
    //let sum = line.reduce_by_key(Fn!(|((vl, cl), (vr, cr))| (vl + vr, cl + cr)), 1);
    //let avg = sum.map(Fn!(|(k, (v, c))| (k, v as f64 / c)));
    //let res = avg.collect().unwrap();
    let res = line.collect().unwrap();
    println!("result: {:?}", res);
    let duration = start.elapsed();
    println!("Time elapsed is: {:?}", duration);
    Ok(())
}
