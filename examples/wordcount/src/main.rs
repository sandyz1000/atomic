/// Word count — canonical atomic example.
///
/// Demonstrates: parallelize, flat_map, map, reduce (via fold + count_by_value).
///
/// Run locally:
///   cargo run --manifest-path examples/wordcount/Cargo.toml
///
/// Run distributed (after `atomic deploy`):
///   ATOMIC_DEPLOYMENT_MODE=distributed ATOMIC_IS_DRIVER=true \
///   ATOMIC_SLAVES=host1@10.0.0.1,host2@10.0.0.2 \
///   cargo run --manifest-path examples/wordcount/Cargo.toml
use atomic_compute::context::Context;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::local()?;

    let lines = vec![
        "hello world".to_string(),
        "hello atomic".to_string(),
        "atomic is fast".to_string(),
        "world is big".to_string(),
    ];

    let counts = ctx
        .parallelize_typed(lines, 2)
        .flat_map(|line| Box::new(line.split_whitespace().map(str::to_string).collect::<Vec<_>>().into_iter()))
        .count_by_value()?;

    let mut pairs: Vec<_> = counts.into_iter().collect();
    pairs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    println!("Word counts (sorted by frequency):");
    for (word, count) in &pairs {
        println!("  {:20} {}", word, count);
    }

    Ok(())
}
