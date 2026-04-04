use atomic_compute::context::Context;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::local()?;

    let values = ctx
        .parallelize_typed(1_u64..=8, 2)
        .map(|value| value * 2)
        .filter(|value| value % 3 != 0)
        .collect()?;

    println!("runtime={:?} values={:?}", ctx.runtime(), values);
    Ok(())
}