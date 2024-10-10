use rand::Rng;
use vega::*;

fn main() -> Result<()> {
    let sc = Context::new()?;
    let col = sc.make_rdd((0..100).collect::<Vec<_>>(), 32);
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    // let y: f64 = rng.gen();
    let coordinate_iter = col.map(Fn!(|_| {
        let mut rng = rand::thread_rng();
        // let y:f64 = rng.gen();
        let pair = (
            rng.gen_range(-100.0, 100.0) as f64,
            rng.gen_range(-100.0, 100.0) as f64,
        );
        if pair.0 * pair.0 + pair.1 * pair.1 >= 100.0 * 100.0 {
            1
        } else {
            0
        }
    }));
    let res = coordinate_iter.fold(0, Fn!(|acc, i| acc + i));
    println!("result: {:?}", res);
    Ok(())
}
