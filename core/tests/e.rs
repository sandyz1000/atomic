// use vega::*;
// use astro_float::ctx::Context as ctx;
// // use astro_float::ex
// fn main() -> Result<()> {
//     let sc = Context::new()?;
//     let col = sc.make_rdd((0..7000).collect::<Vec<_>>(), 3);
//     //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
//     // let y: f64 = rng.gen();
//     let item_iter = col.map(Fn!(|i|{
//         let mut frac=ONE.clone();
        
//         for k in 1..=i{
//             let f_k=BigFloat::from_i32(k);
//             frac=frac.mul(&ONE.div(&f_k));
//         }
//         frac
//     }));
//     // let fraction = item_iter.map(Fn!(|i|1 as f64 / i as f64));
//     let res=item_iter.fold(ZERO, Fn!(|acc,n|{
//         (&acc as &BigFloat).add(&n)
//     }));
//     println!("result: {:?}", res);
//     Ok(())
// }
