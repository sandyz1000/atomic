use serde::{Deserialize, Serialize};

use crate::ser_data::{Data, SerFunc};
use std::marker::PhantomData;


// Aggregator for shuffle tasks.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Aggregator<K, V, C, F1, F2, F3>  {
    pub create_combiner: Box<F1>,
    
    pub merge_value: Box<F2>,

    pub merge_combiners: Box<F3>,
    
    _marker: PhantomData<K>,

    _marker2: PhantomData<V>,

    _marker3: PhantomData<C>

}

impl<K: Data, V: Data, C: Data, F1, F2, F3> Aggregator<K, V, C, F1, F2, F3> 
where
    K: Data, V: Data, C: Data,
    F1: SerFunc<V, Output = C>,
    F2: SerFunc<(C, V), Output = C>,
    F3: SerFunc<(C, C), Output = C>
{
    pub fn new(
        create_combiner: Box<F1>,
        merge_value: Box<F2>,
        merge_combiners: Box<F3>,
    ) -> Self {
        Aggregator {
            create_combiner,
            merge_value,
            merge_combiners,
            _marker: PhantomData,
            _marker2: PhantomData,
            _marker3: PhantomData,
        }
    }
}

impl<K, V, C, F1, F2, F3> Default for Aggregator<K, V, Vec<V>, F1, F2, F3> 
where
    K: Data, V: Data, C: Data,
    F1: SerFunc<V, Output = C>,
    F2: SerFunc<(C, V), Output = C>,
    F3: SerFunc<(C, C), Output = C>
{
    fn default() -> Self {
        let merge_func = serde_closure::Fn!(|mv: (Vec<V>, V)| {
            let (mut buf, v) = mv;
            buf.push(v);
            buf
        });
        let create_combiner = Box::new(serde_closure::Fn!(|v: V| vec![v]));
        let merge_value = Box::new(serde_closure::Fn!(|c: C, v: V| vec![c]));
        let merge_combiners = Box::new(serde_closure::Fn!(|mc: (Vec<V>, Vec<V>)| {
            let (mut b1, mut b2) = mc;
            b1.append(&mut b2);
            b1
        }));
        // Aggregator {
        //     create_combiner,
        //     merge_value,
        //     merge_combiners,
        //     _marker: PhantomData,
        //     _marker2: PhantomData,
        //     _marker3: PhantomData,
        // }

        todo!()
    }
}
