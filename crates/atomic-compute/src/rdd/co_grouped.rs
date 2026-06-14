use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::rdd::rdd_val::RddVals;
use crate::rdd::*;
use atomic_data::dependency::Dependency;
use atomic_data::error::BaseError;
use atomic_data::split::Split;

/// Two-parent cogroup: pairs items from `rdd1: Rdd<(K, V1)>` and `rdd2: Rdd<(K, V2)>` by key.
///
/// Each output partition merges the corresponding partition from each parent by key into a
/// `(K, Vec<V1>, Vec<V2>)` triple.  Both parents must have the same partition count (narrow dep).
/// For mismatched partitioners, shuffle both sides first via `reduce_by_key` before calling
/// `cogroup`.
///
/// This design avoids all `Box<dyn Data>` type-erasure by storing typed RDD references.
#[derive(Clone)]
pub struct CoGroupedRdd<K, V1, V2> {
    vals: Arc<RddVals>,
    rdd1: Arc<dyn Rdd<Item = (K, V1)>>,
    rdd2: Arc<dyn Rdd<Item = (K, V2)>>,
    num_partitions: usize,
    _marker: PhantomData<K>,
}

impl<K, V1, V2> CoGroupedRdd<K, V1, V2>
where
    K: Data + Eq + Hash + Clone,
    V1: Data + Clone,
    V2: Data + Clone,
{
    pub fn new(
        id: usize,
        rdd1: Arc<dyn Rdd<Item = (K, V1)>>,
        rdd2: Arc<dyn Rdd<Item = (K, V2)>>,
    ) -> Self {
        let n1 = rdd1.get_rdd_base().number_of_splits();
        let n2 = rdd2.get_rdd_base().number_of_splits();
        let num_partitions = n1.max(n2);

        let mut vals = RddVals::new(id);
        vals.dependencies.push(Dependency::OneToOne {
            rdd_base: rdd1.get_rdd_base(),
        });
        vals.dependencies.push(Dependency::OneToOne {
            rdd_base: rdd2.get_rdd_base(),
        });

        CoGroupedRdd {
            vals: Arc::new(vals),
            rdd1,
            rdd2,
            num_partitions,
            _marker: PhantomData,
        }
    }
}

impl<K, V1, V2> RddBase for CoGroupedRdd<K, V1, V2>
where
    K: Data + Eq + Hash + Clone,
    V1: Data + Clone,
    V2: Data + Clone,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.num_partitions)
            .map(|i| Box::new(atomic_data::split::ShuffledRddSplit::new(i)) as Box<dyn Split>)
            .collect()
    }

    fn number_of_splits(&self) -> usize {
        self.num_partitions
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        Ok(Box::new(
            self.compute(split)?
                .map(|item| Box::new(item) as Box<dyn Data>),
        ))
    }
}

impl<K, V1, V2> Rdd for CoGroupedRdd<K, V1, V2>
where
    K: Data + Eq + Hash + Clone,
    V1: Data + Clone,
    V2: Data + Clone,
{
    type Item = (K, Vec<V1>, Vec<V2>);

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        let idx = split.get_index();
        let n1 = self.rdd1.get_rdd_base().number_of_splits();
        let n2 = self.rdd2.get_rdd_base().number_of_splits();

        // Map partition index into each parent, wrapping if the parent has fewer partitions.
        let split1 = self.rdd1.get_rdd_base().splits().remove(idx % n1.max(1));
        let split2 = self.rdd2.get_rdd_base().splits().remove(idx % n2.max(1));

        let mut agg: HashMap<K, (Vec<V1>, Vec<V2>)> = HashMap::new();

        for (k, v) in self.rdd1.iterator(split1)? {
            agg.entry(k).or_default().0.push(v);
        }
        for (k, v) in self.rdd2.iterator(split2)? {
            agg.entry(k).or_default().1.push(v);
        }

        Ok(Box::new(
            agg.into_iter().map(|(k, (v1s, v2s))| (k, v1s, v2s)),
        ))
    }

    fn iterator(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        self.compute(split)
    }
}
