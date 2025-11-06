use rustc_hash::FxHasher;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::data::Data;

pub fn hash<T: Hash>(t: &T) -> u64 {
    let mut s: FxHasher = Default::default();
    t.hash(&mut s);
    s.finish()
}

/// Partitioner enum for creating Rdd partitions
#[derive(Clone)]
pub enum Partitioner {
    Hash {
        num_partitions: usize,
        // Type-erased function for computing partition from key
        get_partition_fn: Arc<dyn Fn(&dyn Any) -> usize + Send + Sync>,
    },
}

impl Partitioner {
    /// Create a hash partitioner for keys of type K
    pub fn hash<K: Data + Hash + Eq>(num_partitions: usize) -> Self {
        let get_partition_fn = Arc::new(move |key: &dyn Any| -> usize {
            let key = key.downcast_ref::<K>().unwrap();
            let mut s: FxHasher = Default::default();
            key.hash(&mut s);
            (s.finish() as usize) % num_partitions
        });

        Partitioner::Hash {
            num_partitions,
            get_partition_fn,
        }
    }

    /// Check if two partitioners are equal (same type and same num_partitions)
    pub fn equals(&self, other: &Partitioner) -> bool {
        match (self, other) {
            (
                Partitioner::Hash {
                    num_partitions: n1, ..
                },
                Partitioner::Hash {
                    num_partitions: n2, ..
                },
            ) => n1 == n2,
        }
    }

    /// Get the number of partitions
    pub fn get_num_of_partitions(&self) -> usize {
        match self {
            Partitioner::Hash { num_partitions, .. } => *num_partitions,
        }
    }

    /// Get the partition index for a given key
    pub fn get_partition(&self, key: &dyn Any) -> usize {
        match self {
            Partitioner::Hash {
                get_partition_fn, ..
            } => get_partition_fn(key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_partition() {
        let data = vec![1, 2];
        let num_partition = 3;
        let hash_partitioner = Partitioner::hash::<i32>(num_partition);

        for i in &data {
            println!("value: {:?}-hash: {:?}", i, hash(i));
            println!(
                "value: {:?}-index: {:?}",
                i,
                hash_partitioner.get_partition(i as &dyn Any)
            );
        }

        let mut partition = vec![Vec::new(); num_partition];
        for i in &data {
            let index = hash_partitioner.get_partition(i as &dyn Any);
            partition[index].push(i)
        }
        assert_eq!(partition.len(), 3)
    }

    #[test]
    fn hash_partitioner_eq() {
        let p1 = Partitioner::hash::<i32>(1);
        let p2_1 = Partitioner::hash::<i32>(2);
        let p2_2 = Partitioner::hash::<i32>(2);

        assert!(p1.equals(&p1));
        assert!(p2_1.equals(&p2_1));
        assert!(p2_1.equals(&p2_2));
        assert!(p2_2.equals(&p2_1));
        assert!(!p1.equals(&p2_1));
        assert!(!p1.equals(&p2_2));

        let mut p1 = Some(p1);
        assert!(p1.clone().map(|p| p.equals(&p1.clone().unwrap())) == Some(true));
        assert!(p1.clone().map(|p| p.equals(&p2_1)) == Some(false));
        assert!(p1.clone().map(|p| p.equals(&p2_2)) == Some(false));
        assert!(p1.clone().map(|p| p.equals(&p1.clone().unwrap())) != None);
        assert!(p1.clone().map_or(false, |p| p.equals(&p1.clone().unwrap())));
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_1)));
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_2)));

        p1 = None;
        assert!(p1.clone().map(|p| p.equals(&p1.clone().unwrap())) == None);
        assert!(p1.clone().map(|p| p.equals(&p2_1)) == None);
        assert!(p1.clone().map(|p| p.equals(&p2_2)) == None);
        assert!(!p1.clone().map_or(false, |p| p.equals(&p1.clone().unwrap())));
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_1)));
        assert!(!p1.map_or(false, |p| p.equals(&p2_2)));
    }
}
