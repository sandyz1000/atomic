//! This module implements LocalFs Read RDD for reading files in LocalFs
use std::io::Read;
use std::fs;
use std::sync::{Arc, Weak};

use crate::context::Context;
use crate::dependency::{Dependency, NarrowDependencyTrait, ShuffleDependencyTrait};
use crate::error::Result;
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::ser_data::AnyData;
use crate::split::Split;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Clone)]
pub struct LocalFsReadRddSplit {
    rdd_id: i64,
    index: usize,
    values: Vec<String>,
}

impl Split for LocalFsReadRddSplit {
    fn get_index(&self) -> usize {
        self.index
    }
}

impl LocalFsReadRddSplit {
    fn new(rdd_id: i64, index: usize, values: Vec<String>) -> Self {
        LocalFsReadRddSplit {
            rdd_id,
            index,
            values,
        }
    }
    // Lot of unnecessary cloning is there. Have to refactor for better performance
    fn iterator(&self) -> Box<dyn Iterator<Item = Vec<u8>>> {
        let data = self.values.clone();
        let len = data.len();
        let mut res = Vec::with_capacity(len);
        for path in data {
            let mut file = fs::File::open(path.as_str()).expect("file not found");
            let mut content = vec![];
            //let mut reader = BufReader::new(file);
            file.read_to_end(&mut content).unwrap();
            res.push(content);
        }
        println!("finished reading files");
        Box::new(res.into_iter())
    }
}

/// 结构体LocalFsReadRddVals
/// 成员：
/// RddVals: Rdd的元数据
/// splits_: 分区
/// num_slices: 分区数量
/// context: 环境/上下文(接受一个弱引用)
#[derive(Serialize, Deserialize)]
pub struct LocalFsReadRddVals<ND, SD> {
    vals: Arc<RddVals<ND, SD>>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Weak<Context>,
    splits_: Vec<Vec<String>>,
    num_slices: usize,
}

#[derive(Serialize, Deserialize)]
pub struct LocalFsReadRdd<ND, SD> {
    #[serde(skip_serializing, skip_deserializing)]
    name: Mutex<String>,
    path: String,
    rdd_vals: Arc<LocalFsReadRddVals<ND, SD>>,
}

impl<ND, SD> Clone for LocalFsReadRdd<ND, SD> 
where
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait
{
    fn clone(&self) -> Self {
        LocalFsReadRdd {
            name: Mutex::new(self.name.lock().clone()),
            path: self.path.clone(),
            rdd_vals: self.rdd_vals.clone(),
        }
    }
}

/// 函数LocalFsReadRdd::new
/// 接收一个context, 一个路径path和分区数量num_slices
/// 产生一个LocalFsReadRdd对象
///
impl<ND, SD> LocalFsReadRdd<ND, SD> 
where
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait
{
    pub fn new(context: Arc<Context>, path: String, num_slices: usize) -> Self {

        let path_c = path.clone();

        LocalFsReadRdd {
            name: Mutex::new("parallel_collection".to_owned()),
            path,
            rdd_vals: Arc::new(LocalFsReadRddVals {
                //downgrade()方法返回一个Weak<T>类型的对象，Weak<T>是一个弱引用，不会增加引用计数
                context: Arc::downgrade(&context),
                //由context生成rdd_id
                vals: Arc::new(RddVals::new(context.clone())),
                //由data生成的分区
                splits_: LocalFsReadRdd::slice(path_c.as_str(), num_slices),
                //分区数
                num_slices,
            }),
        }
    }

    /**
     * slice 接收path和分区数量 num_slices
     * 读取hdfs对应路径，将它们分区
     * 如果文件数量少于分区数量，则将分区数量改为文件数量
     */
    fn slice(path: &str, num_slices: usize) -> Vec<Vec<String>> {
        let mut num_slices = num_slices;
        if num_slices < 1 {
            num_slices = 1;
        }
        //let fs = Client::connect(nn).expect("cannot connect to namenode");
        let metadata = fs::metadata(path).expect("cannot get metadata");
        let is_file = metadata.is_file();
        if is_file {
            vec![vec![path.to_string()]]
        } else {
            let dir_entries = fs::read_dir(path).expect("cannot read dir");
            let mut res = Vec::with_capacity(num_slices);
            for _ in 0..num_slices {
                res.push(Vec::<String>::new());
            }
            for (i, entry) in dir_entries.enumerate() {
                let entry = entry.expect("cannot read entry");
                if entry.file_type().unwrap().is_dir() {
                    continue;
                }
                let path = entry.path();
                let index = i % num_slices;
                res[index].push(path.to_str().unwrap().to_string());
            }
            for i in 0..num_slices {
                if res[i].len() == 0 {
                    res.remove(i);
                }
            }
            res
        }
    }
}

impl<ND, SD> RddBase for LocalFsReadRdd<ND, SD> 
where
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait
{
    fn get_rdd_id(&self) -> usize {
        self.rdd_vals.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.rdd_vals.vals.context.upgrade().unwrap()
    }

    fn get_op_name(&self) -> String {
        self.name.lock().to_owned()
    }

    fn register_op_name(&self, name: &str) {
        let own_name = &mut *self.name.lock();
        *own_name = name.to_owned();
    }

    fn get_dependencies(&self) -> Vec<Dependency<ND, SD>> {
        self.rdd_vals.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<impl Split>> {
        (0..self.rdd_vals.splits_.len())
            .map(|i| {
                Box::new(LocalFsReadRddSplit::new(
                    self.rdd_vals.vals.id as i64,
                    i,
                    self.rdd_vals.splits_[i as usize].to_vec(),
                ))
            })
            .collect()
    }

    fn number_of_splits(&self) -> usize {
        self.rdd_vals.splits_.len()
    }

    fn cogroup_iterator_any<S: Split + ?Sized>(&self, split: Box<S>) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        self.iterator_any(split)
    }

    fn iterator_any<S: Split + ?Sized>(&self, split: Box<S>) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        log::debug!("inside iterator_any parallel collection",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x)),
        ))
    }
}

impl<ND, SD> Rdd for LocalFsReadRdd<ND, SD> 
where
    ND: NarrowDependencyTrait + 'static,
    SD: ShuffleDependencyTrait + 'static
{
    type Item = Vec<u8>;
    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        Arc::new(LocalFsReadRdd {
            name: Mutex::new(self.name.lock().clone()),
            path: self.path.clone(),
            rdd_vals: self.rdd_vals.clone(),
        })
    }

    fn get_rdd_base(&self) -> Arc<impl RddBase> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        if let Some(s) = split.downcast_ref::<LocalFsReadRddSplit>() {
            Ok(s.iterator())
        } else {
            panic!("Got split object from different concrete type other than LocalFsReadRddSplit")
        }
    }
}
