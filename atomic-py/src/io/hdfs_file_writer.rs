use dashmap::Map;
use hdrs::{Client, OpenOptions};
use crate::fs::path;
use crate::io::*;
use crate::error::{Error, Result};
use crate::*;
use crate::rdd::MapperRdd;
use std::io::{self, Read};
use atomic::ser_data::{Data, SerFunc};

pub struct HdfsIO {
    nn: String,
    fs: Client,
}

impl HdfsIO {
    pub fn new(nn: String) -> Result<Self> {
        let nn = nn + ":9000";
        let fs = Client::connect(nn.as_str());
        let fs = match fs {
            Ok(fs) => {
                fs
            }
            Err(e) => {
                return Err(Error::HdfsConnect(nn));
            }
        };
        Ok(HdfsIO {
            nn,
            fs,
        })
    }

    pub fn read_to_vec(&mut self, path: &str) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        let mut oo = self.fs.open_file();
        let file = oo.read(true).open(path);
        let mut file = match file {
            Ok(file) => {
                file
            }
            Err(e) => {
                return Err(Error::HdfsFileOpen(self.nn.to_string()));
            }
        };
        let res = file.read_to_end(&mut buf);
        match res {
            Ok(_) => {}
            Err(_) => {
                return Err(Error::HdfsRead(self.nn.to_string()));
            }
        }
        Ok(buf)
    }

    pub fn read_to_rdd<U, F>(&mut self, path: &str, context: &Arc<Context>, num_slices: usize, f: F) -> Result<SerArc<dyn Rdd<Item = U>>> 
    where
        F: SerFunc<Vec<u8>, Output=Vec<U>>,
        U: Data,
    {
        let mut buf = Vec::new();
        let mut oo = self.fs.open_file();
        let file = oo.read(true).open(path);
        let mut file = match file {
            Ok(file) => {
                file
            }
            Err(_) => {
                return Err(Error::HdfsFileOpen(self.nn.to_string()));
            }
        };
        let res = file.read_to_end(&mut buf);
        match res {
            Ok(_) => {}
            Err(_) => {
                return Err(Error::HdfsRead(self.nn.to_string()));
            }
        }
        let buf = f(buf);
        let rdd = context.make_rdd(buf, num_slices);
        Ok(rdd)
    }

    pub fn read_dir_to_vec(&mut self, path: &str) -> Result<Vec<Vec<u8>>> {
        let mut vec = Vec::<Vec<u8>>::new();
        let dir = self.fs.read_dir(path);
        let dir = match dir {
            Ok(dir) => {
                dir
            }
            Err(_) => {
                return Err(Error::HdfsDirOpen(self.nn.to_string()));
            }
        };
        for (_id, entry) in dir.to_owned().enumerate() {
            let mut buf = Vec::<u8>::new();
            let mut oo = self.fs.open_file();
            let file = oo.read(true).open(entry.path());
            let mut file = match file {
                Ok(file) => {
                    file
                }
                Err(_) => {
                    return Err(Error::HdfsFileOpen(self.nn.to_string()));
                }
            };
            let res = file.read_to_end(&mut buf);
            match res {
                Ok(_) => {}
                Err(_) => {
                    return Err(Error::HdfsRead(self.nn.to_string()));
                }
            }
            vec.push(buf);
        }

        Ok(vec)
    }

    pub fn read_dir_to_rdd<U, F>(&mut self, path: &str, context: &Arc<Context>, num_slices: usize, f: F) -> Result<SerArc<dyn Rdd<Item = Vec<U>>>>
    where
        F: SerFunc<Vec<u8>, Output = Vec<U>>,
        U: Data,
    {
        let mut vec = Vec::<Vec<u8>>::new();
        let dir = self.fs.read_dir(path);
        let dir = match dir {
            Ok(dir) => {
                dir
            }
            Err(_) => {
                return Err(Error::HdfsDirOpen(self.nn.to_string()));
            }
        };
        for (_id, entry) in dir.to_owned().enumerate() {
            let mut buf = Vec::<u8>::new();
            let mut oo = self.fs.open_file();
            let file = oo.read(true).open(entry.path());
            let mut file = match file {
                Ok(file) => {
                    file
                }
                Err(_) => {
                    return Err(Error::HdfsFileOpen(self.nn.to_string()));
                }
            };
            let res = file.read_to_end(&mut buf);
            match res {
                Ok(_) => {}
                Err(_) => {
                    return Err(Error::HdfsRead(self.nn.to_string()));
                }
            }
            vec.push(buf);
        }

        let rdd = context.make_rdd(vec, num_slices);

        let rdd = MapperRdd::new(rdd.into(), f);

        Ok(SerArc::new(rdd))
    }

}
