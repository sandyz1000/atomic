use crate::error::{Error, Result};
use crate::io::*;
use crate::rdd::rdd::HdfsReadRdd;
use crate::*;
use hdrs::Client;
use std::io::{Read, Write};

pub struct HdfsIO {
    nn: String,
    fs: Client,
}

impl HdfsIO {
    pub fn new() -> Result<Self> {
        let nn = match std::env::var("namenode") {
            Ok(nn) => nn,
            Err(_) => {
                return Err(Error::HdfsNamenode);
            }
        };
        let nn = nn + ":9000";
        let fs = Client::connect(nn.as_str());
        let fs = match fs {
            Ok(fs) => fs,
            Err(_) => {
                return Err(Error::HdfsConnect(nn));
            }
        };
        Ok(HdfsIO { nn, fs })
    }

    pub fn read_to_vec(&mut self, path: &str) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        let mut oo = self.fs.open_file();
        let file = oo.read(true).open(path);
        let mut file = match file {
            Ok(file) => file,
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
        Ok(buf)
    }

    pub fn read_to_rdd(
        &mut self,
        path: &str,
        context: &Arc<Context>,
        num_slices: usize,
    ) -> HdfsReadRdd
    {
        let rdd = HdfsReadRdd::new(context.clone(), self.nn.clone(), path.to_string(), num_slices);
        rdd
    }

    pub fn read_to_rdd_and_decode<U, F>(
        &mut self,
        path: &str,
        context: &Arc<Context>,
        num_slices: usize,
        decoder: F,
    ) -> Arc<dyn Rdd<Item = U>>
    where
        F: SerFunc(Vec<u8>) -> U,
        U: Data,
    {
        let rdd = HdfsReadRdd::new(context.clone(), self.nn.clone(), path.to_string(), num_slices);
        let rdd = rdd.map(decoder);
        rdd
    }

    pub fn write_to_hdfs (&mut self, data: &[u8], path: &str, create: bool) -> Result<()> {
        let file = self.fs.open_file().create(create).write(true).open(path);
        let mut file = match file {
            Ok(file) => file,
            Err(_) => {
                return Err(Error::HdfsFileOpen(self.nn.clone()));
            },
        };
        let res = file.write(data);
        match res {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::HdfsWrite(self.nn.to_string())),
        }
    }

}
