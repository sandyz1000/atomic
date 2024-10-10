use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct PythonRDD {
    parent: Rdd,
    func: PythonFunction,
    preserve_partitioning: bool,
    is_from_barrier: bool,
    worker_broadcasts: Arc<Mutex<HashMap<Socket, HashSet<i64>>>>,
    auth_helper: Arc<SocketAuthHelper>,
}

impl PythonRDD {
    pub fn get_partitions(self) -> Vec<Partition> {
        self.parent.get_partitions()
    }

    pub fn partitioner(&self) -> Option<Partitioner> {
        if self.preserve_partitioning {
            self.parent.partitioner()
        } else {
            None
        }
    }

    pub fn compute(self, split: Partition, context: TaskContext) -> Iterator<ByteBuf> {
        let runner = PythonRunner::new(self.func);
        runner.compute(self.parent.iterator(split, context), split.index, context)
    }

    pub fn is_barrier(&self) -> bool {
        self.is_from_barrier || self.dependencies.iter().any(|d| d.rdd.is_barrier())
    }
}

trait PythonFunction {
    fn command(&self) -> Vec<u8>;
    fn env_vars(&self) -> HashMap<String, String>;
    fn python_includes(&self) -> Vec<String>;
    fn python_exec(&self) -> String;
    fn python_ver(&self) -> String;
    fn broadcast_vars(&self) -> Vec<Broadcast<String>>;
    fn accumulator(&self) -> PythonAccumulatorV2;
}

pub struct SimplePythonFunction {
    command: Vec<u8>,
    envVars: HashMap<String, String>,
    pythonIncludes: Vec<String>,
    pythonExec: String,
    pythonVer: String,
    broadcastVars: Vec<Broadcast>,
    accumulator: PythonAccumulatorV2,
}

impl SimplePythonFunction {
    pub fn new(
        command: Vec<u8>,
        envVars: HashMap<String, String>,
        pythonIncludes: Vec<String>,
        pythonExec: String,
        pythonVer: String,
        broadcastVars: Vec<Broadcast>,
        accumulator: PythonAccumulatorV2,
    ) -> Self {
        Self {
            command,
            envVars,
            pythonIncludes,
            pythonExec,
            pythonVer,
            broadcastVars,
            accumulator,
        }
    }

}


#[derive(Debug, Clone)]
pub struct PythonException {
    pub msg: String,
    pub cause: Option<Box<dyn Error>>,
}

impl PythonException {
    pub fn new(msg: String, cause: Option<Box<dyn Error>>) -> Self {
        PythonException { msg, cause }
    }
}

impl Display for PythonException {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "PythonException: {}", self.msg)
    }
}

impl Error for PythonException {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.cause {
            Some(e) => Some(e.as_ref()),
            None => None,
        }
    }
}


pub struct PairwiseRDD {
    prev: RDD,
}

impl PairwiseRDD {
    fn new(prev: RDD) -> Self {
        Self { prev }
    }

    fn get_partitions(&self) -> Vec<Partition> {
        self.prev.get_partitions()
    }

    fn partitioner(&self) -> Option<Partitioner> {
        self.prev.partitioner()
    }

    fn compute(&self, split: Partition, context: TaskContext) -> Box<dyn Iterator<Item = (i64, Vec<u8>)>> {
        let mut iter = self.prev.compute(split, context);
        Box::new(iter.by_ref().group_by(|x| x % 2 == 0).map(|(key, mut group)| {
            if key {
                let a = group.next().unwrap();
                let b = group.next().unwrap();
                (a, b)
            } else {
                panic!("PairwiseRDD: unexpected value");
            }
        }))
    }

    fn as_pair_rdd(&self) -> PairRdd {
        PairRdd::from_rdd(self)
    }
}


impl PythonRDD {
    fn new() -> Self {
        PythonRDD {
            worker_broadcasts: Arc::new(Mutex::new(HashMap::new())),
            auth_helper: Arc::new(SocketAuthHelper::new()),
            parent: todo!(),
            func: todo!(),
            preserve_partitioning: todo!(),
            is_from_barrier: todo!(),
        }
    }

    fn get_worker_broadcasts(&self, worker: &Socket) -> HashSet<i64> {
        let mut worker_broadcasts = self.worker_broadcasts.lock().unwrap();
        worker_broadcasts.entry(worker.clone()).or_insert(HashSet::new()).clone()
    }

    /// Return an RDD of values from an RDD of (Long, Array[Byte]), with preservePartitions=true
    /// 
    /// This is useful to have the partitioner after partitionBy()
    fn value_of_pair(&self, pair: &PairRdd<i64, Vec<u8>>) -> Rdd<Vec<u8>> {
        let rdd = pair.rdd();
        let rdd = rdd.map(|it| it.1.clone());
        Rdd::from_rdd(rdd, true)
    }

    fn run_job(
        &self,
        sc: AtomicContext,
        rdd: Rdd<ByteArray>,
        partitions: JArrayList<Int>,
    ) -> Array<Any> {
        type ByteArray = Array<Byte>;
        type UnrolledPartition = Array<ByteArray>;
        let all_partitions: Array<UnrolledPartition> =
            sc.run_job(rdd, |x: Iterator<ByteArray>| x.to_array(), partitions.as_scala().to_seq());
        let flattened_partition: UnrolledPartition = Array::concat(all_partitions);
        self.serve_iterator(
            flattened_partition.to_iterator(),
            format!("serve RDD {} with partitions {}", rdd.id(), partitions.as_scala().to_seq())
        );
    }

    /// A helper function to create a local RDD iterator and serve it via socket. Partitions are
    /// are collected as separate jobs, by order of index. Partition data is first requested by a
    /// non-zero integer to start a collection job. The response is prefaced by an integer with 1
    /// meaning partition data will be served, 0 meaning the local iterator has been consumed,
    /// and -1 meaning an error occurred during collection. This function is used by
    /// rdd::_local_iterator_from_socket().
    /// ## Return
    /// 3-tuple with the port number of a local socket which serves the
    /// data collected from this job, the secret for authentication, and a socket auth
    /// server object that can be used to join the JVM serving thread in Python.
    /// 
    fn to_local_iterator_and_serve<T: Data>(
        rdd: Arc<dyn Rdd<Item = T>>,
        prefetch_partitions: bool,
    ) -> Result<(u16, String, SocketFuncServer), SerError> {
        fn handle_func<T: Data + 'static>(
            rdd: Arc<dyn Rdd<Item = T>>,
            prefetch_partitions: bool,
            mut sock: TcpStream,
            auth_helper: Option<AuthHelper>,
        ) -> Result<(), SerError> {
            let mut out = DataOutputStream::new(&mut sock);
            let mut in_stream = DataInputStream::new(&mut sock);
    
            // Collects a partition on each iteration
            let mut collect_partition_iter = (0..rdd.number_of_splits())
                .into_iter()
                .map(|i| {
                    let rdd = Arc::clone(&rdd);
                    rdd.context()
                        .submit_job(
                            rdd,
                            i,
                            |iter| iter.collect::<Vec<T>>(),
                            vec![],
                            vec![],
                            None,
                            None,
                        )
                        .map(move |res| res.unwrap())
                })
                .peekable();
    
            // Write data until iteration is complete, client stops iteration, or error occurs
            let mut complete = false;
            while !complete {
                // Read request for data, value of zero will stop iteration or non-zero to continue
                if in_stream.read_int()? == 0 {
                    complete = true;
                } else if collect_partition_iter.peek().is_some() {
                    // Client requested more data, attempt to collect the next partition
                    let partition_future = collect_partition_iter.next().unwrap();
                    // Cause the next job to be submitted if prefetchPartitions is enabled.
                    if prefetch_partitions {
                        collect_partition_iter.peek();
                    }
                    let partition_array = partition_future?;
    
                    // Send response there is a partition to read
                    out.write_int(1)?;
    
                    // Write the next object and signal end of data for this iteration
                    write_iterator_to_stream(partition_array.into_iter(), &mut out)?;
                    out.write_int(SpecialLengths::EndOfDataSection as i32)?;
                    out.flush()?;
                } else {
                    // Send response there are no more partitions to read and close
                    out.write_int(0)?;
                    complete = true;
                }
            }
            Ok(())
        }
    
        let auth_helper = None;
        let server = SocketFuncServer::new(
            auth_helper,
            "serve toLocalIterator".to_owned(),
            Arc::new(move |sock| {
                let rdd = Arc::clone(&rdd);
                let prefetch_partitions = prefetch_partitions;
                Box::new(move |sock| handle_func(rdd, prefetch_partitions, sock, auth_helper))
            }),
        )?;
        Ok((server.port, server.secret, server))
    }

    fn read_rdd_from_file(
        sc: &AtomicContext,
        filename: &str,
        parallelism: i32,
    ) -> Rdd {
        Rdd::read_rdd_from_file(sc, filename, parallelism)
    }
    
    // TODO: Fix here
    fn read_rdd_from_input_stream(
        sc: &AtomicContext, input_stream: InputStream, parallelism: i32,
    ) -> Rdd {
        Rdd::read_rdd_from_input_stream(sc, input_stream, parallelism)
    }
    
    fn setup_broadcast(path: &str) -> PythonBroadcast {
        PythonBroadcast::new(path)
    }

    fn write_iterator_to_stream<T>(iter: &mut Iterator<T>, data_out: &mut DataOutputStream) {

        fn write(obj: &Any) {
            match obj {
                &Null => data_out.write_int(SpecialLengths::Null),
                &Array(ref arr) => {
                    data_out.write_int(arr.len());
                    data_out.write(arr);
                },
                &String(ref str) => write_utf(str, data_out),
                &PortableDataStream(ref stream) => write(stream.to_array()),
                &(ref key, ref value) => {
                    write(key);
                    write(value);
                },
                &other => fail!("Unexpected element type {}", other.class)
            }
        }
    
        iter.each(write);
    }

    /// Create an RDD from a path using `SequenceFileInputFormat`, key and value class.
    /// A key and/or value converter class can optionally be passed in
    fn sequence_file<K: Data, V: Data, KC: Data, VC: Data>(
        &self,
        sc: &AtomicContext,
        path: &str,
        key_class_maybe_null: &Option<String>,
        value_class_maybe_null: &Option<String>,
        key_converter_class: &str,
        value_converter_class: &str,
        min_splits: i32,
        batch_size: i32
    ) -> Rdd<u8> {
        let key_class = key_class_maybe_null.as_ref().map_or(
            "org.apache.hadoop.io.Text".to_string(),
            |v| v.to_string());
        let value_class = value_class_maybe_null.as_ref().map_or(
            "org.apache.hadoop.io.Text".to_string(),
            |v| v.to_string());
        let kc = Utils::class_for_name(sc.env(), &key_class).unwrap();
        let vc = Utils::class_for_name(sc.env(), &value_class).unwrap();
        let rdd = sc.sc().sequence_file(path, kc, vc, min_splits);
        let conf_broadcasted = sc.sc().broadcast(SerializableConfiguration::new(sc.hadoop_configuration()));
        let converted = self.convert_rdd(
            rdd, key_converter_class, 
            value_converter_class,
            &WritableToConverter::new(conf_broadcasted)
        );
        Rdd::from_rdd(sc.env(), converted.map(|(k, v)| {
            let mut writer = Vec::new();
            k.write_external(&mut writer).unwrap();
            v.write_external(&mut writer).unwrap();
            writer
        }), &env.get())
    }

    /// Create an RDD from a file path, using an arbitrary `InputFormat`, key and value class.
    fn new_api_hadoop_file<K, V, F>(
        &self,
        sc: AtomicContext,
        path: &str,
        input_format_class: String,
        key_class: String,
        value_class: String,
        key_converter_class: String,
        value_converter_class: String,
        conf_as_map: HashMap<String, String>,
        batch_size: i32
    ) -> Rdd<[u8]> {
        let merged_conf = self.get_merged_conf(conf_as_map, sc.hadoop_configuration());
        let rdd = self.new_api_hadoop_rdd_from_class_names(
            sc, Some(path), input_format_class, 
            key_class, value_class, merged_conf
        );
        let conf_broadcasted = sc.sc.broadcast(SerializableConfiguration::new(merged_conf));
        let converted = self.convert_rdd(
            rdd, key_converter_class, value_converter_class,
            WritableToConverter::new(conf_broadcasted)
        );
      Rdd::from_rdd(serde_util::pair_rdd_to_python(converted, batch_size))
  }

    pub fn new_api_hadoop_rdd_from_class_names<K, V, F: NewInputFormat<K, V>>(
        sc: &AtomicContext,
        path: Option<String>,
        input_format_class: String,
        key_class: String,
        value_class: String,
        conf: &Configuration
    ) -> Result<RDD<(K, V)>> {
        let kc = utils::class_for_name::<K>(key_class);
        let vc = utils::class_for_name::<V>(value_class);
        let fc = utils::class_for_name::<F>(input_format_class);
        if path.is_some() {
            sc.sc.new_api_hadoop_file::<K, V, F>(path.unwrap(), fc, kc, vc, conf)
        } else {
            sc.sc.new_api_hadoop_rdd::<K, V, F>(conf, fc, kc, vc)
        }
    }

    /// Create an RDD from a `Configuration` converted from a map
    /// that is passed in from Python, using an arbitrary `InputFormat`, key and value class
    fn hadoop_rdd<K, V, F>(
        sc: &AtomicContext,
        input_format_class: &str,
        key_class: &str,
        value_class: &str,
        key_converter_class: &str,
        value_converter_class: &str,
        conf_as_map: HashMap<String, String>,
        batch_size: i64,
    ) -> Rdd<u8>
    where
        K: DeserializeOwned,
        V: DeserializeOwned,
        F: InputFormat<K, V>,
    {
        let conf = get_merged_conf(conf_as_map, sc.hadoop_configuration().clone());
        let rdd = hadoop_rdd_from_class_names::<K, V, F>(
            sc.clone(),
            None,
            input_format_class,
            key_class,
            value_class,
            conf,
        );
        let conf_broadcasted = sc.broadcast(SerializableConfiguration::new(conf));
        let converted = convert_rdd(
            rdd,
            key_converter_class,
            value_converter_class,
            WritableToConverter::new(conf_broadcasted),
        );
        Rdd::from_rdd(serde_util::pair_rdd_to_python(converted, batch_size))
    }

    fn hadoop_rdd_from_class_names<K, V, F, S, P>(
        sc: &AtomicContext,
        path: Option<String>,
        input_format_class: S,
        key_class: K,
        value_class: V,
        conf: P)
    where
        S: Into<String>,
        P: Into<Option<Configuration>>,
    {
        let _ = path
            .clone()
            .map(|x| sc.sc.hadoop_file(x, input_format_class, key_class, value_class));
        let _ = sc.sc.hadoop_rdd(conf, input_format_class, key_class, value_class);
    }

    fn write_utf(str: &str, data_out: &mut DataOutputStream) -> () {
        let bytes = str.as_bytes();
        data_out.write_int(bytes.len());
        data_out.write(bytes);
    }

    /// Create a socket server and a background thread to serve the data in `items`,
    ///  
    /// The socket server can only accept one connection, or close if no connection in 15 seconds.
    /// 
    /// Once a connection comes in, it tries to serialize all the data in `items`
    /// and send them into this connection.
    /// 
    /// The thread will terminate after all the data are sent or any exceptions happen.
    /// ## Returns:
    /// 3-tuple with the port number of a local socket which serves the
    /// data collected from this job, the secret for authentication, and a socket auth
    /// server object that can be used to join the JVM serving thread in Python.
    pub fn serve_iterator(
        &self,
        items: &mut dyn Iterator<Item = &dyn Any>, 
        thread_name: String
    ) -> Vec<Box<dyn Any>> {
        self.serve_to_stream(|out| self.write_iterator_to_stream(items, out), thread_name)
    }

    fn get_merged_conf(
        conf_as_map: &HashMap<String, String>,
        base_conf: &Configuration,
    ) -> Configuration {
        let conf = python_hadoop_util::map_to_conf(conf_as_map);
        python_hadoop_util::merge_confs(base_conf, &conf)
    }

    /// Convert an RDD of key-value pairs from internal types to serializable types suitable for
    /// output, or vice versa.
    fn convert_rdd<K, V>(
        rdd: RDD<(K, V)>,
            key_converter_class: String,
            value_converter_class: String,
            default_converter: Converter<Any, Any>) -> RDD<(Any, Any
        )> {
        let (kc, vc) = get_key_value_converters<K, V, Any, Any>(
            key_converter_class, value_converter_class, default_converter
        )
        PythonHadoopUtil::convert_rdd(rdd, kc, vc)
    }

    /// Output a Python RDD of key-value pairs as a Hadoop SequenceFile using the Writable types
    /// we convert from the RDD's key and value types. The `path` can be on any Hadoop file system.
    fn save_as_sequence_file<C: CompressionCodec>(
        &self,
        py_rdd: Rdd<Vec<u8>>,
        batch_serialized: bool,
        path: &str,
        compression_codec_class: &str,
    ) {
        self.save_as_hadoop_file(
            py_rdd,
            batch_serialized,
            path,
            "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            None,
            None,
            None,
            None,
            HashMap::new(),
            compression_codec_class,
        );
    }

    /// Output a Python RDD of key-value pairs to any Hadoop file system, using old Hadoop
    /// `OutputFormat` in mapred package. Keys and values are converted to suitable output
    /// types using either user specified converters or, if not specified. 
    /// Post-conversion types `keyClass` and `valueClass` are automatically inferred if not specified. 
    /// The passed-in `confAsMap` is merged with the default Hadoop conf associated with the AtomicContext of
    /// this RDD.
    fn save_as_hadoop_file<F, C>(
        &self,
        py_rdd: Rdd<array::Array<u8>>,
        batch_serialized: bool,
        path: String,
        output_format_class: String,
        key_class: String,
        value_class: String,
        key_converter_class: Option<String>,
        value_converter_class: Option<String>,
        conf_as_map: HashMap<String, String>,
        compression_codec_class: String)
    where
      F: OutputFormat,
      C: CompressionCodec
    {
        let rdd = SerDeUtil.python_to_pair_rdd(py_rdd, batch_serialized);
        let (kc, vc) = get_key_value_types(key_class, value_class).unwrap_or_else(
            || infer_key_value_types(rdd, key_converter_class, value_converter_class)
        );
        let merged_conf = self.get_merged_conf(conf_as_map, py_rdd.context.hadoop_configuration());
        let codec = compression_codec_class.map(|c| c as Class<C>);
        let converted = self.convert_rdd(
            rdd, key_converter_class, value_converter_class, ToWritableConverter::new()
        );
        let fc = output_format_class.class_for_name<F>();
        converted.save_as_hadoop_file(path, kc, vc, fc, JobConf::new(merged_conf), codec = codec);
    }

    /// Output a Python RDD of key-value pairs to any Hadoop file system, using a Hadoop conf
    /// converted from the passed-in `confAsMap`. The conf should set relevant output params (
    /// e.g., output path, output format, etc), in the same way as it would be configured for
    /// a Hadoop MapReduce job. Both old and new Hadoop OutputFormat APIs are supported
    /// (mapred vs. mapreduce). Keys/values are converted for output using either user specified
    /// converters or, by default.
    fn save_as_hadoop_dataset(
        &self,
        py_rdd: Rdd<u8>,
        batch_serialized: bool,
        conf_as_map: HashMap<String, String>,
        key_converter_class: String,
        value_converter_class: String,
        use_new_api: bool,
    ) -> PyResult<()> {
        let conf = get_merged_conf(conf_as_map, py_rdd.context.hadoop_configuration());
        let converted = convert_rdd(
            serde_util::python_to_pair_rdd(py_rdd, batch_serialized),
            key_converter_class,
            value_converter_class,
            ToWritableConverter {},
        );
        if use_new_api {
            converted.save_as_new_api_hadoop_dataset(conf)
        } else {
            converted.save_as_hadoop_dataset(JobConf::new(conf))
        }
    }

}


#[derive(Clone)]
struct PythonAccumulatorV2 {
    server_host: String,
    server_port: i32,
    secret_token: String,
    socket: Option<Socket>,
}

impl PythonAccumulatorV2 {
    fn new(server_host: String, server_port: i32, secret_token: String) -> PythonAccumulatorV2 {
        PythonAccumulatorV2 {
            server_host,
            server_port,
            secret_token,
            socket: None,
        }
    }

    fn open_socket(&self) -> Result<Socket> {
        if self.socket.is_none() {
            let socket = Socket::new(self.server_host.clone(), self.server_port)?;
            socket.write_all(self.secret_token.as_bytes())?;
            socket.flush()?;
            return Ok(socket);
        }
        Ok(self.socket.clone().unwrap())
    }

    fn copy_and_reset(&self) -> PythonAccumulatorV2 {
        PythonAccumulatorV2::new(
            self.server_host.clone(),
            self.server_port,
            self.secret_token.clone(),
        )
    }

    fn merge(&mut self, other: &PythonAccumulatorV2) -> Result<()> {
        let socket = self.open_socket()?;
        let mut reader = socket.reader()?;
        let mut writer = socket.writer()?;
        let values = other.value();
        writer.write_i32::<BigEndian>(values.len() as i32)?;
        for array in values {
            writer.write_i32::<BigEndian>(array.len() as i32)?;
            writer.write_all(array)?;
        }
        writer.flush()?;
        let mut byte_read = [0u8];
        reader.read_exact(&mut byte_read)?;
        if byte_read[0] == 0 {
            return Err("EOF reached before Python server acknowledged".into());
        }
        Ok(())
    }
}


#[derive(Serialize, Deserialize)]
struct PythonBroadcast {
    path: String,
    broadcast_id: i64,
    encryption_server: SocketAuthServer,
    decryption_server: SocketAuthServer,
}

impl PythonBroadcast {
    /// Write data into disk and map it to a broadcast block.
    fn write_object(&mut self, out: &mut ObjectStream) -> Result<(), Box<dyn Error>> {
        out.write_long(self.broadcast_id)?;
        let mut in_ = FileInputStream::new(&self.path)?;
        copy_stream(&mut in_, out)?;
        Ok(())
    }
    
    /// Read data from disks, then copy it to `out` 
    fn read_object(&mut self, in_: &mut ObjectStream) -> Result<(), Box<dyn Error>> {
        self.broadcast_id = in_.read_long()?;
        let block_id = BroadcastBlockId(self.broadcast_id, "python");
        let block_manager = AtomicEnv::get().block_manager();
        let disk_block_manager = block_manager.disk_block_manager();
        if !disk_block_manager.contains_block(&block_id) {
            let dir = File::new(&get_local_dir(AtomicEnv::get().conf()));
            let mut file = dir.create_temp_file("broadcast", "")?;
            let out = FileOutputStream::new(&mut file)?;
            let size = copy_stream(in_, &mut out)?;
            let ct = ClassTag::of::<Object>();
            let block_store_updater = block_manager.temp_file_based_block_store_updater(
                block_id,
                StorageLevel::DISK_ONLY,
                ct,
                file,
                size,
            );
            block_store_updater.save()?;
        }
        self.path = disk_block_manager.get_file(&block_id).unwrap().to_string();
        Ok(())
    }

    fn set_broadcast_id(&mut self, bid: i64) {
        self.broadcast_id = bid;
    }

    fn setup_encryption_server(&mut self) -> Result<Vec<Any>, Box<dyn Error>> {
        self.encryption_server = SocketAuthServer::new("broadcast-encrypt-server")?;
        Ok(vec![
            Any::Long(self.encryption_server.port()),
            Any::String(self.encryption_server.secret()),
        ])
    }

    fn setup_decryption_server(&mut self) -> Result<Vec<Any>, Box<dyn Error>> {
        self.decryption_server = SocketAuthServer::new("broadcast-decrypt-server-for-driver")?;
        Ok(vec![
            Any::Long(self.decryption_server.port()),
            Any::String(self.decryption_server.secret()),
        ])
    }

    fn wait_till_broadcast_data_sent(&mut self) -> Result<(), Box<dyn Error>> {
        self.decryption_server.get_result()?;
        Ok(())
    }

    fn wait_till_data_received(&mut self) -> Result<(), Box<dyn Error>> {
        self.encryption_server.get_result()?;
        Ok(())
    }
}


use std::io::{BufRead, Read, BufReader};

/// The inverse of ChunkedStream for sending data of unknown size.
/// 
/// We might be serializing a really large object from python -- we don't want
/// python to buffer the whole thing in memory, nor can it write to a file,
/// so we don't know the length in advance.  So python writes it in chunks, each chunk
/// preceded by a length, till we get a "length" of -1 which serves as EOF.
/// 
pub struct DechunkedInputStream {
    reader: BufReader<Box<dyn Read>>,
    remaining_in_chunk: i32,
}

impl DechunkedInputStream {
    pub fn new(wrapped: Box<dyn Read>) -> Self {
        let mut reader = BufReader::new(wrapped);
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf).unwrap();
        let remaining_in_chunk = i32::from_be_bytes(buf);
        DechunkedInputStream {
            reader,
            remaining_in_chunk,
        }
    }
}

impl Read for DechunkedInputStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining_in_chunk == -1 {
            return Ok(0);
        }
        let mut dest_space = buf.len();
        let mut dest_pos = 0;
        while dest_space > 0 && self.remaining_in_chunk != -1 {
            let to_copy = std::cmp::min(self.remaining_in_chunk as usize, dest_space);
            let read = self.reader.read(&mut buf[dest_pos..(dest_pos + to_copy)])?;
            dest_pos += read;
            dest_space -= read;
            self.remaining_in_chunk -= read as i32;
            if self.remaining_in_chunk == 0 {
                let mut buf = [0u8; 4];
                self.reader.read_exact(&mut buf).unwrap();
                self.remaining_in_chunk = i32::from_be_bytes(buf);
            }
        }
        assert!(dest_space == 0 || self.remaining_in_chunk == -1);
        Ok(dest_pos)
    }
}

use std::any::Any;
use std::option::Option;

use atomic::PairRdd;
use atomic_api::ctx::Rdd;

struct Converter<K, KK> {
    _k: K,
    _kk: KK,
}

impl<K, KK> Converter<K, KK> {
    fn get_instance(option: Option<String>,
                    default_converter: Converter<K, KK>) -> Converter<K, KK> {
        match option {
            Some(_) => default_converter,
            None => default_converter,
        }
    }
}

fn get_key_value_converters<K, V, KK, VV>(
    key_converter_class: String,
    value_converter_class: String,
    default_converter: Box<Any>) -> (Converter<K, KK>, Converter<V, VV>) {
    let key_converter = Converter::<K, KK>::get_instance(Some(key_converter_class), default_converter);
    let value_converter = Converter::<V, VV>::get_instance(Some(value_converter_class), default_converter);
    (key_converter, value_converter)
}

fn infer_key_value_types<K, V, KK, VV>(
    rdd: Arc<dyn Rdd<Item = (K, V)>>,
    key_converter_class: Option<String>,
    value_converter_class: Option<String>,
) -> (Class<KK>, Class<VV>)
where
    K: Data,
    V: Data,
    KK: Data,
    VV: Data,
{
    // Peek at an element to figure out key/value types. Since Writables are not serializable,
    // we cannot call first() on the converted RDD. Instead, we call first() on the original RDD
    // and then convert locally.
    let (key, value) = rdd.first().unwrap();
    let (key_converter, value_converter) = get_key_value_converters(
        key_converter_class,
        value_converter_class,
        ToWritableConverter::new(),
    );
    (
        key_converter.convert(key).unwrap().class_tag(),
        value_converter.convert(value).unwrap().class_tag(),
    )
}

fn get_key_value_types<K, V>(key_class: String, value_class: String) -> Option<(K, V)> {
    let key = match key_class {
        None => None,
        Some(k) => Some(k),
    };
    let value = match value_class {
        None => None,
        Some(v) => Some(v),
    };
    key.and_then(|k| value.and_then(|v| Some((k, v))))
}

/// Sends decrypted broadcast data to python worker
#[derive(Debug)]
pub struct EncryptedPythonBroadcastServer {
    env: AtomicEnv,
    ids_and_files: Vec<(i64, String)>,
}

impl EncryptedPythonBroadcastServer {
    fn handle_connection(&self, mut socket: TcpStream) -> Result<()> {
        let mut out = BufWriter::new(&socket);
        let mut socket_in: BufReader<&TcpStream> = BufReader::new(&socket);
        // send the broadcast id, then the decrypted data.  We don't need to send the length, the
        // the python pickle module just needs a stream.
        for &(id, ref path) in self.ids_and_files.iter() {
            out.write_i64::<BigEndian>(id)?;
            let mut in_ = self.env.serializer_manager.wrap_for_encryption(
                File::open(path).map_err(|e| Error::new(ErrorKind::Other, e))?
            );
            io::copy(&mut in_, &mut out)?;
        }
        log_trace!("waiting for python to accept broadcast data over socket");
        out.flush()?;
        let mut buf = [0; 1];
        socket_in.read(&mut buf)?;
        log_trace!("done serving broadcast data");
        Ok(())
    }

    fn wait_till_broadcast_data_sent(&self) -> Result<()> {
        self.get_result()
    }
}


pub struct PythonRDDServer {
    socket_auth_server: SocketAuthServer<Rdd<[u8]>>,
}

impl PythonRDDServer {
    pub fn new() -> PythonRDDServer {
        PythonRDDServer {
            socket_auth_server: SocketAuthServer::new("parallelize-server"),
        }
    }

    fn handle_connection(&self, sock: Socket) -> Rdd<[u8]> {
        let in_stream = sock.input_stream();
        let dechunked_input_stream = DechunkedInputStream::new(in_stream);
        self.stream_to_rdd(dechunked_input_stream)
    }

    fn stream_to_rdd(&self, input_stream: DechunkedInputStream) -> Rdd<[u8]> {
        panic!("unimplemented")
    }
}

pub struct PythonParallelizeServer;

impl PythonParallelizeServer {
    pub fn streamToRDD(input: InputStream) -> RDD<ByteArray> {
        PythonRDD.readRDDFromInputStream(sc, input, parallelism)
    }
}

pub struct PythonPartitioner {
    num_partitions: i32,
    py_partition_function_id: i64,
}

/// A `Partitioner` that performs handling of long-valued keys, for use by the
/// Python API.
/// 
/// Stores the unique id() of the Python-side partitioning function so that it is incorporated into
/// equality comparisons. Correctness requires that the id is a unique identifier for the
/// lifetime of the program (i.e. that it is not re-used as the id of a different partitioning
/// function). This can be ensured by using the Python id() function and maintaining a reference
/// to the Python partitioning function so that its id() is not reused.
impl PythonPartitioner {
    pub fn new(num_partitions: i32, py_partition_function_id: i64) -> Self {
        PythonPartitioner {
            num_partitions,
            py_partition_function_id,
        }
    }
}

impl Partitioner for PythonPartitioner {
    fn num_partitions(&self) -> i32 {
        self.num_partitions
    }

    fn get_partition(&self, key: &Any) -> i32 {
        match key {
            Any::Null => 0,
            Any::Long(key) => non_negative_mod(*key as i32, self.num_partitions),
            _ => non_negative_mod(key.hash_code() as i32, self.num_partitions),
        }
    }

    fn equals(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<Self>() {
            self.num_partitions == other.num_partitions
                && self.py_partition_function_id == other.py_partition_function_id
        } else {
            false
        }
    }

    fn hash_code(&self) -> i32 {
        31 * self.num_partitions + self.py_partition_function_id as i32
    }
}