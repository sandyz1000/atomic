use std::thread;
use std::time::Duration;
use atomic_api::utils::TaskContext;


const END_OF_DATA_SECTION: i64 = -1;
const PYTHON_EXCEPTION_THROWN: i64 = -2;
const TIMING_DATA: i64 = -3;
const END_OF_STREAM: i64 = -4;
const NULL: i64 = -5;
const START_ARROW_STREAM: i64 = -6;

const BARRIER_FUNCTION: i64 = 1;
const ALL_GATHER_FUNCTION: i64 = 2;
const BARRIER_RESULT_SUCCESS: &str = "success";
const ERROR_UNRECOGNIZED_FUNCTION: &str = "Not recognized function call from python side.";


#[derive(Debug)]
pub enum PythonEvalType {
    NonUdf = 0,
    SqlBatchedUdf = 100,
    SqlScalarPandasUdf = 200,
    SqlGroupedMapPandasUdf = 201,
    SqlGroupedAggPandasUdf = 202,
    SqlWindowAggPandasUdf = 203,
    SqlScalarPandasIterUdf = 204,
    SqlMapPandasIterUdf = 205,
    SqlCogroupedMapPandasUdf = 206,
    SqlMapArrowIterUdf = 207,
    SqlGroupedMapPandasUdfWithState = 208,
}

impl PythonEvalType {
    fn to_string(&self) -> String {
        match self {
            PythonEvalType::NonUdf => "NonUdf",
            PythonEvalType::SqlBatchedUdf => "SqlBatchedUdf",
            PythonEvalType::SqlScalarPandasUdf => "SqlScalarPandasUdf",
            PythonEvalType::SqlGroupedMapPandasUdf => "SqlGroupedMapPandasUdf",
            PythonEvalType::SqlGroupedAggPandasUdf => "SqlGroupedAggPandasUdf",
            PythonEvalType::SqlWindowAggPandasUdf => "SqlWindowAggPandasUdf",
            PythonEvalType::SqlScalarPandasIterUdf => "SqlScalarPandasIterUdf",
            PythonEvalType::SqlMapPandasIterUdf => "SqlMapPandasIterUdf",
            PythonEvalType::SqlCogroupedMapPandasUdf => "SqlCogroupedMapPandasUdf",
            PythonEvalType::SqlMapArrowIterUdf => "SqlMapArrowIterUdf",
            PythonEvalType::SqlGroupedMapPandasUdfWithState => "SqlGroupedMapPandasUdfWithState",
        }
        .to_string()
    }
}

/// A helper class to run Python mapPartition/UDFs.
/// funcs is a list of independent Python functions, each one of them is a list of chained Python
/// functions (from bottom to top).
#[derive(Debug)]
pub struct BasePythonRunner {
    funcs: Vec<ChainedPythonFunctions>,
    eval_type: i32,
    arg_offsets: Vec<Vec<i32>>,
    buffer_size: i32,
    reuse_worker: bool,
    fault_handler_enabled: bool,
    env_vars: HashMap<String, String>,
    python_exec: String,
    python_ver: String,
    accumulator: Option<PythonAccumulatorV2>,
    maybe_accumulator: Option<PythonAccumulatorV2>,
    server_socket: Option<ServerSocket>,
    auth_helper: Option<SocketAuthHelper>
}

impl BasePythonRunner {
    /// each python worker gets an equal part of the allocation. the worker pool will grow to the
    /// number of concurrent tasks, which is determined by the number of cores in this executor.
    fn get_worker_memory_mb(mem: Option<i64>, cores: i32) -> Option<i64> {
        mem.map(|x| x / cores as i64)
    }

    fn compute<'a, IN, OUT>(
        input_iterator: &'a mut dyn Iterator<Item = IN>,
        partition_index: i32,
        context: &'a TaskContext,
    ) -> Box<dyn Iterator<Item = OUT> + 'a> {
        let start_time = System::now().elapsed().as_millis();
        let env = AtomicEnv::get();
        // Get the executor cores and memory, they are passed via the local properties when
        // the user specified them in a ResourceProfile.
        let exec_cores_prop = context.get_local_property(EXECUTOR_CORES_LOCAL_PROPERTY);
        let memory_mb = context
            .get_local_property(MEMORY_LOCAL_PROPERTY)
            .map(|s| s.parse::<i64>().unwrap());
        let local_dir = env
            .block_manager
            .disk_block_manager
            .local_dirs
            .iter()
            .map(|f| f.path().to_string_lossy().to_string())
            .collect::<Vec<String>>()
            .join(",");
        // If OMP_NUM_THREADS is not explicitly set, override it with the number of task cpus.
        if conf.get_option("executorEnv.OMP_NUM_THREADS").is_none() {
            env_vars.insert(
                "OMP_NUM_THREADS".to_string(),
                conf.get("task.cpus", "1").to_string(),
            );
        }
        env_vars.insert("LOCAL_DIRS".to_string(), local_dir.clone()); // it's also used in monitor thread
        if reuse_worker {
            env_vars.insert("REUSE_WORKER".to_string(), "1".to_string());
        }
        if simplified_traceback {
            env_vars.insert("SIMPLIFIED_TRACEBACK".to_string(), "1".to_string());
        }
        // this could be wrong with standalone mode when executor
        // cores might not be correct because it defaults to all cores on the box.
        let exec_cores = exec_cores_prop
            .map(|s| s.parse::<i32>().unwrap())
            .unwrap_or(conf.get(EXECUTOR_CORES));
        let worker_memory_mb = get_worker_memory_mb(memory_mb, exec_cores);
        if let Some(memory_mb) = worker_memory_mb {
            env_vars.insert(
                "PYEXECUTOR_MEMORY_MB".to_string(),
                memory_mb.to_string(),
            );
        }
        env_vars.insert("AUTH_SOCKET_TIMEOUT".to_string(), auth_socket_timeout.to_string());
        env_vars.insert("BUFFER_SIZE".to_string(), buffer_size.to_string());
        if fault_handler_enabled {
            env_vars.insert(
                "PYTHON_FAULTHANDLER_DIR".to_string(),
                BasePythonRunner.faultHandlerLogDir.to_string(),
            );
        }

        let (worker, pid) = env.create_python_worker(python_exec, env_vars);
        // Whether is the worker released into idle pool or closed. When any codes try to release or
        // close a worker, they should use `released_or_closed.compare_and_swap` to flip the state to make
        // sure there is only one winner that is going to release or close the worker.
        let released_or_closed = AtomicBool::new(false);

        // Start a thread to feed the process input from our parent's iterator
        let writer_thread = new_writer_thread(env, worker, input_iterator, partition_index, context);
        
        context.add_task_completion_listener(|| {
            writer_thread.shutdown_on_task_completion();
            if !reuse_worker || released_or_closed.compare_and_set(false, true) {
                if let Err(e) = worker.close() {
                    log_warning(format!("Failed to close worker socket: {:?}", e));
                }
            }
        });
        
        writer_thread.start();
        let env = _env.get();
        let writer_monitor_thread = WriterMonitorThread::new(env, worker, writer_thread, context);
        writer_monitor_thread.start();
        
        if reuse_worker {
            let key = (worker, context.task_attempt_id());
            // Avoid creating multiple monitor threads for the same python worker and task context
            if PythonRunner::running_monitor_threads().insert(key) {
                let monitor_thread = MonitorThread::new(env, worker, context);
                monitor_thread.start();
            }
        } else {
            let monitor_thread = MonitorThread::new(env, worker, context);
            monitor_thread.start();
        }
        
        // Return an iterator that reads lines from the process's stdout
        let stream = DataInputStream::new(
            BufferedInputStream::new(worker.get_input_stream(), buffer_size),
        );
        
        let stdout_iterator = new_reader_iterator(
            stream, writer_thread, start_time, env, worker, pid, released_or_closed, context,
        );
        
        InterruptibleIterator::new(context, stdout_iterator)
    }

    pub fn new_writer_thread<'a, T: DataOutput + 'a>(
        env: &'a AtomicEnv,
        worker: TcpStream,
        input_iterator: Box<Iterator<Item = T>>,
        partition_index: i32,
        context: &'a TaskContext) -> WriterThread<'a, T> {
    
    }
    
    pub fn new_reader_iterator<'a, T: DataInput + 'a, U: DataOutput + 'a>(
        stream: &'a mut T,
        writer_thread: &'a mut WriterThread<'a, U>,
        start_time: u64,
        env: &'a AtomicEnv,
        worker: TcpStream,
        pid: Option<i32>,
        released_or_closed: &'a AtomicBool,
        context: &'a TaskContext) -> ReaderIterator<'a, T> {
    
    }
}

#[derive(Debug)]
pub struct WriterThread<IN: Data> {
    env: AtomicEnv,
    worker: Socket,
    input_iterator: Iterator<IN>,
    partition_index: i32,
    context: TaskContext,
    exception: Option<Throwable>
}

impl WriterThread {
    pub fn new(
        env: AtomicEnv, 
        worker: Socket, 
        input_iterator: Iterator<IN>, 
        partition_index: i32, 
        context: TaskContext, exception: Option<Throwable>
    ) -> WriterThread {
        WriterThread {
            env,
            worker,
            input_iterator,
            partition_index,
            context,
            exception
        }
    }

    // Terminates the writer thread and waits for it to exit, ignoring any exceptions that may occur
    // due to cleanup.
    pub fn shutdown_on_task_completion(&self) {
        assert!(self.context.is_completed());
        self.interrupt();
        self.join();
    }

    fn write_command(&self, data_out: &mut dyn Write) -> Result<(), Error> {
        unimplemented!()
    }

    fn write_iterator_to_stream(&self, data_out: &mut dyn Write) -> Result<(), Error> {
        unimplemented!()
    }

    fn run(&self) -> Result<(), Box<dyn Error>> {
        Utils::log_uncaught_exceptions(|| {
            let mut server_socket: Option<ServerSocket> = None;
    
            if let Some(ctxt) = self.context.as_ref() {
                TaskContext::set_task_context(ctxt.clone());
                let stream = BufferedOutputStream::new(self.worker.get_output_stream(), self.buffer_size);
                let mut data_out = DataOutputStream::new(stream);
                // Partition index
                data_out.write_i32(self.partition_index)?;
                // Python version of driver
                PythonRDD::write_utf(self.python_ver, &mut data_out)?;
                // Init a ServerSocket to accept method calls from Python side.
                let is_barrier = ctxt.is_barrier();
                if is_barrier {
                    server_socket = Some(ServerSocket::new(
                        /* port */ 0,
                        /* backlog */ 1,
                        InetAddress::get_by_name("localhost"),
                    )?);
                    // A call to accept() for ServerSocket shall block infinitely.
                    server_socket
                        .as_ref()
                        .map(|s| s.set_so_timeout(0))
                        .ok_or_else(|| "Failed to set SO_TIMEOUT on ServerSocket")?;
                    let thread_server = server_socket.clone().ok_or_else(|| "Server socket is None")?;
                    thread::spawn(move || {
                        while !thread_server.is_closed() {
                            let mut sock: Option<Socket> = None;
                            match thread_server.accept() {
                                Ok(s) => sock = Some(s),
                                Err(e) => {
                                    if e.to_string().contains("Socket closed") {
                                        // It is possible that the ServerSocket is not closed, but the native socket
                                        // has already been closed, we shall catch and silently ignore this case.
                                        continue;
                                    } else {
                                        return Err(e.into());
                                    }
                                }
                            };
                            if let Some(mut s) = sock {
                                // Wait for function call from python side.
                                s.set_so_timeout(10000)?;
                                auth_helper.auth_client(&mut s)?;
                                let mut input = DataInputStream::new(s.get_input_stream());
                                let request_method = input.read_i32()?;
                                // The BarrierTaskContext function may wait infinitely, socket shall not timeout
                                // before the function finishes.
                                s.set_so_timeout(0)?;
                                match request_method {
                                    BarrierTaskContextMessageProtocol::BARRIER_FUNCTION => {
                                        barrier_and_serve(request_method, &mut s)?;
                                    }
                                    BarrierTaskContextMessageProtocol::ALL_GATHER_FUNCTION => {
                                        let length = input.read_i32()?;
                                        let mut message = vec![0; length as usize];
                                        input.read_exact(&mut message)?;
                                        barrier_and_serve(
                                            request_method,
                                            &mut s,
                                            Some(String::from_utf8(message)?),
                                        )?;
                                    }
                                    _ => {
                                        let mut out = DataOutputStream::new(BufferedOutputStream::new(
                                            s.get_output_stream(),
                                        ));
                                        PythonRDD::write_utf(
                                            BarrierTaskContextMessageProtocol::ERROR_UNRECOGNIZED_FUNCTION,
                                            &mut out,
                                        )?;
                                    }
                                };
                                s.close()?;
                            }
                        }
                        Ok(())
                    });
                }
                let secret = if is_barrier {
                    auth_helper.secret()
                } else {
                    String::new()
                };
                // Close ServerSocket on task completion.
                if let Some(s) = server_socket.as_ref() {
                    ctxt.add_task_completion_listener(|_| s.close().unwrap());
                }

                // TODO: Fix rest of the code

                // let bound_port = server_socket
                //     .map
            }
        })
    }    

    pub fn barrier_and_serve(request_method: i32, sock: &mut TcpStream, message: String) -> () {
        assert!(server_socket.is_some(), "No available ServerSocket to redirect the BarrierTaskContext method call.");
        let mut out = DataOutputStream::new(BufferedOutputStream::new(sock));
        let messages = match request_method {
            BarrierTaskContextMessageProtocol::BARRIER_FUNCTION => {
                context.barrier();
                vec![BarrierTaskContextMessageProtocol::BARRIER_RESULT_SUCCESS]
            },
            BarrierTaskContextMessageProtocol::ALL_GATHER_FUNCTION => {
                context.all_gather(message);
            },
        };
        out.write_i32(messages.len());
        for message in messages {
            write_utf(message, &mut out);
        }
    }
    
    pub fn write_utf(str: String, data_out: &mut DataOutputStream) -> () {
        let bytes = str.as_bytes();
        data_out.write_i32(bytes.len());
        data_out.write(bytes);
    }

}

#[derive(Debug)]
pub struct ReaderIterator<OUT: Debug + Clone> {
    stream: DataInputStream,
    writer_thread: WriterThread,
    start_time: u64,
    env: AtomicEnv,
    worker: Socket,
    pid: Option<i32>,
    released_or_closed: Arc<AtomicBool>,
    context: TaskContext,
    next_obj: Option<OUT>,
    eos: bool,
}

impl<OUT: Debug + Clone> Iterator for ReaderIterator<OUT> {
    type Item = OUT;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(obj) = self.next_obj.clone() {
            self.next_obj = None;
            return Some(obj);
        } else if !self.eos {
            let obj = self.read();
            if obj.is_some() {
                return obj;
            } else {
                self.eos = true;
                return None;
            }
        } else {
            return None;
        }
    }
}

impl<OUT: Debug + Clone> ReaderIterator<OUT> {
    fn new(
        stream: DataInputStream,
        writerThread: WriterThread,
        startTime: u64,
        env: AtomicEnv,
        worker: Socket,
        pid: Option<i32>,
        releasedOrClosed: Arc<AtomicBool>,
        context: TaskContext,
    ) -> ReaderIterator<OUT> {
        ReaderIterator {
            stream,
            writer_thread: writerThread,
            start_time: startTime,
            env,
            worker,
            pid,
            released_or_closed: releasedOrClosed,
            context,
            next_obj: None,
            eos: false,
        }
    }

    fn hasNext(&mut self) -> bool {
        if self.next_obj.is_some() {
            return true;
        } else if !self.eos {
            self.next_obj = self.read();
            self.hasNext()
        } else {
            false
        }
    }

    fn read(&mut self) -> Option<OUT> {}

    fn handle_timing_data(&mut self, stream: &mut BufReader<TcpStream>) -> Result<()> {
        let boot_time = stream.read_i64::<BigEndian>()?;
        let init_time = stream.read_i64::<BigEndian>()?;
        let finish_time = stream.read_i64::<BigEndian>()?;
        let boot = boot_time - self.start_time;
        let init = init_time - boot_time;
        let finish = finish_time - init_time;
        let total = finish_time - self.start_time;
        log::info!("Times: total = {}, boot = {}, init = {}, finish = {}",
            total, boot, init, finish);
        let memory_bytes_spilled = stream.read_i64::<BigEndian>()?;
        let disk_bytes_spilled = stream.read_i64::<BigEndian>()?;
        self.context.task_metrics.inc_memory_bytes_spilled(memory_bytes_spilled);
        self.context.task_metrics.inc_disk_bytes_spilled(disk_bytes_spilled);
        Ok(())
    }

    fn handle_python_exception(&mut self, stream: &mut BufReader<TcpStream>) -> Result<PythonException> {
        // Signals that an exception has been thrown in python
        let ex_length = stream.read_i32::<BigEndian>()?;
        let mut obj = vec![0u8; ex_length as usize];
        stream.read_exact(&mut obj)?;
        Ok(PythonException::new(
            String::from_utf8(obj).unwrap(),
            self.writer_thread.exception.take()
        ))
    }
}

#[derive(Debug)]
pub struct MonitorThread {
    env: AtomicEnv,
    worker: Socket,
    context: TaskContext,
}

impl MonitorThread {
    fn new(env: AtomicEnv, worker: Socket, context: TaskContext) -> MonitorThread {
        MonitorThread {
            env,
            worker,
            context,
        }
    }

    fn monitor_worker(&self) -> Result<(), Error> {
        let mut context = Context {
            is_interrupted: false,
            is_completed: false,
            partition_id: 1,
            attempt_number: 1,
            stage_id: 1,
            task_attempt_id: 1
        };
        let env = Env {};
        let python_exec = "python";
        let env_vars = vec!["a", "b", "c"];
        let worker = Worker {};
    
        let task_kill_timeout = 5;
    
        while !context.is_interrupted && !context.is_completed {
            thread::sleep(Duration::from_secs(2));
        }
        if !context.is_completed {
            thread::sleep(Duration::from_secs(task_kill_timeout));
            if !context.is_completed {
                let task_name = format!("{}.{} in stage {} (TID {})",
                                        context.partition_id,
                                        context.attempt_number,
                                        context.stage_id,
                                        context.task_attempt_id);
                println!("Incomplete task {} interrupted: Attempting to kill Python Worker", task_name);
                env.destroy_python_worker(python_exec, env_vars, worker);
            }
        }
    }

    fn run(&self) {
        loop {
            if self.reuse_worker {
                match PythonRunner::running_monitor_threads.remove(&(self.worker, self.context.task_attempt_id)) {
                    Some(_) => break,
                    _ => {}
                }
            }
            self.monitor_worker();
        }
    }
}

#[derive(Debug)]
pub struct Context {
    is_interrupted: bool,
    is_completed: bool,
    partition_id: i32,
    attempt_number: i32,
    stage_id: i32,
    task_attempt_id: i32
}

struct Env {}

impl Env {
    fn destroy_python_worker(&self, python_exec: &str, env_vars: Vec<&str>, worker: Worker) {
        println!("destroy_python_worker: {} {:?} {:?}", python_exec, env_vars, worker);
    }
}

#[derive(Debug)]
pub struct WriterMonitorThread {
    env: AtomicEnv,
    worker: Socket,
    writer_thread: WriterThread,
    context: TaskContext,
}

impl WriterMonitorThread {
    fn new(
        env: AtomicEnv,
        worker: Socket,
        writer_thread: WriterThread,
        context: TaskContext,
    ) -> WriterMonitorThread {
        WriterMonitorThread {
            env,
            worker,
            writer_thread,
            context,
        }
    }
}

impl Thread for WriterMonitorThread {
    fn run(&self) {
        // Wait until the task is completed (or the writer thread exits, in which case this thread has
        // nothing to do).
        while !context.is_completed() && writer_thread.is_alive() {
            Thread::sleep(2000);
        }
        if writer_thread.is_alive() {
            Thread::sleep(task_kill_timeout);
            // If the writer thread continues running, this indicates a deadlock. Kill the worker to
            // resolve the deadlock.
            if writer_thread.is_alive() {
                try {
                    // Mimic the task name used in `Executor` to help the user find out the task to blame.
                    let task_name = format!(
                        "{}.{} in stage {} (TID {})",
                        context.partition_id(),
                        context.attempt_number(),
                        context.stage_id(),
                        context.task_attempt_id(),
                    );
                    log_warning(
                        format!(
                            "Detected deadlock while completing task {}: Attempting to kill Python Worker",
                            task_name,
                        )
                    );
                    env.destroy_python_worker(python_exec, env_vars.as_scala().to_map(), worker);
                } catch {
                    case e: Exception =>
                    log_error(format!("Exception when trying to kill worker: {}", e));
                }
            }
        }
    }
}


#[derive(Debug, Clone)]
pub struct PythonRunner {
    funcs: Vec<ChainedPythonFunctions>,
}

impl PythonRunner {
    fn new(funcs: Vec<ChainedPythonFunctions>) -> PythonRunner {
        PythonRunner {
            funcs: funcs,
        }
    }

    fn new_writer_thread(
        &self,
        env: AtomicEnv,
        worker: Socket,
        input_iterator: Iterator<Vec<u8>>,
        partition_index: i32,
        context: TaskContext,
    ) -> WriterThread {
        WriterThread {
            env,
            worker,
            input_iterator,
            partition_index,
            context,
            exception: todo!()
        }
    }
}

#[derive(Debug, Clone)]
pub struct WriterThread {
    env: AtomicEnv,
    worker: Socket,
    input_iterator: Iterator<Vec<u8>>,
    partition_index: i32,
    context: TaskContext,
}

impl WriterThread {
    fn write_command(&self, data_out: DataOutputStream) {
        let command = self.funcs[0].funcs[0].command;
        data_out.write_int(command.len());
        data_out.write(command);
    }

    fn write_iterator_to_stream(&self, data_out: DataOutputStream) {
        PythonRDD::write_iterator_to_stream(self.input_iterator, data_out);
        data_out.write_int(SpecialLengths.END_OF_DATA_SECTION);
    }
}


impl<'a> ReaderIterator<'a> {
    fn new(
        stream: &'a mut BufReader<ChildStdout>,
        writer_thread: &'a WriterThread,
        start_time: SystemTime,
        env: &'a AtomicEnv,
        worker: &'a mut TcpStream,
        pid: Option<u32>,
        released_or_closed: &'a AtomicBool,
        context: &'a TaskContext,
    ) -> Result<ReaderIterator<'a>, Box<dyn Error>> {
        Ok(ReaderIterator {
            stream,
            writer_thread,
            start_time,
            env,
            worker,
            pid,
            released_or_closed,
            context,
            next_obj: todo!(),
            eos: false
        })
    }

    fn read(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        match self.stream.read_i32::<LittleEndian>()? {
            length if length > 0 => {
                let mut obj = vec![0; length as usize];
                self.stream.read_exact(&mut obj)?;
                Ok(obj)
            }
            0 => Ok(vec![]),
            SpecialLengths::TIMING_DATA => self.handle_timing_data(),
            SpecialLengths::PYTHON_EXCEPTION_THROWN => self.handle_python_exception(),
            SpecialLengths::END_OF_DATA_SECTION => self.handle_end_of_data_section(),
            _ => Ok(vec![]),
        }
    }

    fn handle_timing_data(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        println!("handling timing data");
        Ok(vec![])
    }

    fn handle_python_exception(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        println!("handling python exception");
        Ok(vec![])
    }

    fn handle_end_of_data_section(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        println!("handling end of data section");
        Ok(vec![])
    }
}