//

use std::io;
use std::{collections::HashMap, env};
use std::sync::{Arc, Mutex};
use std::process::{Command, Stdio, Child};

struct PythonWorkerFactory {
    python_exec: String,
    env_vars: HashMap<String, String>,
    use_daemon: bool,
    daemon_module: String,
    worker_module: String,
    auth_helper: SocketAuthHelper,
    daemon: Process,
    daemon_host: SocketAddr,
    daemon_port: u16,
    daemon_workers: WeakHashMap<TcpStream, u16>,
    idle_workers: Vec<TcpStream>,
    last_activity_ns: u64,
    simple_workers: WeakHashMap<TcpStream, Process>
}

impl PythonWorkerFactory {
    fn new(python_exec: String, env_vars: HashMap<String, String>) -> PythonWorkerFactory {
        let use_daemon = match env::var("os.name") {
            Ok(val) => val.starts_with("Windows"),
            Err(_) => false
        };

        let daemon_module = match env::var("python.daemon.module") {
            Ok(val) => {
                println!("Python daemon module is set to [{}] in 'python.daemon.module', 
                    using this to start the daemon up. Note that this configuration only has an effect 
                    when 'python.use.daemon' is enabled and the platform is not Windows.", val);
                val
            },
            Err(_) => String::from("py.daemon")
        };

        let worker_module = match env::var("python.worker.module") {
            Ok(val) => {
                println!("Python worker module is set to [{}] in 'python.worker.module', 
                    using this to start the worker up. Note that this configuration only has an effect 
                    when 'python.use.daemon' is disabled or the platform is Windows.", val);
                val
            },
            Err(_) => String::from("pyworker")
        };

        let auth_helper = SocketAuthHelper::new();

        PythonWorkerFactory {
            python_exec,
            env_vars,
            use_daemon,
            daemon_module,
            worker_module,
            auth_helper,
            daemon: Process::new(),
            daemon_host: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            daemon_port: 0,
            daemon_workers: HashMap::new(),
            idle_workers: Vec::new(),
            last_activity_ns: 0,
            simple_workers: HashMap::new()
        }
    }

    pub fn create(&self) -> (Socket, Option<usize>) {
        if self.use_daemon {
            self.synchronized(|| {
                if !self.idle_workers.is_empty() {
                    let worker = self.idle_workers.dequeue();
                    return (worker, self.daemon_workers.get(worker));
                }
            });
            self.create_through_daemon()
        } else {
            self.create_simple_worker(
                todo!(), 
                todo!(), 
                todo!(), 
                todo!(), todo!(), todo!()
            )
        }
    }

    /// Connect to a worker launched through py/daemon.py (by default). This currently only works
    /// on UNIX-based systems.
    pub fn create_through_daemon(&self) -> Result<(Socket, Option<i32>)> {
        fn create_socket(self) -> Result<(Socket, Option<i32>)> {
            let socket = Socket::new(AddrFamily::Inet, SockType::Stream, None)?;
            let pid = socket.recv_int()?;
            if pid < 0 {
                return Err(Error::new(ErrorKind::Other, "Python daemon failed to launch worker"));
            }
            self.auth_helper.auth_to_server(socket)?;
            self.daemon_workers.insert(socket, pid);
            Ok((socket, Some(pid)))
        }
    
        let mut daemon = self.lock().unwrap();
        daemon.start_daemon()?;
        match create_socket(self.clone()) {
            Ok(s) => Ok(s),
            Err(e) => {
                warn!("Failed to open socket to Python daemon: {:?}", e);
                warn!("Assuming that daemon unexpectedly quit, attempting to restart");
                daemon.stop_daemon()?;
                daemon.start_daemon()?;
                create_socket()
            },
        }
    }

    pub fn create_simple_worker(
        &self,
        worker_common: Arc<Mutex<WorkerCommon>>,
        python_exec: &str,
        worker_module: &str,
        python_path: &str,
        env_vars: HashMap<String, String>,
        auth_secret: &str,
    ) -> (Child, Option<i32>) {
        let mut pb = Command::new(python_exec);
        pb.arg("-m");
        pb.arg(worker_module);
        pb.env("PYTHONPATH", python_path);
        pb.env("PYTHONUNBUFFERED", "YES");
        pb.env("PYTHON_WORKER_FACTORY_SECRET", auth_secret);
        if cfg!(target_os = "linux") {
            pb.env("PREFER_IPV6", "True");
        }
        for (k, v) in env_vars.iter() {
            pb.env(k, v);
        }
        let child = pb
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to execute child");
        let pid = child.id() as i32;
        let worker_common = worker_common.lock().unwrap();
        worker_common.simple_workers.insert(pid, child.clone());
        (child, Some(pid))
    }


    fn start_daemon(&self) {
        // Is it already running?
        if let Some(_) = self.daemon {
            return;
        }
    
        let command = [
            python_exec,
            "-m",
            daemon_module,
        ];
        // Create and start the daemon
        let mut pb = ProcessBuilder::new(&command)
            .env("PYTHONPATH", python_path)
            .env("PYTHON_WORKER_FACTORY_SECRET", auth_helper.secret)
            .env("PYTHONUNBUFFERED", "YES");
        if prefer_ipv6 {
            pb = pb.env("PREFER_IPV6", "True");
        }
        pb = pb.envs(&env_vars);
        daemon = Some(pb.start());
    
        let in = daemon.as_ref().unwrap().stdout.take().unwrap();
        let mut reader = BufReader::new(in);
        let mut buffer = [0; 4];
        reader.read_exact(&mut buffer).unwrap();
        daemon_port = u32::from_be_bytes(buffer);
    
        // test that the returned port number is within a valid range.
        // note: this does not cover the case where the port number
        // is arbitrary data but is also coincidentally within range
        if daemon_port < 1 || daemon_port > 0xffff {
            let exception_message = format!(
                "Bad data in {}'s standard output. Invalid port number:\n  {} (0x{:08x})\nPython command to execute the daemon was:\n  {}\nCheck that you don't have any unexpected modules or libraries in\nyour PYTHONPATH:\n  {}\nAlso, check if you have a sitecustomize.py module in your python path,\nor in your python installation, that is printing to standard output",
                daemon_module,
                daemon_port,
                daemon_port,
                command.join(" "),
                python_path,
            );
            panic!("{}", exception_message);
        }
    
        // Redirect daemon stdout and stderr
        redirect_streams_to_stderr(reader, daemon.as_ref().unwrap().stderr.take().unwrap());
    }

    /// Redirect the given streams to our stderr in separate threads.
    fn redirect_streams_to_stderr(stdout: InputStream, stderr: InputStream) {
        let mut stdout = stdout;
        let mut stderr = stderr;

        std::thread::spawn(move || {
            io::copy(&mut stdout, &mut std::io::stderr()).unwrap();
        });
        std::thread::spawn(move || {
            io::copy(&mut stderr, &mut std::io::stderr()).unwrap();
        });
    }

}


pub struct MonitorThread {
    python_exec: String,
    idle_worker_timeout_ns: i64,
    last_activity_ns: i64,
    idle_workers: Vec<Worker>,
    daemon: Option<std::process::Child>,
    daemon_port: u32,
}

impl MonitorThread {
    pub fn run(&mut self) {
        loop {
            if self.idle_worker_timeout_ns < std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() - self.last_activity_ns {
                self.cleanup_idle_workers()
                self.last_activity_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            }
            std::thread::sleep(std::time::Duration::from_millis(10000))
        }
    }

    fn cleanup_idle_workers(&mut self) {
        while !self.idle_workers.is_empty() {
            let worker = self.idle_workers.remove(0);
            worker.close();
        }
    }

    fn stop_daemon(&mut self) {
        if !self.daemon.is_none() {
            self.cleanup_idle_workers();
            self.daemon.unwrap().kill();
            self.daemon = None;
            self.daemon_port = 0;
        }
    }

    pub fn stop(&mut self) {
        self.stop_daemon();
    }

    pub fn stop_worker(&mut self, worker: Worker) {
        if !self.daemon.is_none() {
            if !self.daemon_workers.get(&worker).is_none() {
                let pid = self.daemon_workers.get(&worker).unwrap();
                let mut output = std::io::BufWriter::new(self.daemon.unwrap().stdin.unwrap());
                output.write_all(&pid.to_be_bytes());
                output.flush();
                self.daemon.unwrap().stdin.unwrap().flush();
            }
        }
        worker.close();
    }

    pub fn release_worker(&mut self, worker: Worker) {
        if !self.daemon.is_none() {
            self.last_activity_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
            self.idle_workers.push(worker);
        } else {
            worker.close();
        }
    }
}



