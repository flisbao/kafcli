use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;

/// Canned answer for one method and exact path (with query).
#[derive(Clone, Debug)]
pub struct FakeRoute {
    pub method: String,
    pub path_and_query: String,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

/// Request seen by the fake server.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordedRequest {
    pub method: String,
    pub path_and_query: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

/// Local HTTP server that answers from fixed routes and records every request.
pub struct FakeRegistryServer {
    address: SocketAddr,
    recorded: Arc<Mutex<Vec<RecordedRequest>>>,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl FakeRegistryServer {
    pub fn start(routes: Vec<FakeRoute>) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let stopping = Arc::new(AtomicBool::new(false));
        let worker = {
            let recorded = Arc::clone(&recorded);
            let stopping = Arc::clone(&stopping);
            std::thread::spawn(move || serve(listener, &routes, &recorded, &stopping))
        };
        Ok(Self {
            address,
            recorded,
            stopping,
            worker: Some(worker),
        })
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn recorded_requests(&self) -> Vec<RecordedRequest> {
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Drop for FakeRegistryServer {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::SeqCst);
        // Connecting to ourselves unblocks the pending accept so the worker sees the stop flag.
        let _ = TcpStream::connect(self.address);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn serve(
    listener: TcpListener,
    routes: &[FakeRoute],
    recorded: &Mutex<Vec<RecordedRequest>>,
    stopping: &AtomicBool,
) {
    for stream in listener.incoming() {
        if stopping.load(Ordering::SeqCst) {
            break;
        }
        if let Ok(stream) = stream {
            let _ = handle_connection(stream, routes, recorded);
        }
    }
}

fn handle_connection(
    stream: TcpStream,
    routes: &[FakeRoute],
    recorded: &Mutex<Vec<RecordedRequest>>,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();

    let mut headers = Vec::new();
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_string();
            let value = value.trim().to_string();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().unwrap_or(0);
            }
            headers.push((name, value));
        }
    }
    let mut body_bytes = vec![0u8; content_length];
    reader.read_exact(&mut body_bytes)?;
    let body = String::from_utf8_lossy(&body_bytes).into_owned();

    let (status, response_headers, response_body) = match routes
        .iter()
        .find(|route| route.method == method && route.path_and_query == path)
    {
        Some(route) => (route.status, route.headers.clone(), route.body.clone()),
        None => (
            404,
            Vec::new(),
            r#"{"error_code":40400,"message":"no fake route"}"#.to_string(),
        ),
    };
    recorded
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(RecordedRequest {
            method,
            path_and_query: path,
            headers,
            body,
        });

    let mut response = format!(
        "HTTP/1.1 {status} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        reason_phrase(status),
        response_body.len()
    );
    for (name, value) in &response_headers {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str("\r\n");
    let mut writer = stream;
    writer.write_all(response.as_bytes())?;
    writer.write_all(response_body.as_bytes())?;
    writer.flush()
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200..=299 => "OK",
        401 => "Unauthorized",
        404 => "Not Found",
        500..=599 => "Server Error",
        _ => "Error",
    }
}
