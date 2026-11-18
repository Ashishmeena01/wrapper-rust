use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const STAGES: &[u64] = &[
    1_000,   // Stage 1:  1,000 req/sec
    5_000,   // Stage 2:  5,000 req/sec
    8_000,   // Stage 3:  8,000 req/sec
    12_000,  // Stage 4: 12,000 req/sec
    16_000,  // Stage 5: 16,000 req/sec
    20_000,  // Stage 6: 20,000 req/sec
    24_000,  // Stage 7: 24,000 req/sec
    28_000,  // Stage 8: 28,000 req/sec
    32_000,  // Stage 9: 32,000 req/sec
];

const DURATION_SECS: u64 = 5; // Test duration per stage in seconds

fn send_request(stream: &mut TcpStream, host: &str, method: &str, path: &str, body: Option<&str>) -> Option<u16> {
    let req = if let Some(b) = body {
        format!(
            "{method} {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{b}",
            b.len()
        )
    } else {
        format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: keep-alive\r\n\r\n")
    };

    if stream.write_all(req.as_bytes()).is_err() {
        return None;
    }

    let mut reader = BufReader::new(stream);
    let mut status_line = String::new();
    if reader.read_line(&mut status_line).is_err() || status_line.is_empty() {
        return None;
    }

    // Parse HTTP/1.1 200 OK
    let parts: Vec<&str> = status_line.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    let status_code: u16 = parts[1].parse().ok()?;

    // Read headers until \r\n\r\n
    let mut content_length = 0;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() || line == "\r\n" || line.is_empty() {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("content-length:") {
            if let Some(val) = line.split(':').nth(1) {
                content_length = val.trim().parse().unwrap_or(0);
            }
        }
    }

    // Discard body
    if content_length > 0 {
        let mut buf = vec![0u8; content_length];
        let _ = reader.read_exact(&mut buf);
    }

    Some(status_code)
}

fn seed_products(target_addr: &str, host_header: &str) -> (String, String) {
    let mut stream = TcpStream::connect(target_addr).expect("Failed to connect to target server");
    stream.set_nodelay(true).ok();

    let seed_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let flash_pid = format!("flash_rate_{seed_id}");
    let reg_pid = format!("reg_rate_{seed_id}");

    let flash_body = format!(
        r#"{{"id":"{flash_pid}","name":"Flash Rate Item","category":"smartphones","price":99999,"stock":10000000}}"#
    );
    send_request(&mut stream, host_header, "POST", "/products", Some(&flash_body));

    let reg_body = format!(
        r#"{{"id":"{reg_pid}","name":"Reg Rate Item","category":"laptops","price":50000,"stock":10000000}}"#
    );
    send_request(&mut stream, host_header, "POST", "/products", Some(&reg_body));

    (flash_pid, reg_pid)
}

fn main() {
    println!("\n======================================================================");
    println!("      LAST SERVER STANDING — NATIVE TARGET RPS SATURATION BENCHMARK   ");
    println!("======================================================================\n");

    let args: Vec<String> = std::env::args().collect();
    let target_host = args.get(1).map(|s| s.as_str()).unwrap_or("127.0.0.1");
    let target_port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8080);
    let target_addr = format!("{target_host}:{target_port}");

    // Check health
    println!("==> Checking server health at http://{target_addr}/health...");
    match TcpStream::connect(&target_addr) {
        Ok(mut stream) => {
            let code = send_request(&mut stream, &target_addr, "GET", "/health", None);
            if code != Some(200) {
                eprintln!("Error: Server returned unexpected health status: {code:?}");
                std::process::exit(1);
            }
            println!("✓ Server is HEALTHY (HTTP 200 OK)\n");
        }
        Err(e) => {
            eprintln!("Error: Cannot connect to {target_addr}: {e}");
            std::process::exit(1);
        }
    }

    println!("==> Seeding catalog dataset for rate testing...");
    let (flash_pid, reg_pid) = seed_products(&target_addr, &target_addr);
    println!("✓ Seeded flash product '{flash_pid}' and catalog product '{reg_pid}'\n");

    println!("Starting Progressive Rate Stages (each stage runs for {DURATION_SECS} seconds):");
    println!(
        "{:<8} | {:<16} | {:<16} | {:<12} | {:<10} | {:<8}",
        "Stage", "Target Rate", "Achieved Rate", "Requests", "Error Rate", "Status"
    );
    println!("{}", "-".repeat(84));

    let mut highest_sustained = 0u64;
    let mut peak_rps = 0.0f64;

    for (stage_idx, &target_rps) in STAGES.iter().enumerate() {
        let stage_num = stage_idx + 1;
        let connections = std::cmp::min(128, std::cmp::max(16, (target_rps / 100) as usize));

        let stop_flag = Arc::new(AtomicBool::new(false));
        let total_sent = Arc::new(AtomicU64::new(0));
        let total_ok = Arc::new(AtomicU64::new(0));
        let total_err = Arc::new(AtomicU64::new(0));
        let total_5xx = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        let start_time = Instant::now();
        let target_interval_micros = (1_000_000.0 / target_rps as f64) * connections as f64;

        for worker_id in 0..connections {
            let addr = target_addr.clone();
            let host_hdr = target_addr.clone();
            let f_id = flash_pid.clone();
            let _r_id = reg_pid.clone();
            let stop = Arc::clone(&stop_flag);
            let s_sent = Arc::clone(&total_sent);
            let s_ok = Arc::clone(&total_ok);
            let s_err = Arc::clone(&total_err);
            let s_5xx = Arc::clone(&total_5xx);

            handles.push(std::thread::spawn(move || {
                let mut stream = match TcpStream::connect(&addr) {
                    Ok(s) => {
                        s.set_nodelay(true).ok();
                        Some(s)
                    }
                    Err(_) => None,
                };

                let mut req_counter = 0u64;
                let mut next_tick = Instant::now();

                while !stop.load(Ordering::Relaxed) {
                    if stream.is_none() {
                        stream = TcpStream::connect(&addr).ok();
                        if let Some(ref s) = stream {
                            s.set_nodelay(true).ok();
                        } else {
                            s_err.fetch_add(1, Ordering::Relaxed);
                            std::thread::sleep(Duration::from_millis(5));
                            continue;
                        }
                    }

                    req_counter += 1;
                    let roll = req_counter % 100;

                    let (method, path, body) = if roll < 60 {
                        ("GET", format!("/products/{f_id}"), None)
                    } else if roll < 75 {
                        ("GET", "/products?category=smartphones&page=1&limit=20".to_string(), None)
                    } else if roll < 95 {
                        let ord_body = format!(r#"{{"userId":"u_{worker_id}_{req_counter}","productId":"{f_id}"}}"#);
                        ("POST", "/orders".to_string(), Some(ord_body))
                    } else {
                        ("GET", format!("/users/u_{worker_id}_1/orders"), None)
                    };

                    s_sent.fetch_add(1, Ordering::Relaxed);
                    let resp = send_request(stream.as_mut().unwrap(), &host_hdr, method, &path, body.as_deref());

                    match resp {
                        Some(code) => {
                            if code == 200 || code == 201 || code == 400 || code == 404 || code == 409 {
                                s_ok.fetch_add(1, Ordering::Relaxed);
                            } else {
                                s_err.fetch_add(1, Ordering::Relaxed);
                                if code >= 500 {
                                    s_5xx.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        None => {
                            s_err.fetch_add(1, Ordering::Relaxed);
                            stream = None;
                        }
                    }

                    // Pacing to meet target RPS
                    next_tick += Duration::from_micros(target_interval_micros as u64);
                    let now = Instant::now();
                    if next_tick > now {
                        std::thread::sleep(next_tick - now);
                    } else if now - next_tick > Duration::from_millis(50) {
                        next_tick = now; // Prevent catch-up bursts if lagging
                    }
                }
            }));
        }

        // Run for DURATION_SECS
        std::thread::sleep(Duration::from_secs(DURATION_SECS));
        stop_flag.store(true, Ordering::Relaxed);

        for h in handles {
            let _ = h.join();
        }

        let elapsed = start_time.elapsed().as_secs_f64();
        let sent = total_sent.load(Ordering::Relaxed);
        let ok = total_ok.load(Ordering::Relaxed);
        let err = total_err.load(Ordering::Relaxed);
        let five_xx = total_5xx.load(Ordering::Relaxed);

        let achieved_rps = (ok as f64) / elapsed;
        let err_pct = if sent > 0 { (err as f64 * 100.0) / sent as f64 } else { 100.0 };

        // Failure threshold: error rate > 3% or achieved RPS < 80% of target
        let failed = err_pct > 3.0 || five_xx > 0 || (achieved_rps < (target_rps as f64 * 0.75));

        let status_str = if failed { "FAILED" } else { "PASSED" };

        println!(
            "#{:<7} | {:>9} req/s | {:>9.1} req/s | {:>12} | {:>9.2}% | {}",
            stage_num,
            target_rps,
            achieved_rps,
            sent,
            err_pct,
            status_str
        );

        if achieved_rps > peak_rps {
            peak_rps = achieved_rps;
        }

        if failed {
            println!("\n==> SERVER SATURATION LIMIT REACHED AT {target_rps} REQ/SEC!");
            println!("    Reason: Error rate: {err_pct:.2}% (5xx: {five_xx}, Failed/Drops: {err})");
            println!("    Highest Sustained Rate: {highest_sustained} req/sec");
            println!("    Peak Achieved Throughput: {peak_rps:.1} req/sec\n");
            break;
        } else {
            highest_sustained = target_rps;
        }

        std::thread::sleep(Duration::from_millis(500));
    }

    if highest_sustained == *STAGES.last().unwrap() {
        println!("\n==> ALL STAGES PASSED! Server handled up to {highest_sustained} req/sec flawlessly!");
        println!("    Peak Throughput Observed: {peak_rps:.1} req/sec\n");
    }
}
