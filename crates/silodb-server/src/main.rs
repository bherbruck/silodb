use silodb_server::{app, boot, maintenance_loop, Config};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env();

    // `--healthcheck` probes an already-running server and exits 0/1.
    // The image is debian-slim running as a non-root user and ships this
    // binary and nothing else — no curl, no wget, no shell utilities — so
    // a self-probe is the only thing a Docker HEALTHCHECK or a k8s exec
    // probe has to call.
    if std::env::args().skip(1).any(|a| a == "--healthcheck") {
        match healthcheck(&config.addr).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                eprintln!("healthcheck failed: {e}");
                std::process::exit(1);
            }
        }
    }

    let state = boot(&config)?;
    tokio::spawn(maintenance_loop(state.writer.clone(), config.maintain_secs));

    let listener = tokio::net::TcpListener::bind(&config.addr).await?;
    println!(
        "silodb-server on http://{} (db: {}, maintain every {}s)",
        config.addr,
        config.db_path.display(),
        config.maintain_secs
    );
    axum::serve(listener, app(state)).await?;
    Ok(())
}

/// GET /health over a plain socket. Hand-rolled rather than pulling in an
/// HTTP client: one request, one response, and the dependency would ship
/// in every image for the sake of this function.
async fn healthcheck(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let target = connect_addr(addr)?;
    // Under Docker's HEALTHCHECK --timeout=5s: report our own failure
    // first, so a hung connect surfaces as "unhealthy" rather than as a
    // probe Docker killed for taking too long.
    let timeout = Duration::from_secs(3);

    let mut sock = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&target))
        .await
        .map_err(|_| format!("connect to {target} timed out"))??;

    // /health touches the reader pool, so a wedged SQLite fails the probe
    // rather than passing it on the strength of the listener being up.
    let req = format!("GET /health HTTP/1.1\r\nHost: {target}\r\nConnection: close\r\n\r\n");
    sock.write_all(req.as_bytes()).await?;

    let mut buf = Vec::new();
    tokio::time::timeout(timeout, sock.read_to_end(&mut buf))
        .await
        .map_err(|_| format!("no response from {target} within {timeout:?}"))??;

    let status = String::from_utf8_lossy(&buf)
        .lines()
        .next()
        .unwrap_or_default()
        .to_string();
    if status.split_whitespace().nth(1) == Some("200") {
        Ok(())
    } else if status.is_empty() {
        Err(format!("{target} closed the connection without responding").into())
    } else {
        Err(format!("/health returned: {status}").into())
    }
}

/// A listen address is not a connect address: `0.0.0.0` and `::` mean
/// "every interface" to bind(2) and nothing useful to connect(2).
fn connect_addr(addr: &str) -> Result<String, Box<dyn std::error::Error>> {
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| format!("SILODB_ADDR has no port: {addr}"))?;
    let host = match host.trim_matches(['[', ']']) {
        "0.0.0.0" | "" | "*" => "127.0.0.1",
        "::" => "::1",
        h => h,
    };
    Ok(if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    })
}

#[cfg(test)]
mod tests {
    use super::connect_addr;

    #[test]
    fn wildcards_become_loopback() {
        assert_eq!(connect_addr("0.0.0.0:8080").unwrap(), "127.0.0.1:8080");
        assert_eq!(connect_addr("[::]:8080").unwrap(), "[::1]:8080");
        assert_eq!(connect_addr("127.0.0.1:9000").unwrap(), "127.0.0.1:9000");
        assert_eq!(connect_addr("silo.internal:80").unwrap(), "silo.internal:80");
        assert_eq!(connect_addr("[::1]:8080").unwrap(), "[::1]:8080");
        assert!(connect_addr("0.0.0.0").is_err());
    }
}
