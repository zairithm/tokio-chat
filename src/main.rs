use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let listener = TcpListener::bind(&addr).await?;
    println!("listening on {addr} (Ctrl-C to stop)");

    let ctrl_c = async {
        // If the signal handler can't be installed, run until the process is killed.
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
        println!("shutting down…");
    };
    tokio_chat::run_until(listener, ctrl_c).await?;
    println!("stopped");
    Ok(())
}
