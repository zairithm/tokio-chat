//! Chat server. Right now every client gets its own lines echoed back;
//! broadcasting to all clients is the next step (see the issues).

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// Accepts clients forever, handling each one on its own task.
pub async fn run(listener: TcpListener) -> std::io::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle(socket).await {
                eprintln!("{peer}: {e}");
            }
        });
    }
}

async fn handle(socket: TcpStream) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        writer.write_all(format!("{line}\n").as_bytes()).await?;
    }
    Ok(())
}
