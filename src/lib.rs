//! Chat server: every line a client sends is broadcast to all other clients.
//!
//! Each connection runs on its own task. They share one `tokio::sync::broadcast`
//! channel: a client publishes its lines into it and, at the same time, listens on
//! its own receiver for lines from everyone else (`tokio::select!` waits on both).

use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::{self, error::RecvError};

/// How many messages a slow client may fall behind before it starts skipping.
const CHANNEL_CAPACITY: usize = 256;

/// First line every client receives. Once it arrives, the client is subscribed.
pub const WELCOME: &str = "Welcome to tokio-chat!";

#[derive(Debug, Clone)]
struct Message {
    from: SocketAddr,
    text: String,
}

/// Accepts clients forever, handling each one on its own task.
pub async fn run(listener: TcpListener) -> std::io::Result<()> {
    let (tx, _) = broadcast::channel::<Message>(CHANNEL_CAPACITY);
    loop {
        let (socket, peer) = listener.accept().await?;
        // Subscribe before spawning, so the client can't miss messages sent after it joins.
        let rx = tx.subscribe();
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(socket, peer, tx, rx).await {
                eprintln!("{peer}: {e}");
            }
        });
    }
}

async fn handle(
    socket: TcpStream,
    peer: SocketAddr,
    tx: broadcast::Sender<Message>,
    mut rx: broadcast::Receiver<Message>,
) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();
    writer.write_all(format!("{WELCOME}\n").as_bytes()).await?;

    loop {
        tokio::select! {
            line = lines.next_line() => match line? {
                // Sending only fails when nobody is listening, which is fine.
                Some(text) => { let _ = tx.send(Message { from: peer, text }); }
                None => break, // client disconnected
            },
            msg = rx.recv() => match msg {
                Ok(m) if m.from != peer => {
                    writer.write_all(format!("{}\n", m.text).as_bytes()).await?;
                }
                Ok(_) => {} // our own message
                Err(RecvError::Lagged(skipped)) => {
                    let note = format!("(you missed {skipped} messages)\n");
                    writer.write_all(note.as_bytes()).await?;
                }
                Err(RecvError::Closed) => break,
            },
        }
    }
    Ok(())
}
