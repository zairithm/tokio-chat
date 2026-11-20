//! Starts a real server on a random port and talks to it over TCP.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

struct Client {
    lines: Lines<BufReader<OwnedReadHalf>>,
    writer: OwnedWriteHalf,
}

impl Client {
    /// Connects and waits for the welcome line, so the client is subscribed.
    async fn connect(addr: SocketAddr) -> Self {
        let (reader, writer) = TcpStream::connect(addr).await.unwrap().into_split();
        let mut client = Self {
            lines: BufReader::new(reader).lines(),
            writer,
        };
        assert_eq!(client.recv().await, tokio_chat::WELCOME);
        client
    }

    async fn send(&mut self, line: &str) {
        self.writer
            .write_all(format!("{line}\n").as_bytes())
            .await
            .unwrap();
    }

    async fn recv(&mut self) -> String {
        timeout(Duration::from_secs(2), self.lines.next_line())
            .await
            .expect("timed out waiting for a line")
            .unwrap()
            .expect("connection closed")
    }

    /// True if nothing arrives within a short wait.
    async fn is_quiet(&mut self) -> bool {
        timeout(Duration::from_millis(200), self.lines.next_line())
            .await
            .is_err()
    }
}

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(tokio_chat::run(listener));
    addr
}

#[tokio::test]
async fn message_reaches_other_clients() {
    let addr = start_server().await;
    let mut alice = Client::connect(addr).await;
    let mut bob = Client::connect(addr).await;
    let mut carol = Client::connect(addr).await;

    alice.send("hello").await;
    assert_eq!(bob.recv().await, "hello");
    assert_eq!(carol.recv().await, "hello");
}

#[tokio::test]
async fn sender_does_not_get_its_own_message() {
    let addr = start_server().await;
    let mut alice = Client::connect(addr).await;
    let mut bob = Client::connect(addr).await;

    alice.send("ping").await;
    assert_eq!(bob.recv().await, "ping");
    assert!(alice.is_quiet().await);
}

#[tokio::test]
async fn a_client_leaving_does_not_break_the_others() {
    let addr = start_server().await;
    let alice = Client::connect(addr).await;
    let mut bob = Client::connect(addr).await;
    let mut carol = Client::connect(addr).await;

    drop(alice);
    bob.send("still here").await;
    assert_eq!(carol.recv().await, "still here");
}
