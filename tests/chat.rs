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
    /// Connects without picking a nickname yet.
    async fn connect_raw(addr: SocketAddr) -> Self {
        let (reader, writer) = TcpStream::connect(addr).await.unwrap().into_split();
        let mut client = Self {
            lines: BufReader::new(reader).lines(),
            writer,
        };
        assert_eq!(client.recv().await, tokio_chat::WELCOME);
        assert_eq!(client.recv().await, tokio_chat::NICK_PROMPT);
        client
    }

    /// Connects and joins as `nick`. Once the greeting arrives, the client is subscribed.
    async fn join(addr: SocketAddr, nick: &str) -> Self {
        let mut client = Self::connect_raw(addr).await;
        client.send(nick).await;
        assert!(client.recv().await.starts_with(&format!("Hi {nick}!")));
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
async fn messages_carry_the_nickname() {
    let addr = start_server().await;
    let mut alice = Client::join(addr, "alice").await;
    let mut bob = Client::join(addr, "bob").await;
    assert_eq!(alice.recv().await, "* bob joined");

    alice.send("hello").await;
    assert_eq!(bob.recv().await, "alice: hello");
    bob.send("hi alice").await;
    assert_eq!(alice.recv().await, "bob: hi alice");
}

#[tokio::test]
async fn sender_does_not_get_its_own_message() {
    let addr = start_server().await;
    let mut alice = Client::join(addr, "alice").await;
    let mut bob = Client::join(addr, "bob").await;
    assert_eq!(alice.recv().await, "* bob joined");

    alice.send("ping").await;
    assert_eq!(bob.recv().await, "alice: ping");
    assert!(alice.is_quiet().await);
}

#[tokio::test]
async fn join_and_leave_are_announced() {
    let addr = start_server().await;
    let mut alice = Client::join(addr, "alice").await;
    let bob = Client::join(addr, "bob").await;
    assert_eq!(alice.recv().await, "* bob joined");

    drop(bob);
    assert_eq!(alice.recv().await, "* bob left");
}

#[tokio::test]
async fn bad_nickname_is_asked_again() {
    let addr = start_server().await;
    let mut client = Client::connect_raw(addr).await;
    client.send("two words").await;
    assert_eq!(client.recv().await, "A nickname can't contain spaces.");
    assert_eq!(client.recv().await, tokio_chat::NICK_PROMPT);
    client.send("ok").await;
    assert!(client.recv().await.starts_with("Hi ok!"));
}

#[tokio::test]
async fn unnamed_clients_hear_nothing() {
    let addr = start_server().await;
    let mut lurker = Client::connect_raw(addr).await;
    let mut alice = Client::join(addr, "alice").await;
    alice.send("secret").await;
    assert!(lurker.is_quiet().await);
}

#[tokio::test]
async fn who_lists_everyone() {
    let addr = start_server().await;
    let mut alice = Client::join(addr, "alice").await;
    let _bob = Client::join(addr, "Bob").await;
    assert_eq!(alice.recv().await, "* Bob joined");

    alice.send("/who").await;
    assert_eq!(alice.recv().await, "2 online: alice, Bob");
}

#[tokio::test]
async fn nick_change_is_announced_and_used() {
    let addr = start_server().await;
    let mut alice = Client::join(addr, "alice").await;
    let mut bob = Client::join(addr, "bob").await;
    assert_eq!(alice.recv().await, "* bob joined");

    alice.send("/nick ally").await;
    assert_eq!(alice.recv().await, "You are now ally.");
    assert_eq!(bob.recv().await, "* alice is now ally");
    alice.send("hi").await;
    assert_eq!(bob.recv().await, "ally: hi");
}

#[tokio::test]
async fn taken_nickname_is_refused() {
    let addr = start_server().await;
    let mut alice = Client::join(addr, "alice").await;
    let mut bob = Client::join(addr, "bob").await;
    assert_eq!(alice.recv().await, "* bob joined");

    bob.send("/nick ALICE").await;
    assert_eq!(bob.recv().await, "ALICE is taken.");

    let mut late = Client::connect_raw(addr).await;
    late.send("alice").await;
    assert_eq!(late.recv().await, "alice is taken.");
    assert_eq!(late.recv().await, tokio_chat::NICK_PROMPT);
}

#[tokio::test]
async fn nickname_is_free_again_after_leaving() {
    let addr = start_server().await;
    let alice = Client::join(addr, "alice").await;
    let mut bob = Client::join(addr, "bob").await;
    drop(alice);
    assert_eq!(bob.recv().await, "* alice left");
    let _again = Client::join(addr, "alice").await;
}

#[tokio::test]
async fn quit_says_bye_and_closes() {
    let addr = start_server().await;
    let mut alice = Client::join(addr, "alice").await;
    let mut bob = Client::join(addr, "bob").await;
    assert_eq!(alice.recv().await, "* bob joined");

    bob.send("/quit").await;
    assert_eq!(bob.recv().await, "Bye!");
    let closed = timeout(Duration::from_secs(2), bob.lines.next_line()).await;
    assert!(
        matches!(closed, Ok(Ok(None))),
        "connection should be closed"
    );
    assert_eq!(alice.recv().await, "* bob left");
}

#[tokio::test]
async fn unknown_command_shows_help() {
    let addr = start_server().await;
    let mut alice = Client::join(addr, "alice").await;
    alice.send("/dance").await;
    assert_eq!(
        alice.recv().await,
        format!("Unknown command /dance. {}", tokio_chat::HELP)
    );
}

#[tokio::test]
async fn shutdown_says_bye_and_stops_the_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(tokio_chat::run_until(listener, async {
        let _ = stopped.await;
    }));

    let mut alice = Client::join(addr, "alice").await;
    let mut naming = Client::connect_raw(addr).await; // still choosing a nickname

    stop.send(()).unwrap();
    assert_eq!(alice.recv().await, tokio_chat::SHUTDOWN_NOTICE);
    assert_eq!(naming.recv().await, tokio_chat::SHUTDOWN_NOTICE);

    // The server task finishes on its own once everyone is gone.
    timeout(Duration::from_secs(3), server)
        .await
        .expect("server did not stop")
        .unwrap()
        .unwrap();
    assert!(
        TcpStream::connect(addr).await.is_err(),
        "no longer accepting"
    );
}

#[tokio::test]
async fn blank_lines_are_ignored() {
    let addr = start_server().await;
    let mut alice = Client::join(addr, "alice").await;
    let mut bob = Client::join(addr, "bob").await;
    assert_eq!(alice.recv().await, "* bob joined");

    alice.send("   ").await;
    alice.send("real").await;
    assert_eq!(bob.recv().await, "alice: real");
}
