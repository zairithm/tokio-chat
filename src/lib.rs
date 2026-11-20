//! Chat server: clients pick a nickname, then every line they send is broadcast
//! to all other clients as `nickname: text`. Joins and leaves are announced.
//!
//! Each connection runs on its own task. They share one `tokio::sync::broadcast`
//! channel: a client publishes its lines into it and, at the same time, listens on
//! its own receiver for lines from everyone else (`tokio::select!` waits on both).

use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::{self, error::RecvError};

/// How many messages a slow client may fall behind before it starts skipping.
const CHANNEL_CAPACITY: usize = 256;
/// Longest nickname allowed, in characters.
pub const MAX_NICK_LEN: usize = 20;

/// First line every client receives.
pub const WELCOME: &str = "Welcome to tokio-chat!";
/// Asked until the client sends a valid nickname.
pub const NICK_PROMPT: &str = "Choose a nickname:";

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
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(socket, peer, tx).await {
                eprintln!("{peer}: {e}");
            }
        });
    }
}

/// Checks a nickname and explains what is wrong with it.
pub fn validate_nick(nick: &str) -> Result<&str, String> {
    let nick = nick.trim();
    if nick.is_empty() {
        Err("A nickname can't be empty.".into())
    } else if nick.chars().count() > MAX_NICK_LEN {
        Err(format!(
            "A nickname can be at most {MAX_NICK_LEN} characters."
        ))
    } else if nick.contains(char::is_whitespace) {
        Err("A nickname can't contain spaces.".into())
    } else {
        Ok(nick)
    }
}

async fn send_line(writer: &mut OwnedWriteHalf, line: &str) -> std::io::Result<()> {
    writer.write_all(format!("{line}\n").as_bytes()).await
}

/// Asks for a nickname until a valid one arrives. `None` if the client hung up first.
async fn ask_nick(
    lines: &mut Lines<BufReader<OwnedReadHalf>>,
    writer: &mut OwnedWriteHalf,
) -> std::io::Result<Option<String>> {
    loop {
        send_line(writer, NICK_PROMPT).await?;
        let Some(line) = lines.next_line().await? else {
            return Ok(None);
        };
        match validate_nick(&line) {
            Ok(nick) => return Ok(Some(nick.to_string())),
            Err(why) => send_line(writer, &why).await?,
        }
    }
}

async fn handle(
    socket: TcpStream,
    peer: SocketAddr,
    tx: broadcast::Sender<Message>,
) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();
    send_line(&mut writer, WELCOME).await?;

    let Some(nick) = ask_nick(&mut lines, &mut writer).await? else {
        return Ok(());
    };

    // Subscribe only once named, so nobody gets a backlog from before they joined.
    let mut rx = tx.subscribe();
    let announce = |text: String| {
        // Sending only fails when nobody is listening, which is fine.
        let _ = tx.send(Message { from: peer, text });
    };
    announce(format!("* {nick} joined"));
    send_line(
        &mut writer,
        &format!("Hi {nick}! Type a message and press Enter."),
    )
    .await?;

    let result = chat_loop(&mut lines, &mut writer, &mut rx, peer, &nick, &announce).await;
    announce(format!("* {nick} left"));
    result
}

async fn chat_loop(
    lines: &mut Lines<BufReader<OwnedReadHalf>>,
    writer: &mut OwnedWriteHalf,
    rx: &mut broadcast::Receiver<Message>,
    peer: SocketAddr,
    nick: &str,
    announce: &impl Fn(String),
) -> std::io::Result<()> {
    loop {
        tokio::select! {
            line = lines.next_line() => match line? {
                Some(text) if text.trim().is_empty() => {}
                Some(text) => announce(format!("{nick}: {text}")),
                None => return Ok(()), // client disconnected
            },
            msg = rx.recv() => match msg {
                Ok(m) if m.from != peer => send_line(writer, &m.text).await?,
                Ok(_) => {} // our own message
                Err(RecvError::Lagged(skipped)) => {
                    send_line(writer, &format!("(you missed {skipped} messages)")).await?;
                }
                Err(RecvError::Closed) => return Ok(()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_and_trims_a_normal_nick() {
        assert_eq!(validate_nick("  zaid  "), Ok("zaid"));
    }

    #[test]
    fn rejects_bad_nicks() {
        assert!(validate_nick("").is_err());
        assert!(validate_nick("   ").is_err());
        assert!(validate_nick("two words").is_err());
        assert!(validate_nick(&"x".repeat(MAX_NICK_LEN + 1)).is_err());
        assert!(validate_nick(&"x".repeat(MAX_NICK_LEN)).is_ok());
    }
}
