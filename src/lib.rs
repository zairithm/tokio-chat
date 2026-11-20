//! Chat server: clients pick a unique nickname, then every line they send is
//! broadcast to all other clients as `nickname: text`. Joins, leaves and nickname
//! changes are announced, and lines starting with `/` are commands.
//!
//! Each connection runs on its own task. They share:
//! - one `tokio::sync::broadcast` channel: a client publishes into it and, at the same
//!   time, listens on its own receiver for everyone else (`tokio::select!` waits on both);
//! - the list of nicknames in use, behind an `Arc<Mutex<_>>`. The lock is only held for
//!   a quick lookup or insert, never across an `.await`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

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
/// Asked until the client sends a valid, unused nickname.
pub const NICK_PROMPT: &str = "Choose a nickname:";
/// Shown for `/help` and unknown commands.
pub const HELP: &str = "Commands: /nick <name>, /who, /help, /quit";

#[derive(Debug, Clone)]
struct Message {
    from: SocketAddr,
    text: String,
}

/// State shared by every connection.
#[derive(Clone)]
struct Shared {
    tx: broadcast::Sender<Message>,
    users: Arc<Mutex<HashMap<SocketAddr, String>>>,
}

impl Shared {
    /// Claims `nick` for `peer` if nobody else has it. Replaces `peer`'s old nickname.
    fn claim(&self, peer: SocketAddr, nick: &str) -> bool {
        let mut users = self.users.lock().unwrap();
        let taken = users
            .iter()
            .any(|(addr, name)| *addr != peer && name.eq_ignore_ascii_case(nick));
        if !taken {
            users.insert(peer, nick.to_string());
        }
        !taken
    }

    fn release(&self, peer: SocketAddr) {
        self.users.lock().unwrap().remove(&peer);
    }

    fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.users.lock().unwrap().values().cloned().collect();
        names.sort_by_key(|n| n.to_lowercase());
        names
    }

    fn broadcast(&self, from: SocketAddr, text: String) {
        // Sending only fails when nobody is listening, which is fine.
        let _ = self.tx.send(Message { from, text });
    }
}

/// Accepts clients forever, handling each one on its own task.
pub async fn run(listener: TcpListener) -> std::io::Result<()> {
    let (tx, _) = broadcast::channel::<Message>(CHANNEL_CAPACITY);
    let shared = Shared {
        tx,
        users: Arc::default(),
    };
    loop {
        let (socket, peer) = listener.accept().await?;
        let shared = shared.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(socket, peer, &shared).await {
                eprintln!("{peer}: {e}");
            }
            shared.release(peer);
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

/// A line typed by a client, after parsing.
#[derive(Debug, PartialEq, Eq)]
pub enum Input<'a> {
    Say(&'a str),
    Nick(&'a str),
    Who,
    Help,
    Quit,
    Unknown(&'a str),
    Blank,
}

/// Splits a line into a chat message or a `/command`.
pub fn parse(line: &str) -> Input<'_> {
    let line = line.trim();
    let Some(rest) = line.strip_prefix('/') else {
        return if line.is_empty() {
            Input::Blank
        } else {
            Input::Say(line)
        };
    };
    let (cmd, arg) = rest.split_once(' ').unwrap_or((rest, ""));
    match cmd.to_ascii_lowercase().as_str() {
        "nick" => Input::Nick(arg.trim()),
        "who" => Input::Who,
        "help" => Input::Help,
        "quit" => Input::Quit,
        _ => Input::Unknown(cmd),
    }
}

async fn send_line(writer: &mut OwnedWriteHalf, line: &str) -> std::io::Result<()> {
    writer.write_all(format!("{line}\n").as_bytes()).await
}

/// Asks until a valid, unused nickname arrives. `None` if the client hung up first.
async fn ask_nick(
    lines: &mut Lines<BufReader<OwnedReadHalf>>,
    writer: &mut OwnedWriteHalf,
    shared: &Shared,
    peer: SocketAddr,
) -> std::io::Result<Option<String>> {
    loop {
        send_line(writer, NICK_PROMPT).await?;
        let Some(line) = lines.next_line().await? else {
            return Ok(None);
        };
        match validate_nick(&line) {
            Ok(nick) if shared.claim(peer, nick) => return Ok(Some(nick.to_string())),
            Ok(nick) => send_line(writer, &format!("{nick} is taken.")).await?,
            Err(why) => send_line(writer, &why).await?,
        }
    }
}

async fn handle(socket: TcpStream, peer: SocketAddr, shared: &Shared) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();
    send_line(&mut writer, WELCOME).await?;

    let Some(mut nick) = ask_nick(&mut lines, &mut writer, shared, peer).await? else {
        return Ok(());
    };

    // Subscribe only once named, so nobody gets a backlog from before they joined.
    let mut rx = shared.tx.subscribe();
    shared.broadcast(peer, format!("* {nick} joined"));
    send_line(&mut writer, &format!("Hi {nick}! {HELP}")).await?;

    let result = chat_loop(&mut lines, &mut writer, &mut rx, shared, peer, &mut nick).await;
    shared.broadcast(peer, format!("* {nick} left"));
    result
}

async fn chat_loop(
    lines: &mut Lines<BufReader<OwnedReadHalf>>,
    writer: &mut OwnedWriteHalf,
    rx: &mut broadcast::Receiver<Message>,
    shared: &Shared,
    peer: SocketAddr,
    nick: &mut String,
) -> std::io::Result<()> {
    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else { return Ok(()) }; // client disconnected
                match parse(&line) {
                    Input::Blank => {}
                    Input::Say(text) => shared.broadcast(peer, format!("{nick}: {text}")),
                    Input::Who => {
                        let names = shared.names();
                        send_line(writer, &format!("{} online: {}", names.len(), names.join(", "))).await?;
                    }
                    Input::Nick(new) => match validate_nick(new) {
                        Ok(new) if new == nick.as_str() => send_line(writer, &format!("You are already {new}.")).await?,
                        Ok(new) if shared.claim(peer, new) => {
                            shared.broadcast(peer, format!("* {nick} is now {new}"));
                            send_line(writer, &format!("You are now {new}.")).await?;
                            *nick = new.to_string();
                        }
                        Ok(new) => send_line(writer, &format!("{new} is taken.")).await?,
                        Err(why) => send_line(writer, &why).await?,
                    },
                    Input::Help => send_line(writer, HELP).await?,
                    Input::Unknown(cmd) => send_line(writer, &format!("Unknown command /{cmd}. {HELP}")).await?,
                    Input::Quit => {
                        send_line(writer, "Bye!").await?;
                        return Ok(());
                    }
                }
            }
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

    #[test]
    fn parses_messages_and_commands() {
        assert_eq!(parse("hello there"), Input::Say("hello there"));
        assert_eq!(parse("   "), Input::Blank);
        assert_eq!(parse("/who"), Input::Who);
        assert_eq!(parse("/WHO"), Input::Who);
        assert_eq!(parse("/nick  zaid "), Input::Nick("zaid"));
        assert_eq!(parse("/nick"), Input::Nick(""));
        assert_eq!(parse("/quit"), Input::Quit);
        assert_eq!(parse("/help"), Input::Help);
        assert_eq!(parse("/dance now"), Input::Unknown("dance"));
    }

    #[test]
    fn claim_is_unique_and_case_insensitive() {
        let (tx, _) = broadcast::channel(1);
        let shared = Shared {
            tx,
            users: Arc::default(),
        };
        let a: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:2".parse().unwrap();
        assert!(shared.claim(a, "zaid"));
        assert!(!shared.claim(b, "ZAID"));
        assert!(shared.claim(a, "Zaid"), "renaming yourself is fine");
        shared.release(a);
        assert!(shared.claim(b, "zaid"));
        assert_eq!(shared.names(), vec!["zaid".to_string()]);
    }
}
