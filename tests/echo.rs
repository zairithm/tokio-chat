use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn echoes_a_line() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(tokio_chat::run(listener));

    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, mut writer) = stream.into_split();
    writer.write_all(b"hello\n").await.unwrap();

    let mut lines = BufReader::new(reader).lines();
    assert_eq!(lines.next_line().await.unwrap().as_deref(), Some("hello"));
}
