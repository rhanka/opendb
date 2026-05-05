use anyhow::Context;
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub async fn serve(addr: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind health listener on {addr}"))?;
    tracing::info!(%addr, "health listener ready");

    loop {
        let (stream, peer) = listener.accept().await.context("accept health client")?;
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream).await {
                tracing::debug!(%peer, %error, "health connection failed");
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut request = [0_u8; 1024];
    let _ = stream.read(&mut request).await?;
    stream
        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 3\r\nconnection: close\r\n\r\nok\n")
        .await?;
    stream.shutdown().await?;
    Ok(())
}
