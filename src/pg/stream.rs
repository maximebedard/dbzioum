use std::{
  io,
  net::SocketAddr,
  path::PathBuf,
  pin::Pin,
  task::{Context, Poll},
};

use bytes::BytesMut;
use tokio::{
  io::{AsyncRead, AsyncWrite, BufStream, ReadBuf},
  net::{TcpStream, UnixStream},
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(feature = "ssl")]
use tokio_openssl::SslStream;

#[cfg(feature = "ssl")]
use openssl::ssl::SslConnector;

const SSL_HANDSHAKE_CODE: i32 = 80877103;

#[derive(Debug)]
pub enum Stream {
  Tcp((BufStream<TcpStream>, Vec<SocketAddr>)),

  Unix((BufStream<UnixStream>, PathBuf)),

  #[cfg(feature = "ssl")]
  Ssl((SslStream<BufStream<TcpStream>>, Vec<SocketAddr>, String, SslConnector)),
}

impl Stream {
  pub async fn connect_tcp(addrs: impl Into<Vec<SocketAddr>>) -> io::Result<Self> {
    let addrs = addrs.into();
    let s = TcpStream::connect(addrs.as_slice()).await.map(BufStream::new)?;
    Ok(Self::Tcp((s, addrs)))
  }

  pub async fn connect_unix(path: impl Into<PathBuf>) -> io::Result<Self> {
    let path = path.into();
    let s = UnixStream::connect(&path).await.map(BufStream::new)?;
    Ok(Self::Unix((s, path)))
  }

  #[cfg(feature = "ssl")]
  pub async fn connect_ssl(
    addrs: impl Into<Vec<SocketAddr>>,
    domain: impl Into<String>,
    ssl_connector: SslConnector,
  ) -> io::Result<Self> {
    let addrs = addrs.into();
    let domain = domain.into();
    let mut s = TcpStream::connect(addrs.as_slice()).await.map(BufStream::new)?;

    s.write_i32(8).await?;
    s.write_i32(SSL_HANDSHAKE_CODE).await?;
    s.flush().await?;

    match s.read_u8().await? {
      b'S' => {
        // https://www.postgresql.org/docs/11/protocol-flow.html#id-1.10.5.7.11
        let connect_configuration = ssl_connector
          .configure()
          .map_err(|err| io::Error::new(io::ErrorKind::Other, "Failed to create SSL configuration"))?;

        let ssl = connect_configuration
          .into_ssl(domain.as_str())
          .map_err(|err| io::Error::new(io::ErrorKind::Other, "Failed to create SSL context"))?;

        let mut ssl_stream =
          SslStream::new(ssl, s).map_err(|err| io::Error::new(io::ErrorKind::Other, "Failed to create SSL stream"))?;

        Pin::new(&mut ssl_stream)
          .connect()
          .await
          .map_err(|err| io::Error::new(io::ErrorKind::ConnectionRefused, err.to_string()))?;

        Ok(Self::Ssl((ssl_stream, addrs, domain, ssl_connector)))
      }
      b'N' => Err(io::Error::new(io::ErrorKind::ConnectionReset, "SSL not available")),
      code => {
        panic!("Unexpected backend message: {:?}", char::from(code))
      }
    }
  }

  pub async fn read_packet(&mut self) -> io::Result<(u8, bytes::Bytes)> {
    let op = self.read_u8().await?;
    let len = (self.read_i32().await? - 4).try_into().unwrap();
    let mut buffer = BytesMut::with_capacity(len);
    if len > 0 {
      self.read_buf(&mut buffer).await?;
    }
    Ok((op, buffer.freeze()))
  }

  pub async fn duplicate(&self) -> io::Result<Self> {
    match self {
      Stream::Tcp((_, addrs)) => Self::connect_tcp(addrs.clone()).await,
      Stream::Unix((_, path)) => Self::connect_unix(path.clone()).await,
      #[cfg(feature = "ssl")]
      Stream::Ssl((_, addrs, domain, ssl_connector)) => {
        Self::connect_ssl(addrs.clone(), domain.clone(), ssl_connector.clone()).await
      }
    }
  }
}

impl AsyncRead for Stream {
  fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Stream::Tcp((s, _)) => Pin::new(s).poll_read(cx, buf),
      Stream::Unix((s, _)) => Pin::new(s).poll_read(cx, buf),
      #[cfg(feature = "ssl")]
      Stream::Ssl((s, _, _, _)) => Pin::new(s).poll_read(cx, buf),
    }
  }
}

impl AsyncWrite for Stream {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, io::Error>> {
    match self.get_mut() {
      Stream::Tcp((s, _)) => Pin::new(s).poll_write(cx, buf),
      Stream::Unix((s, _)) => Pin::new(s).poll_write(cx, buf),
      #[cfg(feature = "ssl")]
      Stream::Ssl((s, _, _, _)) => Pin::new(s).poll_write(cx, buf),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
    match self.get_mut() {
      Stream::Tcp((s, _)) => Pin::new(s).poll_flush(cx),
      Stream::Unix((s, _)) => Pin::new(s).poll_flush(cx),
      #[cfg(feature = "ssl")]
      Stream::Ssl((s, _, _, _)) => Pin::new(s).poll_flush(cx),
    }
  }

  fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
    match self.get_mut() {
      Stream::Tcp((s, _)) => Pin::new(s).poll_shutdown(cx),
      Stream::Unix((s, _)) => Pin::new(s).poll_shutdown(cx),
      #[cfg(feature = "ssl")]
      Stream::Ssl((s, _, _, _)) => Pin::new(s).poll_shutdown(cx),
    }
  }
}
