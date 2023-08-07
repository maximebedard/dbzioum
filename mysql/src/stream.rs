use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  task::{Context, Poll},
};

use tokio::{
  io::{AsyncRead, AsyncWrite, BufStream, ReadBuf},
  net::TcpStream,
};

#[cfg(feature = "ssl")]
use openssl::ssl::SslConnector;

#[cfg(feature = "ssl")]
use tokio_openssl::SslStream;

#[derive(Debug)]
pub enum Stream {
  Tcp((BufStream<TcpStream>, Vec<SocketAddr>)),
  #[cfg(feature = "ssl")]
  Ssl(SslStream<BufStream<TcpStream>>),
}

impl Stream {
  pub async fn connect_tcp(addrs: impl Into<Vec<SocketAddr>>) -> io::Result<Self> {
    let addrs = addrs.into();
    let s = TcpStream::connect(addrs.as_slice()).await.map(BufStream::new)?;
    Ok(Self::Tcp((s, addrs)))
  }

  pub async fn duplicate(&self) -> io::Result<Self> {
    match self {
      Stream::Tcp((_, addrs)) => Self::connect_tcp(addrs.clone()).await,
      #[cfg(feature = "ssl")]
      Stream::Ssl(_) => todo!(),
    }
  }

  #[cfg(feature = "ssl")]
  async fn into_ssl(self, domain: impl Into<String>, ssl_connector: SslConnector) -> io::Result<Self> {
    match self {
      Stream::Tcp((s, _addrs)) => {
        let domain = domain.into();

        let connect_configuration = ssl_connector
          .configure()
          .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to create SSL configuration"))?;

        let ssl = connect_configuration
          .into_ssl(domain.as_str())
          .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to create SSL context"))?;

        let mut ssl_stream =
          SslStream::new(ssl, s).map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to create SSL stream"))?;

        Pin::new(&mut ssl_stream)
          .connect()
          .await
          .map_err(|err| io::Error::new(io::ErrorKind::ConnectionRefused, err.to_string()))?;

        Ok(Self::Ssl(ssl_stream))
      }
      s @ Stream::Ssl(_) => Ok(s),
    }
  }
}

impl AsyncRead for Stream {
  fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Stream::Tcp((s, _)) => Pin::new(s).poll_read(cx, buf),
      #[cfg(feature = "ssl")]
      Stream::Ssl(s) => Pin::new(s).poll_read(cx, buf),
    }
  }
}

impl AsyncWrite for Stream {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Stream::Tcp((s, _)) => Pin::new(s).poll_write(cx, buf),
      #[cfg(feature = "ssl")]
      Stream::Ssl(s) => Pin::new(s).poll_write(cx, buf),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Stream::Tcp((s, _)) => Pin::new(s).poll_flush(cx),
      #[cfg(feature = "ssl")]
      Stream::Ssl(s) => Pin::new(s).poll_flush(cx),
    }
  }

  fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Stream::Tcp((s, _)) => Pin::new(s).poll_shutdown(cx),
      #[cfg(feature = "ssl")]
      Stream::Ssl(s) => Pin::new(s).poll_shutdown(cx),
    }
  }
}
