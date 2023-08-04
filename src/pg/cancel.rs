use std::io;

use tokio::io::AsyncWriteExt;

use super::stream::Stream;

#[derive(Debug)]
pub struct CancelHandle {
  stream: Stream,
  pid: i32,
  secret_key: i32,
}

impl CancelHandle {
  pub(crate) fn new(stream: Stream, pid: i32, secret_key: i32) -> Self {
    Self {
      stream,
      pid,
      secret_key,
    }
  }

  pub async fn cancel(mut self) -> io::Result<()> {
    self.stream.write_i32(16).await?;
    self.stream.write_i32(80877102).await?;
    self.stream.write_i32(self.pid).await?;
    self.stream.write_i32(self.secret_key).await?;
    self.stream.shutdown().await
  }
}
