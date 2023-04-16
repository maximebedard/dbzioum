use dbzioum::{sink, stream};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
  let (sender, receiver) = mpsc::channel(32);
  let (_pg_stream, pg_stream_handle) = stream::pg::RowEventStream::spawn(sender.clone());
  let (_mysql_stream, mysql_stream_handle) = stream::mysql::RowEventStream::spawn(sender);
  let (_console_sink, console_sink_handle) = sink::stdout::RowEventSink::spawn(receiver);

  tokio::try_join!(pg_stream_handle, mysql_stream_handle, console_sink_handle).ok();
}
