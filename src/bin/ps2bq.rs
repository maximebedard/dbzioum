use ps2bq::http::HttpServer;
use ps2bq::sink::BigQuerySink;
use ps2bq::stream::{MysqlStream, PostgresStream};

#[tokio::main]
async fn main() {
  let (_http, http_handle) = HttpServer::spawn();
  let (_pg_stream, pg_stream_handle) = PostgresStream::spawn();
  let (_mysql_stream, mysql_stream_handle) = MysqlStream::spawn();
  let (_sink, sink_handle) = BigQuerySink::spawn();

  tokio::try_join!(http_handle, pg_stream_handle, mysql_stream_handle, sink_handle).ok();
}
