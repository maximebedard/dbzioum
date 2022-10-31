use ps2bq::http::HttpServer;
use ps2bq::pg::{Connection, ConnectionOptions};
use ps2bq::sink::BigQuerySink;
use ps2bq::stream::PostgresStream;

#[tokio::main]
async fn main() {
    let conn = Connection::connect(
        "[::]:5432".parse().unwrap(),
        ConnectionOptions {
            user: "maximebedard".to_string(),
            database: Some("zapper".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let (http, http_handle) = HttpServer::spawn();
    let (stream, stream_handle) = PostgresStream::spawn();
    let (sink, sink_handle) = BigQuerySink::spawn();

    tokio::select! {
        _ = http_handle => {},
        _ = stream_handle => {},
        _ = sink_handle => {},
    }
}
