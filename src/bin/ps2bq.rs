use ps2bq::http::HttpServer;
use ps2bq::pg::{Connection, ConnectionOptions};
use ps2bq::sink::BigQuerySink;
use ps2bq::stream::PostgresStream;

#[tokio::main]
async fn main() {
    let _conn = Connection::connect(ConnectionOptions {
        user: "maximebedard".to_string(),
        database: Some("zapper".to_string()),
        ..Default::default()
    })
    .await
    .unwrap();

    let (_http, http_handle) = HttpServer::spawn();
    let (_stream, stream_handle) = PostgresStream::spawn();
    let (_sink, sink_handle) = BigQuerySink::spawn();

    tokio::select! {
        _ = http_handle => {},
        _ = stream_handle => {},
        _ = sink_handle => {},
    }
}
