use ps2bq::http::HttpServer;
use ps2bq::sink::BigQuerySink;
use ps2bq::stream::PostgresStream;
use ps2bq::{mysql, pg};

#[tokio::main]
async fn main() {
    let conn_pg = pg::Connection::connect(pg::ConnectionOptions {
        user: "postgres".to_string(),
        password: Some("postgres".to_string()),
        database: Some("test".to_string()),
        ..Default::default()
    })
    .await
    .unwrap();

    let conn_mysql = mysql::Connection::connect(mysql::ConnectionOptions {
        user: "mysql".to_string(),
        password: Some("mysql".to_string()),
        database: Some("test".to_string()),
        ..Default::default()
    })
    .await
    .unwrap();

    let (_http, http_handle) = HttpServer::spawn();
    let (_stream, stream_handle) = PostgresStream::spawn();
    let (_sink, sink_handle) = BigQuerySink::spawn();

    tokio::try_join!(http_handle, stream_handle, sink_handle).ok();

    conn_pg.close().await.ok();
    conn_mysql.close().await.ok();
}
