use ps2bq::mysql::{Connection, ConnectionOptions};
use std::{env, io};

#[tokio::test]
async fn test_connection_server_info() {
    let mut conn = setup_connection().await.unwrap();
    assert!(conn.ping().await.is_ok());
    conn.close().await.unwrap();
}

async fn setup_connection() -> io::Result<Connection> {
    Connection::connect(ConnectionOptions {
        addr: env::var("MYSQL_ADDR")
            .unwrap_or_else(|_| "[::]:3306".to_string())
            .parse()
            .unwrap(),
        user: env::var("MYSQL_USER").unwrap_or_else(|_| "mysql".to_string()),
        password: env::var("MYSQL_PASSWORD").ok(),
        database: env::var("MYSQL_DATABASE").ok(),
        ..Default::default()
    })
    .await
}
