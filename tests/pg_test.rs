use ps2bq::pg::{Connection, ConnectionOptions};
use std::env;

#[tokio::test]
async fn test_connection_server_info() {
    let mut conn = setup_connection().await;
    conn.identify_system().await.unwrap();
    assert_eq!(
        conn.show("SERVER_VERSION").await.unwrap().columns[0],
        Some("11.14".to_string())
    );
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_connection_replication() {
    let mut conn = setup_connection().await;

    conn.create_replication_slot("foo").await.unwrap();
    assert!(conn.replication_slot_exists("foo").await.unwrap());

    conn.delete_replication_slot("foo").await.unwrap();
    assert!(!conn.replication_slot_exists("foo").await.unwrap());

    conn.close().await.unwrap();
}

async fn setup_connection() -> Connection {
    Connection::connect(ConnectionOptions {
        user: env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string()),
        password: env::var("POSTGRES_PASSWORD").ok(),
        database: env::var("POSTGRES_DATABASE").ok(),
        ..Default::default()
    })
    .await
    .unwrap()
}
