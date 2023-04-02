use ps2bq::pg::{Connection, ConnectionOptions};
use std::{env, io};

#[tokio::test]
async fn test_ping() {
  let mut conn = setup_connection().await.unwrap();
  assert!(conn.ping().await.is_ok());
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_connection_server_info() {
  let mut conn = setup_connection().await.unwrap();

  let result = conn.identify_system().await.unwrap();
  assert_eq!(result.values.last().unwrap(), &env::var("POSTGRES_DATABASE").ok());

  let result = conn.simple_query("SHOW SERVER_VERSION").await.unwrap();
  assert_eq!(
    result.values.first().unwrap().as_ref().map(String::as_str),
    Some("14.7 (Debian 14.7-1.pgdg110+1)")
  );

  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_query() {
  let mut conn = setup_connection().await.unwrap();
  let results = conn.simple_query("SELECT 1,2,3 UNION ALL SELECT 4,5,6").await.unwrap();
  assert_eq!(results.columns.len(), 3);
  assert_eq!(results.values.len(), 6);
  assert_eq!(results.rows_len(), 2);
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_noop_query() {
  let mut conn = setup_connection().await.unwrap();
  let results = conn.simple_query("NULL").await.unwrap();
  assert_eq!(results.columns.len(), 0);
  assert_eq!(results.values.len(), 0);
  assert_eq!(results.rows_len(), 0);
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_connection_replication() {
  let mut conn = setup_connection().await.unwrap();
  conn.create_replication_slot("foo").await.unwrap();
  assert!(conn.replication_slot_exists("foo").await.unwrap());

  conn.delete_replication_slot("foo").await.unwrap();
  assert!(!conn.replication_slot_exists("foo").await.unwrap());
  conn.close().await.unwrap();
}

async fn setup_connection() -> io::Result<Connection> {
  Connection::connect(ConnectionOptions {
    addr: env::var("POSTGRES_ADDR")
      .unwrap_or_else(|_| "[::]:5432".to_string())
      .parse()
      .unwrap(),
    user: env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string()),
    password: env::var("POSTGRES_PASSWORD").ok(),
    database: env::var("POSTGRES_DATABASE").ok(),
    ..Default::default()
  })
  .await
}
