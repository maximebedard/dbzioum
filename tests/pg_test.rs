use ps2bq::pg::{Connection, ConnectionOptions, CreateReplicationSlot, IdentifySystem};
use std::{env, io, time::Duration};

#[tokio::test]
async fn test_ping() {
  let mut conn = setup_connection().await.unwrap();
  assert!(conn.ping().await.is_ok());
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_connection_server_info() {
  let mut conn = setup_connection().await.unwrap();

  let IdentifySystem { dbname, .. } = conn.identify_system().await.unwrap();
  assert_eq!(dbname, env::var("POSTGRES_DATABASE").ok());

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
  let results = conn.simple_query(";").await.unwrap();
  assert_eq!(results.columns.len(), 0);
  assert_eq!(results.values.len(), 0);
  assert_eq!(results.rows_len(), 0);
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_connection_replication() {
  let mut conn = setup_connection().await.unwrap();
  if conn.replication_slot_exists("foo").await.unwrap() {
    conn.delete_replication_slot("foo").await.unwrap();
    assert!(!conn.replication_slot_exists("foo").await.unwrap());
  }

  conn.create_replication_slot("foo").await.unwrap();
  assert!(conn.replication_slot_exists("foo").await.unwrap());

  conn.delete_replication_slot("foo").await.unwrap();
  assert!(!conn.replication_slot_exists("foo").await.unwrap());

  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_connection_replication_inserts() {
  let mut conn = setup_connection().await.unwrap();

  if conn.replication_slot_exists("bar").await.unwrap() {
    conn.delete_replication_slot("bar").await.unwrap();
    assert!(!conn.replication_slot_exists("bar").await.unwrap());
  }

  let CreateReplicationSlot {
    slot_name,
    consistent_point,
    ..
  } = conn.create_replication_slot("bar").await.unwrap();

  let mut stream = conn
    .duplicate()
    .await
    .unwrap()
    .start_replication_stream(slot_name, consistent_point)
    .await
    .unwrap();

  match conn.simple_query("DROP TABLE Users;").await {
    Err(err) if err.to_string().contains("table \"users\" does not exist") => {}
    Err(err) => panic!("{}", err),
    Ok(_) => {}
  }
  conn
    .simple_query("CREATE TABLE Users (id int, name varchar(255));")
    .await
    .unwrap();
  conn.simple_query("TRUNCATE Users;").await.unwrap();
  conn.simple_query("INSERT INTO Users VALUES (1, 'bob');").await.unwrap();

  let IdentifySystem { wal_cursor, .. } = conn.identify_system().await.unwrap();

  // default healthcheck is configured to 10s.
  let mut interval = tokio::time::interval(Duration::from_secs(10));

  loop {
    if let Some(commited) = stream.commited() {
      if commited >= &wal_cursor {
        break;
      }
    }

    tokio::select! {
      event = stream.recv() => {
        match event {
          Ok(event) => {
            // TODO: do some processing.
            println!("{:?}", event);
            stream.commit().await.unwrap();
          },
          Err(err) => panic!("{}", err),
        }
      },
      _ = interval.tick() => {
        stream.write_status_update().await.unwrap();
      },
    }
  }

  tokio::try_join!(stream.close(), conn.close()).unwrap();
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
