use dbzioum::pg::{
  conn::{Connection, ConnectionOptions},
  openssl,
  query::{CreateReplicationSlot, IdentifySystem},
  wal::ReplicationEvent,
};
use std::{net::SocketAddr, time::Duration};

#[tokio::test]
async fn test_ping_user_postgres() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  assert!(conn.ping().await.is_ok());
  conn.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "ssl")]
async fn test_ping_user_ssl() {
  let mut ssl_connector_builder = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
  ssl_connector_builder.set_ca_file("scripts/pg/server.crt").unwrap();

  let mut conn = Connection::connect_ssl(
    default_addrs(),
    "localhost",
    ConnectionOptions {
      user: "ssl_user".to_string(),
      ..default_connection_options()
    },
    ssl_connector_builder.build(),
  )
  .await
  .unwrap();
  assert!(conn.ping().await.is_ok());
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_ping_user_md5() {
  let mut conn = Connection::connect_tcp(
    default_addrs(),
    ConnectionOptions {
      user: "md5_user".to_string(),
      ..default_connection_options()
    },
  )
  .await
  .unwrap();
  assert!(conn.ping().await.is_ok());
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_ping_user_md5_invalid_password() {
  let err = Connection::connect_tcp(
    default_addrs(),
    ConnectionOptions {
      user: "md5_user".to_string(),
      password: Some("invalid".to_string()),
      ..default_connection_options()
    },
  )
  .await
  .unwrap_err();

  assert_eq!(
    "Server error 28P01: password authentication failed for user \"md5_user\"",
    err.to_string()
  );
}

#[tokio::test]
async fn test_ping_user_scram() {
  let mut conn = Connection::connect_tcp(
    default_addrs(),
    ConnectionOptions {
      user: "scram_user".to_string(),
      ..default_connection_options()
    },
  )
  .await
  .unwrap();
  assert!(conn.ping().await.is_ok());
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_ping_user_scram_invalid_password() {
  let err = Connection::connect_tcp(
    default_addrs(),
    ConnectionOptions {
      user: "scram_user".to_string(),
      password: Some("invalid".to_string()),
      ..default_connection_options()
    },
  )
  .await
  .unwrap_err();

  assert_eq!(
    "Server error 28P01: password authentication failed for user \"scram_user\"",
    err.to_string()
  );
}

#[tokio::test]
async fn test_ping_user_pass() {
  let mut conn = Connection::connect_tcp(
    default_addrs(),
    ConnectionOptions {
      user: "pass_user".to_string(),
      ..default_connection_options()
    },
  )
  .await
  .unwrap();
  assert!(conn.ping().await.is_ok());
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_ping_user_pass_invalid_password() {
  let err = Connection::connect_tcp(
    default_addrs(),
    ConnectionOptions {
      user: "pass_user".to_string(),
      password: Some("invalid".to_string()),
      ..default_connection_options()
    },
  )
  .await
  .unwrap_err();

  assert_eq!(
    "Server error 28P01: password authentication failed for user \"pass_user\"",
    err.to_string()
  );
}

#[tokio::test]
async fn test_password_encryption_sanity_check() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  let result = conn
    .query_first("SHOW PASSWORD_ENCRYPTION;")
    .await
    .unwrap()
    .as_selected_query_result()
    .unwrap();

  assert_eq!(
    result.row(0).first().unwrap().as_ref().map(String::as_str),
    Some("scram-sha-256")
  );
  conn.close().await.unwrap()
}

#[tokio::test]
async fn test_connection_server_info() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();

  let IdentifySystem { dbname, .. } = conn.identify_system().await.unwrap();
  assert_eq!(dbname, Some("test".to_string()));

  let result = conn
    .query_first("SHOW SERVER_VERSION;")
    .await
    .unwrap()
    .as_selected_query_result()
    .unwrap();

  assert_eq!(
    result.row(0).first().unwrap().as_ref().map(String::as_str),
    Some("14.7 (Debian 14.7-1.pgdg110+1)")
  );

  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_query() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  let results = conn
    .query_first("SELECT 1,2,3 UNION ALL SELECT 4,5,6;")
    .await
    .unwrap()
    .as_selected_query_result()
    .unwrap();

  assert_eq!(results.columns_len(), 3);
  assert_eq!(results.rows_len(), 2);
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_empty_query() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  let mut results = conn.query(";").await.unwrap();

  assert!(results.results.pop_front().unwrap().is_successful());
  assert!(conn.query_first(";").await.unwrap().is_successful());

  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_error_query() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  assert_eq!(
    "Server error 22012: division by zero",
    conn
      .query("SELECT 1/0;")
      .await
      .unwrap()
      .results
      .pop_front()
      .unwrap()
      .as_backend_error()
      .unwrap()
      .to_string(),
  );
  assert_eq!(
    "Server error 22012: division by zero",
    conn
      .query_first("SELECT 1/0;")
      .await
      .unwrap()
      .as_backend_error()
      .unwrap()
      .to_string()
  );
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_multiple_queries() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  let mut results = conn.query("SELECT 1; SELECT 2, 3;").await.unwrap();

  let result = results.results.pop_front().unwrap().as_selected_query_result().unwrap();
  assert_eq!(1, result.columns_len());
  assert_eq!(1, result.rows_len());

  let result = results.results.pop_front().unwrap().as_selected_query_result().unwrap();
  assert_eq!(2, result.columns_len());
  assert_eq!(1, result.rows_len());

  assert!(results.results.is_empty());

  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_multiple_queries_interleaved_with_errors() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  let mut results = conn.query("SELECT 1; SELECT 1/0; SELECT 2;").await.unwrap();

  let result = results.results.pop_front().unwrap().as_selected_query_result().unwrap();
  assert_eq!(1, result.columns_len());
  assert_eq!(1, result.rows_len());

  let result = results.results.pop_front().unwrap().as_backend_error().unwrap();
  assert_eq!("Server error 22012: division by zero", result.to_string());

  assert!(results.results.is_empty());

  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_multiple_transactions() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  conn
    .query_first("CREATE TABLE IF NOT EXISTS Test (i int);")
    .await
    .unwrap();

  let mut results = conn
    .query(
      r#"
  BEGIN;
  INSERT INTO Test VALUES(1);
  COMMIT;
  INSERT INTO Test VALUES(2);
  SELECT 1/0;
"#,
    )
    .await
    .unwrap();

  assert_eq!(5, results.results.len());

  let result = results.results.pop_back().unwrap().as_backend_error().unwrap();
  assert_eq!("Server error 22012: division by zero", result.to_string());

  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_connection_replication() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
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
async fn test_query_cancellation() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  let cancel_handle = conn.cancel_handle().await.unwrap();
  tokio::task::spawn(async move {
    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel_handle.cancel().await.unwrap();
  });

  assert_eq!(
    "Server error 57014: canceling statement due to user request",
    conn
      .query_first("SELECT pg_sleep(1000);")
      .await
      .unwrap()
      .as_backend_error()
      .unwrap()
      .to_string()
  );
}

#[tokio::test]
async fn test_connection_replication_inserts() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();

  if conn.replication_slot_exists("bar").await.unwrap() {
    conn.delete_replication_slot("bar").await.unwrap();
    assert!(!conn.replication_slot_exists("bar").await.unwrap());
  }

  let CreateReplicationSlot {
    slot_name,
    consistent_point,
    ..
  } = conn.create_replication_slot("bar").await.unwrap();
  let mut commited = consistent_point.clone();

  let mut stream = conn
    .duplicate()
    .await
    .unwrap()
    .start_replication_stream(slot_name, consistent_point)
    .await
    .unwrap();

  match conn.query("DROP TABLE Users;").await {
    Err(err) if err.to_string().contains("table \"users\" does not exist") => {}
    Err(err) => panic!("{}", err),
    Ok(_) => {}
  }
  conn
    .query_first("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));")
    .await
    .unwrap();
  conn.query_first("TRUNCATE Users;").await.unwrap();
  conn.query_first("INSERT INTO Users VALUES (1, 'bob');").await.unwrap();
  conn.query_first("INSERT INTO Users VALUES (2, 'yan');").await.unwrap();
  conn.query_first("DELETE FROM Users WHERE id = 2;").await.unwrap();
  conn
    .query_first("UPDATE Users SET name = 'chad' WHERE id = 1;")
    .await
    .unwrap();

  let IdentifySystem { wal_cursor, .. } = conn.identify_system().await.unwrap();

  // default healthcheck is configured to 10s.
  let mut interval = tokio::time::interval(Duration::from_secs(10));

  loop {
    // Wait for the stream to have caught up with the master
    if commited >= wal_cursor {
      break;
    }

    tokio::select! {
      event = stream.recv() => {
        match event {
          Some(Ok(ReplicationEvent::Data { end, .. })) => {
            commited.lsn = end;
            // TODO: do something here...
            println!("{:?}", event);
          },
          Some(Ok(ReplicationEvent::ChangeTimeline { tid, lsn })) => {
            commited.tid = tid;
            commited.lsn = lsn;
          }
          Some(Ok(ReplicationEvent::KeepAlive { end, .. })) => {
            commited.lsn = end;
          },
          Some(Err(err)) => panic!("{}", err),
          None => break,
        }
      },
      _ = interval.tick() => {
        stream.write_status_update(commited.lsn).await.unwrap();
      },
    }
  }

  tokio::try_join!(stream.close(), conn.close()).unwrap();
}

fn default_addrs() -> Vec<SocketAddr> {
  vec!["[::]:5432".parse::<SocketAddr>().unwrap()]
}

fn default_connection_options() -> ConnectionOptions {
  ConnectionOptions {
    password: Some("password".to_string()),
    database: Some("test".to_string()),
    ..Default::default()
  }
}
