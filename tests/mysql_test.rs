use dbzioum::mysql::{protocol_binlog::BinlogEvent, Connection, ConnectionOptions};
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
  let results = conn.query("SELECT version()").await.unwrap();
  assert_eq!(
    results.values.last().unwrap().as_ref().map(String::as_str),
    Some("8.0.32")
  );
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_query() {
  let mut conn = setup_connection().await.unwrap();
  let results = conn.query("SELECT 1,2,NULL UNION ALL SELECT 4,5,6").await.unwrap();
  assert_eq!(results.columns.len(), 3);
  assert_eq!(results.values.len(), 6);
  assert_eq!(results.rows_len(), 2);
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_noop_query() {
  let mut conn = setup_connection().await.unwrap();
  let results = conn.query("DO NULL").await.unwrap();
  assert_eq!(results.columns.len(), 0);
  assert_eq!(results.values.len(), 0);
  assert_eq!(results.rows_len(), 0);
  conn.close().await.unwrap();
}

#[tokio::test]

async fn test_binlog_inserts() {
  let mut conn = setup_connection().await.unwrap();
  let binlog_cursor = conn.binlog_cursor().await.unwrap();

  let mut stream = conn
    .duplicate()
    .await
    .unwrap()
    .binlog_stream(1, binlog_cursor)
    .await
    .unwrap();

  match conn.query("DROP TABLE Users;").await {
    Err(err) if err.to_string().contains("Unknown table 'test.Users'") => {}
    Err(err) => panic!("{}", err),
    Ok(_) => {}
  }

  conn
    .query(
      r#"
      CREATE TABLE Users (
        id INT PRIMARY KEY,

        name VARCHAR(255),

        a TINYINT,
        b TINYINT UNSIGNED,

        c SMALLINT,
        d SMALLINT UNSIGNED,

        e MEDIUMINT,
        f MEDIUMINT UNSIGNED,

        g INT,
        h INT UNSIGNED,

        i BIGINT NOT NULL,
        j BIGINT UNSIGNED,

        k FLOAT,
        l DOUBLE,

        m BIT(64),

        n INT,

        o CHAR(3),

        p BINARY(3),
        q VARBINARY(3),

        r BLOB,
        s TEXT,

        t JSON,

        u YEAR,
        v DATE
      );
    "#,
    )
    .await
    .unwrap();
  conn.query("TRUNCATE Users;").await.unwrap();
  conn
    .query(
      r#"
    INSERT INTO Users
    VALUES (1, 'bob', -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, 3.14, 3.14, b'10000001', NULL, 'a', 'b', 'c', 'd', 'e', '{"a": "b"}', '2024', '2024-01-01 01:01:01');
    "#,
    )
    .await
    .unwrap();
  // conn
  //   .query(
  //     r#"
  //   INSERT INTO Users
  //   VALUES (2, 'pat', -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, 3.14, 3.14, b'10000001', NULL, 'a', 'b', 'c', 'd', 'e', '{"a": "b"}', '2024');
  //   "#,
  //   )
  //   .await
  //   .unwrap();
  // conn.query("DELETE FROM Users WHERE id = 2;").await.unwrap();
  // conn
  //   .query("UPDATE Users SET name = 'chad' WHERE id = 1;")
  //   .await
  //   .unwrap();

  let cursor = conn.binlog_cursor().await.unwrap();

  let mut table_map_evt = None;
  let mut events = vec![];
  loop {
    // Wait for the stream to have caught up with the master
    if let Some(commited) = stream.commited() {
      if commited >= &cursor {
        break;
      }
    }

    let packet = stream.recv().await.unwrap().unwrap();

    // Insert/Update/Delete are always preceded by a TableMap event.
    match packet.event {
      BinlogEvent::TableMap(v) => {
        table_map_evt.replace(v);
      }
      v @ (BinlogEvent::Insert(_) | BinlogEvent::Update(_) | BinlogEvent::Delete(_)) => {
        events.push((table_map_evt.take().unwrap(), v));
      }
      _ => {}
    }

    stream.commit().await.unwrap();
  }

  if let (z, BinlogEvent::Insert(v)) = &events[0] {
    let columns = z.columns();
    println!("{:#?}", &columns);
    println!("{:?}", v.values(&columns));
  }
  // println!("{:#?}", events);

  tokio::try_join!(stream.close(), conn.close()).unwrap();
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
