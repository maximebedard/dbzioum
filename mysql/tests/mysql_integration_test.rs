use std::net::SocketAddr;

use mysql::{binlog::BinlogEvent, Connection, ConnectionOptions};

#[tokio::test]
async fn test_ping() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  assert!(conn.ping().await.is_ok());
  conn.close().await.unwrap();
}

#[tokio::test]
#[ignore = "ssl is not supported yet"]
async fn test_ping_user_sha2() {
  let mut conn = Connection::connect_tcp(
    default_addrs(),
    ConnectionOptions {
      user: "sha2_user".to_string(),
      ..default_connection_options()
    },
  )
  .await
  .unwrap();
  assert!(conn.ping().await.is_ok());
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_connection_server_info() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  let results = conn.query("SELECT version()").await.unwrap();
  assert_eq!(
    results.row(0).last().unwrap().as_ref().map(String::as_str),
    Some("8.0.32")
  );
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_query() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  let results = conn.query("SELECT 1,2,NULL UNION ALL SELECT 4,5,6").await.unwrap();
  assert_eq!(results.columns_len(), 3);
  assert_eq!(results.rows_len(), 2);
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_noop_query() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  let results = conn.query("DO NULL").await.unwrap();
  assert_eq!(results.columns_len(), 0);
  assert_eq!(results.rows_len(), 0);
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_binlog_inserts() {
  let mut conn = Connection::connect_tcp(default_addrs(), default_connection_options())
    .await
    .unwrap();
  let binlog_cursor = conn.binlog_cursor().await.unwrap();
  let mut commited = binlog_cursor.clone();

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

        n CHAR(3),
        o VARCHAR(3),

        p BINARY(3),
        q VARBINARY(3),

        r TINYBLOB,
        s TINYTEXT,

        t BLOB,
        u TEXT,

        v MEDIUMBLOB,
        w MEDIUMTEXT,

        x LONGBLOB,
        y LONGTEXT,

        z JSON,

        aa YEAR,
        ab DATE,

        ac INT,
        ad INT NOT NULL,

        -- See https://github.com/mysql/mysql-server/blob/9c3a49ec84b521cb0b35383f119099b2eb25d4ff/sql/log_event.cc#L1988-L2006
        ae CHAR(200)
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
      VALUES (1, 'bob', -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, 3.14, 3.14, b'10000001', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', '{"a": "b"}', '2024', '2024-01-01 01:01:01', NULL, 123, 'm');
      "#,
    )
    .await
    .unwrap();
  conn
    .query(
      r#"
      INSERT INTO Users
      VALUES (2, 'pat', -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, 3.14, 3.14, b'10000001', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', '{"a": "b"}', '2024', '2024-01-01 01:01:01', NULL, 123, 'm');
      "#,
    )
    .await
    .unwrap();
  conn
    .query(
      r#"
      INSERT INTO Users
      VALUES
        (3, 'lel', -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, 3.14, 3.14, b'10000001', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', '{"a": "b"}', '2024', '2024-01-01 01:01:01', NULL, 123, 'm'),
        (4, 'kek', -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, 3.14, 3.14, b'10000001', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', '{"a": "b"}', '2024', '2024-01-01 01:01:01', NULL, 123, 'm');
      "#
    )
    .await
    .unwrap();
  conn.query("DELETE FROM Users WHERE id = 2;").await.unwrap();
  conn
    .query("UPDATE Users SET name = 'chad' WHERE id = 1;")
    .await
    .unwrap();

  let cursor = conn.binlog_cursor().await.unwrap();

  let mut table_map_event = None;
  loop {
    // Wait for the stream to have caught up with the master
    if commited >= cursor {
      break;
    }

    let (header, event) = stream.recv().await.unwrap().unwrap();

    // Insert/Update/Delete are always preceded by a TableMap event.
    match event {
      BinlogEvent::TableMap(v) => {
        table_map_event.replace(v);
      }
      BinlogEvent::Rotate(v) => {
        commited.log_file = v.next_log_file.clone();
        commited.log_position = v.next_log_position;
      }
      BinlogEvent::Insert(v) => {
        let table_map_event = table_map_event.take().unwrap();
        let columns = table_map_event.columns();
        println!(
          "insert {}.{} => {:?}",
          table_map_event.schema,
          table_map_event.table,
          v.rows(&columns)
        );
      }
      BinlogEvent::Update(v) => {
        let table_map_event = table_map_event.take().unwrap();
        let columns = table_map_event.columns();
        println!(
          "update {}.{} => {:?}",
          table_map_event.schema,
          table_map_event.table,
          v.rows(&columns)
        );
      }
      BinlogEvent::Delete(v) => {
        let table_map_event = table_map_event.take().unwrap();
        let columns = table_map_event.columns();
        println!(
          "delete {}.{} => {:?}",
          table_map_event.schema,
          table_map_event.table,
          v.rows(&columns)
        );
      }
      evt => {
        println!("{:?}", evt);
      }
    }

    commited.log_position = header.log_position;
  }

  tokio::try_join!(stream.close(), conn.close()).unwrap();
}

fn default_addrs() -> Vec<SocketAddr> {
  vec!["[::]:3306".parse::<SocketAddr>().unwrap()]
}

fn default_connection_options() -> ConnectionOptions {
  ConnectionOptions {
    user: "native_user".to_string(),
    password: Some("password".to_string()),
    database: Some("test".to_string()),
    ..Default::default()
  }
}
