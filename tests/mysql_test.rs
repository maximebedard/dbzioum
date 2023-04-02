use ps2bq::mysql::{protocol_binlog::BinlogEvent, Connection, ConnectionOptions};
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
    let results = conn
        .query("SELECT 1,2,NULL UNION ALL SELECT 4,5,6")
        .await
        .unwrap();
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

    let mut stream = setup_connection()
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
    conn.query("CREATE TABLE Users (id int, name varchar(255));")
        .await
        .unwrap();
    conn.query("INSERT INTO Users VALUES (1, 'bob');")
        .await
        .unwrap();

    let binlog_status = conn.binlog_cursor().await.unwrap();

    let mut events = vec![];
    loop {
        if stream.binlog_cursor() >= &binlog_status {
            break;
        }
        events.push(stream.recv().await.ok().unwrap());
    }

    events
        .iter()
        .filter_map(|packet| match &packet.event {
            BinlogEvent::Insert(v) => Some(v),
            _ => None,
        })
        .for_each(|v| println!("{:?}", v));
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
