use std::env;
use tokio::task::JoinHandle;

use crate::mysql::{
  self,
  protocol_binlog::{BinlogEvent, BinlogEventPacket, TableMapEvent},
};

use super::pg;

#[derive(Debug)]
pub struct PostgresStream;

impl PostgresStream {
  pub fn spawn() -> (Self, JoinHandle<()>) {
    let handle = tokio::task::spawn(async move {
      let conn_pg = pg::Connection::connect(pg::ConnectionOptions {
        user: "postgres".to_string(),
        password: Some("postgres".to_string()),
        database: Some("test".to_string()),
        ..Default::default()
      })
      .await
      .unwrap();

      tokio::signal::ctrl_c().await.ok();

      conn_pg.close().await.unwrap();
      println!("pg closed");
    });
    (Self, handle)
  }
}

#[derive(Debug)]
pub struct MysqlStream;

impl MysqlStream {
  pub fn spawn() -> (Self, JoinHandle<()>) {
    let handle = tokio::task::spawn(async move {
      let mut conn_mysql = mysql::Connection::connect(mysql::ConnectionOptions {
        user: "mysql".to_string(),
        password: Some("mysql".to_string()),
        database: Some("test".to_string()),
        ..Default::default()
      })
      .await
      .unwrap();

      let server_id = env::var("MYSQL_SERVER_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

      let binlog_cursor = conn_mysql.binlog_cursor().await.unwrap();

      let mut stream = conn_mysql.binlog_stream(server_id, binlog_cursor).await.unwrap();

      let interrupt = tokio::signal::ctrl_c();
      tokio::pin!(interrupt);

      let mut table_map_event = None;
      loop {
        tokio::select! {
            Ok(_) = &mut interrupt => break,
            evt = stream.recv() => {
                match evt {
                    Ok(BinlogEventPacket { event, .. }) => print_binlog_event(&mut table_map_event, event),
                    Err(err) => eprintln!("binlog stream error: {:?}", err),
                }
            },
        }
      }

      stream.close().await.ok();
      println!("mysql closed");
    });
    (Self, handle)
  }
}

fn print_binlog_event(table_map_event: &mut Option<TableMapEvent>, event: BinlogEvent) {
  match event {
    BinlogEvent::TableMap(v) => {
      table_map_event.replace(v);
    }
    v @ (BinlogEvent::Insert(_) | BinlogEvent::Update(_) | BinlogEvent::Delete(_)) => {
      println!("{:?}", (table_map_event.take().unwrap(), v));
    }
    _ => {}
  }
}
