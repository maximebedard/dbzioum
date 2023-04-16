use tokio::{sync::mpsc, task::JoinHandle};

use crate::{mysql, mysql::binlog, sink::RowEvent};

#[derive(Debug)]
pub struct RowEventStream;

impl RowEventStream {
  pub fn spawn(sender: mpsc::Sender<RowEvent>) -> (Self, JoinHandle<()>) {
    let handle = tokio::task::spawn(async move {
      let mut conn_mysql = mysql::Connection::connect(mysql::ConnectionOptions {
        user: "mysql".to_string(),
        password: Some("mysql".to_string()),
        database: Some("test".to_string()),
        ..Default::default()
      })
      .await
      .unwrap();

      let server_id = 1;
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
                    Some(Ok(binlog::BinlogEventPacket { event, .. })) => print_binlog_event(&mut table_map_event, event),
                    Some(Err(err)) => eprintln!("binlog stream error: {:?}", err),
                    None => break,
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

fn print_binlog_event(table_map_event: &mut Option<binlog::TableMapEvent>, event: binlog::BinlogEvent) {
  match event {
    binlog::BinlogEvent::TableMap(v) => {
      table_map_event.replace(v);
    }
    v @ (binlog::BinlogEvent::Insert(_) | binlog::BinlogEvent::Update(_) | binlog::BinlogEvent::Delete(_)) => {
      println!("{:?}", (table_map_event.take().unwrap(), v));
    }
    _ => {}
  }
}
