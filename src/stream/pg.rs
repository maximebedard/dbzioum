use std::time::Duration;

use tokio::{sync::mpsc, task::JoinHandle};

use crate::{pg, sink::RowEvent};

#[derive(Debug)]
pub struct RowEventStream;

impl RowEventStream {
  pub fn spawn(sender: mpsc::Sender<RowEvent>) -> (Self, JoinHandle<()>) {
    let handle = tokio::task::spawn(async move {
      let conn_pg = pg::Connection::connect(pg::ConnectionOptions {
        user: "postgres".to_string(),
        password: Some("postgres".to_string()),
        database: Some("test".to_string()),
        ..Default::default()
      })
      .await
      .unwrap();

      let wal_cursor = "asd/123".parse::<pg::WalCursor>().unwrap();
      let mut stream = conn_pg.start_replication_stream("foo", wal_cursor).await.unwrap();

      let interrupt = tokio::signal::ctrl_c();
      tokio::pin!(interrupt);

      // default healthcheck is configured to 10s.
      let mut interval = tokio::time::interval(Duration::from_secs(10));

      loop {
        tokio::select! {
          Ok(_) = &mut interrupt => break,
          event = stream.recv() => {
            match event {
              Some(Ok(event)) => {
                if let Some(event) = map_replication_event(event) {
                  sender.send(event).await.unwrap();
                }
                stream.commit().await.unwrap();
              },
              Some(Err(err)) => panic!("{}", err),
              None => break,
            }
          },
          _ = interval.tick() => {
            stream.write_status_update().await.unwrap();
          },
        }
      }

      stream.close().await.unwrap();
      println!("pg closed");
    });
    (Self, handle)
  }
}

fn map_replication_event(event: pg::ReplicationEvent) -> Option<RowEvent> {
  match event {
    pg::ReplicationEvent::Data { data_change, .. } => match data_change {
      pg::DataChange::Insert { schema, table, columns } => Some(RowEvent::Insert {
        schema,
        table,
        columns: vec![],
      }),
      pg::DataChange::Update {
        schema,
        table,
        columns,
        identity,
      } => Some(RowEvent::Update {
        schema,
        table,
        columns: vec![],
        identity: vec![],
      }),
      pg::DataChange::Delete {
        schema,
        table,
        identity,
      } => Some(RowEvent::Delete {
        schema,
        table,
        identity: vec![],
      }),
      pg::DataChange::Message { .. } => None,
      pg::DataChange::Truncate { .. } => None,
      pg::DataChange::Begin => None,
      pg::DataChange::Commit => None,
    },
    pg::ReplicationEvent::KeepAlive { .. } => None,
  }
}
