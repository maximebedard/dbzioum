use std::{io, time::Duration};

use clap::{Arg, Command};
use tokio::sync::mpsc;
use url::Url;

use dbzioum::{
  pg::{self, ColumnChange, ReplicationEvent, WalCursor},
  sink::{Column, ColumnType, ColumnValue, RowEvent},
};

#[tokio::main]
async fn main() {
  let mut cmd = Command::new("pg")
    .version("1.0")
    .author("Maxime Bedard <maxime@bedard.dev>")
    .arg(Arg::new("url").required(true).short('u').value_parser(Url::parse))
    .arg(Arg::new("slot").required(true))
    .arg(Arg::new("wal-cursor").value_parser(str::parse::<WalCursor>));

  let matches = cmd.get_matches_mut();

  let url = matches.get_one::<Url>("url").unwrap();
  let slot = matches.get_one::<String>("slot").unwrap();
  let wal_cursor = matches.get_one::<WalCursor>("wal-cursor").cloned();

  let mut conn_pg = pg::Connection::connect_from_url(url).await.unwrap();

  let wal_cursor = match wal_cursor {
    Some(wal_cursor) => wal_cursor,
    None => conn_pg.identify_system().await.unwrap().wal_cursor,
  };

  let mut stream = conn_pg
    .start_replication_stream(slot, wal_cursor.clone())
    .await
    .unwrap();

  let (sender, mut receiver) = mpsc::channel(32);
  tokio::task::spawn(async move {
    while let Some(evt) = receiver.recv().await {
      println!("{:?}", evt);
    }
  });

  let mut processor = EventProcessor {
    wal_cursor,
    sink: sender,
  };

  let interrupt = tokio::signal::ctrl_c();
  tokio::pin!(interrupt);

  // default healthcheck is configured to 10s.
  let mut interval = tokio::time::interval(Duration::from_secs(10));

  loop {
    tokio::select! {
      Ok(_) = &mut interrupt => break,
      event = stream.recv() => {
        match event {
          Some(Ok(event)) => processor.process_event(event).await.unwrap(),
          Some(Err(err)) => panic!("{}", err),
          None => break,
        }
      },
      _ = interval.tick() => {
        stream.write_status_update(processor.wal_cursor.lsn).await.unwrap();
      },
    }
  }

  stream.close().await.unwrap();
}

struct EventProcessor {
  wal_cursor: WalCursor,
  sink: mpsc::Sender<RowEvent>,
}

impl EventProcessor {
  async fn process_event(&mut self, event: ReplicationEvent) -> io::Result<()> {
    match event {
      ReplicationEvent::Data { end, data_change, .. } => {
        if let Some(event) = map_data_change(data_change) {
          self.sink.send(event).await.unwrap();
        }

        self.wal_cursor.lsn = end;
      }
      ReplicationEvent::KeepAlive { end, .. } => {
        self.wal_cursor.lsn = end;
      }
      ReplicationEvent::ChangeTimeline { tid, lsn } => {
        self.wal_cursor.tid = tid;
        self.wal_cursor.lsn = lsn;
      }
    }

    Ok(())
  }
}

fn map_data_change(data_change: pg::DataChange) -> Option<RowEvent> {
  match data_change {
    pg::DataChange::Insert { schema, table, columns } => {
      let columns = map_column_change(columns);
      Some(RowEvent::Insert { schema, table, columns })
    }
    pg::DataChange::Update {
      schema,
      table,
      columns,
      identity,
    } => {
      let columns = map_column_change(columns);
      let identity = map_column_change(identity);
      Some(RowEvent::Update {
        schema,
        table,
        columns,
        identity,
      })
    }
    pg::DataChange::Delete {
      schema,
      table,
      identity,
    } => {
      let identity = map_column_change(identity);
      Some(RowEvent::Delete {
        schema,
        table,
        identity,
      })
    }
    pg::DataChange::Message { .. } => None,
    pg::DataChange::Truncate { .. } => None,
    pg::DataChange::Begin => None,
    pg::DataChange::Commit => None,
  }
}

fn map_column_change(column_changes: Vec<ColumnChange>) -> Vec<Column> {
  column_changes
    .into_iter()
    .map(|ColumnChange { name, .. }| {
      let column_type = ColumnType::U64;
      let nullable = false;
      let value = ColumnValue::Null;
      Column {
        name,
        column_type,
        is_nullable: nullable,
        value,
      }
    })
    .collect()
}