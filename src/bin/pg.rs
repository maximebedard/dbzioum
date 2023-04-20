use std::{
  io,
  path::{Path, PathBuf},
  time::Duration,
};

use clap::{value_parser, Arg, Command};
use tokio::{
  fs::File,
  io::{AsyncReadExt, AsyncWriteExt},
  sync::mpsc,
};
use url::Url;

use dbzioum::{
  pg::{self, Connection, IdentifySystem, ReplicationEvent, ReplicationStream, WalCursor},
  sink::RowEvent,
};

#[tokio::main]
async fn main() {
  let mut cmd = Command::new("pg")
    .version("1.0")
    .author("Maxime Bedard <maxime@bedard.dev>")
    .arg(Arg::new("url").required(true).short('u').value_parser(Url::parse))
    .arg(
      Arg::new("cursor")
        .short('c')
        .default_value("./cursor")
        .value_parser(value_parser!(PathBuf)),
    );

  let matches = cmd.get_matches_mut();

  let url = matches.get_one::<Url>("url").unwrap();
  let cursor = matches.get_one::<PathBuf>("cursor").unwrap();

  let mut conn_pg = pg::Connection::connect_from_url(&url).await.unwrap();

  let (slot, wal_cursor) = replication_stream_options_or_defaults(&mut conn_pg, cursor)
    .await
    .unwrap();

  let stream = conn_pg
    .start_replication_stream(slot, wal_cursor.clone())
    .await
    .unwrap();

  let (sender, mut receiver) = mpsc::channel(32);
  tokio::task::spawn(async move {
    while let Some(evt) = receiver.recv().await {
      println!("{:?}", evt);
    }
  });

  let event_processor = EventProcessor {
    stream,
    sink: sender,
    wal_cursor,
  };
  event_processor.process_stream().await.unwrap();
}

struct EventProcessor {
  stream: ReplicationStream,
  wal_cursor: WalCursor,
  sink: mpsc::Sender<RowEvent>,
}

impl EventProcessor {
  async fn process_stream(mut self) -> io::Result<()> {
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);

    // default healthcheck is configured to 10s.
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    loop {
      tokio::select! {
        Ok(_) = &mut interrupt => break,
        event = self.stream.recv() => {
          match event {
            Some(Ok(event)) => self.process_event(event).await.unwrap(),
            Some(Err(err)) => panic!("{}", err),
            None => break,
          }
        },
        _ = interval.tick() => {
          self.stream.write_status_update(1).await.unwrap();
        },
      }
    }

    self.stream.close().await.unwrap();

    Ok(())
  }

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

async fn replication_stream_options_or_defaults(
  conn_pg: &mut Connection,
  cursor: impl AsRef<Path>,
) -> io::Result<(String, WalCursor)> {
  match File::open(&cursor).await {
    Ok(mut f) => {
      let mut buffer = String::new();
      f.read_to_string(&mut buffer).await?;

      let mut chunks = buffer.splitn(2, "/");
      let driver = chunks.next().unwrap();
      assert_eq!("pg", driver);

      let slot = chunks.next().unwrap().to_string();
      let wal_cursor = chunks.next().unwrap().parse().unwrap();

      Ok((slot, wal_cursor))
    }
    Err(err) if err.kind() == io::ErrorKind::NotFound => {
      let IdentifySystem { wal_cursor, .. } = conn_pg.identify_system().await?;
      let slot = "foo".to_string();
      let mut f = File::create(&cursor).await?;
      let buffer = format!("pg/{}/{}", slot, wal_cursor);
      f.write_all(buffer.as_bytes()).await?;
      Ok((slot, wal_cursor))
    }
    Err(err) => Err(err),
  }
}

fn map_data_change(data_change: pg::DataChange) -> Option<RowEvent> {
  match data_change {
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
  }
}
