use std::{
  io,
  path::{Path, PathBuf},
};

use clap::{value_parser, Arg, Command};
use tokio::{
  fs::{self, File},
  io::{AsyncReadExt, AsyncWriteExt},
  sync::mpsc,
};
use url::Url;

use dbzioum::{
  mysql,
  mysql::{
    binlog::{self, TableMapEvent},
    BinlogCursor, BinlogStream, Connection,
  },
  sink::RowEvent,
};

#[tokio::main]
async fn main() {
  let mut cmd = Command::new("mysql")
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

  let mut conn_mysql = mysql::Connection::connect_from_url(&url).await.unwrap();

  let (server_id, binlog_cursor) = binlog_stream_options_or_defaults(&mut conn_mysql, &cursor)
    .await
    .unwrap();

  let stream = conn_mysql
    .binlog_stream(server_id, binlog_cursor.clone())
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
    binlog_cursor,
    table_map_event: None,
  };

  event_processor.process_stream().await.unwrap();
}

struct EventProcessor {
  binlog_cursor: BinlogCursor,
  stream: BinlogStream,
  sink: mpsc::Sender<RowEvent>,
  table_map_event: Option<TableMapEvent>,
}

impl EventProcessor {
  async fn process_stream(mut self) -> io::Result<()> {
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);

    loop {
      tokio::select! {
          Ok(_) = &mut interrupt => break,
          event = self.stream.recv() => {
              match event {
                  Some(Ok(event)) => self.process_event(event).await.unwrap(),
                  Some(Err(err)) => eprintln!("binlog stream error: {:?}", err),
                  None => break,
              }
          },
      }
    }

    self.stream.close().await.unwrap();

    Ok(())
  }

  async fn process_event(&mut self, event: binlog::BinlogEventPacket) -> io::Result<()> {
    match event.event {
      binlog::BinlogEvent::TableMap(v) => {
        self.table_map_event.replace(v);
      }

      binlog::BinlogEvent::Insert(_v) => {
        let _table_map_event = self.table_map_event.take().unwrap();
        let schema = "".to_string();
        let table = "".to_string();
        let columns = vec![];
        self
          .sink
          .send(RowEvent::Insert { schema, table, columns })
          .await
          .unwrap();
        self.binlog_cursor.log_position = event.log_position;
      }

      binlog::BinlogEvent::Update(_v) => {
        let _table_map_event = self.table_map_event.take().unwrap();
        let schema = "".to_string();
        let table = "".to_string();
        let columns = vec![];
        let identity = vec![];
        self
          .sink
          .send(RowEvent::Update {
            schema,
            table,
            columns,
            identity,
          })
          .await
          .unwrap();
        self.binlog_cursor.log_position = event.log_position;
      }

      binlog::BinlogEvent::Delete(_v) => {
        let _table_map_event = self.table_map_event.take().unwrap();
        let schema = "".to_string();
        let table = "".to_string();
        let identity = vec![];
        self
          .sink
          .send(RowEvent::Delete {
            schema,
            table,
            identity,
          })
          .await
          .unwrap();
        self.binlog_cursor.log_position = event.log_position;
      }
      binlog::BinlogEvent::Rotate(evt) => {
        self.binlog_cursor.log_file = evt.next_log_file.clone();
        self.binlog_cursor.log_position = evt.next_log_position;
      }
      _ => {
        self.binlog_cursor.log_position = event.log_position;
      }
    }

    Ok(())
  }
}

async fn binlog_stream_options_or_defaults(
  conn_mysql: &mut Connection,
  cursor: impl AsRef<Path>,
) -> io::Result<(u32, BinlogCursor)> {
  match File::open(&cursor).await {
    Ok(mut f) => {
      let mut buffer = String::new();
      f.read_to_string(&mut buffer).await?;

      let mut chunks = buffer.splitn(2, "/");
      let driver = chunks.next().unwrap();
      assert_eq!("mysql", driver);

      let server_id = chunks.next().unwrap().parse().unwrap();
      let binlog_cursor = chunks.next().unwrap().parse().unwrap();

      Ok((server_id, binlog_cursor))
    }
    Err(err) if err.kind() == io::ErrorKind::NotFound => {
      let binlog_cursor = conn_mysql.binlog_cursor().await?;
      let server_id = 1;
      let mut f = File::create(&cursor).await?;
      let buffer = format!("mysql/{}/{}", server_id, binlog_cursor);
      f.write_all(buffer.as_bytes()).await?;
      Ok((server_id, binlog_cursor))
    }
    Err(err) => Err(err),
  }
}
