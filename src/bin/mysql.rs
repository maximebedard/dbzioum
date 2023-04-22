use std::io;

use clap::{value_parser, Arg, Command};
use tokio::sync::mpsc;
use url::Url;

use dbzioum::{
  mysql::{
    self,
    binlog::{self, TableMapEvent},
    BinlogCursor,
  },
  sink::{Column, ColumnType, ColumnValue, RowEvent},
};

#[tokio::main]
async fn main() {
  let mut cmd = Command::new("mysql")
    .version("1.0")
    .author("Maxime Bedard <maxime@bedard.dev>")
    .arg(Arg::new("url").required(true).short('u').value_parser(Url::parse))
    .arg(
      Arg::new("server-id")
        .default_value("1")
        .value_parser(value_parser!(u32)),
    )
    .arg(Arg::new("binlog-cursor").value_parser(str::parse::<BinlogCursor>));

  let matches = cmd.get_matches_mut();

  let url = matches.get_one::<Url>("url").cloned().unwrap();
  let server_id = matches.get_one::<u32>("server-id").cloned().unwrap();
  let binlog_cursor = matches.get_one::<BinlogCursor>("binlog-cursor").cloned();

  let mut conn_mysql = mysql::Connection::connect_from_url(&url).await.unwrap();

  let binlog_cursor = match binlog_cursor {
    Some(binlog_cursor) => binlog_cursor,
    None => conn_mysql.binlog_cursor().await.unwrap(),
  };

  let mut stream = conn_mysql
    .binlog_stream(server_id, binlog_cursor.clone())
    .await
    .unwrap();

  let (sender, mut receiver) = mpsc::channel(32);
  tokio::task::spawn(async move {
    while let Some(evt) = receiver.recv().await {
      println!("{:?}", evt);
    }
  });

  let interrupt = tokio::signal::ctrl_c();
  tokio::pin!(interrupt);

  let mut processor = EventProcessor {
    table_map_event: None,
    binlog_cursor,
    sink: sender,
  };

  loop {
    tokio::select! {
        Ok(_) = &mut interrupt => break,
        event = stream.recv() => {
            match event {
                Some(Ok(event)) => processor.process_event(event).await.unwrap(),
                Some(Err(err)) => eprintln!("binlog stream error: {:?}", err),
                None => break,
            }
        },
    }
  }

  stream.close().await.unwrap();
}

struct EventProcessor {
  binlog_cursor: BinlogCursor,
  sink: mpsc::Sender<RowEvent>,
  table_map_event: Option<TableMapEvent>,
}

impl EventProcessor {
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

      binlog::BinlogEvent::Delete(v) => {
        let table_map_event = self.table_map_event.take().unwrap();

        let columns = table_map_event.columns();
        let values = v.values(&columns);

        let schema = table_map_event.schema;
        let table = table_map_event.table;

        let identity = columns
          .iter()
          .zip(&values)
          .map(|(c, v)| {
            let name = c.name.clone();
            let is_nullable = c.is_nullable;
            let column_type = match c.column_type {
              binlog::ColumnTypeDefinition::U64 { .. } => ColumnType::U64,
              binlog::ColumnTypeDefinition::I64 { .. } => ColumnType::I64,
              binlog::ColumnTypeDefinition::F64 { .. } => todo!(),
              binlog::ColumnTypeDefinition::Decimal { .. } => todo!(),
              binlog::ColumnTypeDefinition::Json { .. } => todo!(),
              binlog::ColumnTypeDefinition::String { .. } => todo!(),
              binlog::ColumnTypeDefinition::Blob { .. } => todo!(),
              binlog::ColumnTypeDefinition::Date(_) => todo!(),
              binlog::ColumnTypeDefinition::Year => todo!(),
              binlog::ColumnTypeDefinition::Time(_) => todo!(),
              binlog::ColumnTypeDefinition::Timestamp => todo!(),
            };
            let value = match v {
              binlog::Value::Null => ColumnValue::Null,
              binlog::Value::U64(v) => ColumnValue::U64(*v),
              binlog::Value::I64(v) => ColumnValue::I64(*v),
              binlog::Value::F64(_) => todo!(),
              binlog::Value::Decimal(_) => todo!(),
              binlog::Value::String(v) => ColumnValue::String(v.clone()),
              binlog::Value::Blob(v) => ColumnValue::Bytes(v.clone()),
              binlog::Value::Json(_) => todo!(),
              binlog::Value::Date { .. } => todo!(),
              binlog::Value::Time { .. } => todo!(),
            };
            Column {
              name,
              is_nullable,
              column_type,
              value,
            }
          })
          .collect::<Vec<_>>();

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
