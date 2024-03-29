use clap::{value_parser, Arg, Command};
use url::Url;

use mysql::{
  binlog::{self, TableMapEvent},
  BinlogCursor,
};
use sink::{Column, ColumnType, ColumnValue, RowEvent};

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

  let mut matches = cmd.get_matches_mut();

  let url = matches.remove_one::<Url>("url").unwrap();
  let server_id = matches.remove_one::<u32>("server-id").unwrap();
  let binlog_cursor = matches.remove_one::<BinlogCursor>("binlog-cursor");

  let mut conn_mysql = mysql::Connection::connect_from_url(&url).await.unwrap();

  let binlog_cursor = match binlog_cursor {
    Some(binlog_cursor) => binlog_cursor,
    None => conn_mysql.binlog_cursor().await.unwrap(),
  };

  let mut stream = conn_mysql
    .binlog_stream(server_id, binlog_cursor.clone())
    .await
    .unwrap();

  let interrupt = tokio::signal::ctrl_c();
  tokio::pin!(interrupt);

  let mut processor = EventProcessor {
    table_map_event: None,
    binlog_cursor,
  };

  loop {
    tokio::select! {
        Ok(_) = &mut interrupt => break,
        event = stream.recv() => {
            match event {
                Some(Ok((header, event))) => {
                  if let Some(event) = processor.process_event(header, event) {
                    println!("{:?}", event);
                  }
                },
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
  table_map_event: Option<TableMapEvent>,
}

impl EventProcessor {
  fn process_event(&mut self, header: binlog::BinlogEventHeader, event: binlog::BinlogEvent) -> Option<RowEvent> {
    fn map_column_change(table_map_event: &TableMapEvent, row_event: &binlog::InsertRowEvent) -> Vec<Column> {
      let columns = table_map_event.columns();
      let values = row_event.rows(&columns);

      columns
        .into_iter()
        .zip(values)
        .map(|(c, v)| {
          let name = c.column_name;
          let is_nullable = c.is_nullable;
          let column_type = match c.column_type_definition {
            binlog::ColumnTypeDefinition::U64 { .. } => ColumnType::U64,
            binlog::ColumnTypeDefinition::I64 { .. } => ColumnType::I64,
            binlog::ColumnTypeDefinition::F64 { .. } => ColumnType::F64,
            binlog::ColumnTypeDefinition::Decimal { .. } => ColumnType::Decimal,
            binlog::ColumnTypeDefinition::Json { .. } => ColumnType::Json,
            binlog::ColumnTypeDefinition::String { .. } => ColumnType::String,
            binlog::ColumnTypeDefinition::Blob { .. } => ColumnType::Bytes,
            binlog::ColumnTypeDefinition::Date(_) => ColumnType::Date,
            binlog::ColumnTypeDefinition::Year => ColumnType::U64,
            binlog::ColumnTypeDefinition::Time(_) => ColumnType::Time,
            binlog::ColumnTypeDefinition::Timestamp => ColumnType::Timestamp,
            binlog::ColumnTypeDefinition::Enum => todo!(),
            binlog::ColumnTypeDefinition::Set => todo!(),
          };
          let value = match v {
            binlog::Value::Null => ColumnValue::Null,
            binlog::Value::U64(v) => ColumnValue::U64(v),
            binlog::Value::I64(v) => ColumnValue::I64(v),
            binlog::Value::F64(v) => ColumnValue::F64(v),
            binlog::Value::Decimal(_) => todo!(),
            binlog::Value::String(v) => ColumnValue::String(v),
            binlog::Value::Blob(v) => ColumnValue::Bytes(v),
            binlog::Value::Json(_) => todo!(),
            binlog::Value::Date { .. } => todo!(),
            binlog::Value::Time { .. } => todo!(),
            binlog::Value::Enum => todo!(),
            binlog::Value::Set => todo!(),
          };
          Column {
            name,
            is_nullable,
            column_type,
            value,
          }
        })
        .collect::<Vec<_>>()
    }

    match event {
      binlog::BinlogEvent::TableMap(v) => {
        self.table_map_event.replace(v);
        None
      }

      binlog::BinlogEvent::Insert(v) => {
        let table_map_event = self.table_map_event.take().unwrap();
        let columns = map_column_change(&table_map_event, &v);
        let schema = table_map_event.schema;
        let table = table_map_event.table;
        self.binlog_cursor.log_position = header.log_position;
        Some(RowEvent::Insert { schema, table, columns })
      }

      binlog::BinlogEvent::Update(_v) => {
        None
        // let table_map_event = self.table_map_event.take().unwrap();
        // let columns = vec![];
        // let identity = map_column_change(&table_map_event, &v);
        // let schema = table_map_event.schema;
        // let table = table_map_event.table;
        // self.binlog_cursor.log_position = event.log_position;
        // Some(RowEvent::Update {
        //   schema,
        //   table,
        //   columns,
        //   identity,
        // })
      }

      binlog::BinlogEvent::Delete(_v) => {
        None
        // let table_map_event = self.table_map_event.take().unwrap();
        // let identity = map_column_change(&table_map_event, &v);
        // let schema = table_map_event.schema;
        // let table = table_map_event.table;
        // self.binlog_cursor.log_position = event.log_position;
        // Some(RowEvent::Delete {
        //   schema,
        //   table,
        //   identity,
        // })
      }
      binlog::BinlogEvent::Rotate(evt) => {
        self.binlog_cursor.log_file = evt.next_log_file.clone();
        self.binlog_cursor.log_position = evt.next_log_position;
        None
      }
      _ => {
        self.binlog_cursor.log_position = header.log_position;
        None
      }
    }
  }
}
