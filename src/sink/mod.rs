use bytes::Bytes;

pub mod stdout;

#[derive(Debug)]
pub enum RowEvent {
  Insert {
    schema: String,
    table: String,
    columns: Vec<Column>,
  },

  Update {
    schema: String,
    table: String,
    columns: Vec<Column>,
    identity: Vec<Column>,
  },

  Delete {
    schema: String,
    table: String,
    identity: Vec<Column>,
  },
}

#[derive(Debug)]
pub struct Column {
  name: String,
  nullable: bool,
  column_type: ColumnType,
}

#[derive(Debug)]
pub enum ColumnType {
  I64,
  U64,
  String,
  Bytes,
}

pub enum ColumnValue {
  Null,
  U64(u64),
  I64(i64),
  String(String),
  Bytes(Bytes),
}