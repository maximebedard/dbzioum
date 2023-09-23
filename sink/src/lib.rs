use bytes::Bytes;

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
  pub name: String,
  pub is_nullable: bool,
  pub column_type: ColumnType,
  pub value: ColumnValue,
}

#[derive(Debug)]
pub enum ColumnType {
  I64,
  U64,
  F64,
  String,
  Bytes,
  Date,
  Time,
  Timestamp,
  Decimal,
  Json,
}

#[derive(Debug)]
pub enum ColumnValue {
  Null,
  U64(u64),
  I64(i64),
  F64(f64),
  String(String),
  Bytes(Bytes),
}
