use std::{
  io,
  slice::{ChunksExact, ChunksExactMut},
};

use bytes::{Buf, Bytes};

use super::{
  buf_ext::BufExt,
  constants::{CharacterSet, ColumnFlags, ColumnType},
};

/// Owned results for 0..N rows.
#[derive(Debug, Default)]
pub struct QueryResults {
  pub columns: Vec<Column>,
  pub values: Vec<RowValue>,
}

impl QueryResults {
  pub fn columns_len(&self) -> usize {
    self.columns.len()
  }

  pub fn row(&self, i: usize) -> &[RowValue] {
    let len = self.columns.len();
    let start = i * len;
    let end = start + len;
    &self.values[start..end]
  }

  pub fn row_mut(&mut self, i: usize) -> &mut [RowValue] {
    let len = self.columns.len();
    let start = i * len;
    let end = start + len;
    &mut self.values[start..end]
  }

  pub fn rows_len(&self) -> usize {
    if !self.columns.is_empty() {
      self.values.len() / self.columns.len()
    } else {
      0
    }
  }

  pub fn rows(&self) -> Option<ChunksExact<'_, RowValue>> {
    if !self.columns.is_empty() {
      Some(self.values.chunks_exact(self.columns.len()))
    } else {
      None
    }
  }

  pub fn rows_mut(&mut self) -> Option<ChunksExactMut<'_, RowValue>> {
    if !self.columns.is_empty() {
      Some(self.values.chunks_exact_mut(self.columns.len()))
    } else {
      None
    }
  }
}

// https://mariadb.com/kb/en/connection/#sslrequest-packet
// https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html
pub type RowValue = Option<String>;

#[derive(Debug)]
pub struct Column {
  catalog: String,
  schema: String,
  table: String,
  name: String,
  org_table: String,
  character_set: CharacterSet,
  column_length: u32,
  column_type: ColumnType,
  flags: ColumnFlags,
  decimals: u8,
}

impl Column {
  pub(crate) fn parse(mut b: Bytes) -> io::Result<Self> {
    let catalog = b.mysql_get_lenc_string().unwrap();
    assert_eq!("def", catalog.as_str());
    let schema = b.mysql_get_lenc_string().unwrap();
    let table = b.mysql_get_lenc_string().unwrap();
    let org_table = b.mysql_get_lenc_string().unwrap();
    let name = b.mysql_get_lenc_string().unwrap();
    let _org_name = b.mysql_get_lenc_string().unwrap();
    let fixed_len = b.mysql_get_lenc_uint();
    assert_eq!(0x0C, fixed_len);
    let character_set = (b.get_u16_le() as u8).try_into().unwrap();
    let column_length = b.get_u32_le();
    let column_type = b.get_u8().try_into().unwrap();
    let flags = ColumnFlags::from_bits_truncate(b.get_u16_le());
    let decimals = b.get_u8();

    Ok(Self {
      catalog,
      schema,
      table,
      name,
      org_table,
      character_set,
      column_length,
      column_type,
      flags,
      decimals,
    })
  }
}
