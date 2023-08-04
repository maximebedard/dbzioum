use std::{
  collections::VecDeque,
  io,
  slice::{ChunksExact, ChunksExactMut},
};

use super::wal::WalCursor;

#[derive(Debug)]
pub struct IdentifySystem {
  pub systemid: String,
  pub timeline: u64,
  pub wal_cursor: WalCursor,
  pub dbname: Option<String>,
}

#[derive(Debug)]
pub struct CreateReplicationSlot {
  pub slot_name: String,
  pub consistent_point: WalCursor,
  pub snapshot_name: Option<String>,
  pub output_plugin: Option<String>,
}

#[derive(Debug)]
pub struct Column {
  pub name: String,
  pub oid: i32,
  pub attr_number: i16,
  pub datatype_oid: i32,
  pub datatype_size: i16,
  pub type_modifier: i32,
  pub format: i16,
}

pub type RowValue = Option<String>;

#[derive(Debug)]
pub struct QueryResults {
  pub notices: VecDeque<io::Error>,
  pub results: VecDeque<QueryResult>,
}

#[derive(Debug)]
pub enum QueryResult {
  Success,
  RowsAffected(usize),
  Selected(SelectQueryResult),
  BackendError(io::Error),
}

impl QueryResult {
  pub fn is_successful(self) -> bool {
    matches!(self, Self::Success)
  }

  pub fn rows_affected(self) -> Option<usize> {
    match self {
      Self::RowsAffected(n) => Some(n),
      _ => None,
    }
  }

  pub fn as_selected_query_result(self) -> Option<SelectQueryResult> {
    match self {
      Self::Selected(v) => Some(v),
      _ => None,
    }
  }

  pub fn as_backend_error(self) -> Option<io::Error> {
    match self {
      Self::BackendError(v) => Some(v),
      _ => None,
    }
  }
}

#[derive(Debug, Default)]
pub struct SelectQueryResult {
  pub columns: Vec<Column>,
  pub values: Vec<RowValue>,
}

impl SelectQueryResult {
  pub fn columns(&self) -> &[Column] {
    &self.columns
  }

  pub fn columns_len(&self) -> usize {
    self.columns.len()
  }

  pub fn is_empty(&self) -> bool {
    self.columns().is_empty()
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
