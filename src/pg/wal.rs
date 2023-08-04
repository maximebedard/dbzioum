use std::{fmt, io, str::FromStr};

use super::conn::Connection;

#[derive(Debug)]
pub struct ReplicationStream {
  conn: Connection,
}

impl ReplicationStream {
  pub(crate) fn new(conn: Connection) -> Self {
    Self { conn }
  }

  pub async fn recv(&mut self) -> Option<io::Result<ReplicationEvent>> {
    // TODO: handle disconnects and reconnect here...
    Some(self.conn.read_replication_event().await)
  }

  pub async fn write_status_update(&mut self, lsn: i64) -> io::Result<()> {
    // TODO: maybe support splitting the receiver from the sender...
    self.conn.write_status_update(lsn, lsn, lsn).await
  }

  pub async fn close(self) -> io::Result<()> {
    self.conn.close().await
  }
}

#[derive(Debug)]
pub enum ReplicationEvent {
  Data {
    start: i64,
    end: i64,
    system_clock: i64,
    data_change: DataChange,
  },
  KeepAlive {
    end: i64,
    system_clock: i64,
    must_reply: bool,
  },
  ChangeTimeline {
    tid: i8,
    lsn: i64,
  },
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "action")]
pub enum DataChange {
  #[serde(rename = "M")]
  Message {
    transactional: bool,
    prefix: String,
    content: String,
  },

  #[serde(rename = "T")]
  Truncate { schema: String, table: String },

  #[serde(rename = "B")]
  Begin,

  #[serde(rename = "C")]
  Commit,

  #[serde(rename = "I")]
  Insert {
    schema: String,
    table: String,
    columns: Vec<ColumnChange>,
  },

  #[serde(rename = "U")]
  Update {
    schema: String,
    table: String,
    columns: Vec<ColumnChange>,
    identity: Vec<ColumnChange>,
  },

  #[serde(rename = "D")]
  Delete {
    schema: String,
    table: String,
    identity: Vec<ColumnChange>,
  },
}

#[derive(Debug, serde::Deserialize)]
pub struct ColumnChange {
  pub name: String,
  #[serde(rename = "type")]
  pub column_type: String,
  pub value: serde_json::Value,
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct WalCursor {
  pub tid: i8,
  pub lsn: i64,
}

impl fmt::Display for WalCursor {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}/{:X}", self.tid, self.lsn)
  }
}

impl FromStr for WalCursor {
  type Err = String;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let (tid, lsn) = s
      .split_once('/')
      .ok_or_else(|| "Failed to parse wal cursor. Expected format is <tid>/<lsn>".to_string())?;
    let tid = tid
      .parse()
      .map_err(|_| "Failed to parse wal cursor tid. Expected format is i8.".to_string())?;
    let lsn = i64::from_str_radix(lsn, 16)
      .map_err(|_| "Failed to parse wal cursor lsn. Expected format is i64 hex encoded".to_string())?;
    Ok(Self { tid, lsn })
  }
}
