use std::{
  fmt, io,
  str::FromStr,
  time::{Duration, SystemTime},
};

use bytes::Buf;
use tokio::io::AsyncWriteExt;

use super::stream::Stream;

#[derive(Debug)]
pub struct ReplicationStream {
  pub(crate) stream: Stream,
}

impl ReplicationStream {
  pub async fn recv(&mut self) -> Option<io::Result<ReplicationEvent>> {
    // TODO: handle disconnects and reconnect here...
    Some(self.read_replication_event().await)
  }

  pub async fn write_status_update(&mut self, lsn: i64) -> io::Result<()> {
    // TODO: maybe support splitting the receiver from the sender...
    self.write_status_update2(lsn, lsn, lsn).await
  }

  pub async fn close(mut self) -> io::Result<()> {
    self.stream.shutdown().await
  }

  async fn read_replication_event(&mut self) -> io::Result<ReplicationEvent> {
    let (op, mut buffer) = self.stream.read_packet().await?;

    match op {
      b'E' => {
        todo!("handle backend error")
        // return Err(self.read_backend_error().await);
      }
      b'N' => {
        todo!("handle backend notice")
        // self.read_backend_notice().await;
      }
      b'd' => {
        match buffer.get_u8() {
          b'w' => {
            let start = buffer.get_i64();
            let end = buffer.get_i64();
            let system_clock = buffer.get_i64();

            let data_change = serde_json::from_slice::<DataChange>(buffer.chunk())
              .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

            Ok(ReplicationEvent::Data {
              start,
              end,
              system_clock,
              data_change,
            })
          }
          b'k' => {
            // https://www.postgresql.org/docs/current/protocol-replication.html
            let end = buffer.get_i64();
            let system_clock = buffer.get_i64();
            let must_reply_status = buffer.get_u8();

            Ok(ReplicationEvent::KeepAlive {
              end,
              system_clock,
              must_reply: must_reply_status == 1,
            })
          }
          code => {
            panic!("Unexpected backend message: {:?}", char::from(code))
          }
        }
      }
      code => {
        panic!("Unexpected backend message: {:?}", char::from(code))
      }
    }
  }

  async fn write_status_update2(&mut self, written: i64, flushed: i64, applied: i64) -> io::Result<()> {
    let dt = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH + Duration::from_secs(946_684_800))
      .unwrap();

    let system_clock = dt.as_micros() as i64;

    self.stream.write_u8(b'd').await?;
    self.stream.write_i32(1 + 4 + 8 + 8 + 8 + 8 + 1).await?;
    self.stream.write_u8(b'r').await?;
    self.stream.write_i64(written).await?;
    self.stream.write_i64(flushed).await?;
    self.stream.write_i64(applied).await?;
    self.stream.write_i64(system_clock).await?;
    self.stream.write_u8(0).await?;
    self.stream.flush().await
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
