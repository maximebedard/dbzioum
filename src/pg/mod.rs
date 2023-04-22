use std::collections::BTreeMap;
use std::fmt;
use std::net::{SocketAddrV4, SocketAddrV6};
use std::slice::{ChunksExact, ChunksExactMut};
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use std::{io, net::SocketAddr};

use tokio::net;
use tokio::{
  io::{AsyncReadExt, AsyncWriteExt, BufStream},
  net::TcpStream,
};
use url::Url;

#[derive(Debug)]
pub struct Connection {
  stream: BufStream<TcpStream>,
  options: ConnectionOptions,
}

#[derive(Debug, Clone)]
pub struct ConnectionOptions {
  pub addr: SocketAddr,
  pub user: String,
  pub use_ssl: bool,
  pub password: Option<String>,
  pub database: Option<String>,
  pub handle_notices_as_errors: bool,
}

impl Default for ConnectionOptions {
  fn default() -> Self {
    Self {
      addr: "[::]:5432".parse().unwrap(),
      user: "postgres".to_string(),
      use_ssl: false,
      password: None,
      database: None,
      handle_notices_as_errors: true,
    }
  }
}

const PROTOCOL_VERSION: i32 = 196608;
const SSL_HANDSHAKE_CODE: i32 = 80877103;

impl Connection {
  pub async fn connect_from_url(u: &Url) -> io::Result<Self> {
    assert_eq!("tcp", u.scheme()); // only support tcp for now
    let user = match u.username() {
      "" => "postgres".to_string(),
      user => user.to_string(),
    };
    let password = u.password().map(|v| v.to_string());
    let host = u
      .host()
      .map(|h| h.to_string())
      .unwrap_or_else(|| "localhost".to_string());
    let port = u.port().unwrap_or(5432);
    let addr = match u.host() {
      Some(url::Host::Domain(domain)) => net::lookup_host(format!("{}:{}", domain, port))
        .await
        .and_then(|mut v| {
          v.next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::ConnectionReset, "unable to find host"))
        })?,
      Some(url::Host::Ipv4(ip)) => SocketAddrV4::new(ip, port).into(),
      Some(url::Host::Ipv6(ip)) => SocketAddrV6::new(ip, port, 0, 0).into(),
      None => format!("[::]:{port}").parse().unwrap(),
    };

    let query_pairs = u.query_pairs().collect::<BTreeMap<_, _>>();
    let database = query_pairs.get("database").map(|v| v.to_string());

    let opts = ConnectionOptions {
      addr,
      user,
      password,
      database,
      ..Default::default()
    };
    Self::connect(opts).await
  }

  pub async fn connect(options: impl Into<ConnectionOptions>) -> io::Result<Self> {
    let options = options.into();
    let stream = TcpStream::connect(options.addr).await?;
    let stream = BufStream::new(stream);
    let use_ssl = options.use_ssl;
    let mut connection = Self { stream, options };
    if use_ssl {
      connection.ssl_handshake().await?;
    }
    connection.startup().await?;
    Ok(connection)
  }

  pub async fn duplicate(&self) -> io::Result<Self> {
    Self::connect(self.options.clone()).await
  }

  async fn ssl_handshake(&mut self) -> io::Result<()> {
    self.stream.write_i32(8).await?;
    self.stream.write_i32(SSL_HANDSHAKE_CODE).await?;

    match self.stream.read_u8().await? {
      b'S' => {
        // https://www.postgresql.org/docs/11/protocol-flow.html#id-1.10.5.7.11
        unimplemented!()
      }
      b'N' => {
        // noop
      }
      code => {
        panic!("Unexpected backend message: {:?}", char::from(code))
      }
    }

    Ok(())
  }

  // https://www.postgresql.org/docs/11/protocol.html
  async fn startup(&mut self) -> io::Result<()> {
    let mut params = Vec::new();
    params.push("user");
    params.push(self.options.user.as_str());
    if let Some(ref database) = self.options.database {
      params.push("database");
      params.push(database.as_str());
    }
    params.push("application_name");
    params.push("dbzioum");
    params.push("replication");
    params.push("database");

    let mut len = 4 + 4 + 1;

    for p in &params {
      len += p.as_bytes().len() + 1;
    }

    self.stream.write_i32(len as i32).await?;
    self.stream.write_i32(PROTOCOL_VERSION).await?;

    for p in &params {
      self.stream.write_all(p.as_bytes()).await?;
      self.stream.write_u8(0).await?;
    }

    self.stream.write_u8(0).await?;
    self.stream.flush().await?;

    self.authenticate().await?;

    Ok(())
  }

  async fn authenticate(&mut self) -> io::Result<()> {
    loop {
      match self.stream.read_u8().await? {
        b'R' => {
          self.stream.read_i32().await?; // skip len
          match self.stream.read_i32().await? {
            0 => break,
            2 => {
              return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "AuthenticationKerberosV5 is not supported",
              ));
            }
            3 => {
              // AuthenticationCleartextPassword
              if let Some(ref password) = self.options.password {
                let len = password.as_bytes().len() + 4 + 1;
                self.stream.write_u8(b'p').await?;
                self.stream.write_i32(len as i32).await?;
                self.stream.write_all(password.as_bytes()).await?;
                self.stream.write_u8(0).await?;
                self.stream.flush().await?;
              } else {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "password is required"));
              }
            }
            5 => {
              return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "AuthenticationMD5Password is not supported",
              ));
            }
            6 => {
              return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "AuthenticationSCMCredential is not supported",
              ));
            }
            7 => {
              return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "AuthenticationGSS is not supported",
              ));
            }
            9 => {
              return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "AuthenticationSSPI is not supported",
              ));
            }
            10 => {
              return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "AuthenticationSASL is not supported",
              ));
            }
            code => panic!("Unexpected backend authentication code {:?}", code),
          }
        }
        b'E' => {
          return Err(self.read_backend_error().await);
        }
        code => {
          panic!("Unexpected backend message: {:?}", char::from(code))
        }
      }
    }

    loop {
      match self.stream.read_u8().await? {
        b'K' => {
          // BackendKeyData (B)
          //     Byte1('K')
          //         Identifies the message as cancellation key data. The frontend must save these values if it wishes to be able to issue CancelRequest messages later.
          //     Int32(12)
          //         Length of message contents in bytes, including self.
          //     Int32
          //         The process ID of this backend.
          //     Int32
          //         The secret key of this backend.

          self.stream.read_i32().await?; // skip len
          let id = self.stream.read_i32().await?;
          let secret_key = self.stream.read_i32().await?;
        }
        b'S' => {
          // ParameterStatus (B)
          //     Byte1('S')
          //         Identifies the message as a run-time parameter status report.
          //     Int32
          //         Length of message contents in bytes, including self.
          //     String
          //         The name of the run-time parameter being reported.
          //     String
          //         The current value of the parameter.

          self.stream.read_i32().await?; // skip len
          let param_name = self.read_c_string().await?;
          let param_value = self.read_c_string().await?;
          eprintln!("{:?}: {:?}", param_name, param_value);
        }
        b'Z' => {
          self.read_ready_for_query().await?;
          break;
        }
        b'E' => {
          return Err(self.read_backend_error().await);
        }
        b'N' => {
          self.read_backend_notice().await?;
        }
        code => {
          panic!("Unexpected backend message: {:?}", char::from(code))
        }
      }
    }
    Ok(())
  }

  pub async fn ping(&mut self) -> io::Result<()> {
    self.simple_query("SELECT 1").await.map(|_| ())
  }

  pub async fn start_replication_stream(
    mut self,
    slot: impl AsRef<str>,
    wal_cursor: impl Into<WalCursor>,
  ) -> io::Result<ReplicationStream> {
    let wal_cursor = wal_cursor.into();
    let command = format!(
      "START_REPLICATION SLOT {} LOGICAL {} (\"format-version\" '2')",
      slot.as_ref(),
      &wal_cursor
    );
    self.write_query_command(command).await?;

    // TODO: support timelines
    // After streaming all the WAL on a timeline that is not the latest one, the server will end streaming by exiting the COPY mode. When the client acknowledges this by also exiting COPY mode, the server sends a result set with one row and two columns, indicating the next timeline in this server's history. The first column is the next timeline's ID (type int8), and the second column is the WAL location where the switch happened (type text). Usually, the switch position is the end of the WAL that was streamed, but there are corner cases where the server can send some WAL from the old timeline that it has not itself replayed before promoting. Finally, the server sends two CommandComplete messages (one that ends the CopyData and the other ends the START_REPLICATION itself), and is ready to accept a new command.
    // WAL data is sent as a series of CopyData messages. (This allows other information to be intermixed; in particular the server can send an ErrorResponse message if it encounters a failure after beginning to stream.) The payload of each CopyData message from server to the client contains a message of one of the following formats:

    match self.stream.read_u8().await? {
      b'E' => {
        return Err(self.read_backend_error().await);
      }
      b'W' => {
        self.stream.read_i32().await?; // skip len
        let format = self.stream.read_i8().await?;
        let num_columns = self.stream.read_i16().await?;
        let mut column_formats = vec![0; num_columns.try_into().unwrap()];
        for i in 0..column_formats.len() {
          column_formats.push(self.stream.read_i16().await?);
        }

        assert_eq!(0, format);
        assert_eq!(0, num_columns);
        assert!(column_formats.is_empty());
      }
      code => {
        panic!("Unexpected backend message: {:?}", char::from(code))
      }
    }

    let conn = self;

    Ok(ReplicationStream { conn })
  }

  async fn read_replication_event(&mut self) -> io::Result<ReplicationEvent> {
    loop {
      match self.stream.read_u8().await? {
        b'E' => return Err(self.read_backend_error().await),
        b'N' => {
          self.read_backend_notice().await?;
        }
        b'd' => {
          let len = self.stream.read_i32().await?;
          let len: usize = len.try_into().unwrap();

          match self.stream.read_u8().await? {
            b'w' => {
              let start = self.stream.read_i64().await?;
              let end = self.stream.read_i64().await?;
              let system_clock = self.stream.read_i64().await?;
              let mut buffer = vec![0; len - 4 - 1 - 8 - 8 - 8];
              self.stream.read_exact(&mut buffer).await?;

              let data_change = serde_json::from_slice::<DataChange>(buffer.as_slice())
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

              return Ok(ReplicationEvent::Data {
                start,
                end,
                system_clock,
                data_change,
              });
            }
            b'k' => {
              // https://www.postgresql.org/docs/current/protocol-replication.html
              let end = self.stream.read_i64().await?;
              let system_clock = self.stream.read_i64().await?;
              let must_reply_status = self.stream.read_u8().await?;

              return Ok(ReplicationEvent::KeepAlive {
                end,
                system_clock,
                must_reply: must_reply_status == 1,
              });
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
  }

  pub async fn write_status_update(&mut self, written: i64, flushed: i64, applied: i64) -> io::Result<()> {
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

  pub async fn create_replication_slot(&mut self, slot: impl AsRef<str>) -> io::Result<CreateReplicationSlot> {
    let mut values = self
      .simple_query(format!("CREATE_REPLICATION_SLOT {} LOGICAL wal2json", slot.as_ref()))
      .await
      .map(|mut v| {
        v.values.reverse();
        v.values
      })?;

    let slot_name = values.pop().unwrap().unwrap();
    let consistent_point = values.pop().unwrap().unwrap().parse().unwrap();
    let snapshot_name = values.pop().unwrap();
    let output_plugin = values.pop().unwrap();

    Ok(CreateReplicationSlot {
      slot_name,
      consistent_point,
      snapshot_name,
      output_plugin,
    })
  }

  pub async fn delete_replication_slot(&mut self, slot: impl AsRef<str>) -> io::Result<()> {
    self
      .simple_query(format!("DROP_REPLICATION_SLOT {}", slot.as_ref()))
      .await
      .map(|_| ())
  }

  pub async fn replication_slot_exists(&mut self, slot: impl AsRef<str>) -> io::Result<bool> {
    self
      .simple_query(format!(
        "select * from pg_replication_slots where slot_name = '{}';",
        slot.as_ref()
      ))
      .await
      .map(|result| !result.values.is_empty())
  }

  pub async fn identify_system(&mut self) -> io::Result<IdentifySystem> {
    let mut values = self.simple_query("IDENTIFY_SYSTEM").await.map(|mut v| {
      v.values.reverse();
      v.values
    })?;

    let systemid = values.pop().unwrap().unwrap();
    let timeline = values.pop().unwrap().unwrap().parse().unwrap();
    let wal_cursor = values.pop().unwrap().unwrap().parse().unwrap();
    let dbname = values.pop().unwrap();

    Ok(IdentifySystem {
      systemid,
      timeline,
      wal_cursor,
      dbname,
    })
  }

  pub async fn timeline_history(&mut self, tid: i32) -> io::Result<SimpleQueryResult> {
    self.simple_query(format!("TIMELINE_HISTORY {}", tid)).await
  }

  async fn write_query_command(&mut self, command: impl AsRef<str>) -> io::Result<()> {
    let len = command.as_ref().as_bytes().len() + 1 + 4;
    self.stream.write_u8(b'Q').await?;
    self.stream.write_i32(len as i32).await?;
    self.stream.write_all(command.as_ref().as_bytes()).await?;
    self.stream.write_u8(0).await?;
    self.stream.flush().await?;
    Ok(())
  }

  pub async fn simple_query(&mut self, command: impl AsRef<str>) -> io::Result<SimpleQueryResult> {
    self.write_query_command(command).await?;

    let mut columns: Vec<SimpleColumn> = Vec::new();
    let mut values: Vec<SimpleRowValue> = Vec::new();

    loop {
      match self.stream.read_u8().await? {
        b'C' => {
          // CommandComplete (B)
          //     Byte1('C')
          //         Identifies the message as a command-completed response.
          //     Int32
          //         Length of message contents in bytes, including self.
          //     String
          //         The command tag. This is usually a single word that identifies which SQL command was completed.
          //         For an INSERT command, the tag is INSERT oid rows, where rows is the number of rows inserted. oid is the object ID of the inserted row if rows is 1 and the target table has OIDs; otherwise oid is 0.
          //         For a DELETE command, the tag is DELETE rows where rows is the number of rows deleted.
          //         For an UPDATE command, the tag is UPDATE rows where rows is the number of rows updated.
          //         For a SELECT or CREATE TABLE AS command, the tag is SELECT rows where rows is the number of rows retrieved.
          //         For a MOVE command, the tag is MOVE rows where rows is the number of rows the cursor's position has been changed by.
          //         For a FETCH command, the tag is FETCH rows where rows is the number of rows that have been retrieved from the cursor.
          //         For a COPY command, the tag is COPY rows where rows is the number of rows copied. (Note: the row count appears only in PostgreSQL 8.2 and later.)
          self.stream.read_i32().await?; // skip len
          let command_tag = self.read_c_string().await?;
        }
        b'G' => {
          // CopyInResponse (B)
          //     Byte1('G')
          //         Identifies the message as a Start Copy In response. The frontend must now send copy-in data (if not prepared to do so, send a CopyFail message).
          //     Int32
          //         Length of message contents in bytes, including self.
          //     Int8
          //         0 indicates the overall COPY format is textual (rows separated by newlines, columns separated by separator characters, etc). 1 indicates the overall copy format is binary (similar to DataRow format). See COPY for more information.
          //     Int16
          //         The number of columns in the data to be copied (denoted N below).
          //     Int16[N]
          //         The format codes to be used for each column. Each must presently be zero (text) or one (binary). All must be zero if the overall copy format is textual.
          self.stream.read_i32().await?; // skip len
          let format = self.stream.read_i8().await?;
          let num_columns = self.stream.read_i16().await?;
          for i in 0..num_columns {
            self.stream.read_i16().await?;
          }
        }
        b'H' => {
          // CopyOutResponse (B)
          //     Byte1('H')
          //         Identifies the message as a Start Copy Out response. This message will be followed by copy-out data.
          //     Int32
          //         Length of message contents in bytes, including self.
          //     Int8
          //         0 indicates the overall COPY format is textual (rows separated by newlines, columns separated by separator characters, etc). 1 indicates the overall copy format is binary (similar to DataRow format). See COPY for more information.
          //     Int16
          //         The number of columns in the data to be copied (denoted N below).
          //     Int16[N]
          //         The format codes to be used for each column. Each must presently be zero (text) or one (binary). All must be zero if the overall copy format is textual.
          self.stream.read_i32().await?; // skip len
          let format = self.stream.read_i8().await?;
          let num_columns = self.stream.read_i16().await?;
          for i in 0..num_columns {
            self.stream.read_i16().await?;
          }
        }
        b'T' => {
          // RowDescription (B)
          //     Byte1('T')
          //         Identifies the message as a row description.
          //     Int32
          //         Length of message contents in bytes, including self.
          //     Int16
          //         Specifies the number of fields in a row (can be zero).
          //     Then, for each field, there is the following:
          //     String
          //         The field name.
          //     Int32
          //         If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
          //     Int16
          //         If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
          //     Int32
          //         The object ID of the field's data type.
          //     Int16
          //         The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
          //     Int32
          //         The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
          //     Int16
          //         The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
          self.stream.read_i32().await?; // skip len
          let num_columns = self.stream.read_i16().await?;
          for i in 0..num_columns {
            let name = self.read_c_string().await?;
            let oid = self.stream.read_i32().await?;
            let attr_number = self.stream.read_i16().await?;
            let datatype_oid = self.stream.read_i32().await?;
            let datatype_size = self.stream.read_i16().await?;
            let type_modifier = self.stream.read_i32().await?;
            let format = self.stream.read_i16().await?;

            columns.push(SimpleColumn {
              name,
              oid,
              attr_number,
              datatype_oid,
              datatype_size,
              type_modifier,
              format,
            });
          }
        }
        b'D' => {
          // DataRow (B)
          //     Byte1('D')
          //         Identifies the message as a data row.
          //     Int32
          //         Length of message contents in bytes, including self.
          //     Int16
          //         The number of column values that follow (possibly zero).
          //     Next, the following pair of fields appear for each column:
          //     Int32
          //         The length of the column value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.
          //     Byten
          //         The value of the column, in the format indicated by the associated format code. n is the above length.

          self.stream.read_i32().await?; // skip len
          let num_values = self.stream.read_i16().await?;
          for i in 0..num_values {
            let len = self.stream.read_i32().await?;

            if len > 0 {
              let mut buffer = vec![0; len.try_into().unwrap()];
              self.stream.read_exact(&mut buffer).await?;
              values.push(Some(String::from_utf8(buffer).unwrap()));
            } else if len == 0 {
              values.push(Some("".to_string()));
            } else if len == -1 {
              values.push(None);
            }
          }
        }
        b'I' => {
          // EmptyQueryResponse (B)
          //     Byte1('I')
          //         Identifies the message as a response to an empty query string. (This substitutes for CommandComplete.)
          //     Int32(4)
          //         Length of message contents in bytes, including self.
          self.stream.read_i32().await?;
          break;
        }
        b'Z' => {
          self.read_ready_for_query().await?;
          break;
        }
        b'E' => {
          return Err(self.read_backend_error().await);
        }
        b'N' => {
          self.read_backend_notice().await?;
        }
        code => {
          panic!("Unexpected backend message: {:?}", char::from(code))
        }
      }
    }

    Ok(SimpleQueryResult { columns, values })
  }

  async fn read_ready_for_query(&mut self) -> io::Result<()> {
    // ReadyForQuery (B)
    //     Byte1('Z')
    //         Identifies the message type. ReadyForQuery is sent whenever the backend is ready for a new query cycle.
    //     Int32(5)
    //         Length of message contents in bytes, including self.
    //     Byte1
    //         Current backend transaction status indicator. Possible values are 'I' if idle (not in a transaction block); 'T' if in a transaction block; or 'E' if in a failed transaction block (queries will be rejected until block is ended).
    self.stream.read_i32().await?; // skip len
    let status = self.stream.read_u8().await?;
    Ok(())
  }

  async fn read_backend_notice(&mut self) -> io::Result<()> {
    // https://www.postgresql.org/docs/11/protocol-error-fields.html
    // NoticeResponse (B)
    //     Byte1('N')
    //         Identifies the message as a notice.
    //     Int32
    //         Length of message contents in bytes, including self.
    //     The message body consists of one or more identified fields, followed by a zero byte as a terminator. Fields can appear in any order. For each field there is the following:
    //     Byte1
    //         A code identifying the field type; if zero, this is the message terminator and no string follows. The presently defined field types are listed in Section 53.8. Since more field types might be added in future, frontends should silently ignore fields of unrecognized type.
    //     String
    //         The field value.

    let fields = self.read_fields().await?;
    if self.options.handle_notices_as_errors {
      if fields.is_empty() {
        return Err(io::Error::new(
          io::ErrorKind::InvalidData,
          "missing notice fields from server",
        ));
      }
      return Err(io::Error::new(
        io::ErrorKind::Other,
        format!("Server notice {}: {}", fields[&'C'], fields[&'M']),
      ));
    }
    Ok(())
  }

  async fn read_backend_error(&mut self) -> io::Error {
    // https://www.postgresql.org/docs/11/protocol-error-fields.html
    // ErrorResponse (B)
    //     Byte1('E')
    //         Identifies the message as an error.
    //     Int32
    //         Length of message contents in bytes, including self.
    //     The message body consists of one or more identified fields, followed by a zero byte as a terminator. Fields can appear in any order. For each field there is the following:
    //     Byte1
    //         A code identifying the field type; if zero, this is the message terminator and no string follows. The presently defined field types are listed in Section 53.8. Since more field types might be added in future, frontends should silently ignore fields of unrecognized type.
    //     String
    //         The field value.
    match self.read_fields().await {
      Ok(fields) => {
        if fields.is_empty() {
          return io::Error::new(io::ErrorKind::InvalidData, "missing error fields from server");
        }

        io::Error::new(
          io::ErrorKind::Other,
          format!("Server error {}: {}", fields[&'C'], fields[&'M']),
        )
      }
      Err(err) => err,
    }
  }

  async fn read_fields(&mut self) -> io::Result<BTreeMap<char, String>> {
    self.stream.read_i32().await?; // skip len

    let mut fields = BTreeMap::new();
    loop {
      match self.stream.read_u8().await? {
        0 => break,
        token => {
          let msg = self.read_c_string().await?;
          fields.insert(char::from(token), msg);
        }
      }
    }
    Ok(fields)
  }

  async fn read_c_string(&mut self) -> io::Result<String> {
    let mut bytes = Vec::new();
    loop {
      match self.stream.read_u8().await? {
        0 => break,
        b => bytes.push(b),
      }
    }
    String::from_utf8(bytes).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
  }

  pub async fn close(mut self) -> io::Result<()> {
    self.stream.write_u8(b'X').await?;
    self.stream.write_i32(4).await?;
    self.stream.flush().await?;
    self.stream.shutdown().await?;
    Ok(())
  }
}

#[derive(Debug)]
pub struct SimpleColumn {
  name: String,
  oid: i32,
  attr_number: i16,
  datatype_oid: i32,
  datatype_size: i16,
  type_modifier: i32,
  format: i16,
}

pub type SimpleRowValue = Option<String>;

#[derive(Debug)]
pub struct SimpleQueryResult {
  pub columns: Vec<SimpleColumn>,
  pub values: Vec<SimpleRowValue>,
}

impl SimpleQueryResult {
  pub fn row(&self, i: usize) -> &[SimpleRowValue] {
    let start = i * self.columns.len();
    let end = start + self.columns.len();
    &self.values[start..end]
  }

  pub fn row_mut(&mut self, i: usize) -> &mut [SimpleRowValue] {
    let start = i * self.columns.len();
    let end = start + self.columns.len();
    &mut self.values[start..end]
  }

  pub fn rows_len(&self) -> usize {
    if !self.columns.is_empty() {
      self.values.len() / self.columns.len()
    } else {
      0
    }
  }

  pub fn rows(&self) -> Option<ChunksExact<'_, SimpleRowValue>> {
    if !self.columns.is_empty() {
      Some(self.values.chunks_exact(self.columns.len()))
    } else {
      None
    }
  }

  pub fn rows_mut(&mut self) -> Option<ChunksExactMut<'_, SimpleRowValue>> {
    if !self.columns.is_empty() {
      Some(self.values.chunks_exact_mut(self.columns.len()))
    } else {
      None
    }
  }
}

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
pub struct ReplicationStream {
  conn: Connection,
}

impl ReplicationStream {
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
