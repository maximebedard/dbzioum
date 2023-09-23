use super::binlog::BinlogEvent;
use super::binlog::BinlogEventHeader;
use super::buf_ext::BufExt;
use super::buf_ext::BufMutExt;
use super::constants::{
  BinlogDumpFlags, CapabilityFlags, CharacterSet, Command, StatusFlags, CACHING_SHA2_PASSWORD_PLUGIN_NAME,
  MAX_PAYLOAD_LEN, MYSQL_NATIVE_PASSWORD_PLUGIN_NAME,
};
use super::debug::DebugBytesRef;
use super::query::{Column, QueryResults, RowValue};
use super::scramble;
use super::stream::Stream;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cmp::max;
use std::collections::BTreeMap;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::str::FromStr;
use std::time::Duration;
use std::{fmt, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net;
use url::Url;

#[cfg(feature = "ssl")]
use openssl::ssl::SslConnector;

#[derive(Debug, Clone)]
pub struct ConnectionOptions {
  pub user: String,
  pub password: Option<String>,
  pub database: Option<String>,
  pub connect_timeout: Option<Duration>,
  pub read_timeout: Option<Duration>,
  pub write_timeout: Option<Duration>,
}

impl Default for ConnectionOptions {
  fn default() -> Self {
    Self {
      user: "mysql".to_string(),
      password: None,
      database: None,
      connect_timeout: None,
      read_timeout: None,
      write_timeout: None,
    }
  }
}

impl TryFrom<&Url> for ConnectionOptions {
  type Error = io::Error;

  fn try_from(url: &Url) -> Result<Self, Self::Error> {
    let user = match url.username() {
      "" => "mysql".to_string(),
      user => user.to_string(),
    };
    let password = url.password().map(ToString::to_string);

    let query_pairs = url.query_pairs().collect::<BTreeMap<_, _>>();
    let database = query_pairs.get("database").map(|v| v.to_string());

    let connect_timeout = query_pairs
      .get("connect_timeout_ms")
      .and_then(|v| v.parse().ok())
      .map(Duration::from_millis);

    let read_timeout = query_pairs
      .get("read_timeout_ms")
      .and_then(|v| v.parse().ok())
      .map(Duration::from_millis);

    let write_timeout = query_pairs
      .get("write_timeout_ms")
      .and_then(|v| v.parse().ok())
      .map(Duration::from_millis);

    Ok(Self {
      user,
      password,
      database,
      connect_timeout,
      read_timeout,
      write_timeout,
    })
  }
}

#[derive(Debug)]
pub struct Connection {
  stream: Stream,
  capabilities: CapabilityFlags,
  status_flags: StatusFlags,
  server_character_set: CharacterSet,
  sequence_id: u8,
  last_command_id: u8,
  options: ConnectionOptions,
  max_packet_size: u32,
  warnings: u16,
  affected_rows: u64,
  last_inserted_id: u64,
}

impl Connection {
  pub async fn connect_from_url(url: &Url) -> io::Result<Self> {
    match url.scheme() {
      "tcp" => {
        let port = url.port().unwrap_or(3306);
        let addrs = match url.host() {
          Some(url::Host::Domain(domain)) => net::lookup_host(format!("{}:{}", domain, port))
            .await
            .map(|v| v.collect::<Vec<_>>())?,
          Some(url::Host::Ipv4(ip)) => vec![SocketAddrV4::new(ip, port).into()],
          Some(url::Host::Ipv6(ip)) => vec![SocketAddrV6::new(ip, port, 0, 0).into()],
          None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "url has no host")),
        };
        let options = url.try_into()?;
        Self::connect_tcp(addrs, options).await
      }
      scheme => Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{} is not supported", scheme),
      )),
    }
  }

  #[cfg(feature = "ssl")]
  pub async fn connect_ssl_from_url(url: &Url, ssl_connector: SslConnector) -> io::Result<Self> {
    match url.scheme() {
      "tcp" => {
        let port = url.port().unwrap_or(3306);
        let (domain, addrs) = match url.host() {
          Some(url::Host::Domain(domain)) => net::lookup_host(format!("{}:{}", domain, port))
            .await
            .map(|v| (domain.to_string(), v.collect::<Vec<_>>()))?,
          Some(url::Host::Ipv4(ip)) => (ip.to_string(), vec![SocketAddrV4::new(ip, port).into()]),
          Some(url::Host::Ipv6(ip)) => (ip.to_string(), vec![SocketAddrV6::new(ip, port, 0, 0).into()]),
          None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "url has no host")),
        };
        let options = url.try_into()?;
        Self::connect_ssl(addrs, domain, options, ssl_connector).await
      }
      scheme => Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{} is not supported", scheme),
      )),
    }
  }

  pub async fn connect_tcp(addrs: impl Into<Vec<SocketAddr>>, options: ConnectionOptions) -> io::Result<Self> {
    let stream = Stream::connect_tcp(addrs).await?;
    Self::connect(stream, options).await
  }

  #[cfg(feature = "ssl")]
  pub async fn connect_ssl(
    _addrs: impl Into<Vec<SocketAddr>>,
    _domain: impl Into<String>,
    _options: ConnectionOptions,
    _ssl_connector: SslConnector,
  ) -> io::Result<Self> {
    todo!()
  }

  async fn connect(stream: Stream, options: ConnectionOptions) -> io::Result<Self> {
    let capabilities = CapabilityFlags::empty();
    let status_flags = StatusFlags::empty();
    let server_character_set = CharacterSet::UTF8MB4;
    let sequence_id = 0;
    let last_command_id = 0;
    let last_inserted_id = 0;
    let warnings = 0;
    let affected_rows = 0;
    let max_packet_size = 16_777_216; // 16MB

    let mut connection = Self {
      stream,
      capabilities,
      sequence_id,
      last_command_id,
      last_inserted_id,
      warnings,
      affected_rows,
      max_packet_size, // 16MB
      options,
      status_flags,
      server_character_set,
    };

    connection.handshake().await?;

    Ok(connection)
  }

  pub async fn duplicate(&self) -> io::Result<Self> {
    let stream = self.stream.duplicate().await?;
    Self::connect(stream, self.options.clone()).await
  }

  pub async fn close(mut self) -> io::Result<()> {
    self.write_command(Command::COM_QUIT, &[]).await?;
    let payload = self.read_payload().await;

    match payload {
      Ok(payload) => Err(self.parse_and_handle_server_error(payload)),
      // read_exact will return an UnexpectedEof if the stream is unable to fill the buffer.
      Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(()),
      Err(err) => Err(err),
    }
  }

  async fn handshake(&mut self) -> io::Result<()> {
    // https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
    let payload = self.read_payload().await?;

    match payload.first() {
      Some(0xFF) => Err(self.parse_and_handle_server_error(payload)),
      Some(_) => {
        let handshake = Handshake::parse(payload)?;
        self.handle_handshake(handshake).await
      }
      None => Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "Unexpected EOF while parsing handshake response",
      )),
    }
  }

  fn handle_server_error(&mut self, err: ServerError) -> io::Error {
    io::Error::new(
      io::ErrorKind::Other,
      format!("Server error {}: {}", err.error_code, err.error_message),
    )
  }

  async fn handle_handshake(&mut self, p: Handshake) -> io::Result<()> {
    // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html
    if p.protocol_version != 10u8 {
      unimplemented!()
    }

    if !p.capabilities.contains(CapabilityFlags::CLIENT_PROTOCOL_41) {
      unimplemented!()
    }

    // Intersection between what the server supports, and what our client supports.
    self.capabilities = p.capabilities & default_client_capabilities(&self.options);
    self.status_flags = p.status_flags;
    self.server_character_set = p.character_set;

    // if self.options.use_ssl {
    //   // TODO: ssl
    //   unimplemented!()
    // }

    self
      .write_handshake_response(p.auth_plugin.as_str(), p.nonce().chunk())
      .await?;
    self.read_auth_switch_request().await?;

    Ok(())
  }

  pub async fn read_auth_switch_request(&mut self) -> io::Result<()> {
    loop {
      let mut payload = self.read_payload().await?;

      match payload.first() {
        Some(0x00) => return self.parse_and_handle_server_ok(payload),
        // AuthMoreData
        Some(0x01) => {
          if payload.chunk() == [0x01, 0x04] {
            return Err(io::Error::new(io::ErrorKind::ConnectionReset, "SSL required"));
          }

          todo!("AuthMoreData");
        }
        // AuthNextFactor
        Some(0x02) => {
          todo!("AuthNextFactor");
        }
        // AuthSwitch
        Some(0xFE) => {
          payload.advance(1);
          let auth_plugin = payload.mysql_get_null_terminated_string();
          let nonce = payload.mysql_get_null_terminated_string();
          self
            .write_auth_switch_response(auth_plugin.as_str(), nonce.as_bytes())
            .await?;
        }
        Some(0xFF) => return Err(self.parse_and_handle_server_error(payload)),
        Some(_) => panic!(),
        None => {
          return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Unexpected EOF while parsing login response",
          ))
        }
      }
    }
  }

  /// Send a text query to MYSQL and returns a result set.
  pub async fn query(&mut self, query: impl AsRef<str>) -> io::Result<QueryResults> {
    self
      .write_command(Command::COM_QUERY, query.as_ref().as_bytes())
      .await?;
    self.read_results().await
  }

  pub async fn ping(&mut self) -> io::Result<()> {
    self.write_command(Command::COM_PING, &[]).await?;

    let payload = self.read_payload().await?;
    match payload.first() {
      Some(0x00) => self.parse_and_handle_server_ok(payload),
      _ => Err(io::Error::new(io::ErrorKind::Other, "Unexpected response from mysql")),
    }
  }

  async fn write_command(&mut self, cmd: Command, payload: &[u8]) -> io::Result<()> {
    self.sequence_id = 0;
    self.last_command_id = cmd as u8;

    let mut b = BytesMut::with_capacity(1 + payload.len());
    b.put_u8(cmd as u8);
    b.put(payload);

    self.write_payload(b.into()).await
  }

  async fn write_payload(&mut self, payload: Bytes) -> io::Result<()> {
    for chunk in payload.chunks(MAX_PAYLOAD_LEN) {
      let mut b = BytesMut::with_capacity(4 + chunk.len());
      b.put_uint_le(chunk.len() as u64, 3);
      b.put_u8(self.sequence_id);
      b.put(chunk);

      eprintln!(">> {:?}", DebugBytesRef(chunk));

      self.sequence_id = self.sequence_id.wrapping_add(1);
      self.stream.write_all(&b[..]).await?;
      self.stream.flush().await?;
    }

    Ok(())
  }

  async fn read_generic_reponse(&mut self) -> io::Result<()> {
    let payload = self.read_payload().await?;

    match payload.first() {
      Some(0x00) => self.parse_and_handle_server_ok(payload),
      Some(0xFF) => Err(self.parse_and_handle_server_error(payload)),
      Some(_) => Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid data while parsing generic response",
      )),
      None => Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "Unexpected EOF while parsing generic response",
      )),
    }
  }

  async fn read_results(&mut self) -> io::Result<QueryResults> {
    // https://dev.mysql.com/doc/internals/en/com-query-response.html
    let mut payload = self.read_payload().await?;

    match payload.first() {
      Some(0x00) => {
        self.parse_and_handle_server_ok(payload)?;
        Ok(QueryResults::default())
      }
      Some(0xFF) => Err(self.parse_and_handle_server_error(payload)),
      Some(0xFB) => todo!("infile not supported"),
      Some(_) => {
        let column_count = payload.mysql_get_lenc_uint().try_into().unwrap();
        let columns = self.read_columns(column_count).await?;
        let values = self.read_row_values(&columns).await?;
        let query_results = QueryResults { columns, values };
        Ok(query_results)
      }
      None => Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "Unexpected EOF while parsing query result response",
      )),
    }
  }

  async fn read_columns(&mut self, column_count: usize) -> io::Result<Vec<Column>> {
    // https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
    let mut columns = Vec::with_capacity(column_count);
    for _i in 0..column_count {
      let payload = self.read_payload().await?;
      match payload.first() {
        Some(0x00) => {
          self.parse_and_handle_server_ok(payload)?;
          break;
        }
        Some(_) => {
          let column = Column::parse(payload)?;
          columns.push(column);
        }
        None => {
          return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Unexpected EOF while parsing query column response",
          ))
        }
      }
    }
    Ok(columns)
  }

  async fn read_row_values(&mut self, columns: &Vec<Column>) -> io::Result<Vec<RowValue>> {
    // https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
    let mut row_values = Vec::new();
    loop {
      let mut payload = self.read_payload().await?;

      match payload.first() {
        Some(0x00) | Some(0xFE) => {
          self.parse_and_handle_server_ok(payload)?;
          break;
        }
        Some(_) => {
          for _i in 0..columns.len() {
            match payload.first() {
              Some(0xFB) => {
                payload.advance(1);
                row_values.push(None);
              }
              Some(_) => {
                let value = payload.mysql_get_lenc_string();
                row_values.push(Some(value));
              }
              None => {
                return Err(io::Error::new(
                  io::ErrorKind::UnexpectedEof,
                  "Unexpected EOF while parsing query row value",
                ))
              }
            }
          }
        }
        None => {
          return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Unexpected EOF while parsing query row",
          ))
        }
      }
    }
    Ok(row_values)
  }

  fn handle_server_ok(&mut self, ok: ServerOk) {
    self.affected_rows = ok.affected_rows;
    self.last_inserted_id = ok.last_inserted_id;
    self.status_flags = ok.status_flags.unwrap_or(StatusFlags::empty());
    self.warnings = ok.warnings.unwrap_or(0);
  }

  async fn read_payload(&mut self) -> io::Result<Bytes> {
    let (sequence_id, payload) = self.read_packet().await?;
    if self.sequence_id != sequence_id {
      return Err(io::Error::new(io::ErrorKind::InvalidData, "Packet is out of sync"));
    }
    self.sequence_id = self.sequence_id.wrapping_add(1);
    eprintln!("<< {:?}", DebugBytesRef(payload.chunk()));
    Ok(payload)
  }

  fn scramble_password(&self, auth_plugin: &str, nonce: &[u8]) -> io::Result<Vec<u8>> {
    let password = self.options.password.as_ref().map(String::as_bytes).unwrap_or_default();

    if password.is_empty() {
      return Err(io::Error::new(io::ErrorKind::InvalidInput, "password is required"));
    }

    match auth_plugin {
      MYSQL_NATIVE_PASSWORD_PLUGIN_NAME => Ok(scramble::scramble_native(nonce, password).to_vec()),
      CACHING_SHA2_PASSWORD_PLUGIN_NAME => Ok(scramble::scramble_sha256(nonce, password).to_vec()),
      custom_auth_plugin => Err(io::Error::new(
        io::ErrorKind::Other,
        format!("{} is not supported", custom_auth_plugin),
      )),
    }
  }

  async fn write_auth_switch_response(&mut self, auth_plugin: &str, nonce: &[u8]) -> io::Result<()> {
    let scrambled_data = self.scramble_password(auth_plugin, nonce)?;
    self.write_payload(scrambled_data.into()).await
  }

  async fn write_handshake_response(&mut self, auth_plugin: &str, nonce: &[u8]) -> io::Result<()> {
    let mut b = BytesMut::new();
    b.put_u32_le(self.capabilities.bits());
    b.put_u32_le(self.max_packet_size);
    b.put_u8(CharacterSet::UTF8 as u8);
    b.put(&[0; 23][..]);
    b.put(self.options.user.as_bytes());
    b.put_u8(0);

    let scrambled_data = self.scramble_password(auth_plugin, nonce)?;

    b.mysql_put_lenc_uint(scrambled_data.len() as u64);
    b.put(scrambled_data.as_slice());

    if let Some(db_name) = self.options.database.as_ref() {
      b.put(db_name.as_bytes());
      b.put_u8(0);
    }

    b.put(auth_plugin.as_bytes());
    b.put_u8(0);

    // TODO: connection attributes (e.g. name of the client, version, etc...)
    self.write_payload(b.into()).await
  }

  async fn read_packet(&mut self) -> io::Result<(u8, Bytes)> {
    let mut header = vec![0; 4];
    self.stream.read_exact(&mut header).await?;

    let mut header = header.as_slice();

    let payload_len = header.get_uint_le(3).try_into().unwrap();
    let sequence_id = header.get_u8();

    let mut payload = vec![0; payload_len];
    self.stream.read_exact(&mut payload).await?;

    Ok((sequence_id, payload.into()))
  }

  pub async fn binlog_cursor(&mut self) -> io::Result<BinlogCursor> {
    let mut values = self.query("SHOW MASTER STATUS").await.map(|mut v| {
      v.values.reverse();
      v.values
    })?;
    let log_file = values.pop().unwrap().unwrap();
    let log_position = values.pop().unwrap().unwrap().parse().unwrap();
    Ok(BinlogCursor { log_file, log_position })
  }

  // Returns a stream that yields binlog events, starting from a given position and binlog file.
  pub async fn binlog_stream(
    mut self,
    server_id: u32,
    binlog_cursor: impl Into<BinlogCursor>,
  ) -> io::Result<BinlogStream> {
    let binlog_cursor = binlog_cursor.into();
    self.source_configuration_check().await?;
    self.register_as_replica(server_id).await?;
    self.dump_binlog(server_id, &binlog_cursor).await?;
    let conn = self;
    Ok(BinlogStream { conn })
  }

  async fn read_binlog_event_packet(&mut self) -> io::Result<(BinlogEventHeader, BinlogEvent)> {
    let payload = self.read_payload().await?;

    match payload.first() {
      Some(0x00) => BinlogEventHeader::parse(payload),
      Some(0xFF) => Err(self.parse_and_handle_server_error(payload)),
      Some(_) => Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid data while parsing binlog event response",
      )),
      None => Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "Unexpected EOF while parsing binlog event response",
      )),
    }
  }

  fn parse_and_handle_server_ok(&mut self, payload: Bytes) -> io::Result<()> {
    ServerOk::parse(payload, self.capabilities).map(|ok| self.handle_server_ok(ok))
  }

  fn parse_and_handle_server_error(&mut self, payload: Bytes) -> io::Error {
    match ServerError::parse(payload, self.capabilities) {
      Ok(err) => self.handle_server_error(err),
      Err(err) => err,
    }
  }

  async fn source_configuration_check(&mut self) -> io::Result<()> {
    // This is fucking weird. Setting it on mysql doesn't work the same as setting it on a per-session basis.
    self.query("SET @source_binlog_checksum='NONE'").await?;
    // TODO: Actually remove this check.
    // self.query("SELECT @@GLOBAL.binlog_checksum;").await.map(|v| {
    //   assert_eq!(v.values[0].as_ref().map(String::as_str), Some("NONE"));
    // })?;

    self.query("SELECT @@GLOBAL.binlog_row_metadata;").await.map(|v| {
      assert_eq!(v.values[0].as_deref(), Some("FULL"));
    })?;

    Ok(())
  }

  async fn register_as_replica(&mut self, server_id: u32) -> io::Result<()> {
    // TODO: figure out something else here...
    let hostname = &b"localhost"[..];
    let port = 1322;
    let user = self.options.user.as_bytes();
    let password = self.options.password.as_ref().map(String::as_bytes).unwrap_or(b"");

    let payload_len = 4 + 1 + hostname.len() + 1 + user.len() + 1 + password.len() + 2 + 4 + 4;

    let mut b = BytesMut::with_capacity(payload_len);

    b.put_u32_le(server_id);
    b.put_u8(hostname.len() as u8);
    b.put(hostname);
    b.put_u8(user.len() as u8);
    b.put(user);
    b.put_u8(password.len() as u8);
    b.put(password);
    b.put_u16_le(port);
    b.put_u32(0); // replication_rank ignored.
    b.put_u32(0); // master id is usually 0.

    self.write_command(Command::COM_REGISTER_SLAVE, &b[..]).await?;
    self.read_generic_reponse().await
  }

  async fn dump_binlog(&mut self, server_id: u32, binlog_cursor: &BinlogCursor) -> io::Result<()> {
    let file = binlog_cursor.log_file.as_bytes();
    let file_len = file.len();

    let payload_len = 4 + 2 + 4 + file_len + 1;

    let mut b = BytesMut::with_capacity(payload_len);
    b.put_u32_le(binlog_cursor.log_position);
    b.put_u16_le(BinlogDumpFlags::empty().bits());
    b.put_u32_le(server_id);
    b.put(file);

    self.write_command(Command::COM_BINLOG_DUMP, &b[..]).await
  }
}

// Defines the default capabilities that our client support.
fn default_client_capabilities(opts: &ConnectionOptions) -> CapabilityFlags {
  let mut capabilities = CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_LONG_PASSWORD
        | CapabilityFlags::CLIENT_PLUGIN_AUTH
        | CapabilityFlags::CLIENT_LONG_FLAG
        | CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
        | CapabilityFlags::CLIENT_RESERVED2
        // | CapabilityFlags::CLIENT_CONNECT_ATTRS
        | CapabilityFlags::CLIENT_DEPRECATE_EOF;

  if opts.database.as_ref().filter(|v| !v.is_empty()).is_some() {
    capabilities.insert(CapabilityFlags::CLIENT_CONNECT_WITH_DB);
  }

  // if opts.use_ssl {
  //   capabilities.insert(CapabilityFlags::CLIENT_SSL);
  // }

  capabilities
}

#[derive(Debug)]
pub struct Handshake {
  capabilities: CapabilityFlags,
  protocol_version: u8,
  scramble_1: Bytes,
  scramble_2: Option<Bytes>,
  auth_plugin: String,
  character_set: CharacterSet,
  status_flags: StatusFlags,
}

impl Handshake {
  fn parse(mut b: Bytes) -> io::Result<Self> {
    // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html
    let protocol_version = b.get_u8();
    let _server_version = b.mysql_get_null_terminated_string();
    let _connection_id = b.get_u32_le();
    let scramble_1 = b.split_to(8);
    b.advance(1);
    let capabilities_1 = b.get_u16_le();
    let character_set = b.get_u8().try_into().unwrap();
    let status_flags = StatusFlags::from_bits_truncate(b.get_u16_le());
    let capabilities_2 = b.get_u16_le();

    let capabilities = CapabilityFlags::from_bits_truncate(capabilities_1 as u32 | ((capabilities_2 as u32) << 16));

    if !capabilities.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH) {
      return Err(io::Error::new(
        io::ErrorKind::Other,
        "CLIENT_PLUGIN_AUTH flag is not set",
      ));
    }

    let scramble_len: i16 = b.get_u8().try_into().unwrap();
    b.advance(10);

    let scramble_2_len = max(12, scramble_len - 9).try_into().unwrap();
    let scramble_2 = Some(b.split_to(scramble_2_len));
    b.advance(1);

    let auth_plugin = b.mysql_get_null_terminated_string();

    Ok(Self {
      capabilities,
      protocol_version,
      scramble_1,
      scramble_2,
      auth_plugin,
      status_flags,
      character_set,
    })
  }

  fn nonce(&self) -> Bytes {
    let mut out = BytesMut::new();
    out.extend_from_slice(self.scramble_1.chunk());

    if let Some(scramble_2) = self.scramble_2.as_ref().map(Bytes::chunk) {
      out.extend_from_slice(scramble_2);
    }

    out.freeze()
  }
}

// https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
#[derive(Debug)]
struct ServerOk {
  affected_rows: u64,
  last_inserted_id: u64,
  status_flags: Option<StatusFlags>,
  warnings: Option<u16>,
  info: String,
  session_state_changes: Option<String>,
}

impl ServerOk {
  fn parse(mut b: Bytes, capability_flags: CapabilityFlags) -> io::Result<Self> {
    let _header = b.get_u8();
    let affected_rows = b.mysql_get_lenc_uint();
    let last_inserted_id = b.mysql_get_lenc_uint();

    let mut status_flags = None;
    let mut warnings = None;
    if capability_flags.contains(CapabilityFlags::CLIENT_PROTOCOL_41) {
      status_flags = Some(StatusFlags::from_bits_truncate(b.get_u16_le()));
      warnings = Some(b.get_u16_le());
    } else if capability_flags.contains(CapabilityFlags::CLIENT_TRANSACTIONS) {
      status_flags = Some(StatusFlags::from_bits_truncate(b.get_u16_le()));
    }

    let (info, session_state_changes) = if capability_flags.contains(CapabilityFlags::CLIENT_SESSION_TRACK) {
      let info = b.mysql_get_lenc_string();

      let has_session_state_changes = status_flags
        .map(|f| f.contains(StatusFlags::SERVER_SESSION_STATE_CHANGED))
        .unwrap_or(false);

      let mut session_state_changes = None;
      if has_session_state_changes {
        session_state_changes = Some(b.mysql_get_lenc_string())
      }

      (info, session_state_changes)
    } else {
      let info = b.mysql_get_eof_string();
      (info, None)
    };

    Ok(Self {
      affected_rows,
      last_inserted_id,
      status_flags,
      warnings,
      info,
      session_state_changes,
    })
  }
}

// https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
#[derive(Debug)]
pub struct ServerError {
  error_code: u16,
  state_marker: Option<String>,
  state: Option<String>,
  error_message: String,
}

impl ServerError {
  fn parse(mut b: Bytes, capability_flags: CapabilityFlags) -> io::Result<Self> {
    let _header = b.get_u8();
    let error_code = b.get_u16_le();

    let mut state_marker = None;
    let mut state = None;

    if capability_flags.contains(CapabilityFlags::CLIENT_PROTOCOL_41) {
      state_marker = Some(b.mysql_get_fixed_length_string(1));
      state = Some(b.mysql_get_fixed_length_string(5));
    }

    let error_message = b.mysql_get_eof_string();
    Ok(Self {
      error_code,
      state_marker,
      state,
      error_message,
    })
  }
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct BinlogCursor {
  pub log_file: String,
  pub log_position: u32,
}

impl fmt::Display for BinlogCursor {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}/{}", self.log_file, self.log_position)
  }
}

impl FromStr for BinlogCursor {
  type Err = String;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let (log_file, log_position) = s
      .split_once('/')
      .ok_or_else(|| "Failed to parse binlog cursor. Expected format is <prefix>.<file>/<position>".to_string())?;
    let log_file = log_file.to_string();
    let log_position = log_position
      .parse()
      .map_err(|_| "Failed to parse binlog cursor position. Expected format is u32.".to_string())?;
    Ok(Self { log_file, log_position })
  }
}

#[derive(Debug)]
pub struct BinlogStream {
  conn: Connection,
}

impl BinlogStream {
  pub async fn close(mut self) -> io::Result<()> {
    // force shutdown the underlying stream since the stream is no longer in duplex mode.
    self.conn.stream.shutdown().await
  }

  pub async fn recv(&mut self) -> Option<io::Result<(BinlogEventHeader, BinlogEvent)>> {
    // TODO: handle disconnects and reconnect here...
    Some(self.conn.read_binlog_event_packet().await)
  }
}
