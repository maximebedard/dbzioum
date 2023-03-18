use bytes::{Buf, BufMut, BytesMut};
use std::io;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpStream;

use super::buf_ext::BufExt;
use super::protocol::{
    BinlogDumpFlags, CapabilityFlags, CharacterSet, Column, Command, Handshake, Packet, Row,
    ServerError, ServerOk, StatusFlags, CACHING_SHA2_PASSWORD_PLUGIN_NAME, MAX_PAYLOAD_LEN,
    MYSQL_NATIVE_PASSWORD_PLUGIN_NAME,
};
use super::protocol_binlog::{BinlogEvent, BinlogEventPacket};
use super::value::Value;

#[derive(Debug)]
pub struct ConnectionOptions {
    pub addr: SocketAddr,
    pub user: String,
    pub password: String,
    pub db_name: Option<String>,
    pub hostname: Option<String>,
    pub server_id: Option<u32>,
    pub use_compression: bool,
    pub use_ssl: bool,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            addr: "[::]:3306".parse().unwrap(),
            user: "root".to_string(),
            password: "".to_string(),
            db_name: None,
            hostname: None,
            server_id: None,
            use_compression: false,
            use_ssl: false,
        }
    }
}

pub struct ReplicationOptions {
    pub hostname: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub server_id: u32,
    pub port: u16,
}

impl Default for ReplicationOptions {
    fn default() -> Self {
        let hostname = None;
        let user = None;
        let password = None;
        let server_id = 1;
        let port = 3306;
        Self {
            hostname,
            user,
            password,
            server_id,
            port,
        }
    }
}

pub struct Connection {
    stream: BufStream<TcpStream>,
    capabilities: CapabilityFlags,
    status_flags: StatusFlags,
    server_character_set: CharacterSet,
    buffer: BytesMut,
    sequence_id: u8,
    last_command_id: u8,
    opts: ConnectionOptions,
    max_packet_size: u32,
    warnings: u16,
    affected_rows: u64,
    last_inserted_id: u64,
}

impl Connection {
    pub async fn new(opts: impl Into<ConnectionOptions>) -> io::Result<Self> {
        let capabilities = CapabilityFlags::empty();
        let status_flags = StatusFlags::empty();
        let server_character_set = CharacterSet::UTF8MB4;
        let buffer = BytesMut::with_capacity(4 * 1024);
        let sequence_id = 0;
        let last_command_id = 0;
        let last_inserted_id = 0;
        let warnings = 0;
        let affected_rows = 0;
        let max_packet_size = 16_777_216; // 16MB
        let opts = opts.into();
        let io = TcpStream::connect(opts.addr).await?;
        let io = BufStream::new(io);

        Ok(Self {
            stream: io,
            capabilities,
            buffer,
            sequence_id,
            last_command_id,
            last_inserted_id,
            warnings,
            affected_rows,
            max_packet_size, // 16MB
            opts,
            status_flags,
            server_character_set,
        })
    }

    pub async fn disconnect(&mut self) -> io::Result<()> {
        self.write_command(Command::COM_QUIT, &[]).await?;
        let payload = self.read_payload().await;

        match payload {
            Ok(payload) => self.parse_and_handle_server_ok(payload),
            Err(err) if err.kind() == io::ErrorKind::ConnectionAborted => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub async fn handshake(&mut self) -> io::Result<()> {
        // https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
        let payload = self.read_payload().await?;

        match payload.get(0) {
            Some(0xFF) => {
                let err = ServerError::parse(payload, self.capabilities)?;
                Err(self.handle_server_error(err).into())
            }
            Some(_) => {
                let handshake = Handshake::parse(payload)?;
                self.handle_handshake(handshake).await.map_err(Into::into)
            }
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Unexpected EOF while parsing handshake response",
            )),
        }
    }

    fn handle_server_error(&mut self, err: ServerError) -> io::Error {
        panic!("err = {:?}", err);
    }

    async fn handle_handshake(&mut self, p: Handshake) -> io::Result<()> {
        if p.protocol_version() != 10u8 {
            panic!("not supported")
        }

        if !p
            .capabilities()
            .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
        {
            panic!("not supported")
        }

        // Intersection between what the server supports, and what our client supports.
        self.capabilities = p.capabilities() & default_capabilities(&self.opts);
        self.status_flags = p.status_flags();
        self.server_character_set = p.character_set();
        // potentially keep the server version too?

        if self.opts.use_ssl {
            // TODO: ssl
            panic!("not supported");
        }

        let nonce = p.nonce();
        let auth_plugin_name = p.auth_plugin_name();
        let auth_data = scramble_password(auth_plugin_name, self.opts.password.as_str(), &nonce)?;
        self.write_handshake_response(auth_plugin_name, auth_data)
            .await?;
        self.authenticate(auth_plugin_name, &nonce).await?;

        if self.capabilities.contains(CapabilityFlags::CLIENT_COMPRESS) {
            // TODO: wrap stream to a compressed stream.
            panic!("not supported");
        }

        Ok(())
    }

    /// Send a text query to MYSQL and returns a result set.
    pub async fn query(&mut self, query: impl AsRef<str>) -> io::Result<QueryResults> {
        // TODO: Vec<T> could potentially be a stream if we want to support multi result sets...
        self.write_command(Command::COM_QUERY, query.as_ref().as_bytes())
            .await?;
        self.read_results().await
    }

    /// Send a text query to MYSQL and yield only the first result.
    pub async fn pop(&mut self, query: impl AsRef<str>) -> io::Result<Option<QueryResult>> {
        self.query(query).await.map(QueryResults::pop)
    }

    pub async fn ping(&mut self) -> io::Result<()> {
        self.write_command(Command::COM_PING, &[]).await?;

        let payload = self.read_payload().await?;
        match payload.get(0) {
            Some(0x00) => ServerOk::parse(payload, self.capabilities)
                .map(|ok| self.handle_ok(ok))
                .map_err(Into::into),
            _ => panic!("todo"),
        }
    }

    async fn write_command(&mut self, cmd: Command, payload: &[u8]) -> io::Result<()> {
        self.sequence_id = 0;
        self.last_command_id = cmd as u8;

        let mut b = BytesMut::with_capacity(1 + payload.len());
        b.put_u8(cmd as u8);
        b.put(payload);

        self.write_payload(&b[..]).await
    }

    async fn write_payload(&mut self, payload: &[u8]) -> io::Result<()> {
        for chunk in payload.chunks(MAX_PAYLOAD_LEN) {
            let mut b = BytesMut::with_capacity(4 + chunk.len());
            b.put_uint_le(chunk.len() as u64, 3);
            b.put_u8(self.sequence_id);
            b.put(chunk);

            println!(">> {:02X?}", chunk);

            self.sequence_id = self.sequence_id.wrapping_add(1);
            self.stream.write(&b[..]).await?;
        }

        Ok(())
    }

    async fn read_generic_reponse(&mut self) -> io::Result<()> {
        let payload = self.read_payload().await?;

        match payload.get(0) {
            Some(0x00) => self.parse_and_handle_server_ok(payload),
            Some(0xFF) => {
                let err = ServerError::parse(payload, self.capabilities)?;
                Err(self.handle_server_error(err).into())
            }
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
        let payload = self.read_payload().await?;

        match payload.get(0) {
            Some(0x00) => self.parse_and_handle_server_ok(payload).unwrap_or_default(),
            Some(0xFF) => {
                let err = ServerError::parse(payload, self.capabilities)?;
                Err(self.handle_server_error(err).into())
            }
            Some(0xFB) => todo!("infile not supported"),
            Some(_) => {
                // TODO: potentially try to move this out of here?
                let column_count = payload.as_slice().mysql_get_lenc_uint().unwrap() as usize;
                let columns = self.read_columns(column_count).await?;
                let rows = self.read_rows(&columns).await?;
                let query_results = QueryResults {
                    columns: Arc::new(columns),
                    rows,
                };
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
        for i in 0..column_count {
            let payload = self.read_payload().await?;
            match payload.get(0) {
                Some(0x00) => {
                    let _ =
                        ServerOk::parse(payload, self.capabilities).map(|ok| self.handle_ok(ok))?;
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

    async fn read_rows(&mut self, columns: &Vec<Column>) -> io::Result<Vec<Row>> {
        // https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
        let mut rows = Vec::new();
        loop {
            let payload = self.read_payload().await?;

            match payload.get(0) {
                Some(0x00) | Some(0xFE) => {
                    let _ =
                        ServerOk::parse(payload, self.capabilities).map(|ok| self.handle_ok(ok))?;
                    break;
                }
                Some(_) => {
                    let row = Row::parse(payload, columns)?;
                    rows.push(row);
                }
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Unexpected EOF while parsing query row response",
                    ))
                }
            }
        }
        Ok(rows)
    }

    async fn authenticate(&mut self, auth_plugin_name: &str, nonce: &[u8]) -> io::Result<()> {
        let payload = self.read_payload().await?;

        // 0x00 = Ok, 0xFF = Err, 0xFE = AuthSwitch, 0x01 = AuthMoreData
        match (auth_plugin_name, payload.get(0)) {
            (MYSQL_NATIVE_PASSWORD_PLUGIN_NAME, Some(0x00)) => {
                ServerOk::parse(payload, self.capabilities)
                    .map(|ok| self.handle_ok(ok))
                    .map_err(Into::into)
            }
            (MYSQL_NATIVE_PASSWORD_PLUGIN_NAME, Some(0xFE)) => {
                todo!();
            }
            (CACHING_SHA2_PASSWORD_PLUGIN_NAME, Some(0x00)) => todo!(),
            (CACHING_SHA2_PASSWORD_PLUGIN_NAME, Some(0xFE)) => todo!(),
            (CACHING_SHA2_PASSWORD_PLUGIN_NAME, Some(0x01)) => todo!(),
            (_, Some(0xff)) => {
                let err = ServerError::parse(payload, self.capabilities)?;
                Err(self.handle_server_error(err).into())
            }
            (custom, Some(_)) => panic!("custom not supported"),
            (_, None) => panic!("todo"),
        }
    }

    fn handle_ok(&mut self, ok: ServerOk) {
        self.affected_rows = ok.affected_rows();
        self.last_inserted_id = ok.last_inserted_id();
        self.status_flags = ok.status_flags().unwrap_or(StatusFlags::empty());
        self.warnings = ok.warnings().unwrap_or(0);
    }

    async fn read_payload(&mut self) -> io::Result<Vec<u8>> {
        let packet = self.read_packet().await?;
        self.check_sequence_id(packet.sequence_id())?;
        let payload = packet.take_payload();
        println!("<< {:02X?}", payload);
        Ok(payload)
    }

    fn check_sequence_id(&mut self, sequence_id: u8) -> io::Result<()> {
        if self.sequence_id != sequence_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Packet is out of sync",
            ));
        }

        self.sequence_id = self.sequence_id.wrapping_add(1);
        Ok(())
    }

    async fn write_handshake_response(
        &mut self,
        auth_plugin_name: &str,
        scrambled_data: Option<Vec<u8>>,
    ) -> io::Result<()> {
        let auth_plugin_name = auth_plugin_name.as_bytes();
        let auth_plugin_len = auth_plugin_name.len();
        let user = self.opts.user.as_bytes();
        let db_name = self.opts.db_name.as_ref().map(String::as_bytes);
        let db_name_len = db_name.map(|x| x.len()).unwrap_or(0);
        let scramble_data_len = scrambled_data.as_ref().map(|x| x.len()).unwrap_or(0);

        let mut payload_len = 4 + 4 + 1 + 23 + 1 + scramble_data_len + auth_plugin_len;
        if user.len() > 0 {
            payload_len += user.len() + 1;
        }
        if db_name_len > 0 {
            payload_len += db_name_len + 1;
        }

        let mut b = BytesMut::with_capacity(payload_len);
        b.put_u32_le(self.capabilities.bits());
        b.put_u32_le(self.max_packet_size);
        b.put_u8(client_character_set() as u8);
        b.put(&[0; 23][..]);

        if user.len() > 0 {
            b.put(user);
            b.put_u8(0);
        }

        b.put_u8(scramble_data_len as u8);
        if let Some(scrable_data) = scrambled_data {
            b.put(scrable_data.as_slice());
        }

        if let Some(db_name) = db_name {
            b.put(db_name);
            b.put_u8(0);
        }

        b.put(auth_plugin_name);
        b.put_u8(0);

        // TODO: connection attributes (e.g. name of the client, version, etc...)
        self.write_payload(&b[..]).await
    }

    // TODO: move this out of here...
    async fn read_packet(&mut self) -> io::Result<Packet> {
        loop {
            let mut buf = Cursor::new(&self.buffer[..]);

            // We have enough data to parse a complete MYSQL packet.
            if Packet::check(&mut buf) {
                buf.set_position(0);
                let packet = Packet::parse(&mut buf)?;
                let len = buf.position() as usize;
                self.buffer.advance(len);
                return Ok(packet);
            }

            // There is not enough buffered data to read a frame. Attempt to read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end of stream".
            if self.stream.read(&mut self.buffer).await? == 0 {
                if self.buffer.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "Connection closed",
                    ));
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "Connection reset by peer",
                    ));
                }
            }
        }
    }

    async fn get_system_variable(
        &mut self,
        var: impl AsRef<str>,
    ) -> io::Result<Option<QueryResult>> {
        self.pop(format!("SELECT @@{}", var.as_ref())).await
    }

    /// Returns a stream that yields binlog events, starting from the very beginning of the current log.
    // pub async fn binlog_stream<'a>(
    //     &'a mut self,
    //     replication_opts: impl Into<ReplicationOptions>,
    // ) -> DriverResult<impl Stream<Item = DriverResult<BinlogEvent>> + 'a> {
    //     let master_status = self.pop("SHOW MASTER STATUS").await.and_then(|r| {
    //         r.map(Ok)
    //             .unwrap_or_else(|| Err(DriverError::ReplicationDisabled))
    //     })?;

    //     let values = master_status.values();
    //     println!("{:?}", values);
    //     let file = values[0].as_str().expect("Must be string").to_string();
    //     let position = values[1].as_u32().expect("Must be u32");
    //     let opts = replication_opts.into();
    //     println!("binlog file = {}", file);
    //     println!("position = {}", position);
    //     self.resume_binlog_stream(opts, file, position).await
    // }

    /// Returns a stream that yields binlog events, starting from a given position and binlog file.
    // pub async fn resume_binlog_stream<'a>(
    //     &'a mut self,
    //     replication_opts: impl Into<ReplicationOptions>,
    //     file: impl AsRef<str>,
    //     position: u32,
    // ) -> DriverResult<impl Stream<Item = DriverResult<BinlogEvent>> + 'a> {
    //     let replication_opts = replication_opts.into();
    //     let server_id = replication_opts.server_id();

    //     self.ensure_checksum_is_disabled().await?;
    //     self.register_as_replica(&replication_opts).await?;
    //     self.dump_binlog(server_id, file, position).await?;

    //     let stream = futures::stream::unfold(self, |conn| async move {
    //         conn.read_binlog_event()
    //             .await
    //             .transpose()
    //             .map(|evt| (evt, conn))
    //     });

    //     Ok(stream)
    // }

    async fn read_binlog_event(&mut self) -> io::Result<Option<BinlogEvent>> {
        let payload = self.read_payload().await?;

        match payload.get(0) {
            Some(0x00) => {
                let p = BinlogEventPacket::parse(payload)?;
                Ok(Some(p.into_binlog_event()?))
            }
            Some(0xFF) => self.parse_and_handle_server_error(payload),
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

    fn parse_and_handle_server_ok(&mut self, payload: Vec<u8>) -> io::Result<()> {
        ServerOk::parse(payload, self.capabilities)
            .map(|ok| self.handle_ok(ok))
            .map_err(Into::into)
    }

    fn parse_and_handle_server_error(&mut self, payload: Vec<u8>) -> io::Result<()> {
        let err = ServerError::parse(payload, self.capabilities)?;
        Err(self.handle_server_error(err).into())
    }

    async fn ensure_checksum_is_disabled(&mut self) -> io::Result<()> {
        self.query("SET @master_binlog_checksum='NONE'").await?;
        Ok(())
        // TODO: it most likely better to check the value before actually trying to set it.

        // let checksum = self.get_system_variable("binlog_checksum")
        //   .await
        //   .and_then(QueryResult::value_as_str);

        //       let checksum = self.get_system_var("binlog_checksum")
        //           .map(from_value::<String>)
        //           .unwrap_or("NONE".to_owned());

        //       match checksum.as_ref() {
        //           "NONE" => Ok(()),
        //           "CRC32" => {
        //               self.query("SET @master_binlog_checksum='NONE'")?;
        //               Ok(())
        //           }
        //           _ => Err(DriverError(UnexpectedPacket)),
        //       }
    }

    async fn register_as_replica(
        &mut self,
        replication_opts: &ReplicationOptions,
    ) -> io::Result<()> {
        let hostname = replication_opts
            .hostname
            .as_ref()
            .map(String::as_bytes)
            .unwrap_or(b"");
        let user = replication_opts
            .user
            .as_ref()
            .map(String::as_bytes)
            .unwrap_or(b"");
        let password = replication_opts
            .password
            .as_ref()
            .map(String::as_bytes)
            .unwrap_or(b"");
        let server_id = replication_opts.server_id;
        let port = replication_opts.port;

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

        self.write_command(Command::COM_REGISTER_SLAVE, &b[..])
            .await?;
        self.read_generic_reponse().await?;

        Ok(())
    }

    async fn dump_binlog(
        &mut self,
        server_id: u32,
        file: impl AsRef<str>,
        position: u32,
    ) -> io::Result<()> {
        let file = file.as_ref().as_bytes();
        let file_len = file.len();

        let payload_len = 4 + 2 + 4 + file_len + 1;

        let mut b = BytesMut::with_capacity(payload_len);
        b.put_u32_le(position);
        b.put_u16_le(BinlogDumpFlags::empty().bits());
        b.put_u32_le(server_id);
        b.put(file);

        self.write_command(Command::COM_BINLOG_DUMP, &b[..]).await?;

        Ok(())
    }
}

fn client_character_set() -> CharacterSet {
    // TODO: not 100% sure, but seems to depends on the server version...
    CharacterSet::UTF8
}

// Defines the default capabilities that our client support.
fn default_capabilities(opts: &ConnectionOptions) -> CapabilityFlags {
    let mut capabilities = CapabilityFlags::CLIENT_PROTOCOL_41
    | CapabilityFlags::CLIENT_SECURE_CONNECTION
    | CapabilityFlags::CLIENT_LONG_PASSWORD
    | CapabilityFlags::CLIENT_PLUGIN_AUTH
    | CapabilityFlags::CLIENT_LONG_FLAG
    // | CapabilityFlags::CLIENT_CONNECT_ATTRS // TODO: ...
    | CapabilityFlags::CLIENT_DEPRECATE_EOF;

    if opts.use_compression {
        capabilities.insert(CapabilityFlags::CLIENT_COMPRESS);
    }

    if opts.db_name.as_ref().filter(|v| !v.is_empty()).is_some() {
        capabilities.insert(CapabilityFlags::CLIENT_CONNECT_WITH_DB);
    }

    if opts.use_ssl {
        capabilities.insert(CapabilityFlags::CLIENT_SSL);
    }

    capabilities
}

pub fn scramble_password(
    auth_plugin_name: &str,
    password: &str,
    nonce: &[u8],
) -> io::Result<Option<Vec<u8>>> {
    match (password, auth_plugin_name) {
        ("", _) => Ok(None),
        (password, MYSQL_NATIVE_PASSWORD_PLUGIN_NAME) => {
            Ok(super::scramble::scramble_native(nonce, password.as_bytes()).map(|x| x.to_vec()))
        }
        (password, CACHING_SHA2_PASSWORD_PLUGIN_NAME) => {
            Ok(super::scramble::scramble_sha256(nonce, password.as_bytes()).map(|x| x.to_vec()))
        }
        (password, custom_plugin_name) => unimplemented!(),
    }
}

/// Owned results for 0..N rows.
pub struct QueryResults {
    columns: Arc<Vec<Column>>,
    rows: Vec<Row>,
}

impl QueryResults {
    /// Consumes self and return only the first result.
    pub fn pop(mut self) -> Option<QueryResult> {
        self.rows.pop().map(|row| QueryResult {
            columns: self.columns.clone(),
            row,
        })
    }

    /// Returns a reference to the first result.
    pub fn first(&self) -> Option<QueryResultRef<'_>> {
        self.rows.first().map(|row| QueryResultRef {
            columns: self.columns.clone(),
            row,
        })
    }
}

impl Default for QueryResults {
    fn default() -> Self {
        let columns = Arc::new(Vec::new());
        let rows = Vec::new();
        Self { columns, rows }
    }
}

/// Owned result for a single row.
pub struct QueryResult {
    columns: Arc<Vec<Column>>,
    row: Row,
}

impl QueryResult {
    pub fn values(&self) -> &[Value] {
        self.row.values()
    }
}

/// Reference to a single row.
pub struct QueryResultRef<'a> {
    columns: Arc<Vec<Column>>,
    row: &'a Row,
}

// https://mariadb.com/kb/en/connection/#sslrequest-packet
// https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html
