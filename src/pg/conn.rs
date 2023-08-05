use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::PathBuf;
use std::time::Duration;

use bytes::{Buf, Bytes};
use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use sha2::digest::FixedOutput;
use sha2::Sha256;

use tokio::io::AsyncWriteExt;
use tokio::net;
use url::Url;

use super::buf_ext::BufExt;
use super::cancel::CancelHandle;
use super::query::{Column, CreateReplicationSlot, IdentifySystem, QueryResult, QueryResults, SelectQueryResult};
use super::stream::Stream;
use super::wal::{ReplicationStream, WalCursor};

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
      user: "postgres".to_string(),
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
      "" => "postgres".to_string(),
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

const PROTOCOL_VERSION: i32 = 196608;

#[derive(Debug)]
pub struct Connection {
  stream: Stream,
  options: ConnectionOptions,
  pid: Option<i32>,
  secret_key: Option<i32>,
  metadata: BTreeMap<String, String>,
}

impl Connection {
  pub async fn connect_from_url(url: &Url) -> io::Result<Self> {
    match url.scheme() {
      "tcp" => {
        let port = url.port().unwrap_or(5432);
        let addrs = match url.host() {
          Some(url::Host::Domain(domain)) => net::lookup_host(format!("{}:{}", domain, port))
            .await
            .map(|v| v.collect::<Vec<_>>())?,
          Some(url::Host::Ipv4(ip)) => vec![SocketAddrV4::new(ip, port).into()],
          Some(url::Host::Ipv6(ip)) => vec![SocketAddrV6::new(ip, port, 0, 0).into()],
          None => vec![format!("[::]:{port}").parse().unwrap()],
        };
        let options = url.try_into()?;
        Self::connect_tcp(addrs, options).await
      }
      "unix" => {
        let options = url.try_into()?;
        Self::connect_unix(url.path(), options).await
      }
      scheme => Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{} is not supported", scheme),
      )),
    }
  }

  pub async fn connect_tcp(addrs: impl Into<Vec<SocketAddr>>, options: ConnectionOptions) -> io::Result<Self> {
    let stream = match options.connect_timeout {
      Some(connect_timeout) => tokio::time::timeout(connect_timeout, Stream::connect_tcp(addrs))
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::TimedOut, "connection timed out"))
        .and_then(|r| r),
      None => Stream::connect_tcp(addrs).await,
    }?;
    Self::connect(stream, options).await
  }

  pub async fn connect_unix(path: impl Into<PathBuf>, options: ConnectionOptions) -> io::Result<Self> {
    let stream = match options.connect_timeout {
      Some(connect_timeout) => tokio::time::timeout(connect_timeout, Stream::connect_unix(path))
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::TimedOut, "connection timed out"))
        .and_then(|r| r),
      None => Stream::connect_unix(path).await,
    }?;
    Self::connect(stream, options).await
  }

  #[cfg(feature = "ssl")]
  pub async fn connect_ssl_from_url(url: &Url, ssl_connector: openssl::ssl::SslConnector) -> io::Result<Self> {
    match url.scheme() {
      "tcp" => {
        let options = url.try_into()?;
        let port = url.port().unwrap_or(5432);
        let (domain, addrs) = match url.host() {
          Some(url::Host::Domain(domain)) => net::lookup_host(format!("{}:{}", domain, port))
            .await
            .map(|v| (domain.to_string(), v.collect::<Vec<_>>()))?,
          Some(_) => return Err(io::Error::new(io::ErrorKind::InvalidInput, "")),
          None => (
            "localhost".to_string(),
            vec![format!("[::]:{}", port).parse::<SocketAddr>().unwrap()],
          ),
        };
        Self::connect_ssl(addrs, domain, options, ssl_connector).await
      }
      scheme => Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{} is not supported", scheme),
      )),
    }
  }

  #[cfg(feature = "ssl")]
  pub async fn connect_ssl(
    addrs: impl Into<Vec<SocketAddr>>,
    domain: impl Into<String>,
    options: ConnectionOptions,
    ssl_connector: openssl::ssl::SslConnector,
  ) -> io::Result<Self> {
    let stream = match options.connect_timeout {
      Some(connect_timeout) => tokio::time::timeout(connect_timeout, Stream::connect_ssl(addrs, domain, ssl_connector))
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::TimedOut, "connection timed out"))
        .and_then(|r| r),
      None => Stream::connect_ssl(addrs, domain, ssl_connector).await,
    }?;
    Self::connect(stream, options).await
  }

  async fn connect(stream: Stream, options: ConnectionOptions) -> io::Result<Self> {
    let mut connection = Self {
      stream,
      options,
      pid: None,
      secret_key: None,
      metadata: BTreeMap::new(),
    };
    connection.startup().await?;
    Ok(connection)
  }

  pub async fn duplicate(&self) -> io::Result<Self> {
    let stream = self.stream_duplicate().await?;
    Self::connect(stream, self.options.clone()).await
  }

  pub async fn cancel_handle(&self) -> io::Result<CancelHandle> {
    match (self.pid, self.secret_key) {
      (Some(pid), Some(secret_key)) => {
        let stream = self.stream_duplicate().await?;
        Ok(CancelHandle {
          stream,
          secret_key,
          pid,
        })
      }
      (_, _) => Err(io::Error::new(
        io::ErrorKind::NotConnected,
        "unable to create cancel handle",
      )),
    }
  }

  async fn stream_duplicate(&self) -> io::Result<Stream> {
    match self.options.connect_timeout {
      Some(connect_timeout) => tokio::time::timeout(connect_timeout, self.stream.duplicate())
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))
        .and_then(|r| r),
      None => self.stream.duplicate().await,
    }
  }

  async fn stream_read_packet(&mut self) -> io::Result<(u8, Bytes)> {
    match self.options.read_timeout {
      Some(read_timeout) => tokio::time::timeout(read_timeout, self.stream.read_packet())
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::TimedOut, "read timed out"))
        .and_then(|r| r),
      None => self.stream.read_packet().await,
    }
  }

  async fn stream_flush(&mut self) -> io::Result<()> {
    match self.options.write_timeout {
      Some(write_timeout) => tokio::time::timeout(write_timeout, self.stream.flush())
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::TimedOut, "write timed out"))
        .and_then(|r| r),
      None => self.stream.flush().await,
    }
  }

  // https://www.postgresql.org/docs/11/protocol.html
  async fn startup(&mut self) -> io::Result<()> {
    let mut params = Vec::new();
    params.push("user");
    params.push(self.options.user.as_str());
    if let Some(database) = self.options.database.as_ref() {
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
    self.stream_flush().await?;

    self.authenticate().await?;

    Ok(())
  }

  async fn authenticate(&mut self) -> io::Result<()> {
    loop {
      let (op, mut buffer) = self.stream_read_packet().await?;

      match op {
        b'R' => {
          match buffer.get_i32() {
            0 => break,
            2 => {
              return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "AuthenticationKerberosV5 is not supported",
              ));
            }
            3 => {
              // AuthenticationCleartextPassword
              let password = self
                .options
                .password
                .as_ref()
                .map(String::as_bytes)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "password is required"))?;

              let len = password.len() + 4 + 1;
              self.stream.write_u8(b'p').await?;
              self.stream.write_i32(len as i32).await?;
              self.stream.write_all(password).await?;
              self.stream.write_u8(0).await?;
              self.stream_flush().await?;
            }
            5 => {
              // AuthenticationMD5Password
              let password = self
                .options
                .password
                .as_ref()
                .map(String::as_bytes)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "password is required"))?;

              let mut salt = vec![0; 4];
              buffer.copy_to_slice(&mut salt);

              let mut md5 = Md5::new();
              md5.update(password);
              md5.update(self.options.user.as_bytes());
              let output = md5.finalize_reset();
              md5.update(format!("{:x}", output));
              md5.update(salt);
              let password = format!("md5{:x}", md5.finalize());

              let len = password.len() + 4 + 1;
              self.stream.write_u8(b'p').await?;
              self.stream.write_i32(len as i32).await?;
              self.stream.write_all(password.as_bytes()).await?;
              self.stream.write_u8(0).await?;
              self.stream_flush().await?
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
              // https://datatracker.ietf.org/doc/html/rfc5802#section-3
              let mut mechanisms = Vec::new();
              loop {
                match buffer.pg_get_null_terminated_string()? {
                  m if m.is_empty() => break,
                  m => mechanisms.push(m),
                }
              }

              let mechanism = "SCRAM-SHA-256".to_string();

              if !mechanisms.contains(&mechanism) {
                return Err(io::Error::new(
                  io::ErrorKind::Unsupported,
                  "AuthenticationSASL SCRAM-SHA-256 is not supported upstream",
                ));
              }

              let client_nonce = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(24)
                .map(char::from)
                .collect::<String>();

              // Write SASLInitialResponse
              let gs2_header = "n,,"; // TODO: update this when supporting SCRAM-SHA-256-PLUS
              let cbind_data = "";
              let client_first_message = format!("{}n=,r={}", gs2_header, client_nonce);
              let len = 4 + mechanism.len() + 1 + 4 + client_first_message.len();
              self.stream.write_u8(b'p').await?;
              self.stream.write_i32(len as i32).await?;
              self.stream.write_all(mechanism.as_bytes()).await?;
              self.stream.write_u8(0).await?;
              self.stream.write_i32(client_first_message.len() as i32).await?;
              self.stream.write_all(client_first_message.as_bytes()).await?;
              self.stream_flush().await?;

              let server_first_message = self.read_sasl_response().await?;

              let mut chunks = server_first_message.splitn(3, |v| v == ',');
              let server_nonce = chunks
                .next()
                .and_then(|v| v.strip_prefix("r="))
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid nonce"))?;

              let salt: Vec<u8> = chunks
                .next()
                .and_then(|v| v.strip_prefix("s="))
                .and_then(|v| base64::decode(v).ok())
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid salt"))?;

              let iteration_count = chunks
                .next()
                .and_then(|v| v.strip_prefix("i="))
                .and_then(|v| v.parse::<usize>().ok())
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid iteration count"))?;

              fn sasl_hi(str: &[u8], salt: &[u8], i: usize) -> [u8; 32] {
                let mut prev = Hmac::<Sha256>::new_from_slice(str)
                  .unwrap()
                  .chain_update(salt)
                  .chain_update([0, 0, 0, 1])
                  .finalize()
                  .into_bytes();
                let mut hi = prev;

                for _ in 1..i {
                  prev = Hmac::<Sha256>::new_from_slice(str)
                    .unwrap()
                    .chain_update(prev)
                    .finalize()
                    .into_bytes();

                  for (hi, prev) in hi.iter_mut().zip(prev) {
                    *hi ^= prev;
                  }
                }

                hi.into()
              }

              let password = self
                .options
                .password
                .as_ref()
                .map(String::as_bytes)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "password is required"))?;

              let salted_password = sasl_hi(password, &salt, iteration_count);

              let client_key = Hmac::<Sha256>::new_from_slice(&salted_password)
                .unwrap()
                .chain_update(b"Client Key")
                .finalize()
                .into_bytes();

              let stored_key = Sha256::default().chain_update(client_key.as_slice()).finalize_fixed();

              let encoded_channel_binding = base64::encode(&[gs2_header, cbind_data].concat());

              let auth_message = format!(
                "n=,r={},{},c={},r={}",
                client_nonce, server_first_message, encoded_channel_binding, server_nonce
              );

              let client_signature = Hmac::<Sha256>::new_from_slice(&stored_key)
                .unwrap()
                .chain_update(auth_message.as_bytes())
                .finalize()
                .into_bytes();

              let mut client_proof = client_key;
              for (proof, signature) in client_proof.iter_mut().zip(client_signature) {
                *proof ^= signature;
              }

              let client_final_message = format!(
                "c={},r={},p={}",
                encoded_channel_binding,
                server_nonce,
                base64::encode(client_proof)
              );

              // SASLResponse
              let len = 4 + client_final_message.len();
              self.stream.write_u8(b'p').await?;
              self.stream.write_i32(len as i32).await?;
              self.stream.write_all(client_final_message.as_bytes()).await?;
              self.stream_flush().await?;

              let sasl_final_response = self.read_sasl_response().await?;

              if let Some(err) = sasl_final_response.strip_prefix("e=") {
                return Err(io::Error::new(io::ErrorKind::InvalidData, err.to_string()));
              } else if let Some(verifier) = sasl_final_response.strip_prefix("v=") {
                let verifier = base64::decode(verifier)
                  .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, "Failed to decode base64 sasl verifier"))?;

                let server_key = Hmac::<Sha256>::new_from_slice(&salted_password)
                  .unwrap()
                  .chain_update(b"Server Key")
                  .finalize()
                  .into_bytes();

                Hmac::<Sha256>::new_from_slice(&server_key)
                  .unwrap()
                  .chain_update(auth_message.as_bytes())
                  .verify_slice(&verifier)
                  .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Failed to verify sasl auth_message"))?;
              } else {
                return Err(io::Error::new(
                  io::ErrorKind::InvalidData,
                  "AuthenticationSASL unexpected payload",
                ));
              }
            }
            code => panic!("Unexpected backend authentication code {:?}", code),
          }
        }
        b'E' => return Err(buffer.pg_get_backend_error()),
        code => {
          panic!("Unexpected backend message: {:?}", char::from(code))
        }
      }
    }

    self.metadata.clear();

    loop {
      let (op, mut buffer) = self.stream_read_packet().await?;

      match op {
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
          self.pid.replace(buffer.get_i32());
          self.secret_key.replace(buffer.get_i32());
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
          let key = buffer.pg_get_null_terminated_string()?;
          let value = buffer.pg_get_null_terminated_string()?;
          self.metadata.insert(key, value);
        }
        b'Z' => {
          break;
        }
        b'E' => {
          return Err(buffer.pg_get_backend_error());
        }
        b'N' => {
          buffer.pg_get_backend_notice();
        }
        code => {
          panic!("Unexpected backend message: {:?}", char::from(code))
        }
      }
    }
    Ok(())
  }

  async fn read_sasl_response(&mut self) -> io::Result<String> {
    let (op, mut buffer) = self.stream_read_packet().await?;

    match op {
      b'R' => {
        // AuthenticationSASLContinue (B)
        //   Byte1('R')
        //       Identifies the message as an authentication request.
        //   Int32
        //       Length of message contents in bytes, including self.
        //   Int32(11)
        //       Specifies that this message contains a SASL challenge.
        //   Byten
        //       SASL data, specific to the SASL mechanism being used.
        //
        // AuthenticationSASLFinal (B)
        //   Byte1('R')
        //       Identifies the message as an authentication request.
        //   Int32
        //       Length of message contents in bytes, including self.
        //   Int32(12)
        //       Specifies that SASL authentication has completed.
        //   Byten
        //       SASL outcome "additional data", specific to the SASL mechanism being used.
        buffer.advance(4); // skip 12
        String::from_utf8(buffer.to_vec()).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
      }
      b'E' => Err(buffer.pg_get_backend_error()),
      code => {
        panic!("Unexpected backend message: {:?}", char::from(code))
      }
    }
  }

  pub async fn ping(&mut self) -> io::Result<()> {
    self.query_first("SELECT 1").await.map(|_| ())
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

    let (op, mut buffer) = self.stream_read_packet().await?;

    match op {
      b'E' => {
        return Err(buffer.pg_get_backend_error());
      }
      b'W' => {
        let format = buffer.get_i8();
        let num_columns = buffer.get_i16();
        let mut column_formats = vec![0; num_columns.try_into().unwrap()];
        for i in 0..column_formats.len() {
          column_formats.push(buffer.get_i16());
        }

        assert_eq!(0, format);
        assert_eq!(0, num_columns);
        assert!(column_formats.is_empty());
      }
      code => {
        panic!("Unexpected backend message: {:?}", char::from(code))
      }
    }

    Ok(ReplicationStream { stream: self.stream })
  }

  pub async fn create_replication_slot(&mut self, slot: impl AsRef<str>) -> io::Result<CreateReplicationSlot> {
    let result = self
      .query_first(format!("CREATE_REPLICATION_SLOT {} LOGICAL wal2json", slot.as_ref()))
      .await?;

    let mut values = result.as_selected_query_result().unwrap().values;
    values.reverse();

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
    let result = self.query(format!("DROP_REPLICATION_SLOT {}", slot.as_ref())).await?;
    Ok(())
  }

  pub async fn replication_slot_exists(&mut self, slot: impl AsRef<str>) -> io::Result<bool> {
    let result = self
      .query_first(format!(
        "select * from pg_replication_slots where slot_name = '{}';",
        slot.as_ref()
      ))
      .await?;
    Ok(!result.as_selected_query_result().unwrap().values.is_empty())
  }

  pub async fn identify_system(&mut self) -> io::Result<IdentifySystem> {
    let result = self.query_first("IDENTIFY_SYSTEM").await?;

    let mut values = result.as_selected_query_result().unwrap().values;
    values.reverse();

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

  pub async fn timeline_history(&mut self, tid: i32) -> io::Result<SelectQueryResult> {
    let result = self.query_first(format!("TIMELINE_HISTORY {}", tid)).await?;
    Ok(result.as_selected_query_result().unwrap())
  }

  async fn write_query_command(&mut self, query: impl AsRef<str>) -> io::Result<()> {
    let len = query.as_ref().as_bytes().len() + 1 + 4;
    self.stream.write_u8(b'Q').await?;
    self.stream.write_i32(len as i32).await?;
    self.stream.write_all(query.as_ref().as_bytes()).await?;
    self.stream.write_u8(0).await?;
    self.stream_flush().await
  }

  pub async fn query_first(&mut self, query: impl AsRef<str>) -> io::Result<QueryResult> {
    let QueryResults { mut results, .. } = self.query(query.as_ref()).await?;
    results
      .pop_front()
      .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing query result"))
  }

  pub async fn query(&mut self, query: impl AsRef<str>) -> io::Result<QueryResults> {
    self.write_query_command(query).await?;

    // https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.6.7.4

    let mut notices: VecDeque<io::Error> = VecDeque::new();
    let mut results: VecDeque<QueryResult> = VecDeque::new();
    let mut current: Option<SelectQueryResult> = None;

    loop {
      let (op, mut buffer) = self.stream_read_packet().await?;

      match op {
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
          let op = buffer.pg_get_null_terminated_string()?;
          match current.take() {
            Some(select_query_result) => results.push_back(QueryResult::Selected(select_query_result)),
            None => results.push_back(QueryResult::Success),
          }
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
          todo!("copy in not supported")
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
          todo!("copy out not supported")
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
          let mut columns = Vec::new();
          let num_columns = buffer.get_i16();
          for i in 0..num_columns {
            let name = buffer.pg_get_null_terminated_string()?;
            let oid = buffer.get_i32();
            let attr_number = buffer.get_i16();
            let datatype_oid = buffer.get_i32();
            let datatype_size = buffer.get_i16();
            let type_modifier = buffer.get_i32();
            let format = buffer.get_i16();

            columns.push(Column {
              name,
              oid,
              attr_number,
              datatype_oid,
              datatype_size,
              type_modifier,
              format,
            });
          }
          current = Some(SelectQueryResult {
            columns,
            values: Vec::new(),
          });
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
          let values = &mut current.as_mut().unwrap().values;
          let num_values = buffer.get_i16();
          for i in 0..num_values {
            let len = buffer.get_i32();

            if len > 0 {
              let value = buffer.pg_get_fixed_length_string(len.try_into().unwrap())?;
              values.push(Some(value));
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
          results.push_back(QueryResult::Success);
        }
        b'Z' => {
          // self.read_ready_for_query().await?;
          break;
        }
        b'E' => match buffer.pg_get_backend_error() {
          err if err.kind() == io::ErrorKind::Other => results.push_back(QueryResult::BackendError(err)),
          err => return Err(err),
        },
        b'N' => match buffer.pg_get_backend_notice() {
          notice if notice.kind() == io::ErrorKind::Other => notices.push_back(notice),
          notice => return Err(notice),
        },
        code => {
          panic!("Unexpected backend message: {:?}", char::from(code))
        }
      }
    }

    Ok(QueryResults { notices, results })
  }

  pub async fn close(mut self) -> io::Result<()> {
    self.stream.write_u8(b'X').await?;
    self.stream.write_i32(4).await?;
    self.stream.shutdown().await
  }
}
