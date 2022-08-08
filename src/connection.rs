use std::{io, net::SocketAddr};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::TcpStream,
};

#[derive(Debug)]
pub struct Connection {
    stream: BufStream<TcpStream>,
    options: ConnectionOptions,
}

#[derive(Debug, Default)]
pub struct ConnectionOptions {
    pub user: String,
    pub use_ssl: bool,
    pub password: Option<String>,
    pub database: Option<String>,
}

const PROTOCOL_VERSION: i32 = 196608;
const SSL_HANDSHAKE_CODE: i32 = 80877103;

impl Connection {
    pub async fn connect(
        addr: SocketAddr,
        options: impl Into<ConnectionOptions>,
    ) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let stream = BufStream::new(stream);
        let options = options.into();
        let use_ssl = options.use_ssl;
        let mut connection = Self { stream, options };
        if use_ssl {
            connection.ssl_handshake().await?;
        }
        connection.startup().await?;
        Ok(connection)
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
        params.push("pickup");
        params.push("replication");
        params.push("true");

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
                            } else {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "password is required",
                                ));
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
                    self.read_backend_error().await?;
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
                    println!("{:?}: {:?}", param_name, param_value);
                }
                b'Z' => {
                    // ReadyForQuery (B)
                    //     Byte1('Z')
                    //         Identifies the message type. ReadyForQuery is sent whenever the backend is ready for a new query cycle.
                    //     Int32(5)
                    //         Length of message contents in bytes, including self.
                    //     Byte1
                    //         Current backend transaction status indicator. Possible values are 'I' if idle (not in a transaction block); 'T' if in a transaction block; or 'E' if in a failed transaction block (queries will be rejected until block is ended).

                    self.stream.read_i32().await?; // skip len
                    let status = self.stream.read_u8().await?;
                    break;
                }
                b'E' => {
                    // ErrorResponse
                    self.read_backend_error().await?;
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

    pub async fn exec(&mut self, command: impl AsRef<str>) -> io::Result<()> {
        let len = command.as_ref().as_bytes().len() + 1 + 4;
        self.stream.write_u8(b'Q').await?;
        self.stream.write_i32(len as i32).await?;
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

        self.stream.read_i32().await?; // skip len

        loop {
            match self.stream.read_u8().await? {
                0 => break,
                token => {
                    let msg = self.read_c_string().await?;
                    println!("{:?}: {:?}", char::from(token), msg);
                }
            }
        }
        Ok(())
    }

    async fn read_backend_error(&mut self) -> io::Result<()> {
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

        self.stream.read_i32().await?; // skip len

        loop {
            match self.stream.read_u8().await? {
                0 => break,
                token => {
                    let msg = self.read_c_string().await?;
                    println!("{:?}: {:?}", char::from(token), msg);
                }
            }
        }

        Ok(())
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
        self.stream.shutdown().await?;
        Ok(())
    }
}

struct InFlightQuery {}
