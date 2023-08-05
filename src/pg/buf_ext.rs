use bytes::Buf;
use std::{collections::BTreeMap, io};

pub trait BufExt: Buf {
  fn pg_get_null_terminated_string(&mut self) -> io::Result<String> {
    match self.chunk().iter().position(|x| *x == 0x00) {
      Some(len) => {
        let mut buffer = vec![0; len];
        self.copy_to_slice(buffer.as_mut_slice());
        self.advance(1);

        String::from_utf8(buffer).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
      }
      None => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "missing null terminator")),
    }
  }

  fn pg_get_fixed_length_string(&mut self, len: usize) -> io::Result<String> {
    if self.remaining() >= len {
      let mut bytes = vec![0; len];
      self.copy_to_slice(bytes.as_mut_slice());

      String::from_utf8(bytes).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    } else {
      Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("expected {}, got {}", len, self.remaining()),
      ))
    }
  }

  fn pg_get_fields(&mut self) -> io::Result<BTreeMap<char, String>> {
    let mut fields = BTreeMap::new();
    loop {
      match self.get_u8() {
        0 => break,
        token => {
          let msg = self.pg_get_null_terminated_string()?;
          fields.insert(char::from(token), msg);
        }
      }
    }
    Ok(fields)
  }

  fn pg_get_backend_error(&mut self) -> io::Error {
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
    match self.pg_get_fields() {
      Ok(fields) if fields.is_empty() => io::Error::new(io::ErrorKind::InvalidData, "missing error fields from server"),
      Ok(fields) => io::Error::new(
        io::ErrorKind::Other,
        format!("Server error {}: {}", fields[&'C'], fields[&'M']),
      ),
      Err(err) => err,
    }
  }

  fn pg_get_backend_notice(&mut self) -> io::Error {
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
    match self.pg_get_fields() {
      Ok(fields) if fields.is_empty() => io::Error::new(io::ErrorKind::InvalidData, "missing error fields from server"),
      Ok(fields) => io::Error::new(
        io::ErrorKind::Other,
        format!("Server notice {}: {}", fields[&'C'], fields[&'M']),
      ),
      Err(err) => err,
    }
  }
}

impl<T> BufExt for T where T: Buf {}
