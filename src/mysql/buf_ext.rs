use bytes::{Buf, BufMut};
use std::io;

pub trait BufExt: Buf {
  fn mysql_get_eof_string(&mut self) -> io::Result<String> {
    self.mysql_get_fixed_length_string(self.remaining())
  }

  // Returns a utf-8 encoded string terminated by \0.
  fn mysql_null_terminated_string(&mut self) -> io::Result<String> {
    let len = self.chunk().iter().position(|x| *x == 0x00).unwrap_or(self.remaining());

    self.mysql_get_fixed_length_string(len)
  }

  // Returns a utf-8 encoded string of length N, where N are in bytes.
  fn mysql_get_fixed_length_string(&mut self, len: usize) -> io::Result<String> {
    if self.remaining() >= len {
      let mut bytes = vec![0; len];
      self.copy_to_slice(bytes.as_mut_slice());

      String::from_utf8(bytes).map_err(|err| io::Error::new(io::ErrorKind::Other, err))
    } else {
      Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("expected {}, got {}", len, self.remaining()),
      ))
    }
  }

  // Returns a utf-8 encoded string of variable length. See `BufExt::get_lenc_uint`.
  fn mysql_get_lenc_string(&mut self) -> io::Result<String> {
    let len = self.mysql_get_lenc_uint()?;
    let len = len.try_into().unwrap();
    self.mysql_get_fixed_length_string(len)
  }

  // Same as get_u8, but returns an UnexpectedEof error instead of panicking when remaining < 1;
  fn mysql_get_u8(&mut self) -> io::Result<u8> {
    if self.remaining() >= 1 {
      Ok(self.get_u8())
    } else {
      Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("expected 1, got {}", self.remaining()),
      ))
    }
  }

  // Same as get_uint_le, but returns an UnexpectedEof error instead of panicking when remaining < 1;
  fn mysql_get_uint_le(&mut self, nbytes: usize) -> io::Result<u64> {
    if self.remaining() >= nbytes {
      Ok(self.get_uint_le(nbytes))
    } else {
      Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("expected {}, got {}", nbytes, self.remaining()),
      ))
    }
  }

  fn mysql_get_lenc_uint(&mut self) -> io::Result<u64> {
    match self.mysql_get_u8()? {
      0xfc => self.mysql_get_uint_le(2),
      0xfd => self.mysql_get_uint_le(3),
      0xfe => self.mysql_get_uint_le(8),
      0xff => Err(io::Error::new(
        io::ErrorKind::Other,
        "Invalid length-encoded integer value",
      )),
      x => Ok(x.into()),
    }
  }
}

pub trait BufMutExt: BufMut {
  fn mysql_put_lenc_uint(&mut self, v: u64) {
    if v < 251 {
      self.put_u8(v as u8);
      return;
    }

    if v < 2_u64.pow(16) {
      self.put_u8(0xFC);
      self.put_uint_le(v, 2);
      return;
    }

    if v < 2_u64.pow(24) {
      self.put_u8(0xFD);
      self.put_uint_le(v, 3);
      return;
    }

    self.put_u8(0xFE);
    self.put_uint_le(v, 8);
  }
}

// Blanket implementations
impl<T> BufExt for T where T: Buf {}
impl<T> BufMutExt for T where T: BufMut {}
