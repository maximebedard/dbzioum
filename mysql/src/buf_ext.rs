use bytes::{Buf, BufMut};

pub trait BufExt: Buf {
  fn mysql_get_eof_string(&mut self) -> String {
    self.mysql_get_fixed_length_string(self.remaining())
  }

  // Returns a utf-8 encoded string terminated by \0.
  fn mysql_get_null_terminated_string(&mut self) -> String {
    match self.chunk().iter().position(|x| *x == 0x00) {
      Some(len) => {
        let mut buffer = vec![0; len];
        self.copy_to_slice(buffer.as_mut_slice());
        self.advance(1);
        String::from_utf8(buffer).unwrap()
      }
      None => panic!("missing null terminator"),
    }
  }

  // Returns a utf-8 encoded string of length N, where N are in bytes.
  fn mysql_get_fixed_length_string(&mut self, len: usize) -> String {
    let mut bytes = vec![0; len];
    self.copy_to_slice(bytes.as_mut_slice());
    String::from_utf8(bytes).unwrap()
  }

  // Returns a utf-8 encoded string of variable length. See `BufExt::get_lenc_uint`.
  fn mysql_get_lenc_string(&mut self) -> String {
    let len = self.mysql_get_lenc_uint();
    let len = len.try_into().unwrap();
    self.mysql_get_fixed_length_string(len)
  }

  fn mysql_get_lenc_uint(&mut self) -> u64 {
    match self.get_u8() {
      0xfc => self.get_uint_le(2),
      0xfd => self.get_uint_le(3),
      0xfe => self.get_uint_le(8),
      0xff => panic!("Invalid length-encoded integer value",),
      x => x.into(),
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
