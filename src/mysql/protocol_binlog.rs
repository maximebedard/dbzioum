use crate::mysql::protocol::{CharacterSet, ColumnMetadataType};

use super::protocol::ColumnType;
use super::{buf_ext::BufExt, protocol::BinlogEventType};
use bytes::Buf;
use std::io;

#[derive(Debug)]
pub struct BinlogEventPacket {
  pub timestamp: u32,
  pub server_id: u32,
  pub log_position: u32,
  pub flags: u16,
  pub event: BinlogEvent,
  pub checksum: Vec<u8>,
}

impl BinlogEventPacket {
  pub fn parse(buffer: impl AsRef<[u8]>) -> io::Result<BinlogEventPacket> {
    let mut b = buffer.as_ref();

    // skip OK byte
    b.advance(1);

    let timestamp = b.get_u32_le();
    let event_type = b.get_u8().try_into().unwrap();
    let server_id = b.get_u32_le();
    let event_size = (b.get_u32_le() - 19) as usize;
    let log_position = b.get_u32_le();
    let flags = b.get_u16_le();
    let payload_len = b.remaining() - 4; // TODO: checksum is usually 4 bytes, but can be changed...
    let payload = b[..payload_len].to_vec();
    b = &b[payload_len..];
    let checksum = b.to_vec();

    let event = match event_type {
      BinlogEventType::TABLE_MAP_EVENT => TableMapEvent::parse(&payload).map(BinlogEvent::TableMap),
      BinlogEventType::ROTATE_EVENT => {
        // NOTE: Strangely enough, the checksum is actually the suffix of the binlog file name.
        RotateEvent::parse(&payload, &checksum).map(BinlogEvent::Rotate)
      }
      BinlogEventType::FORMAT_DESCRIPTION_EVENT => FormatDescriptionEvent::parse(&payload).map(BinlogEvent::Format),
      BinlogEventType::WRITE_ROWS_EVENTV0 => RowEvent::parse(&payload, false, false, true).map(BinlogEvent::Insert),
      BinlogEventType::WRITE_ROWS_EVENTV1 => RowEvent::parse(&payload, false, false, true).map(BinlogEvent::Insert),
      BinlogEventType::WRITE_ROWS_EVENTV2 => RowEvent::parse(&payload, true, false, true).map(BinlogEvent::Insert),
      BinlogEventType::UPDATE_ROWS_EVENTV0 => RowEvent::parse(&payload, false, true, true).map(BinlogEvent::Update),
      BinlogEventType::UPDATE_ROWS_EVENTV1 => RowEvent::parse(&payload, false, true, true).map(BinlogEvent::Update),
      BinlogEventType::UPDATE_ROWS_EVENTV2 => RowEvent::parse(&payload, true, true, true).map(BinlogEvent::Update),
      BinlogEventType::DELETE_ROWS_EVENTV0 => RowEvent::parse(&payload, false, true, false).map(BinlogEvent::Delete),
      BinlogEventType::DELETE_ROWS_EVENTV1 => RowEvent::parse(&payload, false, true, false).map(BinlogEvent::Delete),
      BinlogEventType::DELETE_ROWS_EVENTV2 => RowEvent::parse(&payload, true, true, false).map(BinlogEvent::Delete),
      not_supported => Ok(BinlogEvent::NotSupported(not_supported)),
    }?;

    Ok(BinlogEventPacket {
      timestamp,
      server_id,
      log_position,
      flags,
      event,
      checksum,
    })
  }
}

#[derive(Debug)]
pub enum BinlogEvent {
  TableMap(TableMapEvent),
  Rotate(RotateEvent),
  Format(FormatDescriptionEvent),
  Insert(RowEvent),
  Update(RowEvent),
  Delete(RowEvent),
  NotSupported(BinlogEventType),
}

#[derive(Debug)]
pub struct RotateEvent {
  pub next_log_position: u32,
  pub next_log_file: String,
}

impl RotateEvent {
  fn parse(buffer: impl AsRef<[u8]>, checksum_buffer: impl AsRef<[u8]>) -> io::Result<Self> {
    let mut b = buffer.as_ref();
    let cb = checksum_buffer.as_ref();
    let next_log_position = b.get_u64_le() as u32;
    let next_log_file = String::from_utf8([&b[..], &cb[..]].concat().to_vec()).unwrap();

    Ok(Self {
      next_log_position,
      next_log_file,
    })
  }
}

#[derive(Debug)]
pub struct TableMapEvent {
  pub table_id: u64,
  pub flags: u16,
  pub schema: String,
  pub table: String,
  pub column_count: usize,
  pub column_types: Vec<ColumnType>,
  pub column_metas: Vec<u32>,
  pub null_bitmap: Vec<u8>,
  pub metadata: TableMapEventMetadata,
}

#[derive(Debug, Default)]
pub struct TableMapEventMetadata {
  pub signedness: Option<Vec<u8>>,
  pub default_charset: Option<(CharacterSet, Vec<(usize, CharacterSet)>)>,
  pub enum_and_set_default_charsets: Option<(CharacterSet, Vec<(usize, CharacterSet)>)>,
  pub column_charsets: Option<Vec<CharacterSet>>,
  pub enum_and_set_column_charsets: Option<Vec<CharacterSet>>,
  pub column_names: Option<Vec<String>>,
  pub set_str_values: Option<Vec<String>>,
  pub enum_str_values: Option<Vec<String>>,
}

impl TableMapEventMetadata {
  fn parse_default_charset(buffer: impl AsRef<[u8]>) -> io::Result<(CharacterSet, Vec<(usize, CharacterSet)>)> {
    let mut b = buffer.as_ref();
    let default_charset = b.mysql_get_lenc_uint()?;
    let default_charset = (default_charset as u8).try_into().unwrap();

    let mut pairs = Vec::new();
    while b.remaining() > 0 {
      let index = b.mysql_get_lenc_uint()?;
      let index = index as usize;

      let charset = b.mysql_get_lenc_uint()?;
      let charset = (charset as u8).try_into().unwrap();

      pairs.push((index, charset))
    }

    Ok((default_charset, pairs))
  }

  fn parse_column_charsets(buffer: impl AsRef<[u8]>) -> io::Result<Vec<CharacterSet>> {
    let mut b = buffer.as_ref();

    let mut column_charsets = Vec::new();
    while b.remaining() > 0 {
      let column_charset = b.mysql_get_lenc_uint()?;
      let column_charset = (column_charset as u8).try_into().unwrap();
      column_charsets.push(column_charset);
    }

    Ok(column_charsets)
  }

  fn parse_str_value(buffer: impl AsRef<[u8]>) -> io::Result<Vec<String>> {
    todo!()
  }
}

impl TableMapEvent {
  fn parse(buffer: impl AsRef<[u8]>) -> io::Result<Self> {
    let mut b = buffer.as_ref();
    let table_id = b.get_uint_le(6); // this is actually a fixed length (either 4 or 6 bytes)
    let flags = b.get_u16_le();

    let schema_len = b.get_u8() as usize;
    let schema = &b[..schema_len];
    b = &b[schema_len..];
    let schema = String::from_utf8(schema.to_vec()).unwrap();

    // skip 0x00
    b.advance(1);

    let table_len = b.get_u8() as usize;
    let table = &b[..table_len];
    b = &b[table_len..];
    let table = String::from_utf8(table.to_vec()).unwrap();

    // skip 0x00
    b.advance(1);

    let column_count = b.mysql_get_lenc_uint().unwrap() as usize;
    let mut column_types = Vec::with_capacity(column_count);
    for _ in 0..column_count {
      column_types.push(b.get_u8().try_into().unwrap());
    }

    let column_metas_buffer_len = b.mysql_get_lenc_uint().unwrap() as usize;
    let mut column_metas_buffer = &b[..column_metas_buffer_len];
    b = &b[column_metas_buffer_len..];

    let mut column_metas = vec![0; column_count];

    // https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html#a1b84e5b226c76eaf9c0df8ed03ba1393
    for (i, t) in column_types.iter().enumerate() {
      match t {
        ColumnType::MYSQL_TYPE_FLOAT
        | ColumnType::MYSQL_TYPE_DOUBLE
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_GEOMETRY => {
          column_metas[i] = column_metas_buffer.get_u8().into();
        }

        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_BIT
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
          column_metas[i] = column_metas_buffer.get_u16_le().into();
        }

        ColumnType::MYSQL_TYPE_DECIMAL
        | ColumnType::MYSQL_TYPE_TINY
        | ColumnType::MYSQL_TYPE_SHORT
        | ColumnType::MYSQL_TYPE_LONG
        | ColumnType::MYSQL_TYPE_NULL
        | ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_LONGLONG
        | ColumnType::MYSQL_TYPE_INT24
        | ColumnType::MYSQL_TYPE_DATE
        | ColumnType::MYSQL_TYPE_TIME
        | ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_YEAR => {
          column_metas[i] = 0;
        }

        ColumnType::MYSQL_TYPE_TIMESTAMP2
        | ColumnType::MYSQL_TYPE_DATETIME2
        | ColumnType::MYSQL_TYPE_TIME2
        | ColumnType::MYSQL_TYPE_JSON
        | ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_SET
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB => {
          todo!();
        }
      }
    }

    assert_eq!(column_metas_buffer.remaining(), 0);
    let null_bitmap_len = (column_count + 7) / 8;
    let null_bitmap = b[..null_bitmap_len].to_vec();
    b = &b[null_bitmap_len..];

    let mut metadata = TableMapEventMetadata::default();

    while b.remaining() > 0 {
      let metadata_type: ColumnMetadataType = b.get_u8().try_into().unwrap();
      let metadata_len = b.mysql_get_lenc_uint().unwrap() as usize;
      let mut metadata_buffer = &b[..metadata_len];
      b = &b[metadata_len..];

      // https://github.com/mysql/mysql-server/blob/8.0/libbinlogevents/src/rows_event.cpp#L141
      match metadata_type {
        ColumnMetadataType::SIGNEDNESS => {
          let signedness = metadata_buffer.to_vec();
          metadata.signedness = Some(signedness);
        }
        ColumnMetadataType::DEFAULT_CHARSET => {
          let default_charset = TableMapEventMetadata::parse_default_charset(metadata_buffer)?;
          metadata.default_charset = Some(default_charset)
        }
        ColumnMetadataType::COLUMN_CHARSET => {
          let column_charset = TableMapEventMetadata::parse_column_charsets(metadata_buffer)?;
          metadata.column_charsets = Some(column_charset)
        }
        ColumnMetadataType::COLUMN_NAME => {
          let mut column_names = Vec::new();
          while metadata_buffer.remaining() > 0 {
            let len = metadata_buffer.mysql_get_lenc_uint()? as usize;
            let buffer = metadata_buffer[..len].to_vec();
            metadata_buffer = &metadata_buffer[len..];
            column_names.push(String::from_utf8(buffer).unwrap());
          }
          println!("{:?}", column_names);
          metadata.column_names = Some(column_names);
        }
        ColumnMetadataType::SET_STR_VALUE => {
          let set_str_values = TableMapEventMetadata::parse_str_value(metadata_buffer)?;
          metadata.set_str_values = Some(set_str_values);
        }
        ColumnMetadataType::ENUM_STR_VALUE => {
          let enum_str_values = TableMapEventMetadata::parse_str_value(metadata_buffer)?;
          metadata.enum_str_values = Some(enum_str_values);
        }
        ColumnMetadataType::GEOMETRY_TYPE => todo!(),
        ColumnMetadataType::SIMPLE_PRIMARY_KEY => {
          // todo!();
        }
        ColumnMetadataType::PRIMARY_KEY_WITH_PREFIX => {
          todo!()
        }
        ColumnMetadataType::ENUM_AND_SET_DEFAULT_CHARSET => {
          let enum_and_set_default_charsets = TableMapEventMetadata::parse_default_charset(metadata_buffer)?;
          metadata.enum_and_set_default_charsets = Some(enum_and_set_default_charsets)
        }
        ColumnMetadataType::ENUM_AND_SET_COLUMN_CHARSET => {
          let enum_and_set_column_charsets = TableMapEventMetadata::parse_column_charsets(metadata_buffer)?;
          metadata.enum_and_set_column_charsets = Some(enum_and_set_column_charsets);
        }
        ColumnMetadataType::COLUMN_VISIBILITY => {
          todo!()
        }
      }
    }

    Ok(Self {
      table_id,
      flags,
      schema: schema.into(),
      table: table.into(),
      column_count,
      column_types,
      column_metas,
      null_bitmap,
      metadata,
    })
  }

  pub fn columns(&self) -> Vec<Column> {
    let is_unsigned = |i: usize| -> bool {
      self
        .metadata
        .signedness
        .as_ref()
        .filter(|v| v[i / 8] & (1 << (i % 8)) != 0)
        .is_some()
    };

    (0..self.column_count)
      .map(|i| {
        let column_name = self.metadata.column_names.as_ref().unwrap()[i].clone();
        let column_type = self.column_types[i];
        let column_meta = self.column_metas[i];
        let is_nullable = self.null_bitmap[i / 8] & (1 << (i % 8)) != 0;

        let column_type_definition = match column_type {
          ColumnType::MYSQL_TYPE_TINY if is_unsigned(i) => ColumnTypeDefinition::U8,
          ColumnType::MYSQL_TYPE_TINY => ColumnTypeDefinition::I8,

          ColumnType::MYSQL_TYPE_SHORT if is_unsigned(i) => ColumnTypeDefinition::U16,
          ColumnType::MYSQL_TYPE_SHORT => ColumnTypeDefinition::I16,

          ColumnType::MYSQL_TYPE_INT24 if is_unsigned(i) => ColumnTypeDefinition::U32,
          ColumnType::MYSQL_TYPE_INT24 => ColumnTypeDefinition::I32,

          ColumnType::MYSQL_TYPE_LONG if is_unsigned(i) => ColumnTypeDefinition::U32,
          ColumnType::MYSQL_TYPE_LONG => ColumnTypeDefinition::I32,

          ColumnType::MYSQL_TYPE_LONGLONG if is_unsigned(i) => ColumnTypeDefinition::U64,
          ColumnType::MYSQL_TYPE_LONGLONG => ColumnTypeDefinition::I64,

          ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let precision = 0;
            let scale = 0;
            ColumnTypeDefinition::Decimal { precision, scale }
          }

          ColumnType::MYSQL_TYPE_FLOAT => {
            let pack_length = column_meta as u8;
            assert_eq!(pack_length, 4); // Make sure that the server sizeof(float) == 4
            ColumnTypeDefinition::F32 { pack_length }
          }
          ColumnType::MYSQL_TYPE_DOUBLE => {
            let pack_length = column_meta as u8;
            assert_eq!(pack_length, 8); // Make sure that the server sizeof(float) == 8
            ColumnTypeDefinition::F64 { pack_length }
          }

          ColumnType::MYSQL_TYPE_VARCHAR => {
            let max_length = column_meta as u16;
            let pack_length = if max_length > 255 { 2 } else { 1 };
            ColumnTypeDefinition::String { pack_length }
          }

          ColumnType::MYSQL_TYPE_BLOB => {
            let pack_length = column_meta as u8;
            assert!(pack_length <= 4);
            ColumnTypeDefinition::Blob { pack_length }
          }

          ColumnType::MYSQL_TYPE_TIMESTAMP => todo!(),
          ColumnType::MYSQL_TYPE_DATE => todo!(),
          ColumnType::MYSQL_TYPE_TIME => todo!(),
          ColumnType::MYSQL_TYPE_DATETIME => todo!(),
          ColumnType::MYSQL_TYPE_YEAR => todo!(),
          ColumnType::MYSQL_TYPE_BIT => todo!(),
          ColumnType::MYSQL_TYPE_TIMESTAMP2 => todo!(),
          ColumnType::MYSQL_TYPE_DATETIME2 => todo!(),
          ColumnType::MYSQL_TYPE_TIME2 => todo!(),
          ColumnType::MYSQL_TYPE_JSON => todo!(),
          ColumnType::MYSQL_TYPE_ENUM => todo!(),
          ColumnType::MYSQL_TYPE_SET => todo!(),
          ColumnType::MYSQL_TYPE_NULL => {
            unreachable!()
          }
          ColumnType::MYSQL_TYPE_TINY_BLOB | ColumnType::MYSQL_TYPE_MEDIUM_BLOB | ColumnType::MYSQL_TYPE_LONG_BLOB => {
            unreachable!()
          }
          ColumnType::MYSQL_TYPE_VAR_STRING => todo!(),
          ColumnType::MYSQL_TYPE_STRING => todo!(),
          ColumnType::MYSQL_TYPE_GEOMETRY => todo!(),
        };

        Column {
          column_name,
          is_nullable,
          column_type_definition,
        }
      })
      .collect()
  }
}

#[derive(Debug)]
pub struct FormatDescriptionEvent {
  pub version: u16,
  pub server_version: String,
  pub create_timestamp: u32,
  pub event_header_length: u8,
  pub event_type_header_lengths: Vec<u8>,
}

impl FormatDescriptionEvent {
  fn parse(buffer: impl AsRef<[u8]>) -> io::Result<Self> {
    let mut b = buffer.as_ref();
    let version = b.get_u16_le();

    let server_version = &b[..50];
    b = &b[50..];

    let null_terminated = server_version.iter().position(|x| *x == 0x00).unwrap_or(0);
    let server_version = String::from_utf8(server_version[..null_terminated].to_vec()).unwrap();

    let create_timestamp = b.get_u32_le();
    let event_header_length = b.get_u8();

    let event_type_header_lengths = b.to_vec();

    Ok(Self {
      version,
      server_version,
      create_timestamp,
      event_header_length,
      event_type_header_lengths,
    })
  }
}

#[derive(Debug)]
pub struct RowEvent {
  pub table_id: u64,
  pub flags: u16,
  pub extras: Option<Vec<u8>>,
  pub column_count: u64,
  pub columns_before_image: Option<Vec<u8>>,
  pub columns_after_image: Option<Vec<u8>>,
  pub null_bitmap: Option<Vec<u8>>,
  pub rows: Vec<u8>,
}

impl RowEvent {
  fn parse(
    buffer: impl AsRef<[u8]>,
    use_extras: bool,
    use_columns_before_image: bool,
    use_columns_after_image: bool,
  ) -> io::Result<Self> {
    // https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Rows__event.html
    let mut b = buffer.as_ref();
    let table_id = b.get_uint_le(6);
    let flags = b.get_u16_le();

    let mut extras = None;
    if use_extras {
      let extras_len = (b.get_u16_le() - 2) as usize;
      let buffer = &b[..extras_len];
      b = &b[extras_len..];
      extras = Some(buffer.to_vec())
    }

    let column_count = b.mysql_get_lenc_uint().unwrap();

    let bitmap_len = ((column_count + 7) / 8) as usize;

    let mut columns_before_image = None;
    if use_columns_before_image {
      let buffer = &b[..bitmap_len];
      b = &b[bitmap_len..];
      columns_before_image = Some(buffer.to_vec());
    }

    let mut columns_after_image = None;
    if use_columns_after_image {
      let buffer = &b[..bitmap_len];
      b = &b[bitmap_len..];
      columns_after_image = Some(buffer.to_vec());
    }

    let mut null_bitmap = None;
    if bitmap_len > 0 {
      let buffer = &b[..bitmap_len];
      b = &b[bitmap_len..];
      null_bitmap = Some(buffer.to_vec());
    }

    //https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html
    // https://dev.mysql.com/doc/refman/8.0/en/mysqlbinlog-row-events.html
    let rows = b.to_vec();

    Ok(Self {
      table_id,
      flags,
      extras,
      column_count,
      columns_before_image,
      columns_after_image,
      null_bitmap,
      rows,
    })
  }

  pub fn values(&self, columns: &Vec<Column>) -> Vec<Option<Value>> {
    let mut b = self.rows.as_slice();

    columns
      .iter()
      .enumerate()
      .map(|(i, c)| {
        let Column {
          is_nullable,
          column_type_definition,
          ..
        } = c;

        let is_null = self
          .null_bitmap
          .as_ref()
          .filter(|v| v[i / 8] & (1 << i % 8) != 0)
          .is_some();

        println!("{:X?}", b);

        let value = match column_type_definition {
          ColumnTypeDefinition::U8 => Value::U8(b.get_u8()),
          ColumnTypeDefinition::U16 => Value::U16(b.get_u16_le()),
          ColumnTypeDefinition::U32 => Value::U32(b.get_u32_le()),
          ColumnTypeDefinition::U64 => Value::U64(b.get_u64_le()),
          ColumnTypeDefinition::I8 => Value::I8(b.get_i8()),
          ColumnTypeDefinition::I16 => Value::I16(b.get_i16_le()),
          ColumnTypeDefinition::I32 => Value::I32(b.get_i32_le()),
          ColumnTypeDefinition::I64 => Value::I64(b.get_i64_le()),
          ColumnTypeDefinition::F32 { .. } => Value::F32(b.get_f32_le()),
          ColumnTypeDefinition::F64 { .. } => Value::F64(b.get_f64_le()),
          ColumnTypeDefinition::Decimal { precision, scale } => todo!(),
          ColumnTypeDefinition::String { pack_length } => {
            let len = b.get_uint_le(*pack_length as usize) as usize;
            let buffer = b[..len].to_vec();
            b = &b[len..];
            Value::String(String::from_utf8(buffer).unwrap())
          }
          ColumnTypeDefinition::Blob { pack_length } => {
            let len = b.get_uint_le(*pack_length as usize) as usize;
            let blob = b[..len].to_vec();
            b = &b[len..];
            Value::Blob(blob)
          }
        };

        let vv = if *is_nullable && is_null { None } else { Some(value) };
        println!("{:?}", vv);
        vv
      })
      .collect()
  }
}

#[derive(Debug)]
pub enum Value {
  U8(u8),
  U16(u16),
  U32(u32),
  U64(u64),
  I8(i8),
  I16(i16),
  I32(i32),
  I64(i64),
  F32(f32),
  F64(f64),
  String(String),
  Blob(Vec<u8>),
}

#[derive(Debug)]
pub struct Column {
  column_name: String,
  is_nullable: bool,
  column_type_definition: ColumnTypeDefinition,
}

#[derive(Debug)]
pub enum ColumnTypeDefinition {
  U8,
  U16,
  U32,
  U64,
  I8,
  I16,
  I32,
  I64,
  F32 { pack_length: u8 },
  F64 { pack_length: u8 },
  Decimal { precision: u8, scale: u8 },
  String { pack_length: u8 },
  Blob { pack_length: u8 },
}

#[cfg(test)]
mod test {
  use super::{BinlogEvent, BinlogEventPacket, BinlogEventType};

  #[test]
  fn parses_rotate() {
    const ROTATE_EVENT: &[u8] = b"\x00\x00\x00\x00\x00\x04\x01\x00\x00\x00\x2d\x00\x00\x00\x00\x00\x00\
                                       \x00\x20\x00\x96\x00\x00\x00\x00\x00\x00\x00\x73\x68\x6f\x70\x69\x66\
                                       \x79\x2d\x62\x69\x6e\x2e\x30\x30\x30\x30\x30\x35";

    let packet = BinlogEventPacket::parse(ROTATE_EVENT).unwrap();
    match packet.event {
      BinlogEvent::Rotate(packet) => {
        assert_eq!(150, packet.next_log_position);
        assert_eq!("shopify-bin.000005", packet.next_log_file);
      }
      unexpected => panic!("unexpected {:?}", unexpected),
    }
  }

  #[test]
  fn parses_format_description() {
    const FORMAT_DESCRIPTION_EVENT: &[u8] = b"\x00\xf2\x43\x5d\x5d\x0f\x01\x00\x00\x00\x77\x00\x00\x00\x00\x00\x00\
                                                   \x00\x00\x00\x04\x00\x35\x2e\x37\x2e\x31\x38\x2d\x31\x36\x2d\x6c\x6f\
                                                   \x67\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\
                                                   \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\
                                                   \x00\x00\x00\x00\x00\x00\x00\x00\x13\x38\x0d\x00\x08\x00\x12\x00\x04\
                                                   \x04\x04\x04\x12\x00\x00\x5f\x00\x04\x1a\x08\x00\x00\x00\x08\x08\x08\
                                                   \x02\x00\x00\x00\x0a\x0a\x0a\x2a\x2a\x00\x12\x34\x00\x00\xc2\x36\x0c\
                                                   \xdf";

    let packet = BinlogEventPacket::parse(FORMAT_DESCRIPTION_EVENT).unwrap();
    match packet.event {
      BinlogEvent::Format(packet) => {
        assert_eq!(4, packet.version);
        assert_eq!("5.7.18-16-log", packet.server_version);
        assert_eq!(0, packet.create_timestamp);
      }
      unexpected => panic!("unexpected {:?}", unexpected),
    }
  }

  #[test]
  fn parses_anonymous_gtid() {
    const ANONYMOUS_GTID_EVENT: &[u8] = b"\x00\xfc\x5a\x5d\x5d\x22\x01\x00\x00\x00\x3d\x00\x00\x00\xd3\x00\x00\
                                               \x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\
                                               \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\
                                               \x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00";

    let packet = BinlogEventPacket::parse(ANONYMOUS_GTID_EVENT).unwrap();
    match packet.event {
      BinlogEvent::NotSupported(BinlogEventType::ANONYMOUS_GTID_EVENT) => {}
      _ => panic!(),
    }
  }

  #[test]
  fn parses_query() {
    const QUERY_EVENT: &[u8] = b"\x00\xfc\x5a\x5d\x5d\x02\x01\x00\x00\x00\x44\x00\x00\x00\x17\x01\x00\
                                      \x00\x08\x00\x3b\x18\x00\x00\x00\x00\x00\x00\x04\x00\x00\x1a\x00\x00\
                                      \x00\x00\x00\x00\x01\x00\x00\x00\x40\x00\x00\x00\x00\x06\x03\x73\x74\
                                      \x64\x04\x21\x00\x21\x00\x2d\x00\x70\x65\x74\x73\x00\x42\x45\x47\x49\
                                      \x4e";

    let packet = BinlogEventPacket::parse(QUERY_EVENT).unwrap();
    match packet.event {
      BinlogEvent::NotSupported(BinlogEventType::QUERY_EVENT) => {}
      _ => panic!(),
    }
  }

  #[test]
  fn parses_table_map() {
    const TABLE_MAP_EVENT: &[u8] = b"\x00\xfc\x5a\x5d\x5d\x13\x01\x00\x00\x00\x32\x00\x00\x00\x49\x01\x00\
                                          \x00\x00\x00\x2d\x0a\x00\x00\x00\x00\x01\x00\x04\x70\x65\x74\x73\x00\
                                          \x04\x63\x61\x74\x73\x00\x04\x03\x0f\x0f\x0a\x04\x58\x02\x58\x02\x00";

    let packet = BinlogEventPacket::parse(TABLE_MAP_EVENT).unwrap();
    match packet.event {
      BinlogEvent::TableMap(packet) => {
        assert_eq!(2605, packet.table_id);
        assert_eq!(1, packet.flags);
        assert_eq!(4, packet.column_count);
        assert_eq!("pets", packet.schema);
        assert_eq!("cats", packet.table);
        // TODO: remaining fields;
      }
      unexpected => panic!("unexpected {:?}", unexpected),
    }
  }

  #[test]
  fn parses_insert_row() {
    const INSERT_ROW_EVENT: &[u8] = b"\x00\xfc\x5a\x5d\x5d\x1e\x01\x00\x00\x00\x37\x00\x00\x00\x80\x01\x00\
                                           \x00\x00\x00\x2d\x0a\x00\x00\x00\x00\x01\x00\x02\x00\x04\xff\xf0\x04\
                                           \x00\x00\x00\x07\x00\x43\x68\x61\x72\x6c\x69\x65\x05\x00\x52\x69\x76\
                                           \x65\x72\xb5\xc0\x0f";

    let packet = BinlogEventPacket::parse(INSERT_ROW_EVENT).unwrap();
    match packet.event {
      BinlogEvent::Insert(packet) => {
        assert_eq!(2605, packet.table_id);
        assert_eq!(1, packet.flags);
      }
      unexpected => panic!("unexpected {:?}", unexpected),
    }
  }

  #[test]
  fn parses_delete_row() {
    // TODO
  }

  #[test]
  fn parses_update_row() {
    // TODO
  }

  #[test]
  fn parses_xid_event() {
    const XID_EVENT: &[u8] = b"\x00\xfc\x5a\x5d\x5d\x10\x01\x00\x00\x00\x1b\x00\x00\x00\x9b\x01\x00\
                                    \x00\x00\x00\x72\x0e\x00\x00\x00\x00\x00\x00";

    let packet = BinlogEventPacket::parse(XID_EVENT).unwrap();
    match packet.event {
      BinlogEvent::NotSupported(BinlogEventType::XID_EVENT) => {}
      _ => panic!(),
    }
  }
}

// https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html
