use crate::mysql::protocol::{CharacterSet, ColumnMetadataType};

use super::protocol::ColumnType;
use super::{buf_ext::BufExt, protocol::BinlogEventType};
use bytes::{Buf, Bytes};
use std::io;

#[derive(Debug)]
pub struct BinlogEventPacket {
  pub timestamp: u32,
  pub server_id: u32,
  pub log_position: u32,
  pub flags: u16,
  pub event: BinlogEvent,
  pub checksum: Bytes,
}

impl BinlogEventPacket {
  pub fn parse(mut b: Bytes) -> io::Result<BinlogEventPacket> {
    // skip OK byte
    assert_eq!(0x00, b.get_u8());

    let timestamp = b.get_u32_le();
    let event_type = b.get_u8().try_into().unwrap();
    let server_id = b.get_u32_le();
    let event_size = (b.get_u32_le() - 19) as usize;
    let log_position = b.get_u32_le();
    let flags = b.get_u16_le();
    let payload_len = b.remaining() - 4; // TODO: checksum is usually 4 bytes, but can be changed...
    let payload = b.split_to(payload_len);
    let checksum = b;

    let event = match event_type {
      BinlogEventType::TABLE_MAP_EVENT => TableMapEvent::parse(payload).map(BinlogEvent::TableMap),
      BinlogEventType::ROTATE_EVENT => {
        // NOTE: Strangely enough, the checksum is actually the suffix of the binlog file name.
        RotateEvent::parse(payload, checksum.clone()).map(BinlogEvent::Rotate)
      }
      BinlogEventType::FORMAT_DESCRIPTION_EVENT => FormatDescriptionEvent::parse(payload).map(BinlogEvent::Format),
      BinlogEventType::WRITE_ROWS_EVENTV0 => RowEvent::parse(payload, false, false, true).map(BinlogEvent::Insert),
      BinlogEventType::WRITE_ROWS_EVENTV1 => RowEvent::parse(payload, false, false, true).map(BinlogEvent::Insert),
      BinlogEventType::WRITE_ROWS_EVENTV2 => RowEvent::parse(payload, true, false, true).map(BinlogEvent::Insert),
      BinlogEventType::UPDATE_ROWS_EVENTV0 => RowEvent::parse(payload, false, true, true).map(BinlogEvent::Update),
      BinlogEventType::UPDATE_ROWS_EVENTV1 => RowEvent::parse(payload, false, true, true).map(BinlogEvent::Update),
      BinlogEventType::UPDATE_ROWS_EVENTV2 => RowEvent::parse(payload, true, true, true).map(BinlogEvent::Update),
      BinlogEventType::DELETE_ROWS_EVENTV0 => RowEvent::parse(payload, false, true, false).map(BinlogEvent::Delete),
      BinlogEventType::DELETE_ROWS_EVENTV1 => RowEvent::parse(payload, false, true, false).map(BinlogEvent::Delete),
      BinlogEventType::DELETE_ROWS_EVENTV2 => RowEvent::parse(payload, true, true, false).map(BinlogEvent::Delete),
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
  fn parse(mut b: Bytes, cb: Bytes) -> io::Result<Self> {
    let next_log_position = b.get_u64_le() as u32;
    let next_log_file_buffer = &[&b[..], &cb[..]].concat();
    let next_log_file = std::str::from_utf8(next_log_file_buffer).unwrap();

    Ok(Self {
      next_log_position,
      next_log_file: next_log_file.into(),
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
  pub null_bitmap: Bytes,
  pub metadata: TableMapEventMetadata,
}

#[derive(Debug, Default)]
pub struct TableMapEventMetadata {
  pub is_unsigned_integer_bitmap: Option<Bytes>,
  pub default_charset: Option<(CharacterSet, Vec<(usize, CharacterSet)>)>,
  pub enum_and_set_default_charsets: Option<(CharacterSet, Vec<(usize, CharacterSet)>)>,
  pub column_charsets: Option<Vec<CharacterSet>>,
  pub enum_and_set_column_charsets: Option<Vec<CharacterSet>>,
  pub column_names: Option<Vec<String>>,
  pub set_str_values: Option<Vec<String>>,
  pub enum_str_values: Option<Vec<String>>,
}

impl TableMapEvent {
  fn parse(mut b: Bytes) -> io::Result<Self> {
    let table_id = b.get_uint_le(6); // this is actually a fixed length (either 4 or 6 bytes)
    let flags = b.get_u16_le();

    let schema_len = b.get_u8() as usize;
    let schema = b.split_to(schema_len);
    let schema = std::str::from_utf8(schema.chunk()).unwrap();

    // skip 0x00
    assert_eq!(0x00, b.get_u8());

    let table_len = b.mysql_get_lenc_uint().unwrap() as usize;
    let table = b.split_to(table_len);
    let table = std::str::from_utf8(table.chunk()).unwrap();

    // skip 0x00
    assert_eq!(0x00, b.get_u8());

    let column_count = b.mysql_get_lenc_uint().unwrap() as usize;
    let mut column_types = Vec::with_capacity(column_count);
    for _ in 0..column_count {
      column_types.push(b.get_u8().try_into().unwrap());
    }

    let column_metas_buffer_len = b.mysql_get_lenc_uint().unwrap() as usize;
    let mut column_metas_buffer = b.split_to(column_metas_buffer_len);
    let mut column_metas = vec![0; column_count];

    // https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html#a1b84e5b226c76eaf9c0df8ed03ba1393
    for (i, t) in column_types.iter().enumerate() {
      match t {
        ColumnType::MYSQL_TYPE_FLOAT
        | ColumnType::MYSQL_TYPE_DOUBLE
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_JSON => {
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
    let bitmap_len = (column_count + 7) / 8;
    let null_bitmap = b.split_to(bitmap_len);

    let mut metadata = TableMapEventMetadata::default();

    fn parse_default_charset(mut b: Bytes) -> io::Result<(CharacterSet, Vec<(usize, CharacterSet)>)> {
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

    fn parse_column_charsets(mut b: Bytes) -> io::Result<Vec<CharacterSet>> {
      let mut column_charsets = Vec::new();
      while b.remaining() > 0 {
        let column_charset = b.mysql_get_lenc_uint()?;
        let column_charset = (column_charset as u8).try_into().unwrap();
        column_charsets.push(column_charset);
      }
      Ok(column_charsets)
    }

    fn parse_str_value(b: Bytes) -> io::Result<Vec<String>> {
      todo!()
    }

    pub fn parse_strings(mut b: Bytes) -> io::Result<Vec<String>> {
      let mut column_names = Vec::new();
      while b.remaining() > 0 {
        let len = b.mysql_get_lenc_uint()? as usize;
        let column_name = b.split_to(len);
        let column_name = std::str::from_utf8(column_name.chunk()).unwrap();
        column_names.push(column_name.into());
      }
      Ok(column_names)
    }

    while b.remaining() > 0 {
      let metadata_type: ColumnMetadataType = b.get_u8().try_into().unwrap();
      let metadata_len = b.mysql_get_lenc_uint().unwrap() as usize;
      let metadata_value = b.split_to(metadata_len);

      // https://github.com/mysql/mysql-server/blob/8.0/libbinlogevents/src/rows_event.cpp#L141
      match metadata_type {
        ColumnMetadataType::SIGNEDNESS => metadata.is_unsigned_integer_bitmap = Some(metadata_value),
        ColumnMetadataType::DEFAULT_CHARSET => {
          let default_charset = parse_default_charset(metadata_value)?;
          metadata.default_charset = Some(default_charset)
        }
        ColumnMetadataType::COLUMN_CHARSET => {
          let column_charset = parse_column_charsets(metadata_value)?;
          metadata.column_charsets = Some(column_charset)
        }
        ColumnMetadataType::COLUMN_NAME => {
          let column_names = parse_strings(metadata_value)?;
          metadata.column_names = Some(column_names);
        }
        ColumnMetadataType::SET_STR_VALUE => {
          let set_str_values = parse_str_value(metadata_value)?;
          metadata.set_str_values = Some(set_str_values);
        }
        ColumnMetadataType::ENUM_STR_VALUE => {
          let enum_str_values = parse_str_value(metadata_value)?;
          metadata.enum_str_values = Some(enum_str_values);
        }
        ColumnMetadataType::GEOMETRY_TYPE => {}
        ColumnMetadataType::SIMPLE_PRIMARY_KEY => {}
        ColumnMetadataType::PRIMARY_KEY_WITH_PREFIX => {}
        ColumnMetadataType::ENUM_AND_SET_DEFAULT_CHARSET => {
          let enum_and_set_default_charsets = parse_default_charset(metadata_value)?;
          metadata.enum_and_set_default_charsets = Some(enum_and_set_default_charsets)
        }
        ColumnMetadataType::ENUM_AND_SET_COLUMN_CHARSET => {
          let enum_and_set_column_charsets = parse_column_charsets(metadata_value)?;
          metadata.enum_and_set_column_charsets = Some(enum_and_set_column_charsets);
        }
        ColumnMetadataType::COLUMN_VISIBILITY => {}
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
      null_bitmap: null_bitmap.into(),
      metadata,
    })
  }

  pub fn columns(&self) -> Vec<Column> {
    fn is_numeric(t: ColumnType) -> bool {
      if let ColumnType::MYSQL_TYPE_TINY
      | ColumnType::MYSQL_TYPE_SHORT
      | ColumnType::MYSQL_TYPE_INT24
      | ColumnType::MYSQL_TYPE_LONG
      | ColumnType::MYSQL_TYPE_LONGLONG = t
      {
        return true;
      }
      false
    }

    let mut j = 0;
    (0..self.column_count)
      .map(|i| {
        let column_name = self.metadata.column_names.as_ref().unwrap()[i].clone();
        let column_type = self.column_types[i];
        let column_meta = self.column_metas[i];

        // SCAN from LSB to MSB
        let is_nullable = self.null_bitmap[i / 8] & (1 << (i % 8)) != 0;

        // SCAN from MSB to LSB
        let is_unsigned = self
          .metadata
          .is_unsigned_integer_bitmap
          .as_ref()
          .filter(|v| v[j / 8] & (0x80 >> (j % 8)) != 0)
          .is_some();

        let column_type_definition = match column_type {
          ColumnType::MYSQL_TYPE_TINY if is_unsigned => ColumnTypeDefinition::U64 { pack_length: 1 },
          ColumnType::MYSQL_TYPE_TINY => ColumnTypeDefinition::I64 { pack_length: 1 },

          ColumnType::MYSQL_TYPE_SHORT if is_unsigned => ColumnTypeDefinition::U64 { pack_length: 2 },
          ColumnType::MYSQL_TYPE_SHORT => ColumnTypeDefinition::I64 { pack_length: 2 },

          ColumnType::MYSQL_TYPE_INT24 if is_unsigned => ColumnTypeDefinition::U64 { pack_length: 3 },
          ColumnType::MYSQL_TYPE_INT24 => ColumnTypeDefinition::I64 { pack_length: 3 },

          ColumnType::MYSQL_TYPE_LONG if is_unsigned => ColumnTypeDefinition::U64 { pack_length: 4 },
          ColumnType::MYSQL_TYPE_LONG => ColumnTypeDefinition::I64 { pack_length: 4 },

          ColumnType::MYSQL_TYPE_LONGLONG if is_unsigned => ColumnTypeDefinition::U64 { pack_length: 8 },
          ColumnType::MYSQL_TYPE_LONGLONG => ColumnTypeDefinition::I64 { pack_length: 8 },

          ColumnType::MYSQL_TYPE_DECIMAL => unreachable!(),
          ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let bytes = column_meta.to_le_bytes();
            let precision = bytes[0];
            let scale = bytes[1];
            ColumnTypeDefinition::Decimal { precision, scale }
          }

          ColumnType::MYSQL_TYPE_FLOAT => {
            let pack_length = column_meta.try_into().unwrap();
            assert_eq!(pack_length, 4); // Make sure that the server sizeof(float) == 4
            ColumnTypeDefinition::F64 { pack_length }
          }
          ColumnType::MYSQL_TYPE_DOUBLE => {
            let pack_length = column_meta.try_into().unwrap();
            assert_eq!(pack_length, 8); // Make sure that the server sizeof(float) == 8
            ColumnTypeDefinition::F64 { pack_length }
          }

          ColumnType::MYSQL_TYPE_BLOB => {
            let pack_length = column_meta.try_into().unwrap();
            assert!(pack_length <= 4);
            ColumnTypeDefinition::Blob { pack_length }
          }

          ColumnType::MYSQL_TYPE_DATE => ColumnTypeDefinition::Date(ColumnTypeDefinitionDate::U24),
          ColumnType::MYSQL_TYPE_DATETIME => ColumnTypeDefinition::Date(ColumnTypeDefinitionDate::U64),
          ColumnType::MYSQL_TYPE_DATETIME2 => {
            ColumnTypeDefinition::Date(ColumnTypeDefinitionDate::Arbitrary(column_meta.try_into().unwrap()))
          }
          ColumnType::MYSQL_TYPE_TIME => ColumnTypeDefinition::Time(ColumnTypeDefinitionTime::U24),
          ColumnType::MYSQL_TYPE_TIME2 => {
            ColumnTypeDefinition::Time(ColumnTypeDefinitionTime::Arbitrary(column_meta.try_into().unwrap()))
          }
          ColumnType::MYSQL_TYPE_YEAR => ColumnTypeDefinition::Year,
          ColumnType::MYSQL_TYPE_TIMESTAMP => ColumnTypeDefinition::Timestamp,
          ColumnType::MYSQL_TYPE_TIMESTAMP2 => todo!(),

          ColumnType::MYSQL_TYPE_JSON => {
            let pack_length = column_meta.try_into().unwrap();
            ColumnTypeDefinition::Json { pack_length }
          }
          ColumnType::MYSQL_TYPE_ENUM => todo!(),
          ColumnType::MYSQL_TYPE_SET => todo!(),
          ColumnType::MYSQL_TYPE_NULL => {
            unreachable!()
          }
          ColumnType::MYSQL_TYPE_TINY_BLOB | ColumnType::MYSQL_TYPE_MEDIUM_BLOB | ColumnType::MYSQL_TYPE_LONG_BLOB => {
            unreachable!()
          }
          ColumnType::MYSQL_TYPE_BIT => {
            let bytes = column_meta.to_le_bytes();
            let useless = bytes[0];
            assert_eq!(0, useless);
            let pack_length = bytes[1];
            assert!(pack_length <= 8);
            let pack_length = pack_length.try_into().unwrap();
            ColumnTypeDefinition::U64 { pack_length }
          }
          ColumnType::MYSQL_TYPE_VARCHAR => {
            let pack_length = if column_meta > 255 { 2 } else { 1 };
            ColumnTypeDefinition::String { pack_length }
          }
          ColumnType::MYSQL_TYPE_VAR_STRING => {
            todo!()
          }
          ColumnType::MYSQL_TYPE_STRING => {
            let bytes = column_meta.to_le_bytes();
            // Most likely a better way to do this.
            let upper = ((0xFE - bytes[0]) * 4) as u16;
            let lower = bytes[1] as u16;
            let length = upper + lower;
            let pack_length = if length > 255 { 2 } else { 1 };
            ColumnTypeDefinition::String { pack_length }
          }
          ColumnType::MYSQL_TYPE_GEOMETRY => todo!(),
        };

        if is_numeric(column_type) {
          j += 1;
        }

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
  pub event_type_header_lengths: Bytes,
}

impl FormatDescriptionEvent {
  fn parse(mut b: Bytes) -> io::Result<Self> {
    let version = b.get_u16_le();

    let server_version = b.split_to(50);

    let null_terminated = server_version.iter().position(|x| *x == 0x00).unwrap_or(0);
    let server_version = std::str::from_utf8(&server_version[..null_terminated]).unwrap();

    let create_timestamp = b.get_u32_le();
    let event_header_length = b.get_u8();

    let event_type_header_lengths = b;

    Ok(Self {
      version,
      server_version: server_version.into(),
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
  pub extras: Option<Bytes>,
  pub column_count: u64,
  pub columns_before_image: Option<Bytes>,
  pub columns_after_image: Option<Bytes>,
  pub null_bitmap: Option<Bytes>,
  pub rows: Bytes,
}

impl RowEvent {
  fn parse(
    mut b: Bytes,
    use_extras: bool,
    use_columns_before_image: bool,
    use_columns_after_image: bool,
  ) -> io::Result<Self> {
    // https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Rows__event.html
    let table_id = b.get_uint_le(6);
    let flags = b.get_u16_le();

    let mut extras = None;
    if use_extras {
      let extras_len = (b.get_u16_le() - 2) as usize;
      extras = Some(b.split_to(extras_len))
    }

    let column_count = b.mysql_get_lenc_uint().unwrap();

    let bitmap_len = ((column_count + 7) / 8) as usize;

    let mut columns_before_image = None;
    if use_columns_before_image {
      columns_before_image = Some(b.split_to(bitmap_len));
    }

    let mut columns_after_image = None;
    if use_columns_after_image {
      columns_after_image = Some(b.split_to(bitmap_len));
    }

    let mut null_bitmap = None;
    if bitmap_len > 0 {
      null_bitmap = Some(b.split_to(bitmap_len));
    }

    //https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html
    // https://dev.mysql.com/doc/refman/8.0/en/mysqlbinlog-row-events.html
    let rows = b.clone();

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

  pub fn values(&self, columns: &Vec<Column>) -> Vec<Value> {
    let mut b = self.rows.clone();

    let values = columns
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

        if *is_nullable && is_null {
          return Value::Null;
        }

        match column_type_definition {
          ColumnTypeDefinition::U64 { pack_length } => Value::U64(b.get_uint_le(*pack_length)),
          ColumnTypeDefinition::I64 { pack_length } => Value::I64(b.get_int_le(*pack_length)),
          ColumnTypeDefinition::F64 { pack_length } => match pack_length {
            &4 => Value::F64(b.get_f32_le().into()),
            &8 => Value::F64(b.get_f64_le()),
            _ => unreachable!(),
          },
          ColumnTypeDefinition::Decimal { precision, scale } => {
            let len = b.mysql_get_lenc_uint().unwrap().try_into().unwrap();
            let buffer = b.copy_to_bytes(len);
            Value::Decimal(buffer)
          }
          ColumnTypeDefinition::String { pack_length } => {
            let len = b.get_uint_le(*pack_length).try_into().unwrap();
            let buffer = b.copy_to_bytes(len);
            Value::String(String::from_utf8(buffer.into()).unwrap())
          }
          ColumnTypeDefinition::Blob { pack_length } => {
            let len = b.get_uint_le(*pack_length).try_into().unwrap();
            let buffer = b.copy_to_bytes(len);
            Value::Blob(buffer)
          }
          ColumnTypeDefinition::Json { pack_length } => {
            let len = b.get_uint_le(*pack_length).try_into().unwrap();
            let buffer = b.copy_to_bytes(len);
            Value::Json(buffer)
          }
          ColumnTypeDefinition::Year => {
            let year: u64 = b.get_u8().into();
            Value::U64(1900 + year)
          }
          ColumnTypeDefinition::Timestamp => Value::U64(b.get_u32_le().into()),
          ColumnTypeDefinition::Date(ColumnTypeDefinitionDate::U24) => {
            let tmp = b.get_uint_le(3);
            let day = (tmp & 31).try_into().unwrap();
            let month = ((tmp >> 5) & 15).try_into().unwrap();
            let year = (tmp >> 9).try_into().unwrap();
            Value::Date {
              year,
              month,
              day,
              hour: 0,
              minute: 0,
              second: 0,
              micro_second: 0,
            }
          }
          ColumnTypeDefinition::Date(ColumnTypeDefinitionDate::U64) => {
            let tmp = b.get_u64_le();
            let date = tmp / 1_000_000;
            let time = tmp % 1_000_000;
            let year = (date / 10000).try_into().unwrap();
            let month = ((date % 10000) / 100).try_into().unwrap();
            let day = (date % 100).try_into().unwrap();
            let hour = (time / 10000).try_into().unwrap();
            let minute = ((time % 10000) / 100).try_into().unwrap();
            let second = (time % 100).try_into().unwrap();
            Value::Date {
              year,
              month,
              day,
              hour,
              minute,
              second,
              micro_second: 0,
            }
          }
          ColumnTypeDefinition::Date(ColumnTypeDefinitionDate::Arbitrary(_)) => todo!(),
          ColumnTypeDefinition::Time(ColumnTypeDefinitionTime::U24) => {
            let tmp = b.get_uint_le(3);
            let hours = (tmp / 10000).try_into().unwrap();
            let minutes = ((tmp % 10000) / 100).try_into().unwrap();
            let seconds = (tmp % 100).try_into().unwrap();
            Value::Time {
              hours,
              minutes,
              seconds,
              micro_seconds: 0,
            }
          }
          ColumnTypeDefinition::Time(ColumnTypeDefinitionTime::Arbitrary(_)) => todo!(),
        }
      })
      .collect();

    assert_eq!(0, b.remaining());
    values
  }
}

#[derive(Debug)]
pub enum Value {
  Null,
  U64(u64),
  I64(i64),
  F64(f64),
  // TODO: Parse decimals instead of keeping raw binary data.
  Decimal(Bytes),
  String(String),
  Blob(Bytes),
  // TODO: Parse json values, but MYSQL has a custom JSONB protocol because why not.
  Json(Bytes),
  Date {
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micro_second: u32,
  },
  Time {
    hours: u8,
    minutes: u8,
    seconds: u8,
    micro_seconds: u32,
  },
}

#[derive(Debug)]
pub struct Column {
  column_name: String,
  is_nullable: bool,
  column_type_definition: ColumnTypeDefinition,
}

#[derive(Debug)]
pub enum ColumnTypeDefinitionDate {
  U24,
  U64,
  Arbitrary(u8),
}

#[derive(Debug)]
pub enum ColumnTypeDefinitionTime {
  U24,
  Arbitrary(u8),
}

#[derive(Debug)]
pub enum ColumnTypeDefinition {
  U64 { pack_length: usize },
  I64 { pack_length: usize },
  F64 { pack_length: usize },
  Decimal { precision: u8, scale: u8 },
  Json { pack_length: usize },
  String { pack_length: usize },
  Blob { pack_length: usize },
  Date(ColumnTypeDefinitionDate),
  Year,
  Time(ColumnTypeDefinitionTime),
  Timestamp,
}

#[cfg(test)]
mod test {
  use super::{BinlogEvent, BinlogEventPacket, BinlogEventType};

  #[test]
  fn parses_rotate() {
    const ROTATE_EVENT: &[u8] = b"\x00\x00\x00\x00\x00\x04\x01\x00\x00\x00\x2d\x00\x00\x00\x00\x00\x00\
                                       \x00\x20\x00\x96\x00\x00\x00\x00\x00\x00\x00\x73\x68\x6f\x70\x69\x66\
                                       \x79\x2d\x62\x69\x6e\x2e\x30\x30\x30\x30\x30\x35";

    let packet = BinlogEventPacket::parse(ROTATE_EVENT.into()).unwrap();
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

    let packet = BinlogEventPacket::parse(FORMAT_DESCRIPTION_EVENT.into()).unwrap();
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

    let packet = BinlogEventPacket::parse(ANONYMOUS_GTID_EVENT.into()).unwrap();
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

    let packet = BinlogEventPacket::parse(QUERY_EVENT.into()).unwrap();
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

    let packet = BinlogEventPacket::parse(TABLE_MAP_EVENT.into()).unwrap();
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

    let packet = BinlogEventPacket::parse(INSERT_ROW_EVENT.into()).unwrap();
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

    let packet = BinlogEventPacket::parse(XID_EVENT.into()).unwrap();
    match packet.event {
      BinlogEvent::NotSupported(BinlogEventType::XID_EVENT) => {}
      _ => panic!(),
    }
  }
}

// https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html
