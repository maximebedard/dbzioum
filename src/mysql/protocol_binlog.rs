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
    let payload = b.to_vec();

    let event = match event_type {
      BinlogEventType::TABLE_MAP_EVENT => TableMapEvent::parse(&payload).map(BinlogEvent::TableMap),
      BinlogEventType::ROTATE_EVENT => RotateEvent::parse(&payload).map(BinlogEvent::Rotate),
      BinlogEventType::FORMAT_DESCRIPTION_EVENT => FormatDescriptionEvent::parse(&payload).map(BinlogEvent::Format),
      BinlogEventType::WRITE_ROWS_EVENTV0 => RowEvent::parse(&payload, false, false).map(BinlogEvent::Insert),
      BinlogEventType::WRITE_ROWS_EVENTV1 => RowEvent::parse(&payload, false, false).map(BinlogEvent::Insert),
      BinlogEventType::WRITE_ROWS_EVENTV2 => RowEvent::parse(&payload, true, false).map(BinlogEvent::Insert),
      BinlogEventType::UPDATE_ROWS_EVENTV0 => RowEvent::parse(&payload, false, false).map(BinlogEvent::Update),
      BinlogEventType::UPDATE_ROWS_EVENTV1 => RowEvent::parse(&payload, false, true).map(BinlogEvent::Update),
      BinlogEventType::UPDATE_ROWS_EVENTV2 => RowEvent::parse(&payload, true, true).map(BinlogEvent::Update),
      BinlogEventType::DELETE_ROWS_EVENTV0 => RowEvent::parse(&payload, false, false).map(BinlogEvent::Delete),
      BinlogEventType::DELETE_ROWS_EVENTV1 => RowEvent::parse(&payload, false, false).map(BinlogEvent::Delete),
      BinlogEventType::DELETE_ROWS_EVENTV2 => RowEvent::parse(&payload, true, false).map(BinlogEvent::Delete),
      not_supported => Ok(BinlogEvent::NotSupported(not_supported)),
    }?;

    Ok(BinlogEventPacket {
      timestamp,
      server_id,
      log_position,
      flags,
      event,
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
  fn parse(buffer: impl AsRef<[u8]>) -> io::Result<Self> {
    let mut b = buffer.as_ref();
    let next_log_position = b.get_u64_le() as u32;
    let next_log_file = String::from_utf8(b.to_vec()).unwrap();

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
  pub column_count: u64,
  pub column_types: Vec<ColumnType>,
  pub column_metas: Vec<u16>,
  pub null_bitmap: Vec<u8>,
}

impl TableMapEvent {
  fn parse(buffer: impl AsRef<[u8]>) -> io::Result<Self> {
    let mut b = buffer.as_ref();
    let table_id = b.get_uint_le(6); // this is actually a fixed length (either 4 or 6 bytes)
    let flags = b.get_u16_le();

    let schema_len = b.get_u8() as usize;
    let (schema, mut b) = b.split_at(schema_len);
    let schema = String::from_utf8(schema.to_vec()).unwrap();

    // skip 0x00
    b.advance(1);

    let table_len = b.get_u8() as usize;
    let (table, mut b) = b.split_at(table_len);
    let table = String::from_utf8(table.to_vec()).unwrap();

    // skip 0x00
    b.advance(1);

    let column_count = b.mysql_get_lenc_uint().unwrap() as usize;
    let column_types: Vec<ColumnType> = b[..column_count]
      .iter()
      .cloned()
      .map(|v| ColumnType::try_from(v).unwrap())
      .collect();

    let mut column_metas = vec![0; column_count];

    let column_meta_reader_len = b.mysql_get_lenc_uint().unwrap() as usize;
    let (mut column_meta_reader, b) = b.split_at(column_meta_reader_len);

    // https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html#a1b84e5b226c76eaf9c0df8ed03ba1393

    for (i, t) in column_types.iter().enumerate() {
      match t {
        // 2 bytes
        ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_NEWDECIMAL
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_BIT => {
          // TODO: there is a off by one somewhere, and this should be using read_u16;
          // println!("a {:?}, {:?}", t, column_meta_reader);
          column_metas[i] = column_meta_reader.get_u8() as u16;
        }

        // 1 byte
        ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_DOUBLE
        | ColumnType::MYSQL_TYPE_FLOAT
        | ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_JSON => {
          // println!("b {:?}", t);
          column_metas[i] = column_meta_reader.get_u8() as u16;
        }

        // maybe 1 byte?
        ColumnType::MYSQL_TYPE_TIME2 | ColumnType::MYSQL_TYPE_DATETIME2 | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
          // println!("c {:?}", t);
          column_metas[i] = column_meta_reader.get_u8() as u16;
        }

        // 0 byte
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
          // println!("d {:?}", t);
          column_metas[i] = 0_u16;
        }

        _ => unimplemented!(),
      }
    }

    let null_bitmap = if b.len() == (column_count + 7) / 8 {
      b.to_vec()
    } else {
      Vec::new()
    };

    Ok(Self {
      table_id,
      flags,
      schema: schema.into(),
      table: table.into(),
      column_count: column_count as u64,
      column_types,
      column_metas,
      null_bitmap,
    })
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

    let (server_version, mut b) = b.split_at(50);
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
  pub extras: Vec<u8>,
  pub column_count: u64,
  pub column_bitmap1: Vec<u8>,
  pub column_bitmap2: Vec<u8>,
  pub rows: Vec<u8>,
}

impl RowEvent {
  fn parse(buffer: impl AsRef<[u8]>, use_extras: bool, use_bitmap2: bool) -> io::Result<Self> {
    let mut b = buffer.as_ref();
    let table_id = b.get_uint_le(6);
    let flags = b.get_u16_le();

    let extras = if use_extras {
      let extras_len = b.get_u16_le() as usize - 2;
      let (extras_data, b) = b.split_at(extras_len);
      let extras_data = extras_data.to_vec();
      extras_data
    } else {
      Vec::new()
    };

    let column_count = b.mysql_get_lenc_uint().unwrap();

    let bitmap_len = ((column_count + 7) / 8) as usize;

    let (column_bitmap1, b) = b.split_at(bitmap_len);
    let column_bitmap1 = column_bitmap1.to_vec();

    let column_bitmap2 = if use_bitmap2 {
      let (column_bitmap2_data, b) = b.split_at(bitmap_len);
      let column_bitmap2_data = column_bitmap2_data.to_vec();
      column_bitmap2_data
    } else {
      Vec::new()
    };

    //https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html
    // https://dev.mysql.com/doc/refman/8.0/en/mysqlbinlog-row-events.html
    let rows = b.to_vec();

    Ok(Self {
      table_id,
      flags,
      extras,
      column_count,
      column_bitmap1,
      column_bitmap2,
      rows,
    })
  }
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
