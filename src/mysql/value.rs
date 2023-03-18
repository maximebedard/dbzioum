#[derive(Debug)]
pub enum Value {
    Null,
    Bytes(Vec<u8>),
    Int(i64),
    Uint(u64),
    Float(f64),
    Date {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        micro: u32,
    },
    Time {
        negative: bool,
        days: u32,
        hours: u8,
        minutes: u8,
        seconds: u8,
        micros: u32,
    },
}

impl Value {
    // pub fn parse(buffer: impl Into<Bytes>, ct: ColumnType, unsigned: bool) -> io::Result<Self> {
    //   let mut b = buffer.into();
    //   match ct {
    //     ColumnType::MYSQL_TYPE_STRING
    //     | ColumnType::MYSQL_TYPE_VAR_STRING
    //     | ColumnType::MYSQL_TYPE_BLOB
    //     | ColumnType::MYSQL_TYPE_TINY_BLOB
    //     | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
    //     | ColumnType::MYSQL_TYPE_LONG_BLOB
    //     | ColumnType::MYSQL_TYPE_SET
    //     | ColumnType::MYSQL_TYPE_ENUM
    //     | ColumnType::MYSQL_TYPE_DECIMAL
    //     | ColumnType::MYSQL_TYPE_VARCHAR
    //     | ColumnType::MYSQL_TYPE_BIT
    //     | ColumnType::MYSQL_TYPE_NEWDECIMAL
    //     | ColumnType::MYSQL_TYPE_GEOMETRY
    //     | ColumnType::MYSQL_TYPE_JSON => Ok(Self::Bytes(b.to_vec())),

    //     ColumnType::MYSQL_TYPE_TINY if unsigned => Ok(Self::Uint(b.get_u8() as u64)),
    //     ColumnType::MYSQL_TYPE_TINY => Ok(Self::Int(b.get_i8() as i64)),
    //     ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR if unsigned => {
    //       Ok(Self::Uint(b.get_u16_le() as u64))
    //     }
    //     ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
    //       Ok(Self::Int(b.get_i16_le() as i64))
    //     }

    //     ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 if unsigned => {
    //       println!("lollll = {:x?}", b.bytes());
    //       Ok(Self::Uint(b.get_u32_le() as u64))
    //     }
    //     ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
    //       println!("lollll = {:x?}", b.bytes());
    //       Ok(Self::Int(b.get_i32_le() as i64))
    //     }

    //     ColumnType::MYSQL_TYPE_LONGLONG if unsigned => {
    //       println!("lollzll = {:x?}", b.bytes());
    //       Ok(Self::Uint(b.get_u64_le()))
    //     },
    //     ColumnType::MYSQL_TYPE_LONGLONG => Ok(Self::Int(b.get_i64_le())),
    //     ColumnType::MYSQL_TYPE_FLOAT => Ok(Self::Float(b.get_f32_le() as f64)),
    //     ColumnType::MYSQL_TYPE_DOUBLE => Ok(Self::Float(b.get_f64_le())),

    //     ColumnType::MYSQL_TYPE_TIMESTAMP
    //     | ColumnType::MYSQL_TYPE_DATE
    //     | ColumnType::MYSQL_TYPE_DATETIME => {
    //       let len = b.get_u8();
    //       let mut year = 0u16;
    //       let mut month = 0u8;
    //       let mut day = 0u8;
    //       let mut hour = 0u8;
    //       let mut minute = 0u8;
    //       let mut second = 0u8;
    //       let mut micro = 0u32;
    //       if len >= 4u8 {
    //         year = b.get_u16_le();
    //         month = b.get_u8();
    //         day = b.get_u8();
    //       }
    //       if len >= 7u8 {
    //         hour = b.get_u8();
    //         minute = b.get_u8();
    //         second = b.get_u8();
    //       }
    //       if len == 11u8 {
    //         micro = b.get_u32_le();
    //       }
    //       Ok(Self::Date {
    //         year,
    //         month,
    //         day,
    //         hour,
    //         minute,
    //         second,
    //         micro,
    //       })
    //     }
    //     ColumnType::MYSQL_TYPE_TIME => {
    //       let len = b.get_u8();
    //       let mut negative = false;
    //       let mut days = 0u32;
    //       let mut hours = 0u8;
    //       let mut minutes = 0u8;
    //       let mut seconds = 0u8;
    //       let mut micros = 0u32;
    //       if len >= 8u8 {
    //         negative = b.get_u8() == 1u8;
    //         days = b.get_u32_le();
    //         hours = b.get_u8();
    //         minutes = b.get_u8();
    //         seconds = b.get_u8();
    //       }
    //       if len == 12u8 {
    //         micros = b.get_u32_le();
    //       }
    //       Ok(Self::Time {
    //         negative,
    //         days,
    //         hours,
    //         minutes,
    //         seconds,
    //         micros,
    //       })
    //     }
    //     invalid => panic!("type {:?} is not supported", invalid),
    //   }
    // }

    pub fn as_str(&self) -> Option<&str> {
        // works because we assume utf-8
        // this is definitely not the right way to do this kind of conversion.
        match self {
            Value::Bytes(bytes) => std::str::from_utf8(bytes.as_slice()).ok(),
            _ => None,
        }
    }

    pub fn as_u32(&self) -> Option<u32> {
        // works because we assume utf-8
        match self {
            Value::Bytes(bytes) => std::str::from_utf8(bytes.as_slice())
                .unwrap()
                .parse::<u32>()
                .ok(),
            // Value::Uint(v) if u32::parse,
            _ => None,
        }
    }
}
