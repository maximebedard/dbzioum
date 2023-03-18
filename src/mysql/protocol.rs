use super::buf_ext::BufExt;
use super::value::Value;
use bitflags::bitflags;
use bytes::{Buf, Bytes};
use std::cmp::max;
use std::io;

pub const MYSQL_NATIVE_PASSWORD_PLUGIN_NAME: &str = "mysql_native_password";
pub const CACHING_SHA2_PASSWORD_PLUGIN_NAME: &str = "caching_sha2_password";
pub const MAX_PAYLOAD_LEN: usize = 16777215;

// https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
bitflags! {
  #[derive(Debug, Clone, Copy)]
  pub struct ColumnFlags: u16 {
    const NOT_NULL = 0x0001;
    const PRIMARY_KEY = 0x0002;
    const UNIQUE_KEY = 0x0004;
    const MULTIPLE_KEY = 0x0008;
    const BLOB = 0x0010;
    const UNSIGNED = 0x0020;
    const ZEROFILL = 0x0040;
    const BINARY = 0x0080;
    const ENUM = 0x0100;
    const AUTO_INCREMENT = 0x0200;
    const TIMESTAMP = 0x0400;
    const SET = 0x0800;
    const NO_DEFAULT_VALUE = 0x1000;
    const ON_UPDATE_NOW = 0x2000;
  }
}

bitflags! {
  pub struct BinlogDumpFlags: u16 {
    const NON_BLOCK = 0x0001;
  }
}

// https://dev.mysql.com/doc/internals/en/capability-flags.html#flag-CLIENT_PROTOCOL_41
bitflags! {
    #[derive(Debug, Clone, Copy)]
    pub struct CapabilityFlags: u32 {
      const CLIENT_LONG_PASSWORD = 0x00000001;
      const CLIENT_FOUND_ROWS = 0x00000002;
      const CLIENT_LONG_FLAG = 0x00000004;
      const CLIENT_CONNECT_WITH_DB = 0x00000008;
      const CLIENT_NO_SCHEMA = 0x00000010;
      const CLIENT_COMPRESS = 0x00000020;
      const CLIENT_ODBC = 0x00000040;
      const CLIENT_LOCAL_FILES = 0x00000080;
      const CLIENT_IGNORE_SPACE = 0x00000100;
      const CLIENT_PROTOCOL_41 = 0x00000200;
      const CLIENT_INTERACTIVE = 0x00000400;
      const CLIENT_SSL = 0x00000800;
      const CLIENT_IGNORE_SIGPIPE = 0x00001000;
      const CLIENT_TRANSACTIONS = 0x00002000;
      const CLIENT_RESERVED = 0x00004000;
      const CLIENT_SECURE_CONNECTION = 0x00008000;
      const CLIENT_MULTI_STATEMENTS = 0x00010000;
      const CLIENT_MULTI_RESULTS = 0x00020000;
      const CLIENT_PS_MULTI_RESULTS = 0x00040000;
      const CLIENT_PLUGIN_AUTH = 0x00080000;
      const CLIENT_CONNECT_ATTRS = 0x00100000;
      const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
      const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 0x00400000;
      const CLIENT_SESSION_TRACK = 0x00800000;
      const CLIENT_DEPRECATE_EOF = 0x01000000;
      const CLIENT_PROGRESS_OBSOLETE = 0x20000000;
      const CLIENT_SSL_VERIFY_SERVER_CERT = 0x40000000;
      const CLIENT_REMEMBER_OPTIONS = 0x80000000;
    }
}

bitflags! {
  #[derive(Debug, Clone, Copy)]
  pub struct StatusFlags: u16 {
    const SERVER_STATUS_IN_TRANS = 0x0001; //  a transaction is active
    const SERVER_STATUS_AUTOCOMMIT = 0x0002; //  auto-commit is enabled
    const SERVER_MORE_RESULTS_EXISTS = 0x0008;
    const SERVER_STATUS_NO_GOOD_INDEX_USED = 0x0010;
    const SERVER_STATUS_NO_INDEX_USED =  0x0020;
    const SERVER_STATUS_CURSOR_EXISTS =  0x0040; //  Used by Binary Protocol Resultset to signal that COM_STMT_FETCH must be used to fetch the row-data.
    const SERVER_STATUS_LAST_ROW_SENT =  0x0080;
    const SERVER_STATUS_DB_DROPPED = 0x0100;
    const SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;
    const SERVER_STATUS_METADATA_CHANGED = 0x0400;
    const SERVER_QUERY_WAS_SLOW =  0x0800;
    const SERVER_PS_OUT_PARAMS = 0x1000;
    const SERVER_STATUS_IN_TRANS_READONLY =  0x2000; //  in a read-only transaction
    const SERVER_SESSION_STATE_CHANGED = 0x4000; //  connection state information has changed
  }
}

// https://dev.mysql.com/doc/internals/en/character-set.html
#[allow(non_camel_case_types)]
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum CharacterSet {
    BIG5 = 0x01_u8,
    DEC8 = 0x03_u8,
    CP850 = 0x04_u8,
    HP8 = 0x06_u8,
    KOI8R = 0x07_u8,
    LATIN1 = 0x08_u8,
    LATIN2 = 0x09_u8,
    SWE7 = 0x0A_u8,
    ASCII = 0x0B_u8,
    UJIS = 0x0C_u8,
    SJIS = 0x0D_u8,
    HEBREW = 0x10_u8,
    TIS620 = 0x12_u8,
    EUCKR = 0x13_u8,
    KOI8U = 0x16_u8,
    GB2312 = 0x18_u8,
    GREEK = 0x19_u8,
    CP1250 = 0x1A_u8,
    GBK = 0x1C_u8,
    LATIN5 = 0x1E_u8,
    ARMSCII8 = 0x20_u8,
    UTF8 = 0x21_u8,
    UCS2 = 0x23_u8,
    CP866 = 0x24_u8,
    KEYBCS2 = 0x25_u8,
    MACCE = 0x26_u8,
    MACROMAN = 0x27_u8,
    CP852 = 0x28_u8,
    LATIN7 = 0x29_u8,
    CP1251 = 0x53_u8,
    UTF16 = 0x36_u8,
    UTF16LE = 0x38_u8,
    CP1256 = 0x39_u8,
    CP1257 = 0x3B_u8,
    UTF32 = 0x3C_u8,
    BINARY = 0x3F_u8,
    GEOSTD8 = 0x5C_u8,
    CP932 = 0x5F_u8,
    EUCJPMS = 0x61_u8,
    GB18030 = 0xF8_u8,
    UTF8MB4 = 0xFF_u8,
}

// https://dev.mysql.com/doc/internals/en/character-set.html
#[allow(non_camel_case_types)]
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum Collation {
    BIG5_CHINESE_CI = 0x01_u8,
    DEC8_SWEDISH_CI = 0x03_u8,
    CP850_GENERAL_CI = 0x04_u8,
    HP8_ENGLISH_CI = 0x06_u8,
    KOI8R_GENERAL_CI = 0x07_u8,
    LATIN1_SWEDISH_CI = 0x08_u8,
    LATIN2_GENERAL_CI = 0x09_u8,
    SWE7_SWEDISH_CI = 0x0A_u8,
    ASCII_GENERAL_CI = 0x0B_u8,
    UJIS_JAPANESE_CI = 0x0C_u8,
    SJIS_JAPANESE_CI = 0x0D_u8,
    HEBREW_GENERAL_CI = 0x10_u8,
    TIS620_THAI_CI = 0x12_u8,
    EUCKR_KOREAN_CI = 0x13_u8,
    KOI8U_GENERAL_CI = 0x16_u8,
    GB2312_CHINESE_CI = 0x18_u8,
    GREEK_GENERAL_CI = 0x19_u8,
    CP1250_GENERAL_CI = 0x1A_u8,
    GBK_CHINESE_CI = 0x1C_u8,
    LATIN5_TURKISH_CI = 0x1E_u8,
    ARMSCII8_GENERAL_CI = 0x20_u8,
    UTF8_GENERAL_CI = 0x21_u8,
    UCS2_GENERAL_CI = 0x23_u8,
    CP866_GENERAL_CI = 0x24_u8,
    KEYBCS2_GENERAL_CI = 0x25_u8,
    MACCE_GENERAL_CI = 0x26_u8,
    MACROMAN_GENERAL_CI = 0x27_u8,
    CP852_GENERAL_CI = 0x28_u8,
    LATIN7_GENERAL_CI = 0x29_u8,
    CP1251_GENERAL_CI = 0x53_u8,
    UTF16_GENERAL_CI = 0x36_u8,
    UTF16LE_GENERAL_CI = 0x38_u8,
    CP1256_GENERAL_CI = 0x39_u8,
    CP1257_GENERAL_CI = 0x3B_u8,
    UTF32_GENERAL_CI = 0x3C_u8,
    BINARY = 0x3F_u8,
    GEOSTD8_GENERAL_CI = 0x5C_u8,
    CP932_JAPANESE_CI = 0x5F_u8,
    EUCJPMS_JAPANESE_CI = 0x61_u8,
    GB18030_CHINESE_CI = 0xF8_u8,
    UTF8MB4_0900_AI_CI = 0xFF_u8,
}

impl From<u8> for CharacterSet {
    fn from(id: u8) -> Self {
        match id {
            0x01_u8 => CharacterSet::BIG5,
            0x03_u8 => CharacterSet::DEC8,
            0x04_u8 => CharacterSet::CP850,
            0x06_u8 => CharacterSet::HP8,
            0x07_u8 => CharacterSet::KOI8R,
            0x08_u8 => CharacterSet::LATIN1,
            0x09_u8 => CharacterSet::LATIN2,
            0x0A_u8 => CharacterSet::SWE7,
            0x0B_u8 => CharacterSet::ASCII,
            0x0C_u8 => CharacterSet::UJIS,
            0x0D_u8 => CharacterSet::SJIS,
            0x10_u8 => CharacterSet::HEBREW,
            0x12_u8 => CharacterSet::TIS620,
            0x13_u8 => CharacterSet::EUCKR,
            0x16_u8 => CharacterSet::KOI8U,
            0x18_u8 => CharacterSet::GB2312,
            0x19_u8 => CharacterSet::GREEK,
            0x1A_u8 => CharacterSet::CP1250,
            0x1C_u8 => CharacterSet::GBK,
            0x1E_u8 => CharacterSet::LATIN5,
            0x20_u8 => CharacterSet::ARMSCII8,
            0x21_u8 => CharacterSet::UTF8,
            0x23_u8 => CharacterSet::UCS2,
            0x24_u8 => CharacterSet::CP866,
            0x25_u8 => CharacterSet::KEYBCS2,
            0x26_u8 => CharacterSet::MACCE,
            0x27_u8 => CharacterSet::MACROMAN,
            0x28_u8 => CharacterSet::CP852,
            0x29_u8 => CharacterSet::LATIN7,
            0x53_u8 => CharacterSet::CP1251,
            0x36_u8 => CharacterSet::UTF16,
            0x38_u8 => CharacterSet::UTF16LE,
            0x39_u8 => CharacterSet::CP1256,
            0x3B_u8 => CharacterSet::CP1257,
            0x3C_u8 => CharacterSet::UTF32,
            0x3F_u8 => CharacterSet::BINARY,
            0x5C_u8 => CharacterSet::GEOSTD8,
            0x5F_u8 => CharacterSet::CP932,
            0x61_u8 => CharacterSet::EUCJPMS,
            0xF8_u8 => CharacterSet::GB18030,
            0xFF_u8 => CharacterSet::UTF8MB4,
            invalid => panic!("invalid character set {}", invalid),
        }
    }
}

impl From<u8> for Collation {
    fn from(id: u8) -> Self {
        match id {
            0x01_u8 => Collation::BIG5_CHINESE_CI,
            0x03_u8 => Collation::DEC8_SWEDISH_CI,
            0x04_u8 => Collation::CP850_GENERAL_CI,
            0x06_u8 => Collation::HP8_ENGLISH_CI,
            0x07_u8 => Collation::KOI8R_GENERAL_CI,
            0x08_u8 => Collation::LATIN1_SWEDISH_CI,
            0x09_u8 => Collation::LATIN2_GENERAL_CI,
            0x0A_u8 => Collation::SWE7_SWEDISH_CI,
            0x0B_u8 => Collation::ASCII_GENERAL_CI,
            0x0C_u8 => Collation::UJIS_JAPANESE_CI,
            0x0D_u8 => Collation::SJIS_JAPANESE_CI,
            0x10_u8 => Collation::HEBREW_GENERAL_CI,
            0x12_u8 => Collation::TIS620_THAI_CI,
            0x13_u8 => Collation::EUCKR_KOREAN_CI,
            0x16_u8 => Collation::KOI8U_GENERAL_CI,
            0x18_u8 => Collation::GB2312_CHINESE_CI,
            0x19_u8 => Collation::GREEK_GENERAL_CI,
            0x1A_u8 => Collation::CP1250_GENERAL_CI,
            0x1C_u8 => Collation::GBK_CHINESE_CI,
            0x1E_u8 => Collation::LATIN5_TURKISH_CI,
            0x20_u8 => Collation::ARMSCII8_GENERAL_CI,
            0x21_u8 => Collation::UTF8_GENERAL_CI,
            0x23_u8 => Collation::UCS2_GENERAL_CI,
            0x24_u8 => Collation::CP866_GENERAL_CI,
            0x25_u8 => Collation::KEYBCS2_GENERAL_CI,
            0x26_u8 => Collation::MACCE_GENERAL_CI,
            0x27_u8 => Collation::MACROMAN_GENERAL_CI,
            0x28_u8 => Collation::CP852_GENERAL_CI,
            0x29_u8 => Collation::LATIN7_GENERAL_CI,
            0x53_u8 => Collation::CP1251_GENERAL_CI,
            0x36_u8 => Collation::UTF16_GENERAL_CI,
            0x38_u8 => Collation::UTF16LE_GENERAL_CI,
            0x39_u8 => Collation::CP1256_GENERAL_CI,
            0x3B_u8 => Collation::CP1257_GENERAL_CI,
            0x3C_u8 => Collation::UTF32_GENERAL_CI,
            0x3F_u8 => Collation::BINARY,
            0x5C_u8 => Collation::GEOSTD8_GENERAL_CI,
            0x5F_u8 => Collation::CP932_JAPANESE_CI,
            0x61_u8 => Collation::EUCJPMS_JAPANESE_CI,
            0xF8_u8 => Collation::GB18030_CHINESE_CI,
            0xFF_u8 => Collation::UTF8MB4_0900_AI_CI,
            invalid => panic!("invalid collation {}", invalid),
        }
    }
}

impl From<CharacterSet> for Collation {
    fn from(cs: CharacterSet) -> Self {
        match cs {
            CharacterSet::BIG5 => Collation::BIG5_CHINESE_CI,
            CharacterSet::DEC8 => Collation::DEC8_SWEDISH_CI,
            CharacterSet::CP850 => Collation::CP850_GENERAL_CI,
            CharacterSet::HP8 => Collation::HP8_ENGLISH_CI,
            CharacterSet::KOI8R => Collation::KOI8R_GENERAL_CI,
            CharacterSet::LATIN1 => Collation::LATIN1_SWEDISH_CI,
            CharacterSet::LATIN2 => Collation::LATIN2_GENERAL_CI,
            CharacterSet::SWE7 => Collation::SWE7_SWEDISH_CI,
            CharacterSet::ASCII => Collation::ASCII_GENERAL_CI,
            CharacterSet::UJIS => Collation::UJIS_JAPANESE_CI,
            CharacterSet::SJIS => Collation::SJIS_JAPANESE_CI,
            CharacterSet::HEBREW => Collation::HEBREW_GENERAL_CI,
            CharacterSet::TIS620 => Collation::TIS620_THAI_CI,
            CharacterSet::EUCKR => Collation::EUCKR_KOREAN_CI,
            CharacterSet::KOI8U => Collation::KOI8U_GENERAL_CI,
            CharacterSet::GB2312 => Collation::GB2312_CHINESE_CI,
            CharacterSet::GREEK => Collation::GREEK_GENERAL_CI,
            CharacterSet::CP1250 => Collation::CP1250_GENERAL_CI,
            CharacterSet::GBK => Collation::GBK_CHINESE_CI,
            CharacterSet::LATIN5 => Collation::LATIN5_TURKISH_CI,
            CharacterSet::ARMSCII8 => Collation::ARMSCII8_GENERAL_CI,
            CharacterSet::UTF8 => Collation::UTF8_GENERAL_CI,
            CharacterSet::UCS2 => Collation::UCS2_GENERAL_CI,
            CharacterSet::CP866 => Collation::CP866_GENERAL_CI,
            CharacterSet::KEYBCS2 => Collation::KEYBCS2_GENERAL_CI,
            CharacterSet::MACCE => Collation::MACCE_GENERAL_CI,
            CharacterSet::MACROMAN => Collation::MACROMAN_GENERAL_CI,
            CharacterSet::CP852 => Collation::CP852_GENERAL_CI,
            CharacterSet::LATIN7 => Collation::LATIN7_GENERAL_CI,
            CharacterSet::CP1251 => Collation::CP1251_GENERAL_CI,
            CharacterSet::UTF16 => Collation::UTF16_GENERAL_CI,
            CharacterSet::UTF16LE => Collation::UTF16LE_GENERAL_CI,
            CharacterSet::CP1256 => Collation::CP1256_GENERAL_CI,
            CharacterSet::CP1257 => Collation::CP1257_GENERAL_CI,
            CharacterSet::UTF32 => Collation::UTF32_GENERAL_CI,
            CharacterSet::BINARY => Collation::BINARY,
            CharacterSet::GEOSTD8 => Collation::GEOSTD8_GENERAL_CI,
            CharacterSet::CP932 => Collation::CP932_JAPANESE_CI,
            CharacterSet::EUCJPMS => Collation::EUCJPMS_JAPANESE_CI,
            CharacterSet::GB18030 => Collation::GB18030_CHINESE_CI,
            CharacterSet::UTF8MB4 => Collation::UTF8MB4_0900_AI_CI,
        }
    }
}

impl From<Collation> for CharacterSet {
    fn from(c: Collation) -> Self {
        match c {
            Collation::BIG5_CHINESE_CI => CharacterSet::BIG5,
            Collation::DEC8_SWEDISH_CI => CharacterSet::DEC8,
            Collation::CP850_GENERAL_CI => CharacterSet::CP850,
            Collation::HP8_ENGLISH_CI => CharacterSet::HP8,
            Collation::KOI8R_GENERAL_CI => CharacterSet::KOI8R,
            Collation::LATIN1_SWEDISH_CI => CharacterSet::LATIN1,
            Collation::LATIN2_GENERAL_CI => CharacterSet::LATIN2,
            Collation::SWE7_SWEDISH_CI => CharacterSet::SWE7,
            Collation::ASCII_GENERAL_CI => CharacterSet::ASCII,
            Collation::UJIS_JAPANESE_CI => CharacterSet::UJIS,
            Collation::SJIS_JAPANESE_CI => CharacterSet::SJIS,
            Collation::HEBREW_GENERAL_CI => CharacterSet::HEBREW,
            Collation::TIS620_THAI_CI => CharacterSet::TIS620,
            Collation::EUCKR_KOREAN_CI => CharacterSet::EUCKR,
            Collation::KOI8U_GENERAL_CI => CharacterSet::KOI8U,
            Collation::GB2312_CHINESE_CI => CharacterSet::GB2312,
            Collation::GREEK_GENERAL_CI => CharacterSet::GREEK,
            Collation::CP1250_GENERAL_CI => CharacterSet::CP1250,
            Collation::GBK_CHINESE_CI => CharacterSet::GBK,
            Collation::LATIN5_TURKISH_CI => CharacterSet::LATIN5,
            Collation::ARMSCII8_GENERAL_CI => CharacterSet::ARMSCII8,
            Collation::UTF8_GENERAL_CI => CharacterSet::UTF8,
            Collation::UCS2_GENERAL_CI => CharacterSet::UCS2,
            Collation::CP866_GENERAL_CI => CharacterSet::CP866,
            Collation::KEYBCS2_GENERAL_CI => CharacterSet::KEYBCS2,
            Collation::MACCE_GENERAL_CI => CharacterSet::MACCE,
            Collation::MACROMAN_GENERAL_CI => CharacterSet::MACROMAN,
            Collation::CP852_GENERAL_CI => CharacterSet::CP852,
            Collation::LATIN7_GENERAL_CI => CharacterSet::LATIN7,
            Collation::CP1251_GENERAL_CI => CharacterSet::CP1251,
            Collation::UTF16_GENERAL_CI => CharacterSet::UTF16,
            Collation::UTF16LE_GENERAL_CI => CharacterSet::UTF16LE,
            Collation::CP1256_GENERAL_CI => CharacterSet::CP1256,
            Collation::CP1257_GENERAL_CI => CharacterSet::CP1257,
            Collation::UTF32_GENERAL_CI => CharacterSet::UTF32,
            Collation::BINARY => CharacterSet::BINARY,
            Collation::GEOSTD8_GENERAL_CI => CharacterSet::GEOSTD8,
            Collation::CP932_JAPANESE_CI => CharacterSet::CP932,
            Collation::EUCJPMS_JAPANESE_CI => CharacterSet::EUCJPMS,
            Collation::GB18030_CHINESE_CI => CharacterSet::GB18030,
            Collation::UTF8MB4_0900_AI_CI => CharacterSet::UTF8MB4,
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Clone, Eq, PartialEq, Copy, Debug)]
#[repr(u8)]
pub enum Command {
    COM_SLEEP = 0x00_u8,
    COM_QUIT = 0x01_u8,
    COM_INIT_DB = 0x02_u8,
    COM_QUERY = 0x03_u8,
    COM_FIELD_LIST = 0x04_u8,
    COM_CREATE_DB = 0x05_u8,
    COM_DROP_DB = 0x06_u8,
    COM_REFRESH = 0x07_u8,
    COM_SHUTDOWN = 0x08_u8,
    COM_STATISTICS = 0x09_u8,
    COM_PROCESS_INFO = 0x0a_u8,
    COM_CONNECT = 0x0b_u8,
    COM_PROCESS_KILL = 0x0c_u8,
    COM_DEBUG = 0x0d_u8,
    COM_PING = 0x0e_u8,
    COM_TIME = 0x0f_u8,
    COM_DELAYED_INSERT = 0x10_u8,
    COM_CHANGE_USER = 0x11_u8,
    COM_BINLOG_DUMP = 0x12_u8,
    COM_TABLE_DUMP = 0x13_u8,
    COM_CONNECT_OUT = 0x14_u8,
    COM_REGISTER_SLAVE = 0x15_u8,
    COM_STMT_PREPARE = 0x16_u8,
    COM_STMT_EXECUTE = 0x17_u8,
    COM_STMT_SEND_LONG_DATA = 0x18_u8,
    COM_STMT_CLOSE = 0x19_u8,
    COM_STMT_RESET = 0x1a_u8,
    COM_SET_OPTION = 0x1b_u8,
    COM_STMT_FETCH = 0x1c_u8,
    COM_DAEMON = 0x1d_u8,
    COM_BINLOG_DUMP_GTID = 0x1e_u8,
    COM_RESET_CONNECTION = 0x1f_u8,
}

/// Type of MySql column field
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum ColumnType {
    MYSQL_TYPE_DECIMAL = 0,
    MYSQL_TYPE_TINY,
    MYSQL_TYPE_SHORT,
    MYSQL_TYPE_LONG,
    MYSQL_TYPE_FLOAT,
    MYSQL_TYPE_DOUBLE,
    MYSQL_TYPE_NULL,
    MYSQL_TYPE_TIMESTAMP,
    MYSQL_TYPE_LONGLONG,
    MYSQL_TYPE_INT24,
    MYSQL_TYPE_DATE,
    MYSQL_TYPE_TIME,
    MYSQL_TYPE_DATETIME,
    MYSQL_TYPE_YEAR,
    MYSQL_TYPE_NEWDATE,
    MYSQL_TYPE_VARCHAR,
    MYSQL_TYPE_BIT,
    MYSQL_TYPE_TIMESTAMP2,
    MYSQL_TYPE_DATETIME2,
    MYSQL_TYPE_TIME2,
    MYSQL_TYPE_JSON = 245,
    MYSQL_TYPE_NEWDECIMAL = 246,
    MYSQL_TYPE_ENUM = 247,
    MYSQL_TYPE_SET = 248,
    MYSQL_TYPE_TINY_BLOB = 249,
    MYSQL_TYPE_MEDIUM_BLOB = 250,
    MYSQL_TYPE_LONG_BLOB = 251,
    MYSQL_TYPE_BLOB = 252,
    MYSQL_TYPE_VAR_STRING = 253,
    MYSQL_TYPE_STRING = 254,
    MYSQL_TYPE_GEOMETRY = 255,
}

impl From<u8> for ColumnType {
    fn from(x: u8) -> ColumnType {
        match x {
            0x00_u8 => ColumnType::MYSQL_TYPE_DECIMAL,
            0x01_u8 => ColumnType::MYSQL_TYPE_TINY,
            0x02_u8 => ColumnType::MYSQL_TYPE_SHORT,
            0x03_u8 => ColumnType::MYSQL_TYPE_LONG,
            0x04_u8 => ColumnType::MYSQL_TYPE_FLOAT,
            0x05_u8 => ColumnType::MYSQL_TYPE_DOUBLE,
            0x06_u8 => ColumnType::MYSQL_TYPE_NULL,
            0x07_u8 => ColumnType::MYSQL_TYPE_TIMESTAMP,
            0x08_u8 => ColumnType::MYSQL_TYPE_LONGLONG,
            0x09_u8 => ColumnType::MYSQL_TYPE_INT24,
            0x0a_u8 => ColumnType::MYSQL_TYPE_DATE,
            0x0b_u8 => ColumnType::MYSQL_TYPE_TIME,
            0x0c_u8 => ColumnType::MYSQL_TYPE_DATETIME,
            0x0d_u8 => ColumnType::MYSQL_TYPE_YEAR,
            0x0f_u8 => ColumnType::MYSQL_TYPE_VARCHAR,
            0x10_u8 => ColumnType::MYSQL_TYPE_BIT,
            0x11_u8 => ColumnType::MYSQL_TYPE_TIMESTAMP2,
            0x12_u8 => ColumnType::MYSQL_TYPE_DATETIME2,
            0x13_u8 => ColumnType::MYSQL_TYPE_TIME2,
            0xf5_u8 => ColumnType::MYSQL_TYPE_JSON,
            0xf6_u8 => ColumnType::MYSQL_TYPE_NEWDECIMAL,
            0xf7_u8 => ColumnType::MYSQL_TYPE_ENUM,
            0xf8_u8 => ColumnType::MYSQL_TYPE_SET,
            0xf9_u8 => ColumnType::MYSQL_TYPE_TINY_BLOB,
            0xfa_u8 => ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
            0xfb_u8 => ColumnType::MYSQL_TYPE_LONG_BLOB,
            0xfc_u8 => ColumnType::MYSQL_TYPE_BLOB,
            0xfd_u8 => ColumnType::MYSQL_TYPE_VAR_STRING,
            0xfe_u8 => ColumnType::MYSQL_TYPE_STRING,
            0xff_u8 => ColumnType::MYSQL_TYPE_GEOMETRY,
            _ => panic!("Unknown column type {}", x),
        }
    }
}

#[derive(Debug)]
pub struct Handshake {
    capabilities: CapabilityFlags,
    protocol_version: u8,
    scramble_1: Vec<u8>,
    scramble_2: Option<Vec<u8>>,
    auth_plugin_name: Option<String>,
    character_set: CharacterSet,
    status_flags: StatusFlags,
}

impl Handshake {
    pub(crate) fn parse(buffer: impl Into<Bytes>) -> io::Result<Self> {
        let mut b = buffer.into();
        let protocol_version = b.get_u8();
        let server_version = b.split_to(null_terminated_pos(b.chunk()));
        b.advance(1);
        let connection_id = b.get_u32_le();
        let scramble_1 = b.split_to(8).to_vec();
        b.advance(1);
        let capabilities_1 = b.get_u16_le();
        let character_set = b.get_u8().into();
        let status_flags = StatusFlags::from_bits_truncate(b.get_u16_le());
        let capabilities_2 = b.get_u16_le();
        let scramble_len = b.get_u8();
        b.advance(10);

        let capabilities = CapabilityFlags::from_bits_truncate(
            capabilities_1 as u32 | ((capabilities_2 as u32) << 16),
        );

        let mut scramble_2 = None;
        if capabilities.contains(CapabilityFlags::CLIENT_SECURE_CONNECTION) {
            scramble_2 = Some(
                b.split_to(max(12, scramble_len as i8 - 9) as usize)
                    .to_vec(),
            );
            b.advance(1);
        }

        let mut auth_plugin_name = None;
        if capabilities.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH) {
            auth_plugin_name = Some(b.mysql_null_terminated_string().unwrap());
        }

        Ok(Self {
            capabilities,
            protocol_version,
            scramble_1,
            scramble_2,
            auth_plugin_name,
            status_flags,
            character_set,
        })
    }

    pub fn status_flags(&self) -> StatusFlags {
        self.status_flags
    }

    pub fn character_set(&self) -> CharacterSet {
        self.character_set
    }

    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    pub fn capabilities(&self) -> CapabilityFlags {
        self.capabilities
    }

    pub fn nonce(&self) -> Vec<u8> {
        let mut out = self.scramble_1.clone();

        if let Some(ref scramble_2) = self.scramble_2 {
            out.extend_from_slice(scramble_2);
        }

        out
    }

    pub fn auth_plugin_name(&self) -> &str {
        self.auth_plugin_name
            .as_ref()
            .map(String::as_str)
            .unwrap_or("") // TODO: potentially have a saner default here...
    }
}

#[derive(Debug)]
pub struct Packet {
    sequence_id: u8,
    payload: Vec<u8>,
}

impl Packet {
    pub fn check(b: &mut impl Buf) -> bool {
        if b.remaining() < 4 {
            return false;
        }

        let payload_len = b.get_uint_le(3) as usize;
        b.advance(1);
        b.remaining() >= payload_len
    }

    pub fn parse(b: &mut impl Buf) -> io::Result<Self> {
        let payload_len = b.get_uint_le(3) as usize;
        let sequence_id = b.get_u8();

        let mut payload = vec![0; payload_len];
        b.copy_to_slice(payload.as_mut_slice());

        Ok(Self {
            sequence_id,
            payload,
        })
    }

    pub fn sequence_id(&self) -> u8 {
        self.sequence_id
    }

    pub fn take_payload(self) -> Vec<u8> {
        self.payload
    }
}

#[derive(Debug)]
pub struct Row(Vec<Value>);

impl Row {
    pub(crate) fn parse(buffer: impl Into<Bytes>, columns: &Vec<Column>) -> io::Result<Self> {
        let mut values = Vec::with_capacity(columns.len());
        let mut b = buffer.into();

        for i in 0..columns.len() {
            let value = parse_value_from_text(&mut b, &columns[i])?;
            values.push(value);
        }

        Ok(Self(values))
    }

    pub fn values(&self) -> &[Value] {
        self.0.as_slice()
    }
}

#[derive(Debug)]
pub struct Column {
    catalog: String,
    schema: String,
    table: String,
    name: String,
    org_table: String,
    character_set: CharacterSet,
    column_length: u32,
    column_type: ColumnType,
    flags: ColumnFlags,
    decimals: u8,
}

impl Column {
    pub(crate) fn parse(buffer: impl Into<Bytes>) -> io::Result<Self> {
        let mut b = buffer.into();
        let catalog = b.mysql_get_lenc_string().unwrap();
        assert_eq!("def", catalog.as_str());
        let schema = b.mysql_get_lenc_string().unwrap();
        let table = b.mysql_get_lenc_string().unwrap();
        let org_table = b.mysql_get_lenc_string().unwrap();
        let name = b.mysql_get_lenc_string().unwrap();
        let org_name = b.mysql_get_lenc_string().unwrap();
        let fixed_len = b.mysql_get_lenc_uint().unwrap();
        assert_eq!(0x0C, fixed_len);
        let character_set = (b.get_u16_le() as u8).into();
        let column_length = b.get_u32_le();
        let column_type = b.get_u8().into();
        let flags = ColumnFlags::from_bits_truncate(b.get_u16_le());
        let decimals = b.get_u8();

        Ok(Self {
            catalog,
            schema,
            table,
            name,
            org_table,
            character_set,
            column_length,
            column_type,
            flags,
            decimals,
        })
    }

    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }

    pub fn flags(&self) -> ColumnFlags {
        self.flags
    }
}

// https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
#[derive(Debug)]
pub struct ServerError {
    error_code: u16,
    state_marker: Option<String>,
    state: Option<String>,
    error_message: String,
}

impl ServerError {
    pub(crate) fn parse(
        buffer: impl Into<Bytes>,
        capability_flags: CapabilityFlags,
    ) -> io::Result<Self> {
        let mut b = buffer.into();
        let _header = b.get_u8();
        let error_code = b.get_u16_le();

        let mut state_marker = None;
        let mut state = None;

        if capability_flags.contains(CapabilityFlags::CLIENT_PROTOCOL_41) {
            let state_marker = Some(b.mysql_get_fixed_length_string(1).unwrap());
            let state = Some(b.mysql_get_fixed_length_string(5).unwrap());
        }

        let error_message = b.mysql_get_eof_string().unwrap();
        Ok(Self {
            error_code,
            state_marker,
            state,
            error_message,
        })
    }
}

// https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
#[derive(Debug)]
pub struct ServerOk {
    affected_rows: u64,
    last_inserted_id: u64,
    status_flags: Option<StatusFlags>,
    warnings: Option<u16>,
    info: String,
    session_state_changes: Option<String>,
}

impl ServerOk {
    pub(crate) fn parse(
        buffer: impl Into<Bytes>,
        capability_flags: CapabilityFlags,
    ) -> io::Result<Self> {
        let mut b = buffer.into();
        let _header = b.get_u8();
        let affected_rows = b.mysql_get_lenc_uint().unwrap();
        let last_inserted_id = b.mysql_get_lenc_uint().unwrap();

        let mut status_flags = None;
        let mut warnings = None;
        if capability_flags.contains(CapabilityFlags::CLIENT_PROTOCOL_41) {
            status_flags = Some(StatusFlags::from_bits_truncate(b.get_u16_le()));
            warnings = Some(b.get_u16_le());
        } else if capability_flags.contains(CapabilityFlags::CLIENT_TRANSACTIONS) {
            status_flags = Some(StatusFlags::from_bits_truncate(b.get_u16_le()));
        }

        let (info, session_state_changes) =
            if capability_flags.contains(CapabilityFlags::CLIENT_SESSION_TRACK) {
                let info = b.mysql_get_lenc_string().unwrap();

                let has_session_state_changes = status_flags
                    .map(|f| f.contains(StatusFlags::SERVER_SESSION_STATE_CHANGED))
                    .unwrap_or(false);

                let mut session_state_changes = None;
                if has_session_state_changes {
                    session_state_changes = Some(b.mysql_get_lenc_string().unwrap())
                }

                (info, session_state_changes)
            } else {
                let info = b.mysql_get_eof_string().unwrap();
                (info, None)
            };

        Ok(Self {
            affected_rows,
            last_inserted_id,
            status_flags,
            warnings,
            info,
            session_state_changes,
        })
    }

    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }
    pub fn last_inserted_id(&self) -> u64 {
        self.last_inserted_id
    }
    pub fn status_flags(&self) -> Option<StatusFlags> {
        self.status_flags
    }
    pub fn warnings(&self) -> Option<u16> {
        self.warnings
    }
}

fn parse_value_from_text(b: &mut impl Buf, column: &Column) -> io::Result<Value> {
    // TODO: I HAVE NO IDEA HOW TO HANDLE THIS CLEANLY JUST YET...
    // IF MYSQL ALWAYS RETURNS THE VALUES INTO THE CLIENT FORMATTED COLLATION, THEN WE CAN LAZILY CONVERT IT TO UTF8 AND SUPPORT METHODS TO TRANSCODE FROM ONE FORMAT TO THE OTHER
    // OTHERWISE, WE HAVE TO DO THE CONVERSION OURSELVES BASED ON THE COLUMN COLLATION.
    //
    // ALWAYS ASSUME UTF-8, but in theory, the client could have a completely different charset
    // We should convert it into a value based off of the column's charset, back into our client charset, and so on.
    if let Some(0xFB) = b.peek_u8() {
        Ok(Value::Null)
    } else {
        let bytes = b.mysql_get_lenc_bytes().unwrap();
        Ok(Value::Bytes(bytes))
    }
}

pub fn null_terminated_pos(b: &[u8]) -> usize {
    b.iter().position(|b| *b == 0x00).unwrap_or(b.len())
}
