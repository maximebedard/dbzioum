use bitflags::bitflags;

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
    // https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html
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
      const CLIENT_RESERVED2    = 0x00008000;
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

impl TryFrom<u8> for CharacterSet {
    type Error = u8;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0x01_u8 => Ok(CharacterSet::BIG5),
            0x03_u8 => Ok(CharacterSet::DEC8),
            0x04_u8 => Ok(CharacterSet::CP850),
            0x06_u8 => Ok(CharacterSet::HP8),
            0x07_u8 => Ok(CharacterSet::KOI8R),
            0x08_u8 => Ok(CharacterSet::LATIN1),
            0x09_u8 => Ok(CharacterSet::LATIN2),
            0x0A_u8 => Ok(CharacterSet::SWE7),
            0x0B_u8 => Ok(CharacterSet::ASCII),
            0x0C_u8 => Ok(CharacterSet::UJIS),
            0x0D_u8 => Ok(CharacterSet::SJIS),
            0x10_u8 => Ok(CharacterSet::HEBREW),
            0x12_u8 => Ok(CharacterSet::TIS620),
            0x13_u8 => Ok(CharacterSet::EUCKR),
            0x16_u8 => Ok(CharacterSet::KOI8U),
            0x18_u8 => Ok(CharacterSet::GB2312),
            0x19_u8 => Ok(CharacterSet::GREEK),
            0x1A_u8 => Ok(CharacterSet::CP1250),
            0x1C_u8 => Ok(CharacterSet::GBK),
            0x1E_u8 => Ok(CharacterSet::LATIN5),
            0x20_u8 => Ok(CharacterSet::ARMSCII8),
            0x21_u8 => Ok(CharacterSet::UTF8),
            0x23_u8 => Ok(CharacterSet::UCS2),
            0x24_u8 => Ok(CharacterSet::CP866),
            0x25_u8 => Ok(CharacterSet::KEYBCS2),
            0x26_u8 => Ok(CharacterSet::MACCE),
            0x27_u8 => Ok(CharacterSet::MACROMAN),
            0x28_u8 => Ok(CharacterSet::CP852),
            0x29_u8 => Ok(CharacterSet::LATIN7),
            0x53_u8 => Ok(CharacterSet::CP1251),
            0x36_u8 => Ok(CharacterSet::UTF16),
            0x38_u8 => Ok(CharacterSet::UTF16LE),
            0x39_u8 => Ok(CharacterSet::CP1256),
            0x3B_u8 => Ok(CharacterSet::CP1257),
            0x3C_u8 => Ok(CharacterSet::UTF32),
            0x3F_u8 => Ok(CharacterSet::BINARY),
            0x5C_u8 => Ok(CharacterSet::GEOSTD8),
            0x5F_u8 => Ok(CharacterSet::CP932),
            0x61_u8 => Ok(CharacterSet::EUCJPMS),
            0xF8_u8 => Ok(CharacterSet::GB18030),
            0xFF_u8 => Ok(CharacterSet::UTF8MB4),
            unsupported => Err(unsupported),
        }
    }
}

impl TryFrom<u8> for Collation {
    type Error = u8;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0x01_u8 => Ok(Collation::BIG5_CHINESE_CI),
            0x03_u8 => Ok(Collation::DEC8_SWEDISH_CI),
            0x04_u8 => Ok(Collation::CP850_GENERAL_CI),
            0x06_u8 => Ok(Collation::HP8_ENGLISH_CI),
            0x07_u8 => Ok(Collation::KOI8R_GENERAL_CI),
            0x08_u8 => Ok(Collation::LATIN1_SWEDISH_CI),
            0x09_u8 => Ok(Collation::LATIN2_GENERAL_CI),
            0x0A_u8 => Ok(Collation::SWE7_SWEDISH_CI),
            0x0B_u8 => Ok(Collation::ASCII_GENERAL_CI),
            0x0C_u8 => Ok(Collation::UJIS_JAPANESE_CI),
            0x0D_u8 => Ok(Collation::SJIS_JAPANESE_CI),
            0x10_u8 => Ok(Collation::HEBREW_GENERAL_CI),
            0x12_u8 => Ok(Collation::TIS620_THAI_CI),
            0x13_u8 => Ok(Collation::EUCKR_KOREAN_CI),
            0x16_u8 => Ok(Collation::KOI8U_GENERAL_CI),
            0x18_u8 => Ok(Collation::GB2312_CHINESE_CI),
            0x19_u8 => Ok(Collation::GREEK_GENERAL_CI),
            0x1A_u8 => Ok(Collation::CP1250_GENERAL_CI),
            0x1C_u8 => Ok(Collation::GBK_CHINESE_CI),
            0x1E_u8 => Ok(Collation::LATIN5_TURKISH_CI),
            0x20_u8 => Ok(Collation::ARMSCII8_GENERAL_CI),
            0x21_u8 => Ok(Collation::UTF8_GENERAL_CI),
            0x23_u8 => Ok(Collation::UCS2_GENERAL_CI),
            0x24_u8 => Ok(Collation::CP866_GENERAL_CI),
            0x25_u8 => Ok(Collation::KEYBCS2_GENERAL_CI),
            0x26_u8 => Ok(Collation::MACCE_GENERAL_CI),
            0x27_u8 => Ok(Collation::MACROMAN_GENERAL_CI),
            0x28_u8 => Ok(Collation::CP852_GENERAL_CI),
            0x29_u8 => Ok(Collation::LATIN7_GENERAL_CI),
            0x53_u8 => Ok(Collation::CP1251_GENERAL_CI),
            0x36_u8 => Ok(Collation::UTF16_GENERAL_CI),
            0x38_u8 => Ok(Collation::UTF16LE_GENERAL_CI),
            0x39_u8 => Ok(Collation::CP1256_GENERAL_CI),
            0x3B_u8 => Ok(Collation::CP1257_GENERAL_CI),
            0x3C_u8 => Ok(Collation::UTF32_GENERAL_CI),
            0x3F_u8 => Ok(Collation::BINARY),
            0x5C_u8 => Ok(Collation::GEOSTD8_GENERAL_CI),
            0x5F_u8 => Ok(Collation::CP932_JAPANESE_CI),
            0x61_u8 => Ok(Collation::EUCJPMS_JAPANESE_CI),
            0xF8_u8 => Ok(Collation::GB18030_CHINESE_CI),
            0xFF_u8 => Ok(Collation::UTF8MB4_0900_AI_CI),
            unsupported => Err(unsupported),
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
    MYSQL_TYPE_DECIMAL = 0x00_u8,
    MYSQL_TYPE_TINY = 0x01_u8,
    MYSQL_TYPE_SHORT = 0x02_u8,
    MYSQL_TYPE_LONG = 0x03_u8,
    MYSQL_TYPE_FLOAT = 0x04_u8,
    MYSQL_TYPE_DOUBLE = 0x05_u8,
    MYSQL_TYPE_NULL = 0x06_u8,
    MYSQL_TYPE_TIMESTAMP = 0x07_u8,
    MYSQL_TYPE_LONGLONG = 0x08_u8,
    MYSQL_TYPE_INT24 = 0x09_u8,
    MYSQL_TYPE_DATE = 0x0a_u8,
    MYSQL_TYPE_TIME = 0x0b_u8,
    MYSQL_TYPE_DATETIME = 0x0c_u8,
    MYSQL_TYPE_YEAR = 0x0d_u8,
    MYSQL_TYPE_VARCHAR = 0x0f_u8,
    MYSQL_TYPE_BIT = 0x10_u8,
    MYSQL_TYPE_TIMESTAMP2 = 0x11_u8,
    MYSQL_TYPE_DATETIME2 = 0x12_u8,
    MYSQL_TYPE_TIME2 = 0x13_u8,
    MYSQL_TYPE_JSON = 0xf5_u8,
    MYSQL_TYPE_NEWDECIMAL = 0xf6_u8,
    MYSQL_TYPE_ENUM = 0xf7_u8,
    MYSQL_TYPE_SET = 0xf8_u8,
    MYSQL_TYPE_TINY_BLOB = 0xf9_u8,
    MYSQL_TYPE_MEDIUM_BLOB = 0xfa_u8,
    MYSQL_TYPE_LONG_BLOB = 0xfb_u8,
    MYSQL_TYPE_BLOB = 0xfc_u8,
    MYSQL_TYPE_VAR_STRING = 0xfd_u8,
    MYSQL_TYPE_STRING = 0xfe_u8,
    MYSQL_TYPE_GEOMETRY = 0xff_u8,
}

impl TryFrom<u8> for ColumnType {
    type Error = u8;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0x00_u8 => Ok(ColumnType::MYSQL_TYPE_DECIMAL),
            0x01_u8 => Ok(ColumnType::MYSQL_TYPE_TINY),
            0x02_u8 => Ok(ColumnType::MYSQL_TYPE_SHORT),
            0x03_u8 => Ok(ColumnType::MYSQL_TYPE_LONG),
            0x04_u8 => Ok(ColumnType::MYSQL_TYPE_FLOAT),
            0x05_u8 => Ok(ColumnType::MYSQL_TYPE_DOUBLE),
            0x06_u8 => Ok(ColumnType::MYSQL_TYPE_NULL),
            0x07_u8 => Ok(ColumnType::MYSQL_TYPE_TIMESTAMP),
            0x08_u8 => Ok(ColumnType::MYSQL_TYPE_LONGLONG),
            0x09_u8 => Ok(ColumnType::MYSQL_TYPE_INT24),
            0x0a_u8 => Ok(ColumnType::MYSQL_TYPE_DATE),
            0x0b_u8 => Ok(ColumnType::MYSQL_TYPE_TIME),
            0x0c_u8 => Ok(ColumnType::MYSQL_TYPE_DATETIME),
            0x0d_u8 => Ok(ColumnType::MYSQL_TYPE_YEAR),
            0x0f_u8 => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
            0x10_u8 => Ok(ColumnType::MYSQL_TYPE_BIT),
            0x11_u8 => Ok(ColumnType::MYSQL_TYPE_TIMESTAMP2),
            0x12_u8 => Ok(ColumnType::MYSQL_TYPE_DATETIME2),
            0x13_u8 => Ok(ColumnType::MYSQL_TYPE_TIME2),
            0xf5_u8 => Ok(ColumnType::MYSQL_TYPE_JSON),
            0xf6_u8 => Ok(ColumnType::MYSQL_TYPE_NEWDECIMAL),
            0xf7_u8 => Ok(ColumnType::MYSQL_TYPE_ENUM),
            0xf8_u8 => Ok(ColumnType::MYSQL_TYPE_SET),
            0xf9_u8 => Ok(ColumnType::MYSQL_TYPE_TINY_BLOB),
            0xfa_u8 => Ok(ColumnType::MYSQL_TYPE_MEDIUM_BLOB),
            0xfb_u8 => Ok(ColumnType::MYSQL_TYPE_LONG_BLOB),
            0xfc_u8 => Ok(ColumnType::MYSQL_TYPE_BLOB),
            0xfd_u8 => Ok(ColumnType::MYSQL_TYPE_VAR_STRING),
            0xfe_u8 => Ok(ColumnType::MYSQL_TYPE_STRING),
            0xff_u8 => Ok(ColumnType::MYSQL_TYPE_GEOMETRY),
            unsupported => Err(unsupported),
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum BinlogEventType {
    UNKNOWN_EVENT,
    START_EVENT_V3,
    QUERY_EVENT,
    STOP_EVENT,
    ROTATE_EVENT,
    INTVAR_EVENT,
    LOAD_EVENT,
    SLAVE_EVENT,
    CREATE_FILE_EVENT,
    APPEND_BLOCK_EVENT,
    EXEC_LOAD_EVENT,
    DELETE_FILE_EVENT,
    NEW_LOAD_EVENT,
    RAND_EVENT,
    USER_VAR_EVENT,
    FORMAT_DESCRIPTION_EVENT,
    XID_EVENT,
    BEGIN_LOAD_QUERY_EVENT,
    EXECUTE_LOAD_QUERY_EVENT,
    TABLE_MAP_EVENT,
    WRITE_ROWS_EVENTV0,
    UPDATE_ROWS_EVENTV0,
    DELETE_ROWS_EVENTV0,
    WRITE_ROWS_EVENTV1,
    UPDATE_ROWS_EVENTV1,
    DELETE_ROWS_EVENTV1,
    INCIDENT_EVENT,
    HEARTBEAT_EVENT,
    IGNORABLE_EVENT,
    ROWS_QUERY_EVENT,
    WRITE_ROWS_EVENTV2,
    UPDATE_ROWS_EVENTV2,
    DELETE_ROWS_EVENTV2,
    GTID_EVENT,
    ANONYMOUS_GTID_EVENT,
    PREVIOUS_GTIDS_EVENT,
}

impl TryFrom<u8> for BinlogEventType {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0x00_u8 => Ok(BinlogEventType::UNKNOWN_EVENT),
            0x01_u8 => Ok(BinlogEventType::START_EVENT_V3),
            0x02_u8 => Ok(BinlogEventType::QUERY_EVENT),
            0x03_u8 => Ok(BinlogEventType::STOP_EVENT),
            0x04_u8 => Ok(BinlogEventType::ROTATE_EVENT),
            0x05_u8 => Ok(BinlogEventType::INTVAR_EVENT),
            0x06_u8 => Ok(BinlogEventType::LOAD_EVENT),
            0x07_u8 => Ok(BinlogEventType::SLAVE_EVENT),
            0x08_u8 => Ok(BinlogEventType::CREATE_FILE_EVENT),
            0x09_u8 => Ok(BinlogEventType::APPEND_BLOCK_EVENT),
            0x0a_u8 => Ok(BinlogEventType::EXEC_LOAD_EVENT),
            0x0b_u8 => Ok(BinlogEventType::DELETE_FILE_EVENT),
            0x0c_u8 => Ok(BinlogEventType::NEW_LOAD_EVENT),
            0x0d_u8 => Ok(BinlogEventType::RAND_EVENT),
            0x0e_u8 => Ok(BinlogEventType::USER_VAR_EVENT),
            0x0f_u8 => Ok(BinlogEventType::FORMAT_DESCRIPTION_EVENT),
            0x10_u8 => Ok(BinlogEventType::XID_EVENT),
            0x11_u8 => Ok(BinlogEventType::BEGIN_LOAD_QUERY_EVENT),
            0x12_u8 => Ok(BinlogEventType::EXECUTE_LOAD_QUERY_EVENT),
            0x13_u8 => Ok(BinlogEventType::TABLE_MAP_EVENT),
            0x14_u8 => Ok(BinlogEventType::WRITE_ROWS_EVENTV0),
            0x15_u8 => Ok(BinlogEventType::UPDATE_ROWS_EVENTV0),
            0x16_u8 => Ok(BinlogEventType::DELETE_ROWS_EVENTV0),
            0x17_u8 => Ok(BinlogEventType::WRITE_ROWS_EVENTV1),
            0x18_u8 => Ok(BinlogEventType::UPDATE_ROWS_EVENTV1),
            0x19_u8 => Ok(BinlogEventType::DELETE_ROWS_EVENTV1),
            0x1a_u8 => Ok(BinlogEventType::INCIDENT_EVENT),
            0x1b_u8 => Ok(BinlogEventType::HEARTBEAT_EVENT),
            0x1c_u8 => Ok(BinlogEventType::IGNORABLE_EVENT),
            0x1d_u8 => Ok(BinlogEventType::ROWS_QUERY_EVENT),
            0x1e_u8 => Ok(BinlogEventType::WRITE_ROWS_EVENTV2),
            0x1f_u8 => Ok(BinlogEventType::UPDATE_ROWS_EVENTV2),
            0x20_u8 => Ok(BinlogEventType::DELETE_ROWS_EVENTV2),
            0x21_u8 => Ok(BinlogEventType::GTID_EVENT),
            0x22_u8 => Ok(BinlogEventType::ANONYMOUS_GTID_EVENT),
            0x23_u8 => Ok(BinlogEventType::PREVIOUS_GTIDS_EVENT),
            unsupported => Err(unsupported),
        }
    }
}
