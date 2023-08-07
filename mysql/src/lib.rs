#![allow(dead_code)]
pub mod binlog;
mod buf_ext;
mod conn;
mod constants;
mod debug;
mod query;
mod scramble;
mod stream;

pub use conn::{BinlogCursor, BinlogStream, Connection, ConnectionOptions};

#[cfg(feature = "ssl")]
pub use openssl;
