#![allow(dead_code)]
#![allow(unused_variables)]

mod connection;

use connection::{Connection, ConnectionOptions};

#[tokio::main]
async fn main() {
    let mut connection = Connection::connect(
        "[::]:5432".parse().unwrap(),
        ConnectionOptions {
            user: "maximebedard".to_string(),
            database: Some("zapper".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    connection.identify_system().await.unwrap();
    connection.show("asds").await.unwrap();
    connection.close().await.unwrap();
}

enum Column {
    SmallInt,
    Integer,
    BigInt,
    Decimal,
    Numeric(u32, u32),
    Real,
    DoublePrecision,
    SmallSerial,
    Serial,
    BigSerial,
    Money,
    Varchar(u32),
    Char(u32),
    Text,
    Bytea,
    Timestamp(u32),
    TimestampWithTimezone(u32),
    Date,
    Time(u32),
    TimeWithTimezone(u32),
    Interval,
    Boolean,
    Jsonb,
}

struct Schema {
    columns: Vec<Column>,
}

struct BigQueryDestination {}

impl BigQueryDestination {
    async fn create_table(&mut self, schema: &Schema) {}
}
