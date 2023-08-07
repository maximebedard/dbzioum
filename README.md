# dbzioum

Event log streamer for postgresql@11 and mysql@8, heavily inspired from [debezium](https://debezium.io/). Work in progress.

# Features

- [ ] pg: simple postgresql client (>= v11)
  - [x] auth (cleartext password, md5, scram-sha-256, ssl)
  - [x] timeouts (connect, read, write)
  - [x] simple query support
  - [x] query cancellation support
  - [x] create/exists/delete replication slot
  - [ ] wal streaming
    - [x] read wal2json v2 events
    - [x] commit cursor position
    - [ ] read wal events (without wal2json)
    - [ ] timeline support
- [ ] pg2kafka
  - [ ] bridge pg events to row events
- [ ] mysql: simple mysql client (>= v8)
  - [ ] auth
    - [x] mysql_native_password
    - [ ] caching_sha2_password (see https://dev.mysql.com/doc/refman/8.0/en/upgrading-from-previous-series.html#upgrade-caching-sha2-password)
    - [ ] ssl
  - [x] simple query support
  - [ ] query cancellation support
  - [x] switch connection to replica
  - [ ] binlog streaming
    - [x] supports row based replication events
      - [x] support INSERT/UPDATE/DELETE events
      - [ ] parse mysql row event values
        - [x] Integers (integer, int, smallint, tinyint, mediumint, bigint)
        - [ ] _partial_ Fixed point (decimal, numeric) (needs custom parser)
        - [x] Floating point (float, double)
        - [x] Bit
        - [x] Strings/Bytes (~CHAR~, ~VARCHAR~, ~BINARY~, ~VARBINARY~, ~BLOB~, ~TEXT~)
        - [ ] ENUM
        - [ ] SET
        - [x] _partial_ Date and Time
        - [ ] _partial_ JSON (needs custom parser)
    - [x] commit cursor position
- [ ] mysql2kafka
  - [ ] bridge mysql events to row events
- [ ] sink:
  - [x] standardized row events
  - [ ] filter row events by table

# testing

```
docker-compose up
cargo test
```

# Special configs & implementation notes

- pg
  - `wal_level=logical`
- mysql ([ref gcp](https://cloud.google.com/datastream/docs/configure-your-source-mysql-database))

  - `--default-authentication-plugin=mysql_native_password`
  - `--binlog-format=ROW` (default)
  - `--binlog-row-image=FULL` (default)
  - `--binlog-checksum=NONE` (TODO: remove this)
  - `--binlog-row-metadata=FULL`
  - `GRANT REPLICATION SLAVE, SELECT, REPLICATION CLIENT ON *.* TO 'mysql'@'%';`

- Only support UTF8
- MYSQL shenenigans
  - Custom checksum size
  - JSONB parser
  - Decimal parser
- Parsing MYSQL binlog events should probably be lazy.
- https://www.postgresql.org/docs/current/libpq-envars.html
- https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html#Table_table_map_event_optional_metadata
- https://github.com/mysql/mysql-server/blob/8.0/libbinlogevents/src/rows_event.cpp
- https://www.postgresql.org/docs/current/protocol.html
