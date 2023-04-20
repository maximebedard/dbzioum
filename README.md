# dbzioum

Work in progress project that aims to be similar to [debezium](https://debezium.io/). It currently supports postgresql@11 and mysql@8.

# TODOS:

- [ ] simple postgresql client (> v11)
  - [ ] auth
    - [x] password
    - [ ] ~md5~ (deprecated)
    - [ ] scram-sha-256
    - [ ] ssl
  - [x] simple query support
  - [x] create/exists/delete replication slot
  - [ ] create replication stream using replication slot
    - [x] read wal2json v2 events
    - [x] commit cursor position
    - [ ] parse row event values based on their column type
    - [x] support timelines
- [ ] simple mysql client (> v8)
  - [ ] auth
    - [x] mysql_native_password
    - [ ] caching_sha2_password (see https://dev.mysql.com/doc/refman/8.0/en/upgrading-from-previous-series.html#upgrade-caching-sha2-password)
    - [ ] ssl
  - [x] simple query support
  - [x] switch connection to replica
  - [ ] stream binary log events
    - [x] supports row based replication events
      - [x] support INSERT/UPDATE/DELETE events
      - [ ] parse row event values based on their column type
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
- [ ] gcp clients (no longer needed)
  - [x] auth
    - [x] service account
    - [x] user account
    - [x] metadata server
    - [ ] tests
  - [ ] gcs
    - [ ] create/exists/delete file
    - [ ] tests
  - [ ] bq
    - [ ] load job
    - [ ] ...
    - [ ] tests
- [x] bridge pg/mysql schema to standardized schema
- [x] stream values and write them to sink
- [ ] figure out ergonomic way to manage cursors and how events are flushed out

# testing

```
docker-compose up
cargo test
```

# Special configs

- pg
  - `wal_level=logical`
- mysql ([ref gcp](https://cloud.google.com/datastream/docs/configure-your-source-mysql-database))
  - `--default-authentication-plugin=mysql_native_password`
  - `--binlog-format=ROW` (default)
  - `--binlog-row-image=FULL` (default)
  - `--binlog-checksum=NONE` (TODO: remove this)
  - `--binlog-row-metadata=FULL`
  - `GRANT REPLICATION SLAVE, SELECT, REPLICATION CLIENT ON *.* TO 'mysql'@'%';`

# Notes

- Only support UTF8
- MYSQL shenenigans
  - Custom checksum size
  - JSONB parser
  - Decimal parser
- https://www.postgresql.org/docs/current/libpq-envars.html
- https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html#Table_table_map_event_optional_metadata
- https://github.com/mysql/mysql-server/blob/8.0/libbinlogevents/src/rows_event.cpp
