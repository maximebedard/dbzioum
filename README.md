# ps2bq

CDC for postgres with BQ sink. Uses wal2json codec to decode replication events from postgres.

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
    - [ ] buffering
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
      - [ ] support TRUNCATE events (via QUERY_EVENT)
      - [ ] combine TableMapEvents + Insert/Update/Delete events in the streamer
    - [ ] commit cursor position
    - [ ] buffering
- [ ] gcp clients
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
- [] pg streamer task
- [] mysql streamer task
- [] sink task
- [] http api task
- [] ps2bq binary
  - [] standard enviroment variables for postgres client (https://www.postgresql.org/docs/current/libpq-envars.html)

# testing

```
docker-compose up
cargo test
```
