# ps2bq

CDC for postgres with BQ sink. Uses wal2json codec to decode replication events from postgres.

# TODOS:

- [ ] simple postgresql client (> v11)
  - [ ] auth
    - [x] password
    - [ ] md5
    - [ ] scram-sha-256
    - [ ] ssl
  - [x] simple query support
  - [x] create/exists/delete replication slot
  - [ ] create replication stream using replication slot
- [ ] simple mysql client (> v8)
  - [ ] auth
    - [x] mysql_native_password
    - [ ] caching_sha2_password (see https://dev.mysql.com/doc/refman/8.0/en/upgrading-from-previous-series.html#upgrade-caching-sha2-password)
    - [ ] ssl
  - [ ] simple query support
  - [ ] switch connection to replica
  - [ ] stream binary log events
    - [x] supports row based replication events
    - [ ] parse row event values
  - [ ] start replication stream from scratch
    - [ ] reload internal state (fetch tables metadata behind a read lock https://dev.mysql.com/doc/mysql-replication-excerpt/8.0/en/replication-howto-rawdata.html)
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
