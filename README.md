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
  - [ ] start replication stream
- [ ] simple mysql client (> v8)
  - [ ] auth
    - [ ] mysql_native_password
    - [x] caching_sha2_password
    - [ ] ssl
  - [ ] simple query support
  - [ ] switch connection to replica
  - [ ] stream binary log events
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
