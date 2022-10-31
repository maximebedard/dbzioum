# ps2bq

CDC for postgres with BQ sink. Uses wal2json codec to decode replication events from postgres.

# TODOS:

- [] simple postgresql client
  - [x] simple query support
  - [x] create/exists/delete replication slot
  - [ ] start replication stream
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
- [] streamer task
- [] sink task
- [] http api task
- [] ps2bq binary
  - [] standard enviroment variables for postgres client (https://www.postgresql.org/docs/current/libpq-envars.html)

# testing

```
docker-compose up
cargo test
```
