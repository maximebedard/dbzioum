#!/bin/bash
set -e

# self signed cert used for testing purposes
# openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes -keyout server.key -out server.crt -subj "/CN=pg.bedard.dev"

cat > "$PGDATA/server.key" <<-PKEY
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDB5A5qsb83dTbw
YXRUTwaN1r5EYavr0Nc+RmWqrD6rihIu7GURQNOrAJnXq+9emd9A9giN7s3zsznt
/XkzlYw6l5aGlhrsWjWTzoVFwMAEbN51+8BsRTvEOxjuYnG+dU7ySTYpAPYTGAS/
FXCorq/+n+QR2wk7hgLFWZx2nkuuenZ2laMP5/tLNbYGDrlyPRKMDQQBPdsOAFTt
oCNtwzvAoffHVG2y1GZ2hZy05xAh2Haxk+3JOS/q0QHHG38ug3LRIePtkhHxQ4rL
/hbNJXWtaaVQKY33xYVMZ0DJFLB3QZc2pha9kwNzN1IpCl70/BlYRjH1K9NdLBp9
qWvhIfpPAgMBAAECggEAOTnlY2pI5MZsy7AH1KZqacy65ZXqVzSRgujmMuSZrqmW
ylCtV9VJZAxOW9B8WvCRayvyxGl6UfZRH8QTVL1L4TNCk0CUe7P4YkKvJTJJukSj
uDDwz0Pz6uwCZMGbjroy7eHb4WhOOL57ECb01GSSv1VTzE1YwT/Ba5wSSRRZhApP
RxPmO14zbR9z23A7Ua1z9Dsmpxw53Ac506ullW467JVR1i71Jr/D54uevQS2Z0sS
hBXiL498QEIi4urF+BCMD87RlKLrkEMZkdkbrk33ZOoA/PjvDddOAIHJuiOzD30x
kM/HiuTD1k+2ifLVdtDLkQvKFQ8NuyDcaIYc7ml8iQKBgQDt1eOkGye5TcfsZrsH
yJ533I0zrGPVhk9md73hQH12XRQqILtngFsDRd6baMNP4h5khrEv9rZE96im9Zz1
7iDEerHpIomAxQ2NGPA2uAnYTUcoOVm3LD0tvfXEV8aIfnN2ZvrAx22Ccc9Gyo+X
QLKPiXDF5Giisu8LeXcdVu67CwKBgQDQsvhcbf0quA3Ia2EaoPDIpAzAl4RJhuH3
28kENe436Wg2BtSW5Y4JzGD9SsU/ytFhQaGHvTv3/U227nugQXNeTWZQKh5cscfT
u5GDtiqWVhIvSEIKnLQINAoX0dWBYinsexuaAXisu21M86grfFqNm8GKtLnjiysv
Q7Cjyo4oTQKBgBhvKgT5GTqraAe8giiJjuZHjWYqptMQCmY/lQ1oP7uCUokFddtr
T25lpjoXKEGzvGWdhOSllxI802Hbx/VoMoudA1wtRS0qkXrWfg9RFwnW3qGeSr5L
2Dkz0+UJE4eAXkJi0A4wLusA4Eeoldn3NqESgLiD/8//TBnEr90eykYDAoGBAMBK
9v4rNeaDmed90QX5HoKwbTOTKAebaV+4OfpQOsN3o1aMapryvjIXB5K6rw49MTkc
gNoSKUwxL8cK7AvX4pYUWN4qQLmF8SoNHGGwmoLUoYLBYGBozJT2ZgpWhBPnv9st
/1uiWW9GbspFg0E2HjV2OxkztkeLdmnhQ8NXIi7NAoGBAM1QfysRG2OpVplupk7S
alZaK8iXnvYAYA3r2YeiCaL6vTZ985sHhGiOpxUWoSvGuC5fcStmQHXFXZaJyAP9
eyvqz3KJ23MCuA4ez9JDXvFG9HKo9kf+139IonUGEHhdWStFuMmd5PLdNmSfKMjM
KgmkNXDvLoiX71LbYWQ5efl7
-----END PRIVATE KEY-----
PKEY
chmod 0600 "$PGDATA/server.key"

cat > "$PGDATA/server.crt" <<-CERT
-----BEGIN CERTIFICATE-----
MIICrDCCAZQCCQDoRZeVSN5d0DANBgkqhkiG9w0BAQsFADAYMRYwFAYDVQQDDA1w
Zy5iZWRhcmQuZGV2MB4XDTIzMDcyMDE0NTE1NloXDTMzMDcxNzE0NTE1NlowGDEW
MBQGA1UEAwwNcGcuYmVkYXJkLmRldjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBAMHkDmqxvzd1NvBhdFRPBo3WvkRhq+vQ1z5GZaqsPquKEi7sZRFA06sA
mder716Z30D2CI3uzfOzOe39eTOVjDqXloaWGuxaNZPOhUXAwARs3nX7wGxFO8Q7
GO5icb51TvJJNikA9hMYBL8VcKiur/6f5BHbCTuGAsVZnHaeS656dnaVow/n+0s1
tgYOuXI9EowNBAE92w4AVO2gI23DO8Ch98dUbbLUZnaFnLTnECHYdrGT7ck5L+rR
Accbfy6DctEh4+2SEfFDisv+Fs0lda1ppVApjffFhUxnQMkUsHdBlzamFr2TA3M3
UikKXvT8GVhGMfUr010sGn2pa+Eh+k8CAwEAATANBgkqhkiG9w0BAQsFAAOCAQEA
XGlcWoqDv2dZN+ivCNF1fgFQQDPRN5G9cW14cTFiJELPZFgSnuD0wbBfBa2qvEtx
KbS/608v8jw4imY7h3ubbECQtsc+n+fozpXy5pM64KbutzEVtTBAMN75x5nqY2PQ
T8nnTdq2SiY1q9w8s9EpUaQIsgSz8p1WBqrVKjrCRODUctOYM87l5JUsnvCVLjPZ
iLp9MD9tkU+LnN0V7vi4tgz99Bxq9RD1AtEz2K3SIMWBdy3T3FBRJJSY07pwnUxg
0laIm4Nofy4NvUCIno8AFUvt21N3jlWYci3rlZTGbE85KjEvWrfBUn0EGYYtS3BA
iNS6gGUvUj117R1gZ7KHBQ==
-----END CERTIFICATE-----
CERT

cat >> "$PGDATA/postgresql.conf" <<-CONF
port = 5432
password_encryption = 'scram-sha-256'
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
wal_level = 'logical'
CONF

cat > "$PGDATA/pg_hba.conf" <<-CONF
# TYPE  DATABASE        USER            ADDRESS              METHOD
host    all             pass_user       0.0.0.0/0            password
host    all             md5_user        0.0.0.0/0            md5
host    all             scram_user      0.0.0.0/0            scram-sha-256
host    all             pass_user       ::0/0                password
host    all             md5_user        ::0/0                md5
host    all             scram_user      ::0/0                scram-sha-256

hostssl all             ssl_user        0.0.0.0/0            trust
hostssl all             ssl_user        ::0/0                trust
host    all             ssl_user        0.0.0.0/0            reject
host    all             ssl_user        ::0/0                reject

# IPv4 local connections:
host    all             postgres        0.0.0.0/0            trust
# IPv6 local connections:
host    all             postgres        ::0/0                trust
# Unix socket connections:
local   all             postgres                             trust
CONF

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
  CREATE ROLE pass_user PASSWORD 'password' LOGIN;
  SET password_encryption to 'md5';
  CREATE ROLE md5_user PASSWORD 'password' LOGIN;
  SET password_encryption TO 'scram-sha-256';
  CREATE ROLE scram_user PASSWORD 'password' LOGIN;
  CREATE ROLE ssl_user LOGIN;

  ALTER USER pass_user WITH REPLICATION;
  ALTER USER md5_user WITH REPLICATION;
  ALTER USER scram_user WITH REPLICATION;
  ALTER USER ssl_user WITH REPLICATION;
SQL