#!/bin/bash
set -e

# self signed cert used for testing purposes
# openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes -keyout server.key -out server.crt -subj "/CN=localhost"

cat > "$PGDATA/server.key" <<-PKEY
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDjd6xRCRqk+zPY
qgAdNlp96AZSfxFhFTyKLgiWJJCcLyxefCrY3p4UYBKKcNBsduJEyLXmx+cLVlMh
4lEujQZzPsJ8YzSXOqvdDtDFkg/jknehhqR8hQn1Ae29g4MAfO5Ts9KYH0Jr6Mnb
vaxsYs6O1N0jsjo70FQmuij0z0pbCaMPeECCuaCjuxQ02khrda/0+W+P0z5fDtrL
ufdL9vfaU44dsgQasOTh570a69sgcwIg1WAGED+6OKZDIlMh86X2eCQDjdLX8/YG
hXHSDC7KRZmFlHH3OBLMDDgVoMxfCau6oUeWe/6GSmyZTFjO8jNN/mlbBDJqaWvB
eziVwDNvAgMBAAECggEAbJd6PSmCfdaq1rm52jhTlR4KvqGI83cG8tStJriIDPf8
T5MphWUpxIJk95CJ/+31cW78YBN4+pGYmGmJ5hZPSP9iK63UsonA+ISVKGzlhvgq
goNbzVJaAYyNKdc52CbMREtps1PhCtEUZQI06X12LPv0IpF4eYuoUOC4or6/OZwG
wTpQQv4L2lsXewmDx4akdTiMulRDgTeI68NJwi5FirAyZs0kTWFu0e/QWwWLUllH
kq2Ef63Kf90JtE6GpNmmm9JxTZuiHOPgs4ZN1V4l0NLGCwXtf+CI6ezYPat1NFF5
LLFJuVN7SMu/MfoAnm992DBnV6ZHdUWBP/x00vVmgQKBgQDzbFDkZAwAgChxLgEQ
xJHTKfwtFGMhdGQ11idf1chwd3lSQCjOxa8CVwj+yZZfZMEzAP0W2NuSbEqP00Ee
A38w3AjP7sZ2OnnV4cBpJXakSzYflKsJXGCPUNiRjNxMI66/R41kyoN/DFxEc8Zy
l5CvgzH6wXWTLinuGn2xO/P3QQKBgQDvOFEV69jg5g8CYeKL9k3dTSUwlqTqLrGp
5iGCAPhKYyhIwKXg2Gz3F+leryczjLkiBQ4j7/FC7dMKW2wLnX23T4eMDDqQFfYw
ajqxTl1u8lxbwhfB042c17icmtpbXTuKWbVvIGb8paPUKmPQqRkImhZUkDMcMwTH
SgJK2OyurwKBgDQ1PwQ9EPXqhsH/g+r8ven0T0m73acHN5b0X22GhH7aoQKrEBWJ
AIgX8q3yvnWnPmiaBa4oxK8a2sMOJCEzzsvD5X/zTk19LTRaPtJOXqvOFcWgNS7E
yVDznf9ZnsYVwRz1U7YSWTGejQuBaUaai0WZdda6N2VhuUyAEgU5PpIBAoGBALb2
vuXiTi2tJ/utfEWKbAF/+JrSDW5jqlUFA8nYDg9vCaejWPvA7I6Mhlx54KTH0+1W
KnuIEGIdXhWE2P2FRlsHA+g0jjIX6gUbqqpkAohR7BvuNpdzw9MbF0MIGUxZ4aku
ddt5z+sakVQ7274DJ3dxyvSAmebOirAWReyTP2tXAoGBANb6O98sdpz92OxJkGpD
j8rFU9LkhQGnWcWyViO0bdm8d6htEdwDR3pA2ZC0+LSKWEirxa3CGIEVqKiFfcXm
cx2KP6KFkiPWzKJ47O3f0KZBEdGAX6sYBaxhd6W7PUToozd/HqCEx6efDCwJlkye
a9H3xcEVNnhS4dorWS4rHp+I
-----END PRIVATE KEY-----
PKEY
chmod 0600 "$PGDATA/server.key"

cat > "$PGDATA/server.crt" <<-CERT
-----BEGIN CERTIFICATE-----
MIICpDCCAYwCCQCEZomCCbAyvDANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAls
b2NhbGhvc3QwHhcNMjMwODAzMTgyODE4WhcNMzMwNzMxMTgyODE4WjAUMRIwEAYD
VQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDj
d6xRCRqk+zPYqgAdNlp96AZSfxFhFTyKLgiWJJCcLyxefCrY3p4UYBKKcNBsduJE
yLXmx+cLVlMh4lEujQZzPsJ8YzSXOqvdDtDFkg/jknehhqR8hQn1Ae29g4MAfO5T
s9KYH0Jr6MnbvaxsYs6O1N0jsjo70FQmuij0z0pbCaMPeECCuaCjuxQ02khrda/0
+W+P0z5fDtrLufdL9vfaU44dsgQasOTh570a69sgcwIg1WAGED+6OKZDIlMh86X2
eCQDjdLX8/YGhXHSDC7KRZmFlHH3OBLMDDgVoMxfCau6oUeWe/6GSmyZTFjO8jNN
/mlbBDJqaWvBeziVwDNvAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAHJdXqB8y3B9
t38fhox33dh25AQqpNHQPphYfweFisZDwv3fUR8YLucQUWYn4lS++HiWg2HE2pnt
9HZY4VpgsbbUCPiyRbYJoy7D0HaLuNcdjTNLAUC5D80wr9yYvTIDfu4kwkWl0QMk
l1RsPUMMimysNq/UVqkCladBpCFPmPASmiFe3mpphZmEeKxDZJxhPI7jXC3Mz51Z
vKbVQEkOe1atEroUntHnZ8bQ6Qftjsw0Toa75Lla/4DsKeYMM6qPNYlTb2ZtYvzx
wBl/fGllBFiGOOo80We5c3OJUMhOhf1Z22vooqcfgCzilT0KLh7d6eTFY2UbUUcM
i0qJXlcQNWo=
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