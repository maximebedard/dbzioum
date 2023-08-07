#!/bin/bash
set -e

mysql -v -u root --password=$MYSQL_ROOT_PASSWORD <<-SQL
CREATE USER 'sha2_user'@'%' IDENTIFIED WITH caching_sha2_password BY 'password';
GRANT ALL PRIVILEGES ON test.* TO 'sha2_user'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'sha2_user'@'%';

CREATE USER 'native_user'@'%' IDENTIFIED WITH mysql_native_password BY 'password';
GRANT ALL PRIVILEGES ON test.* TO 'native_user'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'native_user'@'%';
SQL