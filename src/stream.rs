use tokio::task::JoinHandle;

use crate::mysql;

use super::pg;

#[derive(Debug)]
pub struct PostgresStream;

impl PostgresStream {
    pub fn spawn() -> (Self, JoinHandle<()>) {
        let handle = tokio::task::spawn(async move {
            let conn_pg = pg::Connection::connect(pg::ConnectionOptions {
                user: "postgres".to_string(),
                password: Some("postgres".to_string()),
                database: Some("test".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

            tokio::signal::ctrl_c().await.ok();

            conn_pg.close().await.unwrap();
        });
        (Self, handle)
    }
}

#[derive(Debug)]
pub struct MysqlStream;

impl MysqlStream {
    pub fn spawn() -> (Self, JoinHandle<()>) {
        let handle = tokio::task::spawn(async move {
            let conn_mysql = mysql::Connection::connect(mysql::ConnectionOptions {
                user: "mysql".to_string(),
                password: Some("mysql".to_string()),
                database: Some("test".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

            tokio::signal::ctrl_c().await.ok();

            conn_mysql.close().await.unwrap();
        });
        (Self, handle)
    }
}
