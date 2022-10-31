use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct PostgresStream;

impl PostgresStream {
    pub fn spawn() -> (Self, JoinHandle<()>) {
        let handle = tokio::task::spawn(async move {});
        (Self, handle)
    }
}
