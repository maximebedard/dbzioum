use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct BigQuerySink;

impl BigQuerySink {
    pub fn spawn() -> (Self, JoinHandle<()>) {
        let handle = tokio::task::spawn(async move {});
        (Self, handle)
    }
}
