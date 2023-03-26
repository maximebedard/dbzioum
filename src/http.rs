use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct HttpServer;

impl HttpServer {
    pub fn spawn() -> (Self, JoinHandle<()>) {
        let handle = tokio::task::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
        });
        (Self, handle)
    }
}
