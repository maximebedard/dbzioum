use tokio::{sync::mpsc, task::JoinHandle};

use super::RowEvent;

pub struct RowEventSink;

impl RowEventSink {
  pub fn spawn(mut receiver: mpsc::Receiver<RowEvent>) -> (Self, JoinHandle<()>) {
    let handle = tokio::task::spawn(async move {
      while let Some(event) = receiver.recv().await {
        println!("{:?}", event);
      }
    });
    (Self, handle)
  }
}
