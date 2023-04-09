use dbzioum::gcp::{AccessTokenManager, Storage};

#[tokio::test]
#[ignore = "incomplete"]
async fn test_gcp_storage() {
  let (access_token_manager, _handle) = AccessTokenManager::spawn(vec![]);
  let storage = Storage::new(access_token_manager);
  let bucket = storage.bucket("foo");
  if let Ok(()) = bucket.exists().await {
    bucket.create().await.unwrap()
  }

  let object = bucket.object("bar");
  if let Ok(()) = object.exists().await {}
}
