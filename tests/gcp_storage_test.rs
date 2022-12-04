use ps2bq::gcp::{Credential, Storage};

#[tokio::test]
async fn test_gcp_storage() {
    let (credential, _handle) = Credential::spawn(vec![]);
    let storage = Storage::new(credential);
    let bucket = storage.bucket("foo");
    if let Ok(false) = bucket.exists().await {
        bucket.create().await.unwrap()
    }

    let object = bucket.object("bar");
    if let Ok(false) = object.exists().await {}
}
