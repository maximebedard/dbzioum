use dbzioum::gcp::{AccessTokenManager, BigQuery};

#[tokio::test]
#[ignore = "incomplete"]
async fn test_gcp_bigquery() {
  let (access_token_manager, _handle) = AccessTokenManager::spawn(vec![]);
  let _big_query = BigQuery::new(access_token_manager);
}
