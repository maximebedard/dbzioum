use core::fmt;
use std::{
  io,
  path::{Path, PathBuf},
};

use tokio::{
  sync::{mpsc, oneshot},
  task::{self, JoinHandle},
};

#[derive(Debug, Clone)]
pub struct AccessToken {
  pub value: String,
  pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl AccessToken {
  fn is_expired(&self) -> bool {
    if let Some(i) = self.expires_at {
      chrono::Utc::now() - chrono::Duration::seconds(10) >= i
    } else {
      false
    }
  }
}

impl fmt::Display for AccessToken {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.value.as_str())
  }
}

#[derive(Debug, Clone)]
pub struct AccessTokenManager {
  pub sender: mpsc::Sender<oneshot::Sender<io::Result<AccessToken>>>,
}

impl AccessTokenManager {
  pub fn spawn(scopes: Vec<String>) -> (Self, JoinHandle<()>) {
    let (sender, mut receiver) = mpsc::channel::<oneshot::Sender<io::Result<AccessToken>>>(32);

    let handle = task::spawn(async move {
      let strategy = AccessTokenStrategy::autodetect().await.unwrap();
      let mut access_token: Option<AccessToken> = None;
      while let Some(sender) = receiver.recv().await {
        match access_token.as_ref().filter(|t| !t.is_expired()) {
          Some(t) => {
            sender.send(Ok(t.clone())).ok();
          }
          None => {
            let result = strategy.refresh_access_token(&scopes).await;
            if let Ok(new_access_token) = result.as_ref() {
              access_token.replace(new_access_token.clone());
            }
            sender.send(result).ok();
          }
        }
      }
    });

    (Self { sender }, handle)
  }

  async fn access_token(&self) -> io::Result<AccessToken> {
    let (sender, receiver) = oneshot::channel();
    self.sender.send(sender).await.ok();
    receiver.await.unwrap()
  }
}

#[derive(Debug, serde::Deserialize)]
struct UserKey {
  client_id: String,
  client_secret: String,
  quota_project_id: String,
  refresh_token: String,
}

struct Signer(Box<dyn rustls::sign::Signer>);

impl Signer {
  fn new(private_key_bytes: Vec<u8>) -> io::Result<Self> {
    let signer = rustls::sign::any_ecdsa_type(&rustls::PrivateKey(private_key_bytes))
      .unwrap()
      .choose_scheme(&[rustls::SignatureScheme::RSA_PKCS1_SHA256])
      .unwrap();

    Ok(Self(signer))
  }

  fn sign(&self, scopes: &Vec<String>, key: &ServiceAccountKey) -> io::Result<String> {
    #[derive(Debug, serde::Serialize)]
    struct Claims<'a> {
      iss: &'a str,
      #[serde(skip_serializing_if = "Option::is_none")]
      scope: Option<&'a str>,
      aud: &'a str,
      exp: Option<i64>,
      iat: Option<i64>,
      #[serde(skip_serializing_if = "Option::is_none")]
      typ: Option<&'a str>,
      #[serde(skip_serializing_if = "Option::is_none")]
      sub: Option<&'a str>,
    }

    let scope = scopes.join(" ");
    let now = chrono::Utc::now() - chrono::Duration::seconds(10);
    let claims = Claims {
      iss: key.client_email.as_str(),
      scope: Some(scope.as_str()),
      aud: key.token_uri.as_str(),
      exp: Some((now + chrono::Duration::hours(1)).timestamp()),
      iat: Some(now.timestamp()),
      sub: None,
      typ: None,
    };

    #[derive(Debug, serde::Serialize)]
    struct Header<'a> {
      alg: &'a str,
      typ: &'a str,
      #[serde(skip_serializing_if = "Option::is_none")]
      kid: Option<&'a str>,
    }

    let header = Header {
      alg: "RS256",
      typ: "JWT",
      kid: None,
    };

    let claims = base64::encode_config(serde_json::to_string(&claims).unwrap(), base64::URL_SAFE_NO_PAD);
    let header = base64::encode_config(serde_json::to_string(&header).unwrap(), base64::URL_SAFE_NO_PAD);

    let ss = format!("{}.{}", header, claims);
    let sig = self.0.sign(ss.as_bytes()).unwrap();

    Ok(format!(
      "{}.{}",
      ss,
      base64::encode_config(sig, base64::URL_SAFE_NO_PAD)
    ))
  }
}

#[derive(Debug, serde::Deserialize)]
struct ServiceAccountKey {
  client_email: String,
  private_key_id: String,
  private_key: String,
  auth_uri: String,
  token_uri: String,
  project_id: String,
}

enum AccessTokenStrategy {
  User { key: UserKey },
  ServiceAccount { signer: Signer, key: ServiceAccountKey },
  MetadataServer { account: String },
}

impl AccessTokenStrategy {
  async fn autodetect() -> io::Result<Self> {
    // 1. GOOGLE_APPLICATION_CREDENTIALS environment variable
    if let Some(path) = Self::application_credentials_path() {
      return Self::from_file(path).await;
    }

    // 2. Well-known locations
    if let Some(path) = Self::default_application_credentials_path() {
      return Self::from_file(path).await;
    }

    // 3. Metadata server
    if Self::is_running_on_gce().await {
      return Ok(Self::MetadataServer {
        account: "default".to_string(),
      });
    }

    Err(io::Error::new(
      io::ErrorKind::Unsupported,
      "Failed to detect GCP credentials",
    ))
  }

  async fn from_file(path: impl AsRef<Path>) -> io::Result<Self> {
    let json = tokio::fs::read_to_string(path).await?;
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    match json.get("type").and_then(serde_json::Value::as_str) {
      Some("authorized_user") => {
        let key = serde_json::from_value::<UserKey>(json).unwrap();
        Ok(Self::User { key })
      }
      Some("service_account") => {
        let key = serde_json::from_value::<ServiceAccountKey>(json).unwrap();
        let private_key_string = tokio::fs::read_to_string(&key.private_key).await?;
        let signer = Signer::new(private_key_string.into_bytes()).unwrap();
        Ok(Self::ServiceAccount { signer, key })
      }
      Some(access_token_type) => Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{} is not supported", access_token_type),
      )),
      _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "file format is invalid")),
    }
  }

  async fn is_running_on_gce() -> bool {
    if std::env::var("GCE_METADATA_HOST").is_ok() {
      return true;
    }

    let client = hyper::Client::default();

    tokio::select! {
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => false,
        Ok(_) = client.get(hyper::Uri::from_static("http://169.254.169.254")) => true,
        Ok(_) = tokio::net::TcpListener::bind(("metadata.google.internal", 0)) => true,
    }
  }

  async fn refresh_access_token(&self, scopes: &Vec<String>) -> io::Result<AccessToken> {
    match self {
      AccessTokenStrategy::User { key } => {
        let client = hyper::Client::new();

        let request = hyper::Request::builder()
          .method(hyper::Method::POST)
          .uri("https://oauth2.googleapis.com/token")
          .header("Content-Type", "application/x-www-form-urlencoded")
          .body(
            format!(
              "grant_type={}&refresh_token={}&client_id={}&client_secret={}",
              "refresh_token", key.refresh_token, key.client_id, key.client_secret,
            )
            .into(),
          )
          .unwrap();

        let response = client.request(request).await.unwrap();

        let response_body = hyper::body::to_bytes(response.into_body()).await.unwrap();

        #[derive(serde::Deserialize)]
        struct AccessTokenResponse {
          access_token: String,
          expires_in: i64,
        }

        let AccessTokenResponse {
          access_token,
          expires_in,
        } = serde_json::from_slice::<AccessTokenResponse>(&response_body).unwrap();

        if expires_in <= 0 || access_token.is_empty() {
          return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Incomplete token received from metadata server",
          ));
        }

        Ok(AccessToken {
          value: access_token,
          expires_at: Some(chrono::Utc::now() + chrono::Duration::seconds(expires_in)),
        })
      }
      AccessTokenStrategy::ServiceAccount { signer, key } => {
        let client = hyper::Client::new();

        let assertion = signer.sign(scopes, key).unwrap();
        let request = hyper::Request::builder()
          .method(hyper::Method::POST)
          .uri("https://oauth2.googleapis.com/token")
          .header("Content-Type", "application/x-www-form-urlencoded")
          .body(
            format!(
              "grant_type={}&assertion={}",
              "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion
            )
            .into(),
          )
          .unwrap();

        let response = client.request(request).await.unwrap();

        let response_body = hyper::body::to_bytes(response.into_body()).await.unwrap();

        #[derive(serde::Deserialize)]
        struct AccessTokenResponse {
          access_token: String,
          expires_in: i64,
        }

        let AccessTokenResponse {
          access_token,
          expires_in,
        } = serde_json::from_slice::<AccessTokenResponse>(&response_body).unwrap();

        if expires_in <= 0 || access_token.is_empty() {
          return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Incomplete token received from metadata server",
          ));
        }

        Ok(AccessToken {
          value: access_token,
          expires_at: Some(chrono::Utc::now() + chrono::Duration::seconds(expires_in)),
        })
      }
      AccessTokenStrategy::MetadataServer { account } => {
        let host = std::env::var("GCE_METADATA_HOST").unwrap_or_else(|_| "169.254.169.254".to_string());
        let uri = format!(
          "http://{}/computeMetadata/v1/instance/service-accounts/{}/token?scopes={}",
          host,
          account,
          scopes.join(",")
        );

        let client = hyper::Client::default();

        let request = hyper::Request::builder()
          .method(hyper::Method::GET)
          .header("Metadata-Flavor", "Google")
          .uri(uri)
          .body(hyper::Body::empty())
          .unwrap();
        let response = client.request(request).await.unwrap();

        let response_body = hyper::body::to_bytes(response.into_body()).await.unwrap();

        #[derive(serde::Deserialize)]
        struct AccessTokenResponse {
          access_token: String,
          expires: i64,
        }

        let AccessTokenResponse { access_token, expires } =
          serde_json::from_slice::<AccessTokenResponse>(&response_body).unwrap();

        if expires <= 0 || access_token.is_empty() {
          return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Incomplete token received from metadata server",
          ));
        }

        Ok(AccessToken {
          value: access_token,
          expires_at: Some(chrono::Utc::now() + chrono::Duration::seconds(expires)),
        })
      }
    }
  }

  fn application_credentials_path() -> Option<PathBuf> {
    std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok().map(Into::into)
  }

  #[cfg(all(unix))]
  fn default_application_credentials_path() -> Option<PathBuf> {
    std::env::var("HOME")
      .map(PathBuf::from)
      .map(|p| p.join(".config/gcloud/application_default_credentials.json"))
      .ok()
  }
}

#[derive(Debug, Clone)]
struct StorageInner {
  access_token_manager: AccessTokenManager,
}

impl StorageInner {
  async fn json_request(&self, mut req: hyper::Request<hyper::Body>) -> io::Result<()> {
    let http = hyper::Client::builder().build_http();
    let access_token = self.access_token_manager.access_token().await.unwrap();
    let headers = req.headers_mut();
    headers.insert("Content-Type", "application/json".parse().unwrap());
    headers.insert("Authorization", format!("Bearer {}", access_token).parse().unwrap());
    http.request(req).await.unwrap();
    Ok(())
  }
}

#[derive(Debug, Clone)]
pub struct Storage {
  inner: StorageInner,
}

impl Storage {
  pub fn new(access_token_manager: AccessTokenManager) -> Self {
    let inner = StorageInner { access_token_manager };
    Self { inner }
  }

  pub fn bucket(&self, name: impl Into<String>) -> StorageBucket {
    let inner = self.inner.clone();
    let name = name.into();
    StorageBucket { inner, name }
  }
}

#[derive(Debug)]
pub struct StorageBucket {
  inner: StorageInner,
  name: String,
}

impl StorageBucket {
  pub async fn exists(&self) -> io::Result<()> {
    let req = hyper::Request::builder()
      .method(hyper::Method::GET)
      .uri(format!("https://storage.googleapis.com/storage/v1/b/{}", self.name))
      .body(hyper::Body::empty())
      .unwrap();
    self.inner.json_request(req).await.unwrap();
    Ok(())
  }

  pub async fn create(&self) -> io::Result<()> {
    let project_id = "";
    let body = serde_json::json!({"name": self.name}).to_string();
    let req = hyper::Request::builder()
      .method(hyper::Method::POST)
      .uri(format!(
        "https://storage.googleapis.com/storage/v1/b?project={}",
        project_id
      ))
      .body(body.into())
      .unwrap();
    self.inner.json_request(req).await.unwrap();
    Ok(())
  }

  pub async fn delete(&self) -> io::Result<()> {
    let req = hyper::Request::builder()
      .method(hyper::Method::DELETE)
      .uri(format!("https://storage.googleapis.com/storage/v1/b/{}", self.name))
      .body(hyper::Body::empty())
      .unwrap();
    self.inner.json_request(req).await.unwrap();
    Ok(())
  }

  pub async fn delete_silently(&self) -> io::Result<()> {
    match self.delete().await {
      Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
      r => r,
    }
  }

  pub fn object(&self, name: impl Into<String>) -> StorageObject {
    let inner = self.inner.clone();
    let bucket_name = self.name.clone();
    let name = name.into();
    StorageObject {
      inner,
      bucket_name,
      name,
    }
  }
}

#[derive(Debug)]
pub struct StorageObject {
  inner: StorageInner,
  bucket_name: String,
  name: String,
}

impl StorageObject {
  pub async fn exists(&self) -> io::Result<()> {
    let req = hyper::Request::builder()
      .method(hyper::Method::GET)
      .uri(format!(
        "https://storage.googleapis.com/storage/v1/b/{}/{}",
        self.bucket_name, self.name,
      ))
      .body(hyper::Body::empty())
      .unwrap();
    self.inner.json_request(req).await.unwrap();
    Ok(())
  }

  pub async fn create(&self) -> io::Result<()> {
    Ok(())
  }

  pub async fn delete(&self) -> io::Result<()> {
    let req = hyper::Request::builder()
      .method(hyper::Method::DELETE)
      .uri(format!(
        "https://storage.googleapis.com/storage/v1/b/{}/{}",
        self.bucket_name, self.name,
      ))
      .body(hyper::Body::empty())
      .unwrap();
    self.inner.json_request(req).await.unwrap();
    Ok(())
  }

  pub async fn delete_silently(&self) -> io::Result<()> {
    match self.delete().await {
      Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
      r => r,
    }
  }
}

#[derive(Debug, Clone)]
pub struct BigQuery {
  access_token_manager: AccessTokenManager,
}

impl BigQuery {
  pub fn new(access_token_manager: AccessTokenManager) -> Self {
    Self { access_token_manager }
  }

  pub async fn append_rows() {}

  pub async fn batch_commit_write_streams() {}

  pub async fn create_write_stream() {}

  pub async fn finalize_write_stream() {}

  pub async fn flush_rows() {}

  pub async fn get_write_stream() {}
}

// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.BigQueryWrite

// https://github.com/googleapis/googleapis/tree/master/google/cloud/bigquery/storage/v1

// https://github.com/tokio-rs/prost
