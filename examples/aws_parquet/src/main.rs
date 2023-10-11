use std::thread;

use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use log::info;
use object_store::aws::AmazonS3Builder;
use object_store::aws::AwsCredential;
use object_store::path::Path;
use object_store::CredentialProvider;
use polars::io::parquet::ParquetWriter;
use polars::{io::cloud::CloudWriter, prelude::*};
use simplelog::SimpleLogger;
use simplelog::{Config, LevelFilter};

#[derive(Debug)]
struct S3CredentialProvider {
    credentials: aws_credential_types::provider::SharedCredentialsProvider,
}

#[async_trait]
impl CredentialProvider for S3CredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let creds = self.credentials.provide_credentials().await.map_err(|e| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(e),
            }
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(ToString::to_string),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), PolarsError> {
    let _ = SimpleLogger::init(LevelFilter::Info, Config::default());

    info!("Connecting to Object Store...");
    let config = aws_config::load_from_env().await;
    let credentials_provider = Arc::new(S3CredentialProvider {
        credentials: config.credentials_provider().unwrap(),
    });
    let object_store = AmazonS3Builder::from_env()
        .with_bucket_name("holapolars")
        .with_credentials(credentials_provider)
        .build()?;

    info!("Writing to cloud storage");
    let handle = thread::spawn(move || {
        let mut s = DataFrame::new(vec![
            Series::new("names", &["a", "b", "c"]),
            Series::new("values", &[1, 2, 3]),
        ])
        .unwrap();
        let cloud_writer = CloudWriter::new_with_object_store(
            Arc::from(object_store),
            Path::from("example.parquet"),
        )
        .unwrap();
        ParquetWriter::new(cloud_writer).finish(&mut s).unwrap();
    });

    handle.join().unwrap();
    info!("Done!");
    Ok(())
}
