use crate::{CustomError, FetchResult};
use anyhow::Result;
use async_trait::async_trait;
use tokio::fs;

#[async_trait]
pub trait Fetch {
    type Error;
    async fn fetch(&self) -> Result<String, Self::Error>;
}

pub async fn retrieve_data(source: impl AsRef<str>) -> FetchResult<String> {
    let name = source.as_ref();
    match &name[..4] {
        "http" => UrlFetcher(name).fetch().await,
        "file" => FileFetcher(name).fetch().await,
        v => Err(CustomError::FetchResourceError(v.to_string())),
    }
}

struct UrlFetcher<'a>(pub(crate) &'a str);
struct FileFetcher<'a>(pub(crate) &'a str);

#[async_trait]
impl<'a> Fetch for UrlFetcher<'a> {
    type Error = CustomError;

    async fn fetch(&self) -> Result<String, Self::Error> {
        let resp = reqwest::get(self.0)
            .await
            .map_err(|e| CustomError::FetchError {
                url: self.0.to_string(),
                error: e.to_string(),
            })?;
        let body = resp.text().await.map_err(|e| CustomError::FetchError {
            url: self.0.to_string(),
            error: e.to_string(),
        })?;
        Ok(body)
    }
}

#[async_trait]
impl<'a> Fetch for FileFetcher<'a> {
    type Error = CustomError;

    async fn fetch(&self) -> Result<String, Self::Error> {
        let body = fs::read_to_string(&self.0[7..])
            .await
            .map_err(|e| CustomError::FetchError {
                url: self.0.to_string(),
                error: e.to_string(),
            })?;
        Ok(body)
    }
}
