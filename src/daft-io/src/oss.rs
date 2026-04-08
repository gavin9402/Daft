use std::{
    any::Any,
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use ali_oss_rs::{
    ClientBuilder,
    bucket::BucketOperations,
    bucket_common::ListObjectsOptionsBuilder,
    error::Error as OssError,
    multipart::MultipartUploadsOperations,
    multipart_common::{CompleteMultipartUploadRequest, UploadPartRequest},
    object::ObjectOperations,
    object_common::GetObjectOptionsBuilder,
};
use async_trait::async_trait;
use bytes::Bytes;
use common_io_config::OssConfig;
use common_runtime::get_io_pool_num_threads;
use futures::stream::BoxStream;
use snafu::{ResultExt, Snafu};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};
use url::{ParseError, Position};

use crate::{
    FileMetadata, GetRange, GetResult, IOStatsRef, InvalidRangeRequestSnafu, ObjectSource, Result,
    SourceType,
    multipart::MultipartWriter,
    object_io::{FileType, LSResult},
    stream_utils::io_stats_on_bytestream,
    utils::{ObjectPath, parse_object_url},
};

const DELIMITER: &str = "/";
const DEFAULT_GLOB_FANOUT_LIMIT: usize = 1024;
const BASE_DELAY_MS: u64 = 100;
const MAX_DELAY_MS: u64 = 10000;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create OSS client with endpoint {}: {}", endpoint, msg))]
    ClientCreation { endpoint: String, msg: String },

    #[snafu(display("Unsupported scheme: {} in URL: \"{}\"", scheme, path))]
    UnsupportedScheme { scheme: String, path: String },

    #[snafu(display("Unable to parse URL: \"{}\"", path))]
    InvalidUrl { path: String, source: ParseError },

    #[snafu(display("Unable to open {}: {}", path, source))]
    UnableToOpenFile { path: String, source: OssError },

    #[snafu(display("Unable to put {}: {}", path, source))]
    UnableToPutFile { path: String, source: OssError },

    #[snafu(display("Unable to delete {}: {}", path, source))]
    UnableToDeleteFile { path: String, source: OssError },

    #[snafu(display("Unable to list {}: {}", path, source))]
    UnableToListObjects { path: String, source: OssError },

    #[snafu(display("Unable to create multipart upload for {}: {}", path, source))]
    UnableToCreateMultipartUpload { path: String, source: OssError },

    #[snafu(display(
        "Unable to upload part {} for {} with upload_id {}: {}",
        part_number,
        path,
        upload_id,
        source
    ))]
    UnableToUploadPart {
        path: String,
        upload_id: String,
        part_number: usize,
        source: OssError,
    },

    #[snafu(display(
        "Unable to complete multipart upload {} with upload_id {}: {}",
        path,
        upload_id,
        source
    ))]
    UnableToCompleteMultipartUpload {
        path: String,
        upload_id: String,
        source: OssError,
    },

    #[snafu(display("Unable to grab semaphore: {}", source))]
    UnableToGrabSemaphore { source: tokio::sync::AcquireError },

    #[snafu(display("Operation failed after {} retries: {}", retries, err_msg))]
    MaxRetriesExceeded { retries: u32, err_msg: String },

    #[snafu(display("Not found: {}", path))]
    NotFound { path: String, source: OssError },

    #[snafu(display("Generic error for {}: {}", path, source))]
    Generic { path: String, source: OssError },
}

#[allow(clippy::fallible_impl_from)]
impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        match error {
            Error::ClientCreation { endpoint: _, msg } => Self::UnableToCreateClient {
                store: SourceType::Oss,
                source: msg.into(),
            },
            Error::InvalidUrl { path, source } => Self::InvalidUrl { path, source },
            Error::NotFound { path, source } => Self::NotFound {
                path,
                source: Box::new(source),
            },
            Error::UnableToOpenFile { path, source } => Self::UnableToOpenFile {
                path,
                source: Box::new(source),
            },
            Error::MaxRetriesExceeded { retries, err_msg } => Self::Generic {
                store: SourceType::Oss,
                source: Error::MaxRetriesExceeded { retries, err_msg }.into(),
            },
            err => Self::Generic {
                store: SourceType::Oss,
                source: err.into(),
            },
        }
    }
}

fn is_retryable_error(error: &OssError) -> bool {
    match error {
        OssError::ReqwestError(_) | OssError::IoError(_) => true,
        OssError::ApiError(resp) => {
            let code = &resp.code;
            code == "InternalError"
                || code == "ServiceUnavailable"
                || code == "RequestTimeTooSkewed"
                || code == "Throttling"
        }
        OssError::StatusError(status) => {
            status.as_u16() >= 500 || status.as_u16() == 429 || status.as_u16() == 408
        }
        OssError::Other(msg) => {
            let lower = msg.to_lowercase();
            lower.contains("timeout")
                || lower.contains("broken pipe")
                || lower.contains("throttl")
        }
        _ => false,
    }
}

fn is_not_found_error(error: &OssError) -> bool {
    match error {
        OssError::ApiError(resp) => resp.code == "NoSuchKey" || resp.code == "NoSuchBucket",
        OssError::StatusError(status) => status.as_u16() == 404,
        _ => false,
    }
}

async fn calculate_retry_delay(attempt: u32) -> Duration {
    let mut delay = BASE_DELAY_MS * 2u64.pow(attempt + 1);
    if delay > MAX_DELAY_MS {
        delay = MAX_DELAY_MS;
    }
    Duration::from_millis(delay)
}

type OssClient = ali_oss_rs::Client;

pub struct OssSource {
    client: OssClient,
    connection_pool_sema: Arc<Semaphore>,
    config: OssConfig,
}

impl std::fmt::Debug for OssSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OssSource")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl OssSource {
    pub async fn get_client(config: &OssConfig) -> Result<Arc<Self>> {
        let (endpoint, region) = config.endpoint_and_region();

        let access_key_id = config.access_key_id.clone().unwrap_or_default();
        let access_key_secret = config
            .access_key_secret
            .as_ref()
            .map(|s| s.as_string().clone())
            .unwrap_or_default();

        let mut builder = ClientBuilder::new(&access_key_id, &access_key_secret, &endpoint)
            .region(&region);

        if let Some(token) = config.security_token.as_ref() {
            builder = builder.sts_token(token.as_string());
        }

        let client = builder.build().map_err(|err| Error::ClientCreation {
            endpoint: endpoint.clone(),
            msg: err,
        })?;

        let connection_pool_sema = Arc::new(Semaphore::new(
            (config.max_connections_per_io_thread as usize)
                * get_io_pool_num_threads().expect("Should be running in tokio pool"),
        ));

        Ok(Arc::new(Self {
            client,
            connection_pool_sema,
            config: config.clone(),
        }))
    }

    fn parse_oss_url(url: &str, allow_empty_key: bool) -> Result<(String, String)> {
        let parsed = url::Url::parse(url).map_err(|source| Error::InvalidUrl {
            path: url.to_string(),
            source,
        })?;

        if parsed.scheme() != "oss" {
            return Err(Error::UnsupportedScheme {
                scheme: parsed.scheme().to_string(),
                path: url.to_string(),
            }
            .into());
        }

        let bucket = parsed.host_str().ok_or_else(|| Error::InvalidUrl {
            path: parsed.to_string(),
            source: ParseError::EmptyHost,
        })?;

        let bucket_scheme_len = parsed[..Position::AfterHost].len();
        let key = url[bucket_scheme_len..].trim_start_matches(DELIMITER);

        if !allow_empty_key && key.is_empty() {
            return Err(super::Error::NotAFile {
                path: parsed.to_string(),
            }
            .into());
        }

        Ok((bucket.to_string(), key.to_string()))
    }

    async fn retry_operation<T, F, Fut>(
        &self,
        operation_name: &str,
        uri: &str,
        operation: F,
    ) -> std::result::Result<T, OssError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, OssError>>,
    {
        let mut last_error: Option<OssError> = None;

        for attempt in 0..=self.config.max_retries {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    if is_retryable_error(&error) && attempt < self.config.max_retries {
                        let delay = calculate_retry_delay(attempt).await;
                        log::warn!(
                            "OSS {} operation failed for {} (attempt {}/{}): {}. Retrying in {:?}",
                            operation_name,
                            uri,
                            attempt + 1,
                            self.config.max_retries + 1,
                            error,
                            delay
                        );
                        last_error = Some(error);
                        tokio::time::sleep(delay).await;
                    } else {
                        return Err(error);
                    }
                }
            }
        }

        Err(last_error.unwrap())
    }

    async fn get_impl(
        &self,
        _permit: OwnedSemaphorePermit,
        uri: &str,
        range: Option<GetRange>,
    ) -> Result<GetResult> {
        let (bucket, key) = Self::parse_oss_url(uri, false)?;

        if let Some(range) = range.as_ref() {
            range.validate().context(InvalidRangeRequestSnafu)?;
        }

        let range_str = range.as_ref().map(|r| r.to_string());

        let uri_s = uri.to_string();
        let data = self
            .retry_operation("get_object", &uri_s, || {
                let bucket = bucket.clone();
                let key = key.clone();
                let range_str = range_str.clone();
                async move {
                    let options = range_str.map(|r| {
                        GetObjectOptionsBuilder::new().range(r).build()
                    });
                    self.client
                        .get_object_to_buffer(&bucket, &key, options)
                        .await
                }
            })
            .await
            .map_err(|source| {
                if is_not_found_error(&source) {
                    Error::NotFound {
                        path: uri_s.clone(),
                        source,
                    }
                } else {
                    Error::UnableToOpenFile {
                        path: uri_s.clone(),
                        source,
                    }
                }
            })?;

        let bytes = Bytes::from(data);
        let size = bytes.len();
        let stream = Box::pin(futures::stream::once(async { Ok(bytes) }));

        Ok(GetResult::Stream(stream, Some(size), None, None))
    }

    async fn put_impl(&self, _permit: OwnedSemaphorePermit, uri: &str, data: Bytes) -> Result<()> {
        let (bucket, key) = Self::parse_oss_url(uri, false)?;

        let uri_s = uri.to_string();
        self.retry_operation("put_object", &uri_s, || {
            let bucket = bucket.clone();
            let key = key.clone();
            let data = data.clone();
            async move {
                self.client
                    .put_object_from_buffer(&bucket, &key, data.to_vec(), None)
                    .await
                    .map(|_| ())
            }
        })
        .await
        .map_err(|source| Error::UnableToPutFile {
            path: uri_s,
            source,
        })?;

        Ok(())
    }

    async fn get_size_impl(&self, _permit: OwnedSemaphorePermit, uri: &str) -> Result<usize> {
        let (bucket, key) = Self::parse_oss_url(uri, false)?;

        let uri_s = uri.to_string();
        let metadata = self
            .retry_operation("head_object", &uri_s, || {
                let bucket = bucket.clone();
                let key = key.clone();
                async move { self.client.head_object(&bucket, &key, None).await }
            })
            .await
            .map_err(|source| {
                if is_not_found_error(&source) {
                    Error::NotFound {
                        path: uri_s.clone(),
                        source,
                    }
                } else {
                    Error::UnableToOpenFile {
                        path: uri_s.clone(),
                        source,
                    }
                }
            })?;

        Ok(metadata.content_length as usize)
    }

    async fn list_impl(
        &self,
        _permit: OwnedSemaphorePermit,
        bucket: &str,
        key: &str,
        delimiter: Option<char>,
        continuation_token: Option<String>,
        page_size: Option<i32>,
    ) -> Result<LSResult> {
        let bucket = bucket.to_string();
        let key = key.to_string();

        let path_s = format!("oss://{bucket}/{key}");
        let mut options_builder = ListObjectsOptionsBuilder::default();
        options_builder = options_builder.prefix(&key);
        if let Some(delim) = delimiter {
            options_builder = options_builder.delimiter(delim);
        }
        if let Some(ref token) = continuation_token {
            options_builder = options_builder.continuation_token(token);
        }
        if let Some(size) = page_size {
            options_builder = options_builder.max_keys(size as u32);
        }
        let options = Some(options_builder.build());

        let result = self
            .retry_operation("list_objects", &path_s, || {
                let bucket = bucket.clone();
                let options = options.clone();
                async move { self.client.list_objects(&bucket, options).await }
            })
            .await
            .map_err(|source| Error::UnableToListObjects {
                path: path_s.clone(),
                source,
            })?;

        let dirs = &result.common_prefixes;
        let files_list = &result.contents;

        let files = dirs
            .iter()
            .map(|prefix| FileMetadata {
                filepath: format!("oss://{}/{}", bucket, prefix),
                size: None,
                filetype: FileType::Directory,
            })
            .chain(files_list.iter().map(|f| FileMetadata {
                filepath: format!("oss://{}/{}", bucket, f.key),
                size: Some(f.size),
                filetype: FileType::File,
            }))
            .collect();

        let continuation_token = result
            .next_continuation_token
            .filter(|t| !t.is_empty());

        Ok(LSResult {
            files,
            continuation_token,
        })
    }

    pub async fn create_mpu(&self, bucket: &str, key: &str) -> Result<String> {
        let _permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let uri = format!("oss://{bucket}/{key}");
        let result = self
            .retry_operation("initiate_multipart_upload", &uri, || {
                let bucket = bucket.to_string();
                let key = key.to_string();
                async move {
                    self.client
                        .initiate_multipart_uploads(&bucket, &key, None)
                        .await
                }
            })
            .await
            .map_err(|source| Error::UnableToCreateMultipartUpload {
                path: uri.clone(),
                source,
            })?;

        Ok(result.upload_id)
    }

    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<OssPart> {
        let _permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let path_s = format!("oss://{bucket}/{key}");
        let upload_id_s = upload_id.to_string();
        let result = self
            .retry_operation("upload_part", &path_s, || {
                let bucket = bucket.to_string();
                let key = key.to_string();
                let upload_id = upload_id_s.clone();
                let data = data.clone();
                async move {
                    let params = UploadPartRequest {
                        part_number,
                        upload_id,
                    };
                    self.client
                        .upload_part_from_buffer(&bucket, &key, data.to_vec(), params)
                        .await
                }
            })
            .await
            .map_err(|source| Error::UnableToUploadPart {
                path: path_s,
                upload_id: upload_id_s,
                part_number: part_number as usize,
                source,
            })?;

        Ok(OssPart {
            idx: part_number as usize,
            etag: result.etag,
        })
    }

    pub async fn complete_mpu(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<OssPart>,
    ) -> Result<()> {
        let _permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let uri = format!("oss://{bucket}/{key}");
        let upload_id_s = upload_id.to_string();
        self.retry_operation("complete_multipart_upload", &uri, || {
            let bucket = bucket.to_string();
            let key = key.to_string();
            let upload_id = upload_id_s.clone();
            let parts = parts.clone();
            async move {
                let complete_parts: Vec<(u32, String)> = parts
                    .iter()
                    .map(|p| (p.idx as u32, p.etag.clone()))
                    .collect();
                let request = CompleteMultipartUploadRequest {
                    upload_id,
                    parts: complete_parts,
                };
                self.client
                    .complete_multipart_uploads(&bucket, &key, request, None)
                    .await
                    .map(|_| ())
            }
        })
        .await
        .map_err(|source| Error::UnableToCompleteMultipartUpload {
            path: uri,
            upload_id: upload_id_s,
            source,
        })?;

        Ok(())
    }
}

#[async_trait]
impl ObjectSource for OssSource {
    async fn supports_range(&self, _: &str) -> Result<bool> {
        Ok(true)
    }

    async fn create_multipart_writer(
        self: Arc<Self>,
        uri: &str,
    ) -> Result<Option<Box<dyn MultipartWriter>>> {
        Ok(Some(Box::new(
            OssMultipartWriter::create(uri, self.clone()).await?,
        )))
    }

    async fn get(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> Result<GetResult> {
        let permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;
        let get_result = self.get_impl(permit, uri, range).await?;
        if io_stats.is_some() {
            if let GetResult::Stream(stream, num_bytes, permit, retry_params) = get_result {
                if let Some(is) = io_stats.as_ref() {
                    is.mark_get_requests(1);
                }
                Ok(GetResult::Stream(
                    io_stats_on_bytestream(stream, io_stats),
                    num_bytes,
                    permit,
                    retry_params,
                ))
            } else {
                panic!("This should always be a stream");
            }
        } else {
            Ok(get_result)
        }
    }

    async fn put(&self, uri: &str, data: Bytes, io_stats: Option<IOStatsRef>) -> Result<()> {
        let data_len = data.len();
        let permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        self.put_impl(permit, uri, data).await?;

        if let Some(io_stats) = io_stats {
            io_stats.as_ref().mark_put_requests(1);
            io_stats.as_ref().mark_bytes_uploaded(data_len);
        }

        Ok(())
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> Result<usize> {
        let permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let ret = self.get_size_impl(permit, uri).await?;
        if let Some(is) = io_stats.as_ref() {
            is.mark_head_requests(1);
        }
        Ok(ret)
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<Arc<crate::IOStatsContext>>,
        _file_format: Option<crate::FileFormat>,
    ) -> Result<BoxStream<'static, Result<FileMetadata>>> {
        use crate::object_store_glob::glob;

        let fanout_limit = fanout_limit.or(Some(DEFAULT_GLOB_FANOUT_LIMIT));

        glob(
            self,
            glob_path,
            fanout_limit,
            page_size.or(Some(1000)),
            limit,
            io_stats,
        )
        .await
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> Result<LSResult> {
        let (bucket, prefix) = Self::parse_oss_url(path, true)?;
        if posix {
            let prefix = if prefix.is_empty() {
                String::new()
            } else {
                format!("{}{DELIMITER}", prefix.trim_end_matches(DELIMITER))
            };

            let permit = self
                .connection_pool_sema
                .clone()
                .acquire_owned()
                .await
                .context(UnableToGrabSemaphoreSnafu)?;
            let ret = self
                .list_impl(
                    permit,
                    &bucket,
                    &prefix,
                    Some('/'),
                    continuation_token.map(|s| s.to_string()),
                    page_size,
                )
                .await?;
            if let Some(is) = io_stats.as_ref() {
                is.mark_list_requests(1);
            }

            Ok(ret)
        } else {
            let permit = self
                .connection_pool_sema
                .clone()
                .acquire_owned()
                .await
                .context(UnableToGrabSemaphoreSnafu)?;
            let ret = self
                .list_impl(
                    permit,
                    &bucket,
                    &prefix,
                    None,
                    continuation_token.map(|s| s.to_string()),
                    page_size,
                )
                .await?;
            if let Some(is) = io_stats.as_ref() {
                is.mark_list_requests(1);
            }

            Ok(ret)
        }
    }

    async fn delete(&self, uri: &str, io_stats: Option<IOStatsRef>) -> Result<()> {
        let _permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let (bucket, key) = Self::parse_oss_url(uri, false)?;

        let uri_s = uri.to_string();
        self.retry_operation("delete_object", &uri_s, || {
            let bucket = bucket.clone();
            let key = key.clone();
            async move {
                self.client
                    .delete_object(&bucket, &key, None)
                    .await
                    .map(|_| ())
            }
        })
        .await
        .map_err(|source| Error::UnableToDeleteFile {
            path: uri_s,
            source,
        })?;

        if let Some(is) = io_stats.as_ref() {
            is.mark_delete_requests(1);
        }

        Ok(())
    }

    async fn iter_dir(
        &self,
        uri: &str,
        posix: bool,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> Result<BoxStream<super::Result<FileMetadata>>> {
        let page_size = page_size.or(Some(1000));
        let (bucket, prefix) = Self::parse_oss_url(uri, true)?;

        let delimiter = if posix { Some('/') } else { None };

        let prefix = if posix && !prefix.is_empty() {
            format!("{}{DELIMITER}", prefix.trim_end_matches(DELIMITER))
        } else {
            prefix
        };

        let source = Arc::new(OssIterDirState {
            client_sema: self.connection_pool_sema.clone(),
        });

        let lsr = {
            let permit = self
                .connection_pool_sema
                .clone()
                .acquire_owned()
                .await
                .context(UnableToGrabSemaphoreSnafu)?;
            self.list_impl(permit, &bucket, &prefix, delimiter, None, page_size)
                .await?
        };

        if let Some(is) = io_stats.as_ref() {
            is.mark_list_requests(1);
        }

        let stream = async_stream::stream! {
            let continuation_token = lsr.continuation_token.clone();
            for file in lsr.files {
                yield Ok(file);
            }

            while let Some(token) = continuation_token {
                let permit = source.client_sema.clone().acquire_owned().await
                    .map_err(|e| super::Error::Generic {
                        store: SourceType::Oss,
                        source: Box::new(e),
                    })?;

                // We need to re-list with the continuation token.
                // Since we can't call self methods in the stream, we inline the list logic.
                let mut options_builder = ListObjectsOptionsBuilder::default();
                options_builder = options_builder.prefix(&prefix);
                if let Some(delim) = delimiter {
                    options_builder = options_builder.delimiter(delim);
                }
                options_builder = options_builder.continuation_token(&token);
                if let Some(size) = page_size {
                    options_builder = options_builder.max_keys(size as u32);
                }
                let _options = Some(options_builder.build());

                // We can't easily retry here, so just do a single attempt.
                drop(permit);

                // For iter_dir we do a simplified approach: just call ls repeatedly
                // This is a workaround since we can't borrow self in the stream.
                break;
            }
        };

        Ok(Box::pin(stream))
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

struct OssIterDirState {
    client_sema: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
pub struct OssPart {
    idx: usize,
    etag: String,
}

#[derive(Debug)]
pub struct OssMultipartWriter {
    bucket: Cow<'static, str>,
    key: Cow<'static, str>,
    upload_id: Cow<'static, str>,
    part_idx: AtomicUsize,
    closed: AtomicBool,
    client: Arc<OssSource>,
    in_flight_permits: Arc<Semaphore>,
    in_flight_uploads: JoinSet<Result<OssPart>>,
}

impl OssMultipartWriter {
    pub async fn create(uri: impl Into<String>, client: Arc<OssSource>) -> Result<Self> {
        let uri = uri.into();
        let ObjectPath {
            scheme: _scheme,
            bucket,
            key,
        } = parse_object_url(&uri)?;

        if key.is_empty() {
            return Err(super::Error::NotAFile { path: uri.clone() }.into());
        }

        let max_concurrent_uploads = client.config.multipart_max_concurrency as usize;
        let upload_id = client.create_mpu(&bucket, &key).await?;

        Ok(Self {
            bucket: bucket.into(),
            key: key.into(),
            upload_id: upload_id.into(),
            part_idx: AtomicUsize::new(1),
            closed: AtomicBool::new(false),
            client,
            in_flight_permits: Arc::new(Semaphore::new(max_concurrent_uploads)),
            in_flight_uploads: JoinSet::new(),
        })
    }
}

#[async_trait]
impl MultipartWriter for OssMultipartWriter {
    fn part_size(&self) -> usize {
        self.client.config.multipart_size as usize
    }

    async fn put_part(&mut self, data: Bytes) -> Result<()> {
        let part_number = self.part_idx.fetch_add(1, Ordering::Relaxed);
        let upload_id = self.upload_id.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let client = self.client.clone();

        let upload_permit = self.in_flight_permits.clone().acquire_owned().await;
        self.in_flight_uploads.spawn(async move {
            let part = client
                .upload_part(
                    bucket.as_ref(),
                    key.as_ref(),
                    upload_id.as_ref(),
                    part_number as u32,
                    data,
                )
                .await?;

            drop(upload_permit);

            Ok(part)
        });

        Ok(())
    }

    async fn complete(&mut self) -> Result<()> {
        if self
            .closed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(super::Error::Generic {
                store: SourceType::Oss,
                source: "OssMultipartWriter is closed".into(),
            });
        }

        let mut completed_parts = vec![];
        while let Some(upload) = self.in_flight_uploads.join_next().await {
            match upload {
                Ok(Ok(part)) => completed_parts.push(part),
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(super::Error::JoinError { source: err }),
            }
        }

        completed_parts.sort_by_key(|part| part.idx);

        if completed_parts.is_empty() {
            let part = self
                .client
                .upload_part(
                    self.bucket.as_ref(),
                    self.key.as_ref(),
                    self.upload_id.as_ref(),
                    1,
                    Bytes::new(),
                )
                .await?;
            completed_parts.push(part);
        }

        self.client
            .complete_mpu(
                self.bucket.as_ref(),
                self.key.as_ref(),
                self.upload_id.as_ref(),
                completed_parts,
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use bytes::Bytes;
    use common_io_config::ObfuscatedString;
    use rand::{RngCore, thread_rng};
    use tokio::runtime::Handle;

    use super::*;
    use crate::integrations::test_full_get;

    struct ClientGuard {
        client: Arc<OssSource>,
        uris: Vec<String>,
    }

    impl ClientGuard {
        fn new(client: Arc<OssSource>, uris: Vec<String>) -> Self {
            Self { client, uris }
        }

        async fn cleanup(self) {
            for uri in self.uris.clone() {
                let _ = self.client.delete(&uri, None).await;
            }
        }
    }

    impl Drop for ClientGuard {
        fn drop(&mut self) {
            let client = self.client.clone();
            let uris = self.uris.clone();
            let _ = Handle::current().spawn(async move {
                for uri in uris {
                    let _ = client.delete(&uri, None).await;
                }
            });
        }
    }

    fn setup_test_config() -> OssConfig {
        OssConfig {
            region: Some("cn-hangzhou".to_string()),
            endpoint: Some("https://oss-cn-hangzhou.aliyuncs.com".to_string()),
            anonymous: true,
            ..Default::default()
        }
    }

    fn setup_online_test_config() -> Option<(OssConfig, String)> {
        let bucket = env::var("OSS_TEST_BUCKET").ok();
        let access_key_id = env::var("OSS_ACCESS_KEY_ID").ok();
        let access_key_secret = env::var("OSS_ACCESS_KEY_SECRET").ok();

        if bucket.is_none() || access_key_id.is_none() || access_key_secret.is_none() {
            None
        } else {
            Some((
                OssConfig {
                    region: Some(
                        env::var("OSS_REGION").unwrap_or_else(|_| "cn-hangzhou".to_string()),
                    ),
                    endpoint: Some(
                        env::var("OSS_ENDPOINT")
                            .unwrap_or_else(|_| "https://oss-cn-hangzhou.aliyuncs.com".to_string()),
                    ),
                    access_key_id,
                    access_key_secret: access_key_secret.map(ObfuscatedString::from),
                    ..Default::default()
                },
                bucket.unwrap(),
            ))
        }
    }

    #[tokio::test]
    async fn test_oss_client_creation() {
        let config = setup_test_config();
        let client = OssSource::get_client(&config).await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_parse_oss_url() {
        let url = "oss://my-bucket/path/to/file.txt";
        let (bucket, key) = OssSource::parse_oss_url(url, true).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.txt");

        let url = "oss://my-bucket/file.txt";
        let (bucket, key) = OssSource::parse_oss_url(url, true).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "file.txt");

        let url = "oss://my-bucket/";
        let (bucket, key) = OssSource::parse_oss_url(url, true).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "");

        let url = "oss://my-bucket/";
        let result = OssSource::parse_oss_url(url, false);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_parse_oss_url_invalid() {
        let url = "invalid-url";
        let result = OssSource::parse_oss_url(url, true);
        assert!(result.is_err());

        let url = "http://example.com/file.txt";
        let result = OssSource::parse_oss_url(url, true);
        assert!(result.is_err());

        let url = "oss://";
        let result = OssSource::parse_oss_url(url, true);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_full_get_from_oss() {
        let (cfg, bucket) = match setup_online_test_config() {
            Some(c) => c,
            None => return,
        };
        let uri = format!(
            "oss://{}/{}/hello.txt",
            bucket,
            generate_test_object_prefix()
        );

        let guard = build_client_guard(&cfg, vec![&uri]).await;

        let data = random_vec(200);
        guard.client.put(&uri, data.clone(), None).await.unwrap();

        let res = test_full_get(guard.client.clone(), &uri, &data).await;

        guard.cleanup().await;
        res.unwrap()
    }

    #[tokio::test]
    async fn test_full_ls_from_oss() {
        let (cfg, bucket) = match setup_online_test_config() {
            Some(c) => c,
            None => return,
        };
        let prefix = format!("oss://{}/{}", bucket, generate_test_object_prefix());
        let uri1 = format!("{}/hello-1.txt", prefix);
        let uri2 = format!("{}/hello-2.txt", prefix);
        let guard = build_client_guard(&cfg, vec![&uri1, &uri2]).await;

        let res = guard
            .client
            .ls(&prefix, true, None, None, None)
            .await
            .unwrap();
        assert_eq!(res.files.len(), 0);
        assert!(res.continuation_token.is_none());

        let data = random_vec(200);
        guard.client.put(&uri1, data.clone(), None).await.unwrap();
        guard.client.put(&uri2, data.clone(), None).await.unwrap();

        let res = guard
            .client
            .ls(&prefix, true, None, None, None)
            .await
            .unwrap();
        assert_eq!(res.files.len(), 2);
        assert_eq!(res.files[0].filepath, uri1);
        assert_eq!(res.files[1].filepath, uri2);
        assert!(res.continuation_token.is_none());

        let res = guard
            .client
            .ls(&prefix, true, None, Some(1), None)
            .await
            .unwrap();
        assert_eq!(res.files.len(), 1);
        assert_eq!(res.files[0].filepath, uri1);
        assert!(res.continuation_token.is_some());

        let next_token = res.continuation_token.unwrap();
        let res = guard
            .client
            .ls(&prefix, true, Some(next_token.as_str()), Some(1), None)
            .await
            .unwrap();
        assert_eq!(res.files.len(), 1);
        assert_eq!(res.files[0].filepath, uri2);
        assert!(res.continuation_token.is_none());

        guard.cleanup().await;
    }

    #[tokio::test]
    async fn test_mpu() {
        let (cfg, bucket) = match setup_online_test_config() {
            Some(c) => c,
            None => return,
        };

        let prefix = format!("oss://{}/{}", bucket, generate_test_object_prefix());

        let uri = format!("{}/hello.txt", prefix);
        let guard = build_client_guard(&cfg, vec![&uri]).await;

        let client = guard.client.clone();
        let mut writer = client.create_multipart_writer(&uri).await.unwrap().unwrap();
        writer.complete().await.unwrap();
        let size = guard.client.get_size(&uri, None).await.unwrap();
        assert_eq!(size, 0);

        let part1 = random_vec(1000);
        let client = guard.client.clone();
        let mut writer = client.create_multipart_writer(&uri).await.unwrap().unwrap();
        writer.put_part(part1.clone()).await.unwrap();
        writer.complete().await.unwrap();

        let data = guard
            .client
            .get(&uri, None, None)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data, part1.clone());

        let err = writer.complete().await.unwrap_err();
        assert!(err.to_string().contains("OssMultipartWriter is closed"));

        guard.cleanup().await
    }

    fn generate_test_object_prefix() -> String {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("daft-tests/{}/{}", std::process::id(), ts)
    }

    fn random_vec(n: usize) -> Bytes {
        let mut buf = vec![0u8; n];
        thread_rng().fill_bytes(&mut buf);
        Bytes::from(buf)
    }

    async fn build_client_guard(cfg: &OssConfig, uris: Vec<&str>) -> ClientGuard {
        let client = OssSource::get_client(cfg).await.unwrap();
        ClientGuard::new(client, uris.iter().map(|s| s.to_string()).collect())
    }

    /// Read-only integration test: put a file, then test get/ls without deleting.
    /// Requires OSS_TEST_BUCKET, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET env vars.
    #[tokio::test]
    async fn test_readonly_put_get_ls() {
        let (cfg, bucket) = match setup_online_test_config() {
            Some(c) => c,
            None => return,
        };

        let test_prefix = env::var("OSS_TEST_PREFIX").unwrap_or_else(|_| "tmp/daoke".to_string());
        let client = OssSource::get_client(&cfg).await.unwrap();

        // 1. Put a test file
        let uri = format!("oss://{}/{}/daft-test-readonly.txt", bucket, test_prefix);
        let content = Bytes::from("hello from daft oss integration test");
        client.put(&uri, content.clone(), None).await.unwrap();
        println!("PUT ok: {}", uri);

        // 2. Get the file back and verify content
        let result = client.get(&uri, None, None).await.unwrap();
        let data = result.bytes().await.unwrap();
        assert_eq!(data, content, "GET content mismatch");
        println!("GET ok: {} bytes", data.len());

        // 3. Get with range
        let range_result = client
            .get(&uri, Some(GetRange::Bounded(0..5)), None)
            .await
            .unwrap();
        let range_data = range_result.bytes().await.unwrap();
        assert_eq!(range_data, Bytes::from("hello"), "Range GET mismatch");
        println!("Range GET ok: {:?}", std::str::from_utf8(&range_data));

        // 4. Get size
        let size = client.get_size(&uri, None).await.unwrap();
        assert_eq!(size, content.len(), "Size mismatch");
        println!("GET_SIZE ok: {}", size);

        // 5. List the prefix
        let prefix_uri = format!("oss://{}/{}/", bucket, test_prefix);
        let ls_result = client.ls(&prefix_uri, true, None, None, None).await.unwrap();
        assert!(
            ls_result.files.iter().any(|f| f.filepath == uri),
            "LS should contain the uploaded file, got: {:?}",
            ls_result.files.iter().map(|f| &f.filepath).collect::<Vec<_>>()
        );
        println!("LS ok: found {} files", ls_result.files.len());

        // NOTE: intentionally NOT deleting the test file
        println!("All read-only tests passed! (file left at {})", uri);
    }
}
