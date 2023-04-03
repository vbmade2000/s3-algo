// use super::*;

use std::{pin::Pin, sync::Arc};

use aws_sdk_s3::{
    operation::list_objects_v2::ListObjectsV2Output, primitives::ByteStream, types::Object,
};
use futures::{future::ok, stream, Future, Stream, StreamExt, TryStreamExt};
use snafu::ResultExt;
use tokio::{io, sync::Mutex};

use crate::{err, timeout::TimeoutState, Config, Error, RequestReport};

pub type NewListObjectsV2Result = Result<(RequestReport, ListObjectsV2Output), Error>;

/// A stream that can list objects, and (using member functions) delete or copy listed files.
pub struct NewListObjects<S> {
    s3: aws_sdk_s3::Client,
    config: Config,
    bucket: String,
    /// Common prefix (as requested) of the listed objects. Empty string if all objects were
    /// requestd.
    prefix: String,
    stream: S,
}
impl<S> NewListObjects<S>
where
    S: Stream<Item = NewListObjectsV2Result> + Sized + Send + 'static,
{
    pub fn boxed(
        self,
    ) -> NewListObjects<Pin<Box<dyn Stream<Item = NewListObjectsV2Result> + Send>>> {
        NewListObjects {
            s3: self.s3,
            config: self.config,
            bucket: self.bucket,
            stream: self.stream.boxed(),
            prefix: self.prefix,
        }
    }

    /// Calls an async closure on all the individual objects of the list operation
    pub async fn process<P, F>(self, f: P) -> Result<(), Error>
    where
        P: Fn(Object) -> F + Clone,
        F: Future<Output = ()>,
    {
        let NewListObjects {
            stream, prefix: _, ..
        } = self;
        stream
            .try_filter_map(|response| ok(response.1.contents))
            .map_ok(|x| stream::iter(x).map(Ok))
            .try_flatten()
            .try_for_each_concurrent(None, move |object| {
                let f = f.clone();
                async move {
                    f(object).await;
                    Ok(())
                }
            })
            .await
    }

    /// Download all listed objects - returns a stream of the contents.
    /// Used as a basis for other `download_all_*` functions.
    pub fn download_all_stream<R>(
        self,
        default_request: R,
    ) -> impl Stream<Item = Result<(String, ByteStream, i64), Error>>
    where
        R: Fn() -> GetObjectRequest + Clone + Unpin + Sync + Send + 'static,
    {
        let ListObjects {
            s3,
            config,
            bucket,
            stream,
            prefix: _,
        } = self;

        let timeout = Arc::new(Mutex::new(TimeoutState::new(
            config.algorithm.clone(),
            config.put_requests.clone(),
        )));
        let n_retries = self.config.algorithm.n_retries;
        stream
            .try_filter_map(|response| ok(response.1.contents))
            .map_ok(|x| stream::iter(x).map(Ok))
            .try_flatten()
            .map(|result| {
                result.and_then(|obj| {
                    let Object { key, size, .. } = obj;
                    if let (Some(key), Some(size)) = (key, size) {
                        Ok((key, size))
                    } else {
                        Err(err::Error::MissingKeyOrSize)
                    }
                })
            })
    }

    pub fn download_all_to_vec<R>(
        self,
        default_request: R,
    ) -> impl Stream<Item = Result<(String, Vec<u8>), Error>>
    where
        R: Fn() -> GetObjectRequest + Clone + Unpin + Sync + Send + 'static,
    {
        self.download_all_stream(default_request)
            .and_then(|(key, body, _)| async move {
                let mut contents = vec![];
                io::copy(&mut body.into_async_read(), &mut contents)
                    .await
                    .context(err::TokioIo)?;
                Ok((key, contents))
            })
    }
}
