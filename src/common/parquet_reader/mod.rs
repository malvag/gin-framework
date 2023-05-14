use std::error::Error;
use futures::future::BoxFuture;
use regex::Regex;

use s3::{
    bucket::Bucket,
    region::Region,
    creds::Credentials,
};

use range_reader::{ RangedAsyncReader, RangeOutput };

use arrow2::io::parquet::{
    read,
    write::FileMetaData,
};

use crate::common::common::S3Configuration;

pub struct ParquetReader {
    reader: RangedAsyncReader,
}

impl ParquetReader {

    pub async fn new (s3_conf: &S3Configuration, uri: &String) -> Result<ParquetReader, Box<dyn Error>> {
        let S3Configuration { region, endpoint, access_key, secret_key } = s3_conf;
        let region = Region::Custom {
            region: region.to_string(),
            endpoint: endpoint.to_string(),
        };
        let credentials = Credentials {
            access_key: Some(access_key.to_string()),
            secret_key: Some(secret_key.to_string()),
            security_token: None,
            session_token: None,
            expiration: None,
        };

        let (bucket_name, path) = ParquetReader::split_uri(uri);

        let bucket = Bucket::new(bucket_name.as_str(), region, credentials)?.with_path_style();
        let length = bucket.head_object(&path).await?.0.content_length.unwrap() as usize;

        let ranged_get = Box::new(move |start: u64, length: usize| {
            let bucket = bucket.clone();
            let path = path.clone();

            Box::pin(async move {
                let bucket = bucket.clone();
                let path = path.clone();
                let mut data = bucket
                    .get_object_range(path, start, Some(start + length as u64 - 1))
                    .await
                    .map_err(|x| {
                        std::io::Error::new(std::io::ErrorKind::Other, x.to_string())
                    })?
                    .to_vec();
                data.truncate(length);
                Ok(RangeOutput { start, data })
            }) as BoxFuture<'static, std::io::Result<RangeOutput>>
        });

        let min_request_size = 4 * 1024;
        // reader_factory
        let reader_factory = || {
            Box::pin(futures::future::ready(Ok(RangedAsyncReader::new(
                length,
                min_request_size,
                ranged_get.clone()
            )))) as BoxFuture<'static, std::result::Result<RangedAsyncReader, std::io::Error>>
        };

        let reader = reader_factory().await?;

        Ok(ParquetReader { reader })
    }

    // Note: try send metadata to executors + rowgroup number
    pub async fn read_metadata(&mut self) -> Result<FileMetaData, Box<dyn Error>> {
        let metadata = read::read_metadata_async(&mut self.reader).await?;
        Ok(metadata)
    }

    // TODO: wrong uri error
    fn split_uri(uri: &String) -> (String, String) {
        let reg = Regex::new(r"s3:\/\/([^\/]+)\/(.*)").unwrap(); // `s3://{bucket_name}/{path}`
        let captures = reg.captures(&uri).unwrap();
        
        (captures[1].to_string(), captures[2].to_string())
    }
}