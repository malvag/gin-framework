#  Gin - A Distributed Data Processing Framework

Gin is a Distributed Data Processing Framework implemented in Rust, offering a DataFrame interface for processing Parquet files. Gin Library provides an API that allows `reading` Parquet files from S3 into a Dataframe and lazily run the transformations on it which are then triggered by actions.

Gin currently supports:

* Transformations:
    * `select` - Select columns.
    * `filter` - Filter rows.
* Actions:
    * `width` - Get the number of columns.
    * `count` - Count the number of rows.
    * `sum` - Sum of the values of the specified column.

## Prerequisites

```
$ apt install -y protobuf-compiler
```

## Installation

```
$ git clone https://github.com/malvag/gin-framework.git
$ cd gin-framework
$ cargo build
```

## Run Gin

Start a Gin Cluster, specifying the number of executors.

```
$ ./gin-context start 4
```

Run the test client.

Note: You should change the S3 Credentials first and set up the bucket. We used MinIO for simplicity.

```
$ cargo run
```

Stop the cluster.

```
$ ./gin-context stop
```

## Run MinIO

Run MinIO localy using docker.

```
$ docker run -p 9000:9000 -p 9090:9090 --name minio \
    -v ~/rust_c/s3-read/minio/data:/data \
    -e "MINIO_ROOT_USER=minioadmin" \
    -e "MINIO_ROOT_PASSWORD=minioadmin" \
    quay.io/minio/minio server /data --console-address ":9090"
```

As dataset, we used the yellow-taxi parquet file.

## Contributors

* Alexandra Alexandridi
* Evangelos Maliaroudakis
* Ioannis Melidonis
