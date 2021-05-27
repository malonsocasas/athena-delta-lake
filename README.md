# Presentation

This is a very naive Scala implementation of a Athena-DeltaLake connector.

It leverages the power of [athena-federated-queries](https://github.com/awslabs/aws-athena-query-federation)
to be able to query [DeltaLake](https://delta.io/) tables stored on S3 without having to register those tables on AWS Glue (or any other Metastore).

This project is just intended to be a POC and is not prod-ready !

## Install

### Requirements

- SBT
- Scala 2.12
- Java 8

### Deployment

- Create Jar:
```
sbt fetchAthenaSdk assembly
```

- Upload Jar on S3
```
aws s3 cp target/scala-2.12/athena-delta-lake-assembly-0.1.jar <S3_LOCATION>
```

- Create an AWS Lambda and specify this Jar as the code source
- Specify 2 environment variables for this Lambda:
    - `spill_bucket` a bucket for athena to store temporary data
    - `data_bucket` the bucket containing your DeltaLake tables
- Configure Athena to use this Lambda as a DataSource
    - Data Sources > Connect data source > Query a data source > All other data sources > Choose Lambda Function
    - See [documentation](https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source-lambda.html)
- Select your new Data source
- Do some queries !

## Local run

### Launch Lambda locally 

Run docker image:

```bash
sbt fetchAthenaSdk assemblyPackageDependency
docker build --no-cache -t athenadelta .
docker run -p 9000:8080 --env-file .env athenadelta
```

### Call endpoints

#### Get Table
```bash
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"@type":"GetTableRequest","identity":{"id":"UNKNOWN","principal":"UNKNOWN","account":"0123456789","arn":"testArn","tags":{},"groups":[]},"queryId":"test-query-id","catalogName":"test-catalog","tableName":{"schemaName":"<MY_SCHEMA>","tableName":"<MY_TABLE>"}}'
```

# Known limitations

- Only primitives types are handled
- `SHOW PARTITIONS` is not working yet
- Only tables with year/month/day partitionning supported
- Only one bucket is handled for the moment
- All databases and tables should be in lower case
- No predicate pushdown
- Many things to do... (and add some unit tests)
- Clean assembly to avoid Jar exceeding the 250 MB Lambda code size limit