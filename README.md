# Unloading data from Redshift to Spectrum

You may wish to occasionally unload older or less-queried data out of your Redshift cluster to free space, or to downsize and reduce Redshift spending. High-volume but infrequently queried data are especially good candidates for this. Unloaded data can be stored in S3 in a format that remains queryable in Redshift via [Redshift Spectrum](https://aws.amazon.com/redshift/spectrum/). This script provides a quick way of doing so. It assumes that you have the necessary AWS IAM permissions for resources in Redshift, S3, and AWS Glue.

## Limitations
1. There is some hackiness here involving the manual escaping of text fields; look in the `unload_data` function to see details. `VARCHAR` values will be truncated after the first 50,000 characters.
1. This script unloads data in a newline-delimited text format, and therefore is only suitable for infrequently-queried data. For data you expect to work with more often you should consider some other alternative that will allow you to unload to a columnar format such as Parquet. [Spectrify](https://github.com/hellonarrativ/spectrify) might be a good choice, but do be aware that as of this writing it has [issues with round-trip handling of null values](https://github.com/hellonarrativ/spectrify/issues/6).
1. The script uses the `get_cluster_credentials` call on the Boto3 Redshift client to get Redshift login credentials, so whatever IAM context you run this under will need to have that permission (and several others). Changing the script to accept credentials from the environment or command line should be easy though.

## Usage

The script pulls rows from a Redshift table, archives them at a standardized location in S3, and registers the unloaded data as a table in the AWS Glue data catalog so that it can be queried by Spectrum.

|Argument|Purpose|
|---|---|
|--region|AWS region for the Redshift cluster and S3 bucket|
|--s3-bucket|S3 bucket in which unloads will be stored|
|--s3-path-prefix|Optional S3 path prefix for specifying where unloads are stored|
|--cluster|Name of the Redshift cluster being unloaded from|
|--db|Name of the database within the cluster that contains the table to be unloaded|
|--schema|Name of the schema that contains the table to be unloaded|
|--table|Name of the table to be unloaded|
|--db-user|Redshift user name to log in as when running UNLOADs and other queries. Will need to be pre-provisioned by you.|
|--iam-role-arn|ARN of the AWS IAM role used in the Redshift UNLOAD command|
|--audit-col|Optional name of the column used to select a subset of the tables records for unloading (e.g., a last_updated timestamp or similar). Pick something stable!|
|--audit-min-val|Optional, inclusive minimum value of the `audit_col` used to select records for unloading. If absent, no minimum bound is applied. This will be used exactly as provided in the WHERE clause of SQL queries, so the source DB will be responsible for interpreting e.g. its time zone. Provided you used `--delete-on-success` for prior runs you can usually leave this unspecified.|
|--audit-max-val|Optional, exclusive maximum value of the `audit_col` used to select records for unloading. If absent, no maximum bound is applied. This will be used exactly as provided in the WHERE clause of SQL queries, so the source DB will be responsible for interpreting e.g. its time zone.|
|--delete-on-success|If specified, unloaded rows will be deleted from the source Redshift tables if the unload finishes successfully. False by default but when using this "for real" you probably want it to be True!|
|--keep-on-failure|If specified, unloaded rows will be kept in the TARGET S3 (i.e. Spectrum) table even if the unload validation step fails. Keep unspecified (defaults to False) unless you need to debug failing unloads|

## Example invocation

Unload all rows of table `myschema.mytable` with values for `timestamp` up through and including `2018-01-31`. Once done, the data will ve queryable in Redshift from table `myschema_external.mytable`:

```
./unload_rs \
  --region us-east-1 \
  --s3-bucket my-unloads-bucket \
  --s3-path-prefix redshift-unloads \
  --cluster my-cluster \
  --db my-db \
  --schema myschema \
  --table mytable \
  --db-user awsglue \
  --iam-role-arn 'arn:aws:iam::111111111111:role/My-Redshift-Role' \
  --audit-col timestamp \
  --audit-max-val '2018-01-31'
```
