# repository-s3

The repository-s3 plugin enables the use of S3 as a place to store snapshots.

## Testing

### Unit Tests

```
./gradlew :plugins:repository-s3:test
```

### Integration Tests

Integration tests require several environment variables.

-  `amazon_s3_bucket`: Name of the S3 bucket to use.
-  `amazon_s3_access_key`: The access key ID (`AWS_ACCESS_KEY_ID`) with r/w access to the S3 bucket.
-  `amazon_s3_secret_key`: The secret access key (`AWS_SECRET_ACCESS_KEY`).
-  `amazon_s3_base_path`: A relative path inside the S3 bucket, e.g. `opensearch`.
-  `AWS_REGION`: The region in which the S3 bucket was created. While S3 buckets are global, credentials must scoped to a specific region and cross-region access is not allowed. (TODO: rename this to `amazon_s3_region` in https://github.com/opensearch-project/opensearch-build/issues/3615 and https://github.com/opensearch-project/OpenSearch/pull/7974.)

```
AWS_REGION=us-west-2 amazon_s3_access_key=$AWS_ACCESS_KEY_ID amazon_s3_secret_key=$AWS_SECRET_ACCESS_KEY amazon_s3_base_path=path amazon_s3_bucket=dblock-opensearch ./gradlew :plugins:repository-s3:s3ThirdPartyTest
```
