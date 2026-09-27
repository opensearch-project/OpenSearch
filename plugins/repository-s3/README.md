# repository-s3

The repository-s3 plugin enables the use of S3 as a place to store snapshots.

## S3-compatible stores without CRC32 response checksums

Parallel multipart uploads with remote integrity checking enabled require CRC32
checksums from the S3 service. A missing `UploadPart` response checksum fails
the upload with a descriptive error rather
than silently bypassing remote integrity verification. For a compatible service
that does not implement these checksums, set `remote_integrity_check_enabled` to
`false` in the S3 repository settings. For uploads with an expected file
checksum, this verifies the bytes read locally, but cannot verify the bytes
stored by the service.

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
Optional environment variables:

- `amazon_s3_path_style_access`: Possible values true or false. Default is false.
- `amazon_s3_endpoint`: s3 custom endpoint url if aws s3 default endpoint is not being used.
