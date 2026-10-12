# S3 API call timeout

The `api_call_timeout` setting limits one AWS SDK API call, including its retry
attempts and backoff delays. It applies to both the synchronous and asynchronous
S3 clients.

The default is `0`, which leaves the SDK API call timeout unset and preserves
existing behavior. Negative values are rejected. The existing `request_timeout`
setting continues to limit each individual attempt.

Configure a named client in `opensearch.yml`:

```yaml
s3.client.default.api_call_timeout: 10m
```

A repository can override its client's value through its repository settings:

```json
PUT _snapshot/my_repository
{
  "type": "s3",
  "settings": {
    "bucket": "my-bucket",
    "client": "default",
    "api_call_timeout": "15m"
  }
}
```

Set the repository value to `0` to disable a timeout inherited from its client.
A total timeout shorter than `request_timeout` ends the entire call before the
individual attempt's timeout can expire.

This setting limits individual SDK calls. It does not set a deadline for an
entire snapshot or multipart upload. For a synchronous GET that returns an
input stream, the API call ends when the SDK returns that stream; subsequent
body reads remain subject to the socket timeout.
