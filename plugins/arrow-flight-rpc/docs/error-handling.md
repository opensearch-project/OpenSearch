# Arrow Flight RPC Error Handling Guidelines

## Overview

This document describes the error handling model for the Arrow Flight RPC transport in OpenSearch. The model is inspired by gRPC's error handling approach and provides a consistent way to handle errors across the transport boundary.

At the OpenSearch layer, `FlightRuntimeException` isn't directly exposed. Instead, `StreamException` is used, which is converted to and from `FlightRuntimeException` at the flight transport layer.

## Error Codes

The following error codes are available in `StreamErrorCode`:

| StreamErrorCode    | Description                                                |
|--------------------|------------------------------------------------------------|
| OK                 | Operation completed successfully                           |
| CANCELLED          | Operation was cancelled by the client                      |
| UNKNOWN            | Unknown error or unhandled server exception                |
| INVALID_ARGUMENT   | Invalid arguments provided                                 |
| TIMED_OUT          | Operation timed out                                        |
| NOT_FOUND          | Requested resource not found                               |
| ALREADY_EXISTS     | Resource already exists                                    |
| UNAUTHENTICATED    | Client not authenticated                                   |
| UNAUTHORIZED       | Client lacks permission for the operation                  |
| RESOURCE_EXHAUSTED | Resource limits exceeded                                   |
| UNIMPLEMENTED      | Operation not implemented                                  |
| INTERNAL           | Internal server error                                      |
| UNAVAILABLE        | Service unavailable or resource temporarily inaccessible   |

## Best Practices

### Throwing Errors

When throwing errors in server-side code:

```java
// For validation errors
throw new StreamException(StreamErrorCode.INVALID_ARGUMENT, "Invalid parameter: " + paramName);

// For resource not found
throw new StreamException(StreamErrorCode.NOT_FOUND, "Resource not found: " + resourceId);

// For internal errors
throw new StreamException(StreamErrorCode.INTERNAL, "Internal error", exception);

// For unavailable resources
throw new StreamException(StreamErrorCode.UNAVAILABLE, "Resource temporarily unavailable");

// For cancelled operations
throw StreamException.cancelled("Operation cancelled by user");
```

### Handling Errors

When handling errors in client-side code:

```java
try {
    // Operation that might throw StreamException
} catch (StreamException e) {
    switch (e.getErrorCode()) {
        case CANCELLED:
            // Handle cancellation
            break;
        case NOT_FOUND:
            // Handle resource not found
            break;
        case INVALID_ARGUMENT:
            // Handle validation error
            break;
        case UNAVAILABLE:
            // Handle temporary unavailability, maybe retry
            break;
        default:
            // Handle other errors
            break;
    }
}
```

### Stream Cancellation

When a stream is cancelled:

1. The client calls `streamResponse.cancel(reason, cause)`
2. The server receives a `StreamException` with `StreamErrorCode.CANCELLED`
3. The server should exit gracefully and not call `completeStream()` or `sendResponse()`

```java
try {
    while (hasMoreData()) {
        channel.sendResponseBatch(createResponse());
    }
    channel.completeStream();
} catch (StreamException e) {
    if (e.getErrorCode() == StreamErrorCode.CANCELLED) {
        // Client cancelled - exit gracefully
        logger.info("Stream cancelled by client: {}", e.getMessage());
        // Do NOT call completeStream() or sendResponse()
        return;
    }
    // Handle other stream errors
    throw e;
}
```

## Error Metadata

`StreamException` supports adding metadata for additional error context:

```java
StreamException exception = new StreamException(StreamErrorCode.INVALID_ARGUMENT, "Invalid query");
exception.addMetadata("query_id", queryId);
exception.addMetadata("index_name", indexName);
throw exception;
```

This metadata is preserved across the transport boundary and can be accessed on the receiving side:

```java
Map<String, String> metadata = streamException.getMetadata();
String queryId = metadata.get("query_id");
```