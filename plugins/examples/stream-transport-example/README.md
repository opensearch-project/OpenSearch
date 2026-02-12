# Stream Transport Example

Example plugin demonstrating streaming transport actions in OpenSearch.

## Overview

This plugin demonstrates how to implement streaming transport actions that can send multiple responses for a single request. It shows two patterns:

1. **Basic Streaming Action** - Simple streaming action for single-target operations
2. **Nodes Streaming Action** - Streaming action that coordinates responses from multiple nodes

## Architecture

The streaming implementation uses a simplified architecture:

- **StreamTransportAction** - Base class that extends `TransportAction` and implements `doExecute()` to extract the channel from the listener
- **HandledStreamTransportAction** - Convenience class that registers streaming handlers automatically (similar to `HandledTransportAction`)
- **StreamTransportNodesAction** - Base class for multi-node streaming operations
- **NodeClient.executeStream()** - Client method to invoke streaming actions

## Client Usage

Use `NodeClient.executeStream()` to invoke streaming actions:

```java
nodeClient.executeStream(
    StreamDataAction.INSTANCE,
    new StreamDataRequest(5, 100),
    new StreamTransportResponseHandler<StreamDataResponse>() {
        @Override
        public void handleStreamResponse(StreamTransportResponse<StreamDataResponse> streamResponse) {
            try {
                StreamDataResponse response;
                while ((response = streamResponse.nextResponse()) != null) {
                    // Process each response
                    System.out.println("Received: " + response.getMessage());
                }
                streamResponse.close();
            } catch (Exception e) {
                streamResponse.cancel("Client error", e);
            }
        }

        @Override
        public void handleException(TransportException exp) {
            // Handle transport error
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public StreamDataResponse read(StreamInput in) throws IOException {
            return new StreamDataResponse(in);
        }
    }
);
```

## Basic Streaming Action

### 1. Define Action
```java
public class StreamDataAction extends ActionType<StreamDataResponse> {
    public static final StreamDataAction INSTANCE = new StreamDataAction();
    public static final String NAME = "cluster:admin/stream_data";

    private StreamDataAction() {
        super(NAME, StreamDataResponse::new);
    }
}
```

### 2. Implement Transport Action

Extend `HandledStreamTransportAction` and implement `executeStream()`:

```java
public class StreamTransportDataAction extends HandledStreamTransportAction<StreamDataRequest, StreamDataResponse> {

    @Inject
    public StreamTransportDataAction(
        StreamTransportService streamTransportService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(StreamDataAction.NAME, transportService, streamTransportService, actionFilters, StreamDataRequest::new);
    }

    @Override
    protected void executeStream(Task task, StreamDataRequest request, TransportChannel channel) throws IOException {
        try {
            // Send multiple batched responses
            for (int i = 1; i <= request.getCount(); i++) {
                StreamDataResponse response = new StreamDataResponse("Item " + i, i, i == request.getCount());
                channel.sendResponseBatch(response);

                if (i < request.getCount() && request.getDelayMs() > 0) {
                    Thread.sleep(request.getDelayMs());
                }
            }

            // Complete the stream
            channel.completeStream();
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }
}
```

### 3. Register Action
```java
public class MyPlugin extends Plugin implements ActionPlugin {
    @Override
    public List<ActionHandler<?, ?>> getActions() {
        return List.of(
            new ActionHandler<>(StreamDataAction.INSTANCE, StreamTransportDataAction.class)
        );
    }
}
```

## Nodes Streaming Action

For multi-node operations, extend `StreamTransportNodesAction`:

```java
public class StreamTransportNodesDataAction extends StreamTransportNodesAction<
    StreamNodesDataRequest,
    StreamNodesDataResponse,
    NodeStreamDataRequest,
    NodeStreamDataResponse> {

    @Inject
    public StreamTransportNodesDataAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        StreamTransportService streamTransportService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(
            StreamNodesDataAction.NAME,
            threadPool,
            clusterService,
            streamTransportService,
            transportService,
            actionFilters,
            StreamNodesDataRequest::new,
            NodeStreamDataRequest::new,
            ThreadPool.Names.GENERIC
        );
    }

    @Override
    protected void nodeStreamOperation(NodeStreamDataRequest request, TransportChannel channel, Task task) throws IOException {
        try {
            DiscoveryNode localNode = transportService.getLocalNode();

            for (int i = 1; i <= request.getCount(); i++) {
                NodeStreamDataResponse response = new NodeStreamDataResponse(
                    localNode,
                    "Node " + localNode.getName() + " - item " + i,
                    i
                );
                channel.sendResponseBatch(response);
            }

            channel.completeStream();
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }

    @Override
    protected NodeStreamDataRequest newNodeRequest(StreamNodesDataRequest request) {
        return new NodeStreamDataRequest(request);
    }

    @Override
    protected NodeStreamDataResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeStreamDataResponse(in);
    }

    @Override
    protected StreamNodesDataResponse newResponse(
        StreamNodesDataRequest request,
        List<NodeStreamDataResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new StreamNodesDataResponse(clusterService.getClusterName(), responses, failures);
    }
}
```

## Key Points

### Server Side (Action Implementation)
- Extend `HandledStreamTransportAction` for basic streaming or `StreamTransportNodesAction` for multi-node streaming
- Implement `executeStream()` (basic) or `nodeStreamOperation()` (nodes) method
- Use `channel.sendResponseBatch()` to send each response
- Always call `channel.completeStream()` when done
- Use `channel.sendResponse(exception)` for errors

### Client Side (Using the Action)
- Use `NodeClient.executeStream()` with `StreamTransportResponseHandler`
- Iterate responses with `while ((response = streamResponse.nextResponse()) != null)`
- Always call `streamResponse.close()` on success
- Use `streamResponse.cancel()` on error

### How It Works
1. Client calls `NodeClient.executeStream()` which gets the action from the registry
2. Action is cast to `StreamTransportAction` and `executeStreamRequest()` is called
3. Request is sent to local node via `StreamTransportService` using the streaming port
4. `StreamTransportService` routes to the registered handler
5. Handler creates a `StreamingActionListener` that provides access to the channel
6. Action's `executeStream()` is called through the standard action filter chain
7. Action sends multiple responses via `channel.sendResponseBatch()`
8. Client receives responses as they arrive

### Benefits
- **Efficient**: Stream large result sets without loading everything in memory
- **Responsive**: Client can process results as they arrive
- **Flexible**: Handler controls how to process the stream
- **Standard**: Uses familiar OpenSearch patterns (extends `TransportAction`, uses action filters)

## Running the Example

1. Build OpenSearch with the plugin:
```bash
./gradlew :plugins:examples:stream-transport-example:assemble
```

2. Start OpenSearch with streaming enabled:
```bash
./gradlew run -Dopensearch.experimental.feature.transport.stream.enabled=true
```

3. Run the integration tests:
```bash
./gradlew :plugins:examples:stream-transport-example:internalClusterTest
```

## Example Actions

### Basic Streaming (`StreamDataAction`)
- Sends multiple data items in sequence
- Demonstrates basic streaming pattern
- Located in `org.opensearch.example.stream.basic`

### Nodes Streaming (`StreamNodesDataAction`)
- Coordinates streaming from multiple nodes
- Each node sends multiple responses
- Coordinator aggregates and streams back to client
- Located in `org.opensearch.example.stream.nodes`

## Notes

- Streaming transport requires the `STREAM_TRANSPORT` feature flag to be enabled
- The example uses Apache Arrow Flight for the underlying streaming protocol
- Both actions are registered in `StreamTransportExamplePlugin`
- The implementation follows the same pattern as `StreamSearchTransportService`
