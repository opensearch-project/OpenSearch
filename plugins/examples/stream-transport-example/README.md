# Stream Transport Example

Step-by-step guide to implement streaming transport actions in OpenSearch.

## Step 1: Create Action Definition

```java
public class MyStreamAction extends ActionType<MyResponse> {
    public static final MyStreamAction INSTANCE = new MyStreamAction();
    public static final String NAME = "cluster:admin/my_stream";

    private MyStreamAction() {
        super(NAME, MyResponse::new);
    }
}
```

## Step 2: Create Request/Response Classes

```java
public class MyRequest extends ActionRequest {
    private int count;

    public MyRequest(int count) { this.count = count; }
    public MyRequest(StreamInput in) throws IOException { count = in.readInt(); }

    @Override
    public void writeTo(StreamOutput out) throws IOException { out.writeInt(count); }
}

public class MyResponse extends ActionResponse {
    private String message;

    public MyResponse(String message) { this.message = message; }
    public MyResponse(StreamInput in) throws IOException { message = in.readString(); }

    @Override
    public void writeTo(StreamOutput out) throws IOException { out.writeString(message); }
}
```

## Step 3: Create Transport Action

```java
public class TransportMyStreamAction extends TransportAction<MyRequest, MyResponse> {

    @Inject
    public TransportMyStreamAction(StreamTransportService streamTransportService, ActionFilters actionFilters) {
        super(MyStreamAction.NAME, actionFilters, streamTransportService.getTaskManager());

        // Register streaming handler
        streamTransportService.registerRequestHandler(
            MyStreamAction.NAME,
            ThreadPool.Names.GENERIC,
            MyRequest::new,
            this::handleStreamRequest
        );
    }

    @Override
    protected void doExecute(Task task, MyRequest request, ActionListener<MyResponse> listener) {
        listener.onFailure(new UnsupportedOperationException("Use StreamTransportService"));
    }

    private void handleStreamRequest(MyRequest request, TransportChannel channel, Task task) {
        try {
            for (int i = 1; i <= request.getCount(); i++) {
                MyResponse response = new MyResponse("Item " + i);
                channel.sendResponseBatch(response);
            }
            channel.completeStream();
        } catch (StreamException e) {
            if (e.getErrorCode() == StreamErrorCode.CANCELLED) {
                // Client cancelled - exit gracefully
            } else {
                channel.sendResponse(e);
            }
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }
}
```

## Step 4: Register in Plugin

```java
public class MyPlugin extends Plugin implements ActionPlugin {
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.singletonList(
            new ActionHandler<>(MyStreamAction.INSTANCE, TransportMyStreamAction.class)
        );
    }
}
```

## Step 5: Client Usage

```java
StreamTransportResponseHandler<MyResponse> handler = new StreamTransportResponseHandler<MyResponse>() {
    @Override
    public void handleStreamResponse(StreamTransportResponse<MyResponse> streamResponse) {
        try {
            MyResponse response;
            while ((response = streamResponse.nextResponse()) != null) {
                // Process each response
                System.out.println(response.getMessage());
            }
            streamResponse.close();
        } catch (Exception e) {
            streamResponse.cancel("Error", e);
        }
    }

    @Override
    public void handleException(TransportException exp) {
        // Handle errors
    }

    @Override
    public String executor() { return ThreadPool.Names.GENERIC; }

    @Override
    public MyResponse read(StreamInput in) throws IOException {
        return new MyResponse(in);
    }
};

streamTransportService.sendRequest(node, MyStreamAction.NAME, request, handler);
```

## Key Rules

1. **Server**: Always call `completeStream()` or `sendResponse(exception)`
2. **Client**: Always call `close()` or `cancel()` on stream
3. **Cancellation**: Handle `StreamException` with `CANCELLED` code gracefully
4. **Node-to-Node Only**: Streaming works only between cluster nodes
