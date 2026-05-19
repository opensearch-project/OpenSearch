# Client-Side Streaming API Flow

```mermaid
flowchart TD
    %% Simple Client Flow
    START[Client sends streaming request] --> WAIT[Wait for response]
    
    WAIT --> RESPONSE{Response Type?}
    RESPONSE -->|Success| STREAM[handleStreamResponse called]
    RESPONSE -->|Error| ERROR[handleException called]
    RESPONSE -->|Timeout| TIMEOUT[Timeout exception]
    
    %% Stream Processing
    STREAM --> NEXT[Get next response]
    NEXT --> PROCESS[Process response]
    PROCESS --> CONTINUE{Continue?}
    CONTINUE -->|Yes| NEXT
    CONTINUE -->|No - Complete| CLOSE[streamResponse.close]
    CONTINUE -->|No - Cancel| CANCEL[streamResponse.cancel]
    
    %% Error & Completion
    ERROR --> HANDLE_ERROR[Handle error]
    TIMEOUT --> HANDLE_ERROR
    CLOSE --> SUCCESS[Complete]
    CANCEL --> SUCCESS
    HANDLE_ERROR --> SUCCESS
    
    %% Simple styling
    classDef client fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    classDef framework fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    classDef error fill:#ffebee,stroke:#c62828,stroke-width:2px
    
    class START,NEXT,PROCESS,CLOSE,CANCEL client
    class WAIT,STREAM,ERROR,TIMEOUT framework
    class HANDLE_ERROR error
    class RESPONSE,CONTINUE decision
```

## Simple Client Usage

### **Thread-Safe Implementation**:
```java
StreamTransportResponseHandler<MyResponse> handler = new StreamTransportResponseHandler<MyResponse>() {
    private volatile boolean cancelled = false;
    private volatile StreamTransportResponse<MyResponse> currentStream;
    
    @Override
    public void handleStreamResponse(StreamTransportResponse<MyResponse> streamResponse) {
        currentStream = streamResponse;
        
        if (cancelled) {
            handleTermination(streamResponse, "Handler already cancelled", null);
            return;
        }
        
        try {
            MyResponse response;
            while ((response = streamResponse.nextResponse()) != null) {  // BLOCKING CALL
                if (cancelled) {
                    handleTermination(streamResponse, "Processing cancelled", null);
                    return;
                }
                processResponse(response);
            }
            streamResponse.close();
        } catch (Exception e) {
            handleTermination(streamResponse, "Error: " + e.getMessage(), e);
        }
    }
    
    @Override
    public void handleException(TransportException exp) {
        cancelled = true;
        if (currentStream != null) {
            handleTermination(currentStream, "Exception occurred: " + exp.getMessage(), exp);
        }
        handleError(exp);
    }
    
    // Placeholder for custom termination logic
    private void handleTermination(StreamTransportResponse<MyResponse> streamResponse, String reason, Exception cause) {
        // Add custom cleanup/logging logic here
        streamResponse.cancel(reason, cause);
    }
};

transportService.sendRequest(node, "action", request, 
    TransportRequestOptions.builder().withType(STREAM).withTimeout(30s).build(), 
    handler);
```

### **Key Points**:
- **Blocking**: `nextResponse()` blocks waiting for server data - use background threads
- **Timeout Handling**: `handleException` can cancel active streams for timeout scenarios
- **Always Close/Cancel**: Stream must be closed or cancelled to prevent resource leaks