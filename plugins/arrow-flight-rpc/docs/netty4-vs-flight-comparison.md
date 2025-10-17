# Netty4 vs Flight Transport Comparison

This document compares the traditional Netty4 transport with the new Arrow Flight transport across all four communication flows.

## 1. Outbound Client: Netty4 vs. Flight

```mermaid
sequenceDiagram
    participant Client
    participant TS as TransportService
    participant CM as ConnectionManager
    participant C as Connection
    participant TC as TcpChannel<br/>(Netty4TcpChannel)
    participant NOH as NativeOutboundHandler
    participant N as Network

    Note over Client,N: Netty4 Flow
    Client->>TS: Send TransportRequest
    TS->>TS: Generate reqID
    TS->>CM: Get Connection
    CM->>C: Provide Connection
    C->>TC: Use Channel
    TC->>NOH: Serialize to BytesReference<br/>(StreamOutput) with reqID
    NOH->>N: Send BytesReference

    participant Client2
    participant STS as StreamTransportService
    participant CM2 as ConnectionManager
    participant C2 as Connection
    participant FTC as FlightTcpChannel
    participant FMH as FlightMessageHandler
    participant FC as FlightClientChannel
    participant N2 as Network

    Note over Client2,N2: Flight Flow
    Client2->>STS: Send TransportRequest
    STS->>STS: Generate reqID
    STS->>CM2: Get Connection
    CM2->>C2: Provide Connection
    C2->>FTC: Use Channel
    FTC->>FMH: Serialize to Flight Ticket<br/>(ArrowStreamOutput) with reqID
    FMH->>FC: Send Flight Ticket
    FC->>N2: Transmit Request
```

## 2. Inbound Server: Netty4 vs. Flight

```mermaid
sequenceDiagram
    participant STC as Server TcpChannel<br/>(Netty4TcpChannel)
    participant IP as InboundPipeline
    participant IH as InboundHandler
    participant NMH as NativeMessageHandler
    participant RH as RequestHandler

    Note over STC,RH: Netty4 Flow
    STC->>IP: Receive BytesReference
    IP->>IH: Deserialize to InboundMessage<br/>(StreamInput)
    IH->>NMH: Interpret as TransportRequest
    NMH->>RH: Process Request

    participant FS as FlightServer
    participant FP as FlightProducer
    participant IP2 as InboundPipeline
    participant IH2 as InboundHandler
    participant NMH2 as NativeMessageHandler
    participant RH2 as RequestHandler

    Note over FS,RH2: Flight Flow
    FS->>FP: Receive Flight Ticket
    FP->>FP: Create VectorSchemaRoot
    FP->>FP: Create FlightServerChannel
    FP->>IP2: Pass to InboundPipeline
    IP2->>IH2: Deserialize with ArrowStreamInput
    IH2->>NMH2: Interpret as TransportRequest
    NMH2->>RH2: Process Request
```

## 3. Outbound Server: Netty4 vs. Flight

```mermaid
sequenceDiagram
    participant RH as RequestHandler
    participant OH as OutboundHandler
    participant TTC as TcpTransportChannel
    participant TC as TcpChannel

    Note over RH,TC: Netty4 Flow
    RH->>TTC: sendResponse(TransportResponse)
    TTC->>OH: Serialize TransportResponse<br/>(via sendResponse)
    OH->>TC: Send Serialized Data to Client

    participant RH2 as RequestHandler
    participant FTC as FlightTransportChannel
    participant FOH as FlightOutboundHandler
    participant FSC as FlightServerChannel
    participant SSL as ServerStreamListener

    Note over RH2,SSL: Flight Flow
    RH2->>FTC: sendResponseBatch(TransportResponse)
    FTC->>FOH: sendResponseBatch
    FOH->>FSC: sendBatch(VectorSchemaRoot)
    FSC->>SSL: start(root) (first batch)
    FSC->>SSL: putNext() (stream batch)
    RH2->>FTC: completeStream()
    FTC->>FOH: completeStream
    FOH->>FSC: completeStream
    FSC->>SSL: completed() (end stream)
```

## 4. Inbound Client: Netty4 vs. Flight

```mermaid
sequenceDiagram
    participant CTC as Client TcpChannel<br/>(Netty4TcpChannel)
    participant CIP as Client InboundPipeline
    participant CIH as Client InboundHandler
    participant RH as ResponseHandler

    Note over CTC,RH: Netty4 Flow
    CTC->>CIP: Receive BytesReference
    CIP->>CIH: Deserialize to TransportResponse<br/>(StreamInput)
    CIH->>RH: Deliver Response

    participant FC as FlightClient
    participant FCC as FlightClientChannel
    participant FTR as FlightTransportResponse
    participant RH2 as ResponseHandler

    Note over FC,RH2: Flight Flow (Async Response Handling)
    FC->>FCC: handleInboundStream(Ticket, Listener)
    FCC->>FTR: Create FlightTransportResponse
    FCC->>FCC: Retrieve Header and reqID
    FCC->>RH2: Get TransportResponseHandler<br/>using reqID
    FCC->>RH2: handler.handleStreamResponse(streamResponse)<br/>(Async Processing)
```

## Key Differences Summary

### **Netty4 Transport (Traditional)**:
- **Request/Response**: Single request → single response pattern
- **Serialization**: BytesReference with StreamOutput/StreamInput
- **Channel**: Netty4TcpChannel with native handlers
- **Processing**: Synchronous response handling
- **Protocol**: Custom binary protocol over TCP

### **Flight Transport (New)**:
- **Streaming**: Single request → multiple response batches
- **Serialization**: Arrow Flight Ticket with ArrowStreamOutput/ArrowStreamInput
- **Channel**: FlightClientChannel/FlightServerChannel with Flight handlers
- **Processing**: Asynchronous stream processing with `nextResponse()` loop
- **Protocol**: Arrow Flight RPC over gRPC
