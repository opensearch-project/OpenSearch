/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.protobufs.services.SearchServiceGrpc;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.grpc.listeners.CreatePitRequestActionListener;
import org.opensearch.transport.grpc.listeners.DeletePitRequestActionListener;
import org.opensearch.transport.grpc.listeners.SearchRequestActionListener;
import org.opensearch.transport.grpc.proto.request.search.CreatePitRequestProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.DeletePitRequestProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.SearchRequestProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;
import org.opensearch.transport.grpc.spi.AggregationBuilderProtoConverterRegistry;
import org.opensearch.transport.grpc.spi.StreamSearchFormatHandler;
import org.opensearch.transport.grpc.stream.GrpcStreamSearchResponseChannel;
import org.opensearch.transport.grpc.stream.InputStreamMarshaller;
import org.opensearch.transport.grpc.stream.StreamReset;
import org.opensearch.transport.grpc.util.CircuitBreakerStreamObserver;
import org.opensearch.transport.grpc.util.GrpcErrorHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

/**
 * Implementation of the gRPC SearchService.
 * This class handles incoming gRPC search requests, converts them to OpenSearch search requests,
 * executes them using the provided client, and returns the results back to the gRPC client.
 */
public class SearchServiceImpl extends SearchServiceGrpc.SearchServiceImplBase {
    private static final Logger logger = LogManager.getLogger(SearchServiceImpl.class);
    private final Client client;
    private final AbstractQueryBuilderProtoUtils queryUtils;
    private final AggregationBuilderProtoConverterRegistry aggregationRegistry;
    private final AggregateProtoConverterRegistry aggregateRegistry;
    private final CircuitBreakerService circuitBreakerService;
    private final Map<String, StreamSearchFormatHandler> streamSearchHandlers;
    private final AtomicBoolean reportedUnresettableCalls = new AtomicBoolean();

    private static final Metadata.Key<String> STREAM_FORMAT_HEADER = Metadata.Key.of(
        StreamSearchFormatHandler.FORMAT_HEADER,
        Metadata.ASCII_STRING_MARSHALLER
    );
    private static final Context.Key<String> STREAM_FORMAT_CONTEXT_KEY = Context.key(StreamSearchFormatHandler.FORMAT_HEADER);
    private static final Context.Key<Runnable> STREAM_RESET_CONTEXT_KEY = Context.key("opensearch-stream-reset");

    /**
     * Creates a new SearchServiceImpl.
     *
     * @param client Client for executing actions on the local node
     * @param queryUtils Query utils instance for parsing protobuf queries
     * @param aggregationRegistry Aggregation registry for parsing protobuf aggregations (request-side)
     * @param aggregateRegistry Aggregate registry for converting aggregation results (response-side)
     * @param circuitBreakerService Circuit breaker service for tracking in-flight requests
     */
    public SearchServiceImpl(
        Client client,
        AbstractQueryBuilderProtoUtils queryUtils,
        AggregationBuilderProtoConverterRegistry aggregationRegistry,
        AggregateProtoConverterRegistry aggregateRegistry,
        CircuitBreakerService circuitBreakerService
    ) {
        this(client, queryUtils, aggregationRegistry, aggregateRegistry, circuitBreakerService, Map.of());
    }

    /**
     * Creates a new SearchServiceImpl that also serves {@code ServerStreamSearch}.
     *
     * @param client Client for executing actions on the local node
     * @param queryUtils Query utils instance for parsing protobuf queries
     * @param aggregationRegistry Aggregation registry for parsing protobuf aggregations (request-side)
     * @param aggregateRegistry Aggregate registry for converting aggregation results (response-side)
     * @param circuitBreakerService Circuit breaker service for tracking in-flight requests
     * @param streamSearchHandlers Handlers of {@code ServerStreamSearch}, keyed by the format they serve
     */
    public SearchServiceImpl(
        Client client,
        AbstractQueryBuilderProtoUtils queryUtils,
        AggregationBuilderProtoConverterRegistry aggregationRegistry,
        AggregateProtoConverterRegistry aggregateRegistry,
        CircuitBreakerService circuitBreakerService,
        Map<String, StreamSearchFormatHandler> streamSearchHandlers
    ) {
        if (client == null) {
            throw new IllegalArgumentException("Client cannot be null");
        }
        if (queryUtils == null) {
            throw new IllegalArgumentException("Query utils cannot be null");
        }
        if (aggregationRegistry == null) {
            throw new IllegalArgumentException("Aggregation registry cannot be null");
        }
        if (aggregateRegistry == null) {
            throw new IllegalArgumentException("Aggregate registry cannot be null");
        }
        if (circuitBreakerService == null) {
            throw new IllegalArgumentException("Circuit breaker service cannot be null");
        }
        if (streamSearchHandlers == null) {
            throw new IllegalArgumentException("Stream search handlers cannot be null");
        }

        this.client = client;
        this.queryUtils = queryUtils;
        this.aggregationRegistry = aggregationRegistry;
        this.aggregateRegistry = aggregateRegistry;
        this.circuitBreakerService = circuitBreakerService;
        this.streamSearchHandlers = Map.copyOf(streamSearchHandlers);
    }

    /**
     * Returns this service with {@code ServerStreamSearch} bound to a pass-through response marshaller, so that format
     * handlers write the wire bytes of each message. The bound method also receives the
     * {@value StreamSearchFormatHandler#FORMAT_HEADER} request header. Register the result instead of this instance.
     *
     * @return the service to register with the gRPC server
     */
    public BindableService withServerStreamSearch() {
        return this::bindServiceWithServerStreamSearch;
    }

    private ServerServiceDefinition bindServiceWithServerStreamSearch() {
        final ServerServiceDefinition generated = bindService();
        final ServiceDescriptor generatedDescriptor = generated.getServiceDescriptor();
        final MethodDescriptor<org.opensearch.protobufs.SearchRequest, org.opensearch.protobufs.SearchResponse> generatedStreamMethod =
            SearchServiceGrpc.getServerStreamSearchMethod();
        final MethodDescriptor<org.opensearch.protobufs.SearchRequest, InputStream> streamMethod = generatedStreamMethod.toBuilder(
            generatedStreamMethod.getRequestMarshaller(),
            InputStreamMarshaller.INSTANCE
        ).build();

        final ServiceDescriptor.Builder descriptor = ServiceDescriptor.newBuilder(generatedDescriptor.getName())
            .setSchemaDescriptor(generatedDescriptor.getSchemaDescriptor());
        for (MethodDescriptor<?, ?> method : generatedDescriptor.getMethods()) {
            descriptor.addMethod(method.getFullMethodName().equals(streamMethod.getFullMethodName()) ? streamMethod : method);
        }
        final ServerServiceDefinition.Builder definition = ServerServiceDefinition.builder(descriptor.build());
        for (ServerMethodDefinition<?, ?> method : generated.getMethods()) {
            if (method.getMethodDescriptor().getFullMethodName().equals(streamMethod.getFullMethodName()) == false) {
                definition.addMethod(method);
            }
        }
        final ServerCallHandler<org.opensearch.protobufs.SearchRequest, InputStream> streamHandler = ServerCalls.asyncServerStreamingCall(
            this::serverStreamSearchRaw
        );
        // The method's handler receives the call that gRPC created, unless an interceptor wrapped it. A stream reset needs
        // that call.
        definition.addMethod(
            streamMethod,
            (call, headers) -> Contexts.interceptCall(
                Context.current()
                    .withValues(STREAM_FORMAT_CONTEXT_KEY, headers.get(STREAM_FORMAT_HEADER), STREAM_RESET_CONTEXT_KEY, streamReset(call)),
                call,
                headers,
                streamHandler
            )
        );
        return definition.build();
    }

    /**
     * Returns a task that resets the stream of {@code call}, see {@link StreamReset}, or {@code null} if the stream cannot
     * be reset. Warns once about calls that an interceptor wrapped.
     */
    @Nullable
    private Runnable streamReset(ServerCall<?, ?> call) {
        final Runnable reset = StreamReset.of(call);
        // StreamReset logs missing internals when it looks them up. Otherwise a null reset means an interceptor wrapped the call.
        if (reset == null && StreamReset.isAvailable() && reportedUnresettableCalls.compareAndSet(false, true)) {
            logger.warn(
                "cannot reset the streams of ServerStreamSearch calls of type [{}], which wraps gRPC's own: a client that stops "
                    + "reading keeps the messages queued for it, and their memory, until its connection closes",
                call.getClass().getName()
            );
        }
        return reset;
    }

    /**
     * Streams search results in the format named by the {@value StreamSearchFormatHandler#FORMAT_HEADER} request header.
     *
     * @param request The search request to process
     * @param responseObserver The observer of the call; responses are raw message streams
     */
    void serverStreamSearchRaw(org.opensearch.protobufs.SearchRequest request, StreamObserver<InputStream> responseObserver) {
        final ServerCallStreamObserver<InputStream> call = (ServerCallStreamObserver<InputStream>) responseObserver;
        final String format = STREAM_FORMAT_CONTEXT_KEY.get();
        if (format == null) {
            call.onError(
                Status.UNIMPLEMENTED.withDescription(
                    "ServerStreamSearch requires the [" + StreamSearchFormatHandler.FORMAT_HEADER + "] request header"
                ).asRuntimeException()
            );
            return;
        }
        final StreamSearchFormatHandler handler = streamSearchHandlers.get(format);
        if (handler == null) {
            call.onError(
                Status.INVALID_ARGUMENT.withDescription(
                    "unsupported ServerStreamSearch format [" + format + "], supported formats are " + streamSearchHandlers.keySet()
                ).asRuntimeException()
            );
            return;
        }

        final int requestSize = request.getSerializedSize();
        final CircuitBreaker breaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        try {
            breaker.addEstimateBytesAndMaybeBreak(requestSize, "<grpc_request>");
        } catch (CircuitBreakingException e) {
            logger.debug("Circuit breaker tripped for gRPC stream search request: {}", e.getMessage());
            call.onError(GrpcErrorHandler.convertToGrpcError(e));
            return;
        }
        final GrpcStreamSearchResponseChannel channel = new GrpcStreamSearchResponseChannel(
            call,
            STREAM_RESET_CONTEXT_KEY.get(),
            aggregateRegistry,
            () -> breaker.addWithoutBreaking(-requestSize)
        );
        try {
            final org.opensearch.action.search.SearchRequest searchRequest = SearchRequestProtoUtils.prepareRequest(
                request,
                client,
                queryUtils,
                aggregationRegistry
            );
            handler.execute(searchRequest, channel);
        } catch (RuntimeException | IOException e) {
            logger.debug("SearchServiceImpl failed to process stream search request, error={}", e.getMessage());
            channel.fail(e);
        }
    }

    /**
     * Processes a search request.
     *
     * @param request The search request to process
     * @param responseObserver The observer to send the response back to the client
     */
    @Override
    public void search(
        org.opensearch.protobufs.SearchRequest request,
        StreamObserver<org.opensearch.protobufs.SearchResponse> responseObserver
    ) {
        int requestSize = request.getSerializedSize();
        CircuitBreaker breaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);

        try {
            breaker.addEstimateBytesAndMaybeBreak(requestSize, "<grpc_request>");

            StreamObserver<org.opensearch.protobufs.SearchResponse> wrappedObserver = new CircuitBreakerStreamObserver<>(
                responseObserver,
                circuitBreakerService,
                requestSize
            );

            org.opensearch.action.search.SearchRequest searchRequest = SearchRequestProtoUtils.prepareRequest(
                request,
                client,
                queryUtils,
                aggregationRegistry
            );
            SearchRequestActionListener listener = new SearchRequestActionListener(wrappedObserver, aggregateRegistry);
            client.search(searchRequest, listener);
        } catch (CircuitBreakingException e) {
            logger.debug("Circuit breaker tripped for gRPC search request: {}", e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        } catch (RuntimeException | IOException e) {
            breaker.addWithoutBreaking(-requestSize);
            logger.debug("SearchServiceImpl failed to process search request, request=" + request + ", error=" + e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    /**
     * Processes a create PIT request.
     *
     * @param request The PIT request to process
     * @param responseObserver The observer to send the response back to the client
     */
    @Override
    public void createPit(
        org.opensearch.protobufs.CreatePitRequest request,
        StreamObserver<org.opensearch.protobufs.CreatePITResponse> responseObserver
    ) {
        int requestSize = request.getSerializedSize();
        CircuitBreaker breaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);

        try {
            breaker.addEstimateBytesAndMaybeBreak(requestSize, "<grpc_request>");

            StreamObserver<org.opensearch.protobufs.CreatePITResponse> wrappedObserver = new CircuitBreakerStreamObserver<>(
                responseObserver,
                circuitBreakerService,
                requestSize
            );

            org.opensearch.action.search.CreatePitRequest createPitRequest = CreatePitRequestProtoUtils.prepareRequest(request);
            CreatePitRequestActionListener listener = new CreatePitRequestActionListener(wrappedObserver);
            client.createPit(createPitRequest, listener);
        } catch (CircuitBreakingException e) {
            logger.debug("Circuit breaker tripped for gRPC create PIT request: {}", e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        } catch (RuntimeException e) {
            breaker.addWithoutBreaking(-requestSize);
            logger.debug("SearchServiceImpl failed to process create PIT request, request={}, error={}", request, e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    /**
     * Processes a delete PIT request.
     *
     * @param request The delete PIT request to process
     * @param responseObserver The observer to send the response back to the client
     */
    @Override
    public void deletePit(
        org.opensearch.protobufs.DeletePitRequest request,
        StreamObserver<org.opensearch.protobufs.DeletePITResponse> responseObserver
    ) {
        int requestSize = request.getSerializedSize();
        CircuitBreaker breaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);

        try {
            breaker.addEstimateBytesAndMaybeBreak(requestSize, "<grpc_request>");

            StreamObserver<org.opensearch.protobufs.DeletePITResponse> wrappedObserver = new CircuitBreakerStreamObserver<>(
                responseObserver,
                circuitBreakerService,
                requestSize
            );

            org.opensearch.action.search.DeletePitRequest deletePitRequest = DeletePitRequestProtoUtils.prepareRequest(request);
            DeletePitRequestActionListener listener = new DeletePitRequestActionListener(wrappedObserver);
            client.deletePits(deletePitRequest, listener);
        } catch (CircuitBreakingException e) {
            logger.debug("Circuit breaker tripped for gRPC delete PIT request: {}", e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        } catch (RuntimeException e) {
            breaker.addWithoutBreaking(-requestSize);
            logger.debug("SearchServiceImpl failed to process delete PIT request, request={}, error={}", request, e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }
}
