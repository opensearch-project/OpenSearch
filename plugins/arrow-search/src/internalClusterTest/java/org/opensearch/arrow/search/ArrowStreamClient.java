/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchResponse;
import org.opensearch.protobufs.services.SearchServiceGrpc;
import org.opensearch.transport.grpc.spi.StreamSearchFormatHandler;
import org.opensearch.transport.grpc.spi.StreamSearchResponseChannel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;

/**
 * A minimal Arrow-aware gRPC client, as a user would write one: it calls {@code ServerStreamSearch} with the Arrow
 * format header, takes payload bytes without protobuf decoding and reads them with a stock Arrow IPC reader.
 */
final class ArrowStreamClient implements AutoCloseable {

    static final MethodDescriptor<SearchRequest, byte[]> RAW_STREAM_SEARCH = SearchServiceGrpc.getServerStreamSearchMethod()
        .toBuilder(SearchServiceGrpc.getServerStreamSearchMethod().getRequestMarshaller(), new MethodDescriptor.Marshaller<byte[]>() {
            @Override
            public InputStream stream(byte[] value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[] parse(InputStream stream) {
                try {
                    return stream.readAllBytes();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        })
        .build();

    /** Rows, schema and summary of one search. */
    record Result(Schema schema, List<Map<String, Object>> rows, int batches, SearchResponse summary) {
    }

    private final ManagedChannel channel;

    ArrowStreamClient(TransportAddress address) {
        this(address, false);
    }

    /**
     * Creates a client of the gRPC endpoint at {@code address}.
     *
     * @param onTransportThread whether call callbacks run on the transport's event loop; a callback that blocks then stops the
     *                          client from reading the connection at all, as a stuck consumer would
     */
    ArrowStreamClient(TransportAddress address, boolean onTransportThread) {
        final NettyChannelBuilder builder = NettyChannelBuilder.forAddress(address.getAddress(), address.getPort())
            .usePlaintext()
            .maxInboundMessageSize(8 * 1024 * 1024)
            // A small fixed window, without auto-tuning, makes a client that stops reading stall the server quickly.
            .flowControlWindow(16 * 1024);
        if (onTransportThread) {
            builder.directExecutor();
        }
        this.channel = builder.build();
    }

    Channel arrowChannel() {
        final Metadata headers = new Metadata();
        headers.put(Metadata.Key.of(StreamSearchFormatHandler.FORMAT_HEADER, Metadata.ASCII_STRING_MARSHALLER), "arrow");
        return ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    Result search(SearchRequest request, BufferAllocator allocator) throws IOException {
        final Iterator<byte[]> messages = ClientCalls.blockingServerStreamingCall(
            arrowChannel(),
            RAW_STREAM_SEARCH,
            CallOptions.DEFAULT,
            request
        );
        final ByteArrayOutputStream ipc = new ByteArrayOutputStream();
        SearchResponse summary = null;
        int payloads = 0;
        while (messages.hasNext()) {
            final byte[] message = messages.next();
            final byte[] payload = payloadOf(message);
            if (payload != null) {
                if (summary != null) {
                    throw new AssertionError("payload after the summary");
                }
                ipc.write(payload);
                payloads++;
            } else {
                summary = SearchResponse.parseFrom(message);
            }
        }
        if (summary == null) {
            throw new AssertionError("the stream ended without a summary");
        }
        final List<Map<String, Object>> rows = new ArrayList<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc.toByteArray()), allocator)) {
            final VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                for (int row = 0; row < root.getRowCount(); row++) {
                    final Map<String, Object> values = new HashMap<>();
                    for (FieldVector vector : root.getFieldVectors()) {
                        values.put(vector.getName(), vector.isNull(row) ? null : vector.getObject(row));
                    }
                    rows.add(values);
                }
            }
            return new Result(root.getSchema(), rows, payloads - 1, summary);
        }
    }

    /** Returns the Arrow IPC payload of a payload message, or {@code null} for any other message. */
    static byte[] payloadOf(byte[] message) throws IOException {
        final CodedInputStream in = CodedInputStream.newInstance(message);
        final int tag = in.readTag();
        if (WireFormat.getTagFieldNumber(tag) != StreamSearchResponseChannel.PAYLOAD_FIELD_NUMBER) {
            return null;
        }
        return in.readByteArray();
    }

    /**
     * Starts a call that only receives the messages it explicitly requests.
     */
    ClientCall<SearchRequest, byte[]> startManual(SearchRequest request, ClientCall.Listener<byte[]> listener) {
        final ClientCall<SearchRequest, byte[]> call = arrowChannel().newCall(RAW_STREAM_SEARCH, CallOptions.DEFAULT);
        call.start(listener, new Metadata());
        call.sendMessage(request);
        call.halfClose();
        return call;
    }

    @Override
    public void close() throws InterruptedException {
        channel.shutdownNow();
        channel.awaitTermination(10, TimeUnit.SECONDS);
    }
}
