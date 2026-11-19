/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.stream;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.spi.StreamPayload;
import org.opensearch.transport.grpc.spi.StreamSearchResponseChannel;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.Drainable;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GrpcStreamSearchResponseChannelTests extends OpenSearchTestCase {

    private ServerCallStreamObserver<InputStream> call;
    private final AtomicInteger closes = new AtomicInteger();
    private final AtomicInteger releases = new AtomicInteger();
    private Runnable grpcOnReady;
    private Runnable grpcOnCancel;
    private Runnable grpcOnClose;
    private final AtomicInteger streamResets = new AtomicInteger();
    private GrpcStreamSearchResponseChannel channel;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpChannel() {
        call = mock(ServerCallStreamObserver.class);
        ArgumentCaptor<Runnable> onReady = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Runnable> onCancel = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Runnable> onClose = ArgumentCaptor.forClass(Runnable.class);
        channel = new GrpcStreamSearchResponseChannel(
            call,
            streamResets::incrementAndGet,
            new AggregateProtoConverterRegistryImpl(),
            closes::incrementAndGet
        );
        verify(call).setOnReadyHandler(onReady.capture());
        verify(call).setOnCancelHandler(onCancel.capture());
        verify(call).setOnCloseHandler(onClose.capture());
        grpcOnReady = onReady.getValue();
        grpcOnCancel = onCancel.getValue();
        grpcOnClose = onClose.getValue();
    }

    private StreamPayload payload(String value) {
        return new StreamPayload(List.of(ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8))), releases::incrementAndGet);
    }

    private static SearchResponse summary() {
        return new SearchResponse(
            new SearchResponseSections(SearchHits.empty(), null, null, false, false, null, 1),
            null,
            3,
            3,
            0,
            42,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    public void testReadyCallbackIsDispatched() {
        grpcOnReady.run();
        AtomicInteger readies = new AtomicInteger();
        channel.setOnReadyHandler(readies::incrementAndGet);
        grpcOnReady.run();
        grpcOnReady.run();
        assertEquals(2, readies.get());
    }

    public void testIsReadyFollowsCallUntilCancelled() {
        when(call.isReady()).thenReturn(true);
        assertTrue(channel.isReady());
        grpcOnCancel.run();
        assertFalse(channel.isReady());
        assertTrue(channel.isCancelled());
    }

    public void testCancelCallbackRunsExactlyOnce() {
        AtomicInteger before = new AtomicInteger();
        channel.setOnCancelHandler(before::incrementAndGet);
        grpcOnCancel.run();
        grpcOnCancel.run();
        assertEquals(1, before.get());
        assertEquals(1, closes.get());

        AtomicInteger after = new AtomicInteger();
        channel.setOnCancelHandler(after::incrementAndGet);
        assertEquals("a handler registered after cancellation runs immediately", 1, after.get());
    }

    public void testSendPayloadWritesPrefixedMessage() throws IOException {
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        doAnswer(invocation -> {
            ((Drainable) invocation.getArgument(0)).drainTo(wire);
            return null;
        }).when(call).onNext(any());

        channel.sendPayload(payload("arrow-bytes"));

        assertEquals(1, releases.get());
        CodedInputStream in = CodedInputStream.newInstance(wire.toByteArray());
        int tag = in.readTag();
        assertEquals(StreamSearchResponseChannel.PAYLOAD_FIELD_NUMBER, WireFormat.getTagFieldNumber(tag));
        assertEquals(WireFormat.WIRETYPE_LENGTH_DELIMITED, WireFormat.getTagWireType(tag));
        assertEquals("arrow-bytes", in.readBytes().toStringUtf8());
        assertTrue(in.isAtEnd());
    }

    public void testPayloadMessageIsAValidSearchResponse() throws IOException {
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        doAnswer(invocation -> {
            ((Drainable) invocation.getArgument(0)).drainTo(wire);
            return null;
        }).when(call).onNext(any());

        channel.sendPayload(payload("x".repeat(300)));

        org.opensearch.protobufs.SearchResponse parsed = org.opensearch.protobufs.SearchResponse.parseFrom(wire.toByteArray());
        assertEquals(0, parsed.getTook());
        assertTrue(parsed.getUnknownFields().hasField(StreamSearchResponseChannel.PAYLOAD_FIELD_NUMBER));
    }

    public void testPayloadDroppedByGrpcIsReleased() {
        channel.sendPayload(payload("never-drained"));
        verify(call).onNext(any());
        assertEquals(1, releases.get());
    }

    public void testPayloadIsReleasedWhenSendFails() {
        doAnswer(invocation -> { throw new IllegalStateException("call closed"); }).when(call).onNext(any());
        expectThrows(IllegalStateException.class, () -> channel.sendPayload(payload("x")));
        assertEquals(1, releases.get());
    }

    public void testPayloadAfterCancelIsReleasedWithoutSending() {
        grpcOnCancel.run();
        channel.sendPayload(payload("x"));
        verify(call, never()).onNext(any());
        assertEquals(1, releases.get());
    }

    public void testPayloadAfterCompleteIsReleasedWithoutSending() {
        channel.complete(summary());
        channel.sendPayload(payload("x"));
        verify(call, times(1)).onNext(any());
        assertEquals(1, releases.get());
    }

    public void testCompleteSendsSummaryAndCloses() throws IOException {
        ArgumentCaptor<InputStream> message = ArgumentCaptor.forClass(InputStream.class);
        channel.complete(summary());
        channel.complete(summary());

        verify(call).onNext(message.capture());
        verify(call).onCompleted();
        org.opensearch.protobufs.SearchResponse parsed = org.opensearch.protobufs.SearchResponse.parseFrom(message.getValue());
        assertEquals(42, parsed.getTook());
        assertEquals(3, parsed.getXShards().getSuccessful());
        assertEquals(1, closes.get());
    }

    public void testCompleteSendsTotalHitsWithoutHits() throws IOException {
        SearchHit hit = new SearchHit(0, "doc", Map.of(), Map.of());
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(7, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), 1f);
        SearchResponse withHits = new SearchResponse(
            new SearchResponseSections(hits, null, null, false, true, null, 1),
            null,
            1,
            1,
            0,
            5,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        ArgumentCaptor<InputStream> message = ArgumentCaptor.forClass(InputStream.class);
        channel.complete(withHits);

        verify(call).onNext(message.capture());
        org.opensearch.protobufs.SearchResponse parsed = org.opensearch.protobufs.SearchResponse.parseFrom(message.getValue());
        assertEquals(0, parsed.getHits().getHitsCount());
        assertEquals(7, parsed.getHits().getTotal().getTotalHits().getValue());
        assertTrue(parsed.getTerminatedEarly());
    }

    public void testCompleteAfterCancelDoesNotWrite() {
        grpcOnCancel.run();
        channel.complete(summary());
        verify(call, never()).onNext(any());
        verify(call, never()).onCompleted();
        assertEquals(1, closes.get());
    }

    public void testFailMapsStatusOnce() {
        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        channel.fail(new IllegalArgumentException("bad request"));
        channel.fail(new IllegalStateException("ignored"));
        channel.complete(summary());

        verify(call).onError(error.capture());
        verify(call, never()).onCompleted();
        assertEquals(Status.Code.INVALID_ARGUMENT, ((StatusRuntimeException) error.getValue()).getStatus().getCode());
        assertEquals(1, closes.get());
    }

    public void testFailAfterCancelDoesNotWrite() {
        grpcOnCancel.run();
        channel.fail(new IllegalStateException("cancelled"));
        verify(call, never()).onError(any());
        assertEquals(1, closes.get());
    }

    public void testResetAfterFailResetsTheStream() {
        channel.fail(new IllegalStateException("the client did not read"));
        channel.reset();

        verify(call, times(1)).onError(any());
        assertEquals(1, streamResets.get());
        assertEquals(1, closes.get());
    }

    public void testResetAfterCompleteResetsTheStream() {
        channel.complete(summary());
        channel.reset();

        verify(call).onCompleted();
        verify(call, never()).onError(any());
        assertEquals(1, streamResets.get());
        assertEquals(1, closes.get());
    }

    public void testResetBeforeTheEndFailsTheCall() {
        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        channel.reset();
        channel.sendPayload(payload("x"));
        channel.complete(summary());

        verify(call).onError(error.capture());
        assertEquals(Status.Code.CANCELLED, ((StatusRuntimeException) error.getValue()).getStatus().getCode());
        verify(call, never()).onNext(any());
        verify(call, never()).onCompleted();
        assertEquals(1, releases.get());
        assertEquals(1, streamResets.get());
        assertEquals(1, closes.get());
    }

    public void testResetAfterCancelDoesNothing() {
        grpcOnCancel.run();
        channel.reset();
        verify(call, never()).onError(any());
        assertEquals(0, streamResets.get());
        assertEquals(1, closes.get());
    }

    public void testResetOfAStreamGrpcClosedDoesNothing() {
        channel.complete(summary());
        grpcOnClose.run();
        channel.reset();
        assertEquals(0, streamResets.get());
    }

    public void testResetWithoutStreamResetOnlyEndsTheCall() {
        GrpcStreamSearchResponseChannel withoutReset = new GrpcStreamSearchResponseChannel(
            call,
            null,
            new AggregateProtoConverterRegistryImpl(),
            closes::incrementAndGet
        );
        withoutReset.reset();
        verify(call).onError(any());
        assertEquals(0, streamResets.get());
    }

    public void testPrefixUsesMinimalVarints() {
        StreamPayload small = new StreamPayload(List.of(ByteBuffer.allocate(127)), () -> {});
        StreamPayload large = new StreamPayload(List.of(ByteBuffer.allocate(128)), () -> {});
        assertEquals(2 + 1, GrpcStreamSearchResponseChannel.payloadPrefix(small).remaining());
        assertEquals(2 + 2, GrpcStreamSearchResponseChannel.payloadPrefix(large).remaining());
    }
}
