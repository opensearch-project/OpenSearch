/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.channels;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.http.HttpChunk;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.StreamingRestChannel;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.Subscriber;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link TraceableStreamingRestChannel}
 */
public class TraceableStreamingRestChannelTests extends OpenSearchTestCase {

    private StreamingRestChannel delegate;
    private Span span;
    private Tracer tracer;
    private SpanScope spanScope;

    @Before
    public void setup() {
        delegate = mock(StreamingRestChannel.class);
        span = mock(Span.class);
        tracer = mock(Tracer.class);
        spanScope = mock(SpanScope.class);

        // Default behavior: tracer is recording and creates span scopes
        when(tracer.isRecording()).thenReturn(true);
        when(tracer.withSpanInScope(span)).thenReturn(spanScope);
    }

    public void testCreateReturnsTracingChannelWhenRecording() {
        when(tracer.isRecording()).thenReturn(true);

        StreamingRestChannel result = (StreamingRestChannel) TraceableStreamingRestChannel.create(delegate, span, tracer);

        assertNotNull(result);
        assertThat(result, instanceOf(TraceableStreamingRestChannel.class));
    }

    public void testCreateReturnsDelegateWhenNotRecording() {
        when(tracer.isRecording()).thenReturn(false);

        StreamingRestChannel result = (StreamingRestChannel) TraceableStreamingRestChannel.create(delegate, span, tracer);

        assertSame(delegate, result);
    }

    public void testPrepareResponseDelegatesToUnderlying() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        Map<String, List<String>> headers = Map.of("Content-Type", List.of("application/json"));
        channel.prepareResponse(RestStatus.OK, headers);

        verify(delegate, times(1)).prepareResponse(RestStatus.OK, headers);
    }

    public void testPrepareResponseExecutesWithinSpanScope() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        Map<String, List<String>> headers = Map.of("Content-Type", List.of("application/json"));
        channel.prepareResponse(RestStatus.OK, headers);

        // Verify span scope was created and closed
        verify(tracer, times(1)).withSpanInScope(span);
        verify(spanScope, times(1)).close();
    }

    public void testSendChunkDelegatesToUnderlying() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        HttpChunk chunk = createMockChunk(false);
        channel.sendChunk(chunk);

        verify(delegate, times(1)).sendChunk(chunk);
    }

    public void testSendChunkExecutesWithinSpanScope() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        HttpChunk chunk = createMockChunk(false);
        channel.sendChunk(chunk);

        // Verify span scope was created and closed for each chunk
        verify(tracer, times(1)).withSpanInScope(span);
        verify(spanScope, times(1)).close();
    }

    public void testSendChunkDoesNotEndSpanForNonLastChunk() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        HttpChunk chunk = createMockChunk(false);
        channel.sendChunk(chunk);

        verify(span, never()).endSpan();
    }

    public void testSendChunkEndsSpanForLastChunk() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        HttpChunk lastChunk = createMockChunk(true);
        channel.sendChunk(lastChunk);

        verify(span, times(1)).endSpan();
    }

    public void testSendChunkEndsSpanOnlyOnceWithMultipleChunks() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        // Send multiple non-last chunks
        HttpChunk chunk1 = createMockChunk(false);
        HttpChunk chunk2 = createMockChunk(false);
        HttpChunk chunk3 = createMockChunk(false);
        HttpChunk lastChunk = createMockChunk(true);

        channel.sendChunk(chunk1);
        channel.sendChunk(chunk2);
        channel.sendChunk(chunk3);
        channel.sendChunk(lastChunk);

        // Verify span scope was created for each chunk
        verify(tracer, times(4)).withSpanInScope(span);
        verify(spanScope, times(4)).close();

        // Verify span ended only once (for last chunk)
        verify(span, times(1)).endSpan();
    }

    public void testSendResponseDelegatesToUnderlying() throws IOException {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        RestResponse response = mock(RestResponse.class);
        channel.sendResponse(response);

        verify(delegate, times(1)).sendResponse(response);
    }

    public void testSendResponseExecutesWithinSpanScope() throws IOException {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        RestResponse response = mock(RestResponse.class);
        channel.sendResponse(response);

        // Verify span scope was created and closed
        verify(tracer, times(1)).withSpanInScope(span);
        verify(spanScope, times(1)).close();
    }

    public void testSendResponseEndsSpan() throws IOException {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        RestResponse response = mock(RestResponse.class);
        channel.sendResponse(response);

        verify(span, times(1)).endSpan();
    }

    public void testSpanEndedOnlyOnceWhenBothPathsUsed() throws IOException {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        // First send via streaming path
        HttpChunk lastChunk = createMockChunk(true);
        channel.sendChunk(lastChunk);

        // Then try non-streaming path (shouldn't normally happen, but guarded)
        RestResponse response = mock(RestResponse.class);
        channel.sendResponse(response);

        // Verify span ended only once
        verify(span, times(1)).endSpan();
    }

    public void testConcurrentSendChunkCallsAreThreadSafe() throws InterruptedException {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        int numThreads = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        AtomicInteger chunksProcessed = new AtomicInteger(0);

        // Simulate concurrent chunk sending from multiple threads
        for (int i = 0; i < numThreads; i++) {
            final boolean isLast = (i == numThreads - 1);
            executor.submit(() -> {
                try {
                    startLatch.await();
                    HttpChunk chunk = createMockChunk(isLast);
                    channel.sendChunk(chunk);
                    chunksProcessed.incrementAndGet();
                } catch (Exception e) {
                    fail("Exception in thread: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Start all threads
        assertTrue("Threads did not complete", doneLatch.await(5, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(numThreads, chunksProcessed.get());
        verify(delegate, times(numThreads)).sendChunk(any(HttpChunk.class));

        // Verify span ended exactly once despite concurrent access
        verify(span, times(1)).endSpan();
    }

    public void testIsReadableDelegates() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        when(delegate.isReadable()).thenReturn(true);
        assertTrue(channel.isReadable());

        when(delegate.isReadable()).thenReturn(false);
        assertFalse(channel.isReadable());

        verify(delegate, times(2)).isReadable();
    }

    public void testIsWritableDelegates() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        when(delegate.isWritable()).thenReturn(true);
        assertTrue(channel.isWritable());

        when(delegate.isWritable()).thenReturn(false);
        assertFalse(channel.isWritable());

        verify(delegate, times(2)).isWritable();
    }

    public void testNewBuilderDelegates() throws IOException {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        channel.newBuilder();

        verify(delegate, times(1)).newBuilder();
    }

    public void testNewErrorBuilderDelegates() throws IOException {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        channel.newErrorBuilder();

        verify(delegate, times(1)).newErrorBuilder();
    }

    public void testNewBuilderWithMediaTypeDelegates() throws IOException {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        MediaType mediaType = mock(MediaType.class);
        channel.newBuilder(mediaType, true);

        verify(delegate, times(1)).newBuilder(mediaType, true);
    }

    public void testNewBuilderWithResponseContentTypeDelegates() throws IOException {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        MediaType requestType = mock(MediaType.class);
        MediaType responseType = mock(MediaType.class);
        channel.newBuilder(requestType, responseType, false);

        verify(delegate, times(1)).newBuilder(requestType, responseType, false);
    }

    public void testBytesOutputDelegates() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        BytesStreamOutput expectedOutput = new BytesStreamOutput();
        when(delegate.bytesOutput()).thenReturn(expectedOutput);

        BytesStreamOutput result = channel.bytesOutput();

        assertSame(expectedOutput, result);
        verify(delegate, times(1)).bytesOutput();
    }

    public void testRequestDelegates() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        RestRequest expectedRequest = mock(RestRequest.class);
        when(delegate.request()).thenReturn(expectedRequest);

        RestRequest result = channel.request();

        assertSame(expectedRequest, result);
        verify(delegate, times(1)).request();
    }

    public void testDetailedErrorsEnabledDelegates() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        when(delegate.detailedErrorsEnabled()).thenReturn(true);
        assertTrue(channel.detailedErrorsEnabled());

        when(delegate.detailedErrorsEnabled()).thenReturn(false);
        assertFalse(channel.detailedErrorsEnabled());

        verify(delegate, times(2)).detailedErrorsEnabled();
    }

    public void testDetailedErrorStackTraceEnabledDelegates() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        when(delegate.detailedErrorStackTraceEnabled()).thenReturn(true);
        assertTrue(channel.detailedErrorStackTraceEnabled());

        when(delegate.detailedErrorStackTraceEnabled()).thenReturn(false);
        assertFalse(channel.detailedErrorStackTraceEnabled());

        verify(delegate, times(2)).detailedErrorStackTraceEnabled();
    }

    public void testSubscribeDelegates() {
        TraceableStreamingRestChannel channel = new TraceableStreamingRestChannel(delegate, span, tracer);

        @SuppressWarnings("unchecked")
        Subscriber<HttpChunk> subscriber = mock(Subscriber.class);
        channel.subscribe(subscriber);

        verify(delegate, times(1)).subscribe(subscriber);
    }

    // Helper method to create mock HttpChunk
    private HttpChunk createMockChunk(boolean isLast) {
        return new HttpChunk() {
            @Override
            public boolean isLast() {
                return isLast;
            }

            @Override
            public BytesReference content() {
                return new BytesArray("test chunk content".getBytes());
            }

            @Override
            public void close() {
                // no-op for test
            }
        };
    }
}
