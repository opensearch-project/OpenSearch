/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.OriginalIndices;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests streaming transport response handler contract validation.
 */
public class StreamTransportResponseHandlerContractTests extends OpenSearchTestCase {

    /**
     * Mock implementation of StreamTransportResponse for testing.
     */
    private static class TestStreamTransportResponse<T extends TransportResponse> implements StreamTransportResponse<T> {
        private final List<T> responses;
        private final AtomicInteger currentIndex = new AtomicInteger(0);
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private volatile boolean cancelled = false;

        TestStreamTransportResponse(List<T> responses) {
            this.responses = responses != null ? responses : List.of();
        }

        @Override
        public T nextResponse() {
            if (cancelled) {
                throw new IllegalStateException("Stream has been cancelled");
            }
            if (closed.get()) {
                throw new IllegalStateException("Stream has been closed");
            }

            int index = currentIndex.getAndIncrement();
            if (index < responses.size()) {
                return responses.get(index);
            }
            return null;
        }

        @Override
        public void cancel(String reason, Throwable cause) {
            cancelled = true;
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    /**
     * Test implementation of streaming listener for testing.
     */
    private static class TestStreamingListener extends StreamSearchActionListener<QuerySearchResult> {
        private final List<QuerySearchResult> streamResponses = new ArrayList<>();
        private QuerySearchResult completeResponse;
        private Throwable failure;

        TestStreamingListener(SearchShardTarget target, int shardIndex) {
            super(target, shardIndex);
        }

        @Override
        protected void innerOnStreamResponse(QuerySearchResult response) {
            streamResponses.add(response);
        }

        @Override
        protected void innerOnCompleteResponse(QuerySearchResult response) {
            completeResponse = response;
        }

        @Override
        public void onFailure(Exception e) {
            failure = e;
        }

        public List<QuerySearchResult> getStreamResponses() {
            return streamResponses;
        }

        public QuerySearchResult getCompleteResponse() {
            return completeResponse;
        }

        public Throwable getFailure() {
            return failure;
        }
    }

    /**
     * Test implementation of non-streaming listener for testing.
     */
    private static class TestNonStreamingListener extends SearchActionListener<QuerySearchResult> {
        private QuerySearchResult response;
        private Throwable failure;

        TestNonStreamingListener(SearchShardTarget target, int shardIndex) {
            super(target, shardIndex);
        }

        @Override
        protected void innerOnResponse(QuerySearchResult response) {
            this.response = response;
        }

        @Override
        public void onFailure(Exception e) {
            failure = e;
        }

        public QuerySearchResult getResponse() {
            return response;
        }

        public Throwable getFailure() {
            return failure;
        }
    }

    public void testStreamingHandlerWithStreamingListener() throws IOException {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
        TestStreamingListener listener = new TestStreamingListener(target, 0);
        StreamTransportResponseHandler<SearchPhaseResult> handler = new StreamTransportResponseHandler<SearchPhaseResult>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<SearchPhaseResult> response) {
                try {
                    SearchPhaseResult currentResult;
                    SearchPhaseResult lastResult = null;

                    while ((currentResult = response.nextResponse()) != null) {
                        if (lastResult != null) {
                            listener.onStreamResponse((QuerySearchResult) lastResult, false);
                        }
                        lastResult = currentResult;
                    }

                    if (lastResult != null) {
                        listener.onStreamResponse((QuerySearchResult) lastResult, true);
                    }

                    response.close();
                } catch (Exception e) {
                    response.cancel("Client error during search phase", e);
                    listener.onFailure(e);
                }
            }

            @Override
            public void handleException(TransportException e) {
                listener.onFailure(e);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.STREAM_SEARCH;
            }

            @Override
            public SearchPhaseResult read(StreamInput in) throws IOException {
                return new QuerySearchResult(in);
            }
        };

        List<SearchPhaseResult> responses = new ArrayList<>();
        responses.add(new QuerySearchResult(new ShardSearchContextId("session1", 1L), target, null));
        responses.add(new QuerySearchResult(new ShardSearchContextId("session1", 2L), target, null));
        responses.add(new QuerySearchResult(new ShardSearchContextId("session1", 3L), target, null));

        TestStreamTransportResponse<SearchPhaseResult> streamResponse = new TestStreamTransportResponse<>(responses);
        handler.handleStreamResponse(streamResponse);

        assertEquals(2, listener.getStreamResponses().size());
        assertEquals(responses.get(0), listener.getStreamResponses().get(0));
        assertEquals(responses.get(1), listener.getStreamResponses().get(1));
        assertNotNull(listener.getCompleteResponse());
        assertEquals(responses.get(2), listener.getCompleteResponse());
    }

    public void testStreamingHandlerWithNonStreamingListener() throws IOException {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
        TestNonStreamingListener listener = new TestNonStreamingListener(target, 0);
        StreamTransportResponseHandler<SearchPhaseResult> handler = new StreamTransportResponseHandler<SearchPhaseResult>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<SearchPhaseResult> response) {
                try {
                    SearchPhaseResult currentResult;
                    SearchPhaseResult lastResult = null;

                    while ((currentResult = response.nextResponse()) != null) {
                        lastResult = currentResult;
                    }

                    if (lastResult != null) {
                        listener.onResponse((QuerySearchResult) lastResult);
                    }

                    response.close();
                } catch (Exception e) {
                    response.cancel("Client error during search phase", e);
                    listener.onFailure(e);
                }
            }

            @Override
            public void handleException(TransportException e) {
                listener.onFailure(e);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.STREAM_SEARCH;
            }

            @Override
            public SearchPhaseResult read(StreamInput in) throws IOException {
                return new QuerySearchResult(in);
            }
        };

        List<SearchPhaseResult> responses = new ArrayList<>();
        responses.add(new QuerySearchResult(new ShardSearchContextId("session1", 1L), target, null));
        responses.add(new QuerySearchResult(new ShardSearchContextId("session1", 2L), target, null));
        responses.add(new QuerySearchResult(new ShardSearchContextId("session1", 3L), target, null));

        TestStreamTransportResponse<SearchPhaseResult> streamResponse = new TestStreamTransportResponse<>(responses);
        handler.handleStreamResponse(streamResponse);

        assertNotNull(listener.getResponse());
        assertEquals(responses.get(2), listener.getResponse());
    }

    public void testHandlerClosesStreamAfterProcessing() throws IOException {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
        TestStreamingListener listener = new TestStreamingListener(target, 0);

        StreamTransportResponseHandler<SearchPhaseResult> handler = new StreamTransportResponseHandler<SearchPhaseResult>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<SearchPhaseResult> response) {
                try {
                    SearchPhaseResult result;
                    while ((result = response.nextResponse()) != null) {}
                    response.close();
                } catch (Exception e) {
                    response.cancel("Error", e);
                }
            }

            @Override
            public void handleException(TransportException e) {
                listener.onFailure(e);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.STREAM_SEARCH;
            }

            @Override
            public SearchPhaseResult read(StreamInput in) throws IOException {
                return new QuerySearchResult(in);
            }
        };

        List<SearchPhaseResult> responses = new ArrayList<>();
        responses.add(new QuerySearchResult(new ShardSearchContextId("session1", 1L), target, null));

        TestStreamTransportResponse<SearchPhaseResult> streamResponse = new TestStreamTransportResponse<>(responses);
        handler.handleStreamResponse(streamResponse);

        assertTrue(streamResponse.closed.get());
    }

    public void testHandlerCancelsStreamOnError() throws IOException {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
        TestStreamingListener listener = new TestStreamingListener(target, 0);

        StreamTransportResponseHandler<SearchPhaseResult> handler = new StreamTransportResponseHandler<SearchPhaseResult>() {
            @Override
            public void handleStreamResponse(StreamTransportResponse<SearchPhaseResult> response) {
                try {
                    throw new RuntimeException("Test error");
                } catch (Exception e) {
                    response.cancel("Client error during search phase", e);
                    listener.onFailure(e);
                }
            }

            @Override
            public void handleException(TransportException e) {
                listener.onFailure(e);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.STREAM_SEARCH;
            }

            @Override
            public SearchPhaseResult read(StreamInput in) throws IOException {
                return new QuerySearchResult(in);
            }
        };

        List<SearchPhaseResult> responses = new ArrayList<>();
        responses.add(new QuerySearchResult(new ShardSearchContextId("session1", 1L), target, null));

        TestStreamTransportResponse<SearchPhaseResult> streamResponse = new TestStreamTransportResponse<>(responses);
        handler.handleStreamResponse(streamResponse);

        assertTrue(streamResponse.cancelled);
        assertNotNull(listener.getFailure());
    }
}
