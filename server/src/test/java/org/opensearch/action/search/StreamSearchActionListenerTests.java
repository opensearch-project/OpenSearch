/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.OriginalIndices;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests StreamSearchActionListener behavior.
 */
public class StreamSearchActionListenerTests extends OpenSearchTestCase {

    /**
     * Test implementation of StreamSearchActionListener for testing purposes.
     */
    private static class TestStreamSearchActionListener extends StreamSearchActionListener<QuerySearchResult> {
        private final List<QuerySearchResult> streamResponses = new ArrayList<>();
        private QuerySearchResult completeResponse;
        private Throwable failure;

        TestStreamSearchActionListener(SearchShardTarget searchShardTarget, int shardIndex) {
            super(searchShardTarget, shardIndex);
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

    public void testMultipleStreamResponsesThenFinal() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
        TestStreamSearchActionListener listener = new TestStreamSearchActionListener(target, 0);

        QuerySearchResult partial1 = new QuerySearchResult(new ShardSearchContextId("session1", 1L), target, null);
        QuerySearchResult partial2 = new QuerySearchResult(new ShardSearchContextId("session1", 2L), target, null);
        QuerySearchResult partial3 = new QuerySearchResult(new ShardSearchContextId("session1", 3L), target, null);

        listener.onStreamResponse(partial1, false);
        listener.onStreamResponse(partial2, false);
        listener.onStreamResponse(partial3, false);

        assertEquals(3, listener.getStreamResponses().size());
        assertSame(partial1, listener.getStreamResponses().get(0));
        assertSame(partial2, listener.getStreamResponses().get(1));
        assertSame(partial3, listener.getStreamResponses().get(2));
        assertNull(listener.getCompleteResponse());

        QuerySearchResult finalResult = new QuerySearchResult(new ShardSearchContextId("session1", 4L), target, null);
        listener.onStreamResponse(finalResult, true);

        assertNotNull(listener.getCompleteResponse());
        assertSame(finalResult, listener.getCompleteResponse());
        assertEquals(3, listener.getStreamResponses().size());
    }

    public void testOnlyFinalResponseWithIsLastTrue() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
        TestStreamSearchActionListener listener = new TestStreamSearchActionListener(target, 0);

        QuerySearchResult finalResult = new QuerySearchResult(new ShardSearchContextId("session1", 1L), target, null);
        listener.onStreamResponse(finalResult, true);

        assertEquals(0, listener.getStreamResponses().size());
        assertNotNull(listener.getCompleteResponse());
        assertSame(finalResult, listener.getCompleteResponse());
    }

    public void testInnerOnResponseThrowsException() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
        TestStreamSearchActionListener listener = new TestStreamSearchActionListener(target, 0);

        QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId("session1", 1L), target, null);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> listener.innerOnResponse(result));
        assertEquals(
            "innerOnResponse is not allowed for streaming search, please use innerOnStreamResponse instead",
            exception.getMessage()
        );
    }

    public void testShardIndexIsSetOnStreamResponse() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);

        int shardIndex = 5;
        TestStreamSearchActionListener listener = new TestStreamSearchActionListener(target, shardIndex);

        QuerySearchResult partial = new QuerySearchResult(new ShardSearchContextId("session1", 1L), null, null);
        listener.onStreamResponse(partial, false);

        assertEquals(shardIndex, partial.getShardIndex());
    }

    public void testSearchShardTargetIsSetOnStreamResponse() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
        TestStreamSearchActionListener listener = new TestStreamSearchActionListener(target, 0);

        QuerySearchResult partial = new QuerySearchResult(new ShardSearchContextId("session1", 1L), null, null);
        listener.onStreamResponse(partial, false);

        assertNotNull(partial.getSearchShardTarget());
        assertEquals(target, partial.getSearchShardTarget());
    }

    public void testFailureHandling() {
        ShardId shardId = new ShardId("test-index", "test-uuid", 0);
        SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
        TestStreamSearchActionListener listener = new TestStreamSearchActionListener(target, 0);

        QuerySearchResult partial1 = new QuerySearchResult(new ShardSearchContextId("session1", 1L), target, null);
        listener.onStreamResponse(partial1, false);

        assertEquals(1, listener.getStreamResponses().size());

        Exception testException = new Exception("Test failure");
        listener.onFailure(testException);

        assertNotNull(listener.getFailure());
        assertSame(testException, listener.getFailure());
        assertNull(listener.getCompleteResponse());
    }
}
