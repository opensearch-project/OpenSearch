/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

public class StreamingSearchProgressListenerTests extends OpenSearchTestCase {

    public void testShardTargetAttribution() {
        // Setup
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        SearchPhaseController controller = null;
        SearchRequest request = new SearchRequest();

        // Capture the partial response
        AtomicReference<SearchResponse> capturedResponse = new AtomicReference<>();
        ActionListener<SearchResponse> capturingListener = new StreamingSearchResponseListener(listener, request) {
            @Override
            public void onPartialResponse(SearchResponse response) {
                capturedResponse.set(response);
            }

            @Override
            public void onResponse(SearchResponse searchResponse) {}

            @Override
            public void onFailure(Exception e) {}
        };

        StreamingSearchProgressListener listenerUnderTest = new StreamingSearchProgressListener(capturingListener, controller, request);

        // Register a shard target
        ShardId shardId = new ShardId("test_index", "uuid", 0);
        SearchShardTarget target = new SearchShardTarget("node1", shardId, null, OriginalIndices.NONE);
        listenerUnderTest.onQueryResult(0, target);

        // Create TopDocs with a hit from that shard
        ScoreDoc scoreDoc = new ScoreDoc(1, 1.0f, 0); // doc=1, score=1.0, shardIndex=0
        TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { scoreDoc });

        // Trigger partial reduce
        listenerUnderTest.onPartialReduceWithTopDocs(
            Collections.emptyList(), // shards list (not used in this check)
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            topDocs,
            null,
            1
        );

        // Verify
        SearchResponse response = capturedResponse.get();
        assertNotNull("Should have captured a partial response", response);
        SearchHit[] hits = response.getHits().getHits();
        assertEquals(1, hits.length);
        assertNotNull("Hit should have shard info", hits[0].getShard());
        assertEquals("test_index", hits[0].getShard().getIndex());
        assertEquals(0, hits[0].getShard().getShardId().getId());
        assertEquals("node1", hits[0].getShard().getNodeId());
    }

}
