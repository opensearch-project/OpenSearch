/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search;

import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.protobufs.PhaseTook;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchResponseProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithBasicResponse() throws IOException {
        // Create a mock SearchResponse
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mock(SearchResponseSections.class));

        // Call the method under test
        org.opensearch.protobufs.SearchResponse protoResponse = SearchResponseProtoUtils.toProto(mockResponse);

        // Verify the result
        assertNotNull("Proto response should not be null", protoResponse);
        assertEquals("Took should match", 100, protoResponse.getTook());
        assertFalse("Timed out should be false", protoResponse.getTimedOut());
        assertEquals("Total shards should match", 5, protoResponse.getXShards().getTotal());
        assertEquals("Successful shards should match", 5, protoResponse.getXShards().getSuccessful());
        assertEquals("Skipped shards should match", 0, protoResponse.getXShards().getSkipped());
        assertEquals("Failed shards should match", 0, protoResponse.getXShards().getFailed());
    }

    public void testToProtoWithScrollId() throws IOException {
        // Create a mock SearchResponse with scroll ID
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));
        when(mockResponse.getScrollId()).thenReturn("test_scroll_id");
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mock(SearchResponseSections.class));

        // Call the method under test
        org.opensearch.protobufs.SearchResponse protoResponse = SearchResponseProtoUtils.toProto(mockResponse);

        // Verify the result
        assertNotNull("Proto response should not be null", protoResponse);
        assertEquals("Scroll ID should match", "test_scroll_id", protoResponse.getXScrollId());
    }

    public void testToProtoWithPointInTimeId() throws IOException {
        // Create a mock SearchResponse with point in time ID
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));
        when(mockResponse.pointInTimeId()).thenReturn("test_pit_id");
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mock(SearchResponseSections.class));

        // Call the method under test
        org.opensearch.protobufs.SearchResponse protoResponse = SearchResponseProtoUtils.toProto(mockResponse);

        // Verify the result
        assertNotNull("Proto response should not be null", protoResponse);
        assertEquals("Point in time ID should match", "test_pit_id", protoResponse.getPitId());
    }

    public void testToProtoWithPhaseTook() throws IOException {
        // Create a mock SearchResponse.PhaseTook
        Map<String, Long> phaseTookMap = new HashMap<>();
        phaseTookMap.put(SearchPhaseName.QUERY.getName(), 50L);
        phaseTookMap.put(SearchPhaseName.FETCH.getName(), 30L);
        phaseTookMap.put(SearchPhaseName.DFS_QUERY.getName(), 20L);
        phaseTookMap.put(SearchPhaseName.DFS_PRE_QUERY.getName(), 10L);
        phaseTookMap.put(SearchPhaseName.EXPAND.getName(), 5L);
        phaseTookMap.put(SearchPhaseName.CAN_MATCH.getName(), 5L);

        SearchResponse.PhaseTook phaseTook = new SearchResponse.PhaseTook(phaseTookMap);

        // Create a mock SearchResponse with phase took
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));
        when(mockResponse.getPhaseTook()).thenReturn(phaseTook);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mock(SearchResponseSections.class));

        // Call the method under test
        org.opensearch.protobufs.SearchResponse protoResponse = SearchResponseProtoUtils.toProto(mockResponse);

        // Verify the result
        assertNotNull("Proto response should not be null", protoResponse);
        assertTrue("Phase took should be present", protoResponse.hasPhaseTook());
        assertEquals("Query phase took should match", 50L, protoResponse.getPhaseTook().getQuery());
        assertEquals("Fetch phase took should match", 30L, protoResponse.getPhaseTook().getFetch());
        assertEquals("DFS query phase took should match", 20L, protoResponse.getPhaseTook().getDfsQuery());
        assertEquals("DFS pre-query phase took should match", 10L, protoResponse.getPhaseTook().getDfsPreQuery());
        assertEquals("Expand phase took should match", 5L, protoResponse.getPhaseTook().getExpand());
        assertEquals("Can match phase took should match", 5L, protoResponse.getPhaseTook().getCanMatch());
    }

    public void testToProtoWithTerminatedEarly() throws IOException {
        // Create a mock SearchResponse with terminated early
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));
        when(mockResponse.isTerminatedEarly()).thenReturn(true);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mock(SearchResponseSections.class));

        // Call the method under test
        org.opensearch.protobufs.SearchResponse protoResponse = SearchResponseProtoUtils.toProto(mockResponse);

        // Verify the result
        assertNotNull("Proto response should not be null", protoResponse);
        assertTrue("Terminated early should be true", protoResponse.getTerminatedEarly());
    }

    public void testToProtoWithNumReducePhases() throws IOException {
        // Create a mock SearchResponse with num reduce phases
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));
        when(mockResponse.getNumReducePhases()).thenReturn(3);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mock(SearchResponseSections.class));

        // Call the method under test
        org.opensearch.protobufs.SearchResponse protoResponse = SearchResponseProtoUtils.toProto(mockResponse);

        // Verify the result
        assertNotNull("Proto response should not be null", protoResponse);
        assertEquals("Num reduce phases should match", 3, protoResponse.getNumReducePhases());
    }

    public void testToProtoWithClusters() throws IOException {
        // Create a mock SearchResponse with clusters
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(3, 2, 1));
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mock(SearchResponseSections.class));

        // Call the method under test
        org.opensearch.protobufs.SearchResponse protoResponse = SearchResponseProtoUtils.toProto(mockResponse);

        // Verify the result
        assertNotNull("Proto response should not be null", protoResponse);
        assertTrue("Clusters should be present", protoResponse.hasXClusters());
        assertEquals("Total clusters should match", 3, protoResponse.getXClusters().getTotal());
        assertEquals("Successful clusters should match", 2, protoResponse.getXClusters().getSuccessful());
        assertEquals("Skipped clusters should match", 1, protoResponse.getXClusters().getSkipped());
    }

    public void testPhaseTookProtoUtilsToProto() {
        // Create a mock SearchResponse.PhaseTook
        Map<String, Long> phaseTookMap = new HashMap<>();
        phaseTookMap.put(SearchPhaseName.QUERY.getName(), 50L);
        phaseTookMap.put(SearchPhaseName.FETCH.getName(), 30L);
        phaseTookMap.put(SearchPhaseName.DFS_QUERY.getName(), 20L);
        phaseTookMap.put(SearchPhaseName.DFS_PRE_QUERY.getName(), 10L);
        phaseTookMap.put(SearchPhaseName.EXPAND.getName(), 5L);
        phaseTookMap.put(SearchPhaseName.CAN_MATCH.getName(), 5L);

        SearchResponse.PhaseTook phaseTook = new SearchResponse.PhaseTook(phaseTookMap);

        // Create a builder and call the method under test
        PhaseTook.Builder phaseTookBuilder = PhaseTook.newBuilder();
        SearchResponseProtoUtils.PhaseTookProtoUtils.toProto(phaseTook, phaseTookBuilder);
        PhaseTook protoPhaseTook = phaseTookBuilder.build();

        // Verify the result
        assertNotNull("Proto phase took should not be null", protoPhaseTook);
        assertEquals("Query phase took should match", 50L, protoPhaseTook.getQuery());
        assertEquals("Fetch phase took should match", 30L, protoPhaseTook.getFetch());
        assertEquals("DFS query phase took should match", 20L, protoPhaseTook.getDfsQuery());
        assertEquals("DFS pre-query phase took should match", 10L, protoPhaseTook.getDfsPreQuery());
        assertEquals("Expand phase took should match", 5L, protoPhaseTook.getExpand());
        assertEquals("Can match phase took should match", 5L, protoPhaseTook.getCanMatch());
    }

    public void testPhaseTookProtoUtilsToProtoWithNullPhaseTook() {
        // Create a builder and call the method under test with null
        PhaseTook.Builder phaseTookBuilder = PhaseTook.newBuilder();
        SearchResponseProtoUtils.PhaseTookProtoUtils.toProto(null, phaseTookBuilder);
        PhaseTook protoPhaseTook = phaseTookBuilder.build();

        // Verify the result
        assertNotNull("Proto phase took should not be null", protoPhaseTook);
        assertEquals("Query phase took should be 0", 0L, protoPhaseTook.getQuery());
        assertEquals("Fetch phase took should be 0", 0L, protoPhaseTook.getFetch());
        assertEquals("DFS query phase took should be 0", 0L, protoPhaseTook.getDfsQuery());
        assertEquals("DFS pre-query phase took should be 0", 0L, protoPhaseTook.getDfsPreQuery());
        assertEquals("Expand phase took should be 0", 0L, protoPhaseTook.getExpand());
        assertEquals("Can match phase took should be 0", 0L, protoPhaseTook.getCanMatch());
    }

    public void testClustersProtoUtilsToProtoWithNonZeroClusters() throws IOException {
        // Create a mock SearchResponse.Clusters
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(3, 2, 1);

        // Create a builder to populate
        org.opensearch.protobufs.SearchResponse.Builder builder = org.opensearch.protobufs.SearchResponse.newBuilder();

        // Call the method under test
        SearchResponseProtoUtils.ClustersProtoUtils.toProto(builder, clusters);

        // Verify the result
        assertTrue("Clusters should be present", builder.hasXClusters());
        assertEquals("Total clusters should match", 3, builder.getXClusters().getTotal());
        assertEquals("Successful clusters should match", 2, builder.getXClusters().getSuccessful());
        assertEquals("Skipped clusters should match", 1, builder.getXClusters().getSkipped());
    }

    public void testClustersProtoUtilsToProtoWithZeroClusters() throws IOException {
        // Create a mock SearchResponse.Clusters with zero total
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(0, 0, 0);

        // Create a builder to populate
        org.opensearch.protobufs.SearchResponse.Builder builder = org.opensearch.protobufs.SearchResponse.newBuilder();

        // Call the method under test
        SearchResponseProtoUtils.ClustersProtoUtils.toProto(builder, clusters);

        // Verify the result
        assertFalse("Clusters should not be present", builder.hasXClusters());
    }
}
