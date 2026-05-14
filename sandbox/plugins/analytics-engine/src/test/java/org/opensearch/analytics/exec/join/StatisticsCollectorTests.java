/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.index.Index;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatisticsCollectorTests extends OpenSearchTestCase {

    public void testCollectWithoutClientReturnsZeroRowCount() {
        ClusterState clusterState = mockClusterStateWithIndex("a", 3);

        Map<String, TableStatistics> stats = StatisticsCollector.collect(clusterState, List.of("a"));

        assertEquals(1, stats.size());
        assertEquals(3, stats.get("a").shardCount());
        assertEquals("row count must default to 0 (unknown) when no Client is supplied", 0L, stats.get("a").rowCount());
    }

    public void testCollectPopulatesPrimaryDocCountFromIndicesStats() {
        ClusterState clusterState = mockClusterStateWithIndex("dim", 1);
        Client client = mockClientReturningPrimaryDocCount(Map.of("dim", 42L));

        Map<String, TableStatistics> stats = StatisticsCollector.collect(clusterState, client, List.of("dim"));

        assertEquals(1, stats.get("dim").shardCount());
        assertEquals("row count must come from primaries.docs.count", 42L, stats.get("dim").rowCount());
    }

    public void testCollectFailSafeOnIndicesStatsException() {
        ClusterState clusterState = mockClusterStateWithIndex("dim", 2);
        Client client = mock(Client.class);
        AdminClient admin = mock(AdminClient.class);
        IndicesAdminClient indices = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(admin);
        when(admin.indices()).thenReturn(indices);
        when(indices.stats(any(IndicesStatsRequest.class))).thenThrow(new RuntimeException("boom"));

        Map<String, TableStatistics> stats = StatisticsCollector.collect(clusterState, client, List.of("dim"));

        assertEquals("shardCount must still come from cluster metadata", 2, stats.get("dim").shardCount());
        assertEquals("rowCount must remain 0 (unknown) on IndicesStats failure — fail-safe", 0L, stats.get("dim").rowCount());
    }

    public void testCollectIndexMissingFromClusterStateIsSkipped() {
        ClusterState clusterState = mockClusterStateWithIndex("present", 1);

        Map<String, TableStatistics> stats = StatisticsCollector.collect(clusterState, List.of("present", "missing"));

        assertEquals("missing index must be skipped", 1, stats.size());
        assertTrue(stats.containsKey("present"));
        assertFalse(stats.containsKey("missing"));
    }

    public void testCollectMultipleIndicesUsesSingleIndicesStatsCall() {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);
        IndexMetadata aMeta = mock(IndexMetadata.class);
        when(aMeta.getIndex()).thenReturn(new Index("a", "uuid-a"));
        when(aMeta.getNumberOfShards()).thenReturn(1);
        when(metadata.index("a")).thenReturn(aMeta);
        IndexMetadata bMeta = mock(IndexMetadata.class);
        when(bMeta.getIndex()).thenReturn(new Index("b", "uuid-b"));
        when(bMeta.getNumberOfShards()).thenReturn(5);
        when(metadata.index("b")).thenReturn(bMeta);

        Client client = mockClientReturningPrimaryDocCount(Map.of("a", 100L, "b", 999_999L));

        Map<String, TableStatistics> stats = StatisticsCollector.collect(clusterState, client, List.of("a", "b"));

        assertEquals(2, stats.size());
        assertEquals(100L, stats.get("a").rowCount());
        assertEquals(999_999L, stats.get("b").rowCount());
    }

    // ─── helpers ────────────────────────────────────────────────────────────────

    private static ClusterState mockClusterStateWithIndex(String indexName, int shardCount) {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index(indexName, "uuid-" + indexName));
        when(indexMetadata.getNumberOfShards()).thenReturn(shardCount);
        when(metadata.index(indexName)).thenReturn(indexMetadata);
        return clusterState;
    }

    @SuppressWarnings("unchecked")
    private static Client mockClientReturningPrimaryDocCount(Map<String, Long> primaryDocsByIndex) {
        Client client = mock(Client.class);
        AdminClient admin = mock(AdminClient.class);
        IndicesAdminClient indices = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(admin);
        when(admin.indices()).thenReturn(indices);

        IndicesStatsResponse response = mock(IndicesStatsResponse.class);
        Map<String, IndexStats> indexStatsByName = new java.util.HashMap<>();
        for (Map.Entry<String, Long> entry : primaryDocsByIndex.entrySet()) {
            IndexStats indexStats = mock(IndexStats.class);
            CommonStats primaries = mock(CommonStats.class);
            DocsStats docs = mock(DocsStats.class);
            when(docs.getCount()).thenReturn(entry.getValue());
            when(primaries.getDocs()).thenReturn(docs);
            when(indexStats.getPrimaries()).thenReturn(primaries);
            indexStatsByName.put(entry.getKey(), indexStats);
        }
        when(response.getIndices()).thenReturn(indexStatsByName);

        ActionFuture<IndicesStatsResponse> future = mock(ActionFuture.class);
        when(future.actionGet()).thenReturn(response);
        when(indices.stats(any(IndicesStatsRequest.class))).thenReturn(future);
        return client;
    }
}
